package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/observability"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Message is the payload carried through Redis priority queues, retry sets, and DLQ.
type Message struct {
	NotificationID uuid.UUID         `json:"notification_id"`
	Channel        domain.Channel    `json:"channel"`
	Priority       domain.Priority   `json:"priority"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	TraceContext   map[string]string `json:"trace_context,omitempty"`
	EnqueuedAt     time.Time         `json:"enqueued_at"`
}

// Queue defines operations for enqueuing, dequeuing, retrying, dead-lettering, and inspecting depth.
type Queue interface {
	EnqueueBatch(ctx context.Context, msgs []Message) error
	Enqueue(ctx context.Context, msg Message) error
	Dequeue(ctx context.Context, channel domain.Channel) (*Message, error)
	ScheduleRetry(ctx context.Context, msg Message, retryAt time.Time) error
	PromoteDueRetries(ctx context.Context, channel domain.Channel, limit int) (int64, error)
	PushDLQ(ctx context.Context, msg Message, reason string) error
	Depths(ctx context.Context) (map[string]observability.QueueDepthSnapshot, error)
}

var dequeueWeightedScript = redis.NewScript(`
for i = 1, #KEYS do
	local item = redis.call('RPOP', KEYS[i])
	if item then
		return item
	end
end
return nil
`)

var promoteRetriesScript = redis.NewScript(`
local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
if #items == 0 then
	return 0
end
for _, raw in ipairs(items) do
	redis.call('ZREM', KEYS[1], raw)
	local sep = string.find(raw, '|', 1, true)
	local prio = 'normal'
	local payload = raw
	if sep then
		prio = string.sub(raw, 1, sep - 1)
		payload = string.sub(raw, sep + 1)
	end
	local dest = KEYS[3]
	if prio == 'high' then
		dest = KEYS[2]
	elseif prio == 'low' then
		dest = KEYS[4]
	end
	redis.call('LPUSH', dest, payload)
end
return #items
`)

// RedisQueue implements Queue backed by Redis Lists (priority & DLQ) and ZSETs (delayed retries).
type RedisQueue struct {
	client *redis.Client
	clock  func() time.Time

	mu      sync.Mutex
	cursors map[domain.Channel]*atomic.Uint64
}

// NewRedisQueue constructs a Redis-backed priority queue.
func NewRedisQueue(client *redis.Client) *RedisQueue {
	cursors := make(map[domain.Channel]*atomic.Uint64, len(domain.AllChannels))
	for _, ch := range domain.AllChannels {
		cursors[ch] = &atomic.Uint64{}
	}
	return &RedisQueue{
		client:  client,
		clock:   time.Now,
		cursors: cursors,
	}
}

func queueKey(channel domain.Channel, priority domain.Priority) string {
	return fmt.Sprintf("notif:queue:%s:%s", channel, priority)
}

func retryKey(channel domain.Channel) string {
	return fmt.Sprintf("notif:retry:%s", channel)
}

func dlqKey(channel domain.Channel) string {
	return fmt.Sprintf("notif:dlq:%s", channel)
}

// Enqueue pushes a single notification message onto its channel/priority list.
func (q *RedisQueue) Enqueue(ctx context.Context, msg Message) error {
	return q.EnqueueBatch(ctx, []Message{msg})
}

// EnqueueBatch pipelines multiple notification messages into Redis in one round-trip.
func (q *RedisQueue) EnqueueBatch(ctx context.Context, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}
	pipe := q.client.Pipeline()
	for _, m := range msgs {
		if m.EnqueuedAt.IsZero() {
			m.EnqueuedAt = q.clock().UTC()
		}
		payload, err := json.Marshal(m)
		if err != nil {
			return fmt.Errorf("marshal queue message: %w", err)
		}
		pipe.LPush(ctx, queueKey(m.Channel, m.Priority), payload)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("pipeline enqueue: %w", err)
	}
	return nil
}

// Dequeue pops the next message for a channel using a 6:3:1 weighted fair priority order
// (high:normal:low) that falls back immediately to any non-empty queue to prevent starvation
// while keeping full throughput. Returns (nil, nil) if all channel queues are empty.
func (q *RedisQueue) Dequeue(ctx context.Context, channel domain.Channel) (*Message, error) {
	slot := q.nextCursor(channel) % 10
	high := queueKey(channel, domain.PriorityHigh)
	normal := queueKey(channel, domain.PriorityNormal)
	low := queueKey(channel, domain.PriorityLow)

	var keys []string
	switch {
	case slot < 6:
		keys = []string{high, normal, low}
	case slot < 9:
		keys = []string{normal, high, low}
	default:
		keys = []string{low, high, normal}
	}

	raw, err := dequeueWeightedScript.Run(ctx, q.client, keys).Text()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("dequeue %s: %w", channel, err)
	}

	var msg Message
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		return nil, fmt.Errorf("unmarshal queue message: %w", err)
	}
	return &msg, nil
}

func (q *RedisQueue) nextCursor(channel domain.Channel) uint64 {
	q.mu.Lock()
	c, ok := q.cursors[channel]
	if !ok {
		c = &atomic.Uint64{}
		q.cursors[channel] = c
	}
	q.mu.Unlock()
	return c.Add(1) - 1
}

// ScheduleRetry places a message into the channel's delayed retry ZSET scored by retryAt.
func (q *RedisQueue) ScheduleRetry(ctx context.Context, msg Message, retryAt time.Time) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal retry message: %w", err)
	}
	member := fmt.Sprintf("%s|%s", msg.Priority, string(payload))
	if err := q.client.ZAdd(ctx, retryKey(msg.Channel), redis.Z{
		Score:  float64(retryAt.UnixMilli()),
		Member: member,
	}).Err(); err != nil {
		return fmt.Errorf("schedule retry: %w", err)
	}
	return nil
}

// PromoteDueRetries atomically moves up to limit due entries from the retry ZSET back to priority queues.
func (q *RedisQueue) PromoteDueRetries(ctx context.Context, channel domain.Channel, limit int) (int64, error) {
	if limit <= 0 {
		limit = 100
	}
	keys := []string{
		retryKey(channel),
		queueKey(channel, domain.PriorityHigh),
		queueKey(channel, domain.PriorityNormal),
		queueKey(channel, domain.PriorityLow),
	}
	nowMs := q.clock().UnixMilli()
	moved, err := promoteRetriesScript.Run(ctx, q.client, keys, nowMs, limit).Int64()
	if err != nil {
		return 0, fmt.Errorf("promote retries for %s: %w", channel, err)
	}
	return moved, nil
}

// DLQEntry wraps a permanently failed queue message with its failure reason and timestamp.
type DLQEntry struct {
	Message  Message   `json:"message"`
	Reason   string    `json:"reason"`
	FailedAt time.Time `json:"failed_at"`
}

// PushDLQ pushes a permanently failed message onto the channel's dead-letter queue.
func (q *RedisQueue) PushDLQ(ctx context.Context, msg Message, reason string) error {
	entry := DLQEntry{
		Message:  msg,
		Reason:   reason,
		FailedAt: q.clock().UTC(),
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal dlq entry: %w", err)
	}
	if err := q.client.LPush(ctx, dlqKey(msg.Channel), raw).Err(); err != nil {
		return fmt.Errorf("push dlq: %w", err)
	}
	return nil
}

// Depths returns real-time queue depths across all channels and tiers in a single Redis pipeline.
func (q *RedisQueue) Depths(ctx context.Context) (map[string]observability.QueueDepthSnapshot, error) {
	pipe := q.client.Pipeline()
	type cmds struct {
		high   *redis.IntCmd
		normal *redis.IntCmd
		low    *redis.IntCmd
		retry  *redis.IntCmd
		dlq    *redis.IntCmd
	}
	perCh := make(map[domain.Channel]cmds, len(domain.AllChannels))
	for _, ch := range domain.AllChannels {
		perCh[ch] = cmds{
			high:   pipe.LLen(ctx, queueKey(ch, domain.PriorityHigh)),
			normal: pipe.LLen(ctx, queueKey(ch, domain.PriorityNormal)),
			low:    pipe.LLen(ctx, queueKey(ch, domain.PriorityLow)),
			retry:  pipe.ZCard(ctx, retryKey(ch)),
			dlq:    pipe.LLen(ctx, dlqKey(ch)),
		}
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("fetch queue depths: %w", err)
	}

	result := make(map[string]observability.QueueDepthSnapshot, len(domain.AllChannels))
	for _, ch := range domain.AllChannels {
		c := perCh[ch]
		h := c.high.Val()
		n := c.normal.Val()
		l := c.low.Val()
		r := c.retry.Val()
		d := c.dlq.Val()
		result[string(ch)] = observability.QueueDepthSnapshot{
			High:   h,
			Normal: n,
			Low:    l,
			Retry:  r,
			DLQ:    d,
			Total:  h + n + l + r,
		}
	}
	return result, nil
}
