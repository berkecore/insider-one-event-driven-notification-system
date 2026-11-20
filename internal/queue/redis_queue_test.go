package queue

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func newTestRedisQueue(t *testing.T) (*RedisQueue, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisQueue(client), mr
}

func TestPriorityOrderingAndAntiStarvation(t *testing.T) {
	q, _ := newTestRedisQueue(t)
	ctx := context.Background()

	lowID := uuid.New()
	normalID := uuid.New()
	highID := uuid.New()

	// Enqueue in reverse priority order
	err := q.EnqueueBatch(ctx, []Message{
		{NotificationID: lowID, Channel: domain.ChannelSMS, Priority: domain.PriorityLow},
		{NotificationID: normalID, Channel: domain.ChannelSMS, Priority: domain.PriorityNormal},
		{NotificationID: highID, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh},
	})
	if err != nil {
		t.Fatalf("enqueue batch failed: %v", err)
	}

	// First 3 dequeues should respect high -> normal -> low
	got1, err := q.Dequeue(ctx, domain.ChannelSMS)
	if err != nil || got1 == nil || got1.NotificationID != highID {
		t.Fatalf("expected high priority first, got %+v (err=%v)", got1, err)
	}
	got2, err := q.Dequeue(ctx, domain.ChannelSMS)
	if err != nil || got2 == nil || got2.NotificationID != normalID {
		t.Fatalf("expected normal priority second, got %+v (err=%v)", got2, err)
	}
	got3, err := q.Dequeue(ctx, domain.ChannelSMS)
	if err != nil || got3 == nil || got3.NotificationID != lowID {
		t.Fatalf("expected low priority third, got %+v (err=%v)", got3, err)
	}

	// Empty queue returns nil, nil
	gotEmpty, err := q.Dequeue(ctx, domain.ChannelSMS)
	if err != nil || gotEmpty != nil {
		t.Fatalf("expected nil on empty queue, got %+v, %v", gotEmpty, err)
	}

	// Anti-starvation: under heavy high-priority flood, normal and low still get served within 10 slots
	var flood []Message
	for i := 0; i < 15; i++ {
		flood = append(flood, Message{NotificationID: uuid.New(), Channel: domain.ChannelEmail, Priority: domain.PriorityHigh})
	}
	flood = append(flood,
		Message{NotificationID: uuid.New(), Channel: domain.ChannelEmail, Priority: domain.PriorityNormal},
		Message{NotificationID: uuid.New(), Channel: domain.ChannelEmail, Priority: domain.PriorityLow},
	)
	if err := q.EnqueueBatch(ctx, flood); err != nil {
		t.Fatalf("enqueue flood failed: %v", err)
	}

	seen := map[domain.Priority]int{}
	for i := 0; i < 10; i++ {
		m, err := q.Dequeue(ctx, domain.ChannelEmail)
		if err != nil || m == nil {
			t.Fatalf("dequeue %d failed: %v", i, err)
		}
		seen[m.Priority]++
	}
	if seen[domain.PriorityHigh] == 0 || seen[domain.PriorityNormal] == 0 || seen[domain.PriorityLow] == 0 {
		t.Fatalf("expected anti-starvation mix across priorities, got %+v", seen)
	}
}

func TestRetryPromotionAndDLQDepths(t *testing.T) {
	q, _ := newTestRedisQueue(t)
	ctx := context.Background()

	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	q.clock = func() time.Time { return now }

	dueMsg := Message{NotificationID: uuid.New(), Channel: domain.ChannelPush, Priority: domain.PriorityHigh}
	futureMsg := Message{NotificationID: uuid.New(), Channel: domain.ChannelPush, Priority: domain.PriorityLow}

	if err := q.ScheduleRetry(ctx, dueMsg, now.Add(-1*time.Second)); err != nil {
		t.Fatalf("schedule due retry failed: %v", err)
	}
	if err := q.ScheduleRetry(ctx, futureMsg, now.Add(30*time.Second)); err != nil {
		t.Fatalf("schedule future retry failed: %v", err)
	}

	moved, err := q.PromoteDueRetries(ctx, domain.ChannelPush, 10)
	if err != nil {
		t.Fatalf("promote retries failed: %v", err)
	}
	if moved != 1 {
		t.Fatalf("expected 1 due retry promoted, got %d", moved)
	}

	// Push one message to DLQ and check Depths
	if err := q.PushDLQ(ctx, futureMsg, "permanent 400 error"); err != nil {
		t.Fatalf("push dlq failed: %v", err)
	}

	depths, err := q.Depths(ctx)
	if err != nil {
		t.Fatalf("depths failed: %v", err)
	}
	pushDepth := depths[string(domain.ChannelPush)]
	if pushDepth.High != 1 || pushDepth.Retry != 1 || pushDepth.DLQ != 1 {
		t.Fatalf("unexpected push depths: %+v", pushDepth)
	}
}
