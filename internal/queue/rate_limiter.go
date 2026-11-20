package queue

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// RateLimiter enforces per-channel throughput caps (default: 100 messages/second/channel).
type RateLimiter interface {
	Allow(ctx context.Context, channel domain.Channel) (allowed bool, wait time.Duration, err error)
	Wait(ctx context.Context, channel domain.Channel) error
}

var slidingWindowRateLimitScript = redis.NewScript(`
local key = KEYS[1]
local now_ms = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

local window_start = now_ms - window_ms
redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

local count = redis.call('ZCARD', key)
if count < limit then
	redis.call('ZADD', key, now_ms, member)
	redis.call('PEXPIRE', key, window_ms * 2)
	return {1, 0}
end

local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
local wait_ms = 10
if #oldest >= 2 then
	local oldest_ts = tonumber(oldest[2])
	local diff = (oldest_ts + window_ms) - now_ms
	if diff > 0 then
		wait_ms = diff
	end
end
return {0, wait_ms}
`)

// RedisRateLimiter implements a distributed sliding-window rate limiter in Redis Lua
// so per-channel limits hold globally across all worker goroutines and replicas.
type RedisRateLimiter struct {
	client     *redis.Client
	limits     map[domain.Channel]int
	window     time.Duration
	instanceID string
	seq        atomic.Uint64
	clock      func() time.Time
}

// NewRedisRateLimiter creates a per-channel distributed rate limiter with a 1-second rolling window.
func NewRedisRateLimiter(client *redis.Client, limits map[domain.Channel]int) *RedisRateLimiter {
	copied := make(map[domain.Channel]int, len(limits))
	for k, v := range limits {
		copied[k] = v
	}
	return &RedisRateLimiter{
		client:     client,
		limits:     copied,
		window:     time.Second,
		instanceID: uuid.NewString()[:8],
		clock:      time.Now,
	}
}

func (rl *RedisRateLimiter) limitFor(channel domain.Channel) int {
	if v, ok := rl.limits[channel]; ok && v > 0 {
		return v
	}
	return 100
}

// Allow checks and consumes a rate-limit slot for the given channel.
// When denied, wait indicates how long the caller should sleep before retrying.
func (rl *RedisRateLimiter) Allow(ctx context.Context, channel domain.Channel) (bool, time.Duration, error) {
	nowMs := rl.clock().UnixMilli()
	member := fmt.Sprintf("%s-%d-%d", rl.instanceID, nowMs, rl.seq.Add(1))
	key := fmt.Sprintf("notif:ratelimit:%s", channel)

	res, err := slidingWindowRateLimitScript.Run(
		ctx,
		rl.client,
		[]string{key},
		nowMs,
		rl.window.Milliseconds(),
		rl.limitFor(channel),
		member,
	).Int64Slice()
	if err != nil {
		return false, 0, fmt.Errorf("rate limit script for %s: %w", channel, err)
	}
	if len(res) < 2 {
		return false, 0, fmt.Errorf("unexpected rate limit script response length: %d", len(res))
	}
	if res[0] == 1 {
		return true, 0, nil
	}
	wait := time.Duration(res[1]) * time.Millisecond
	if wait < 5*time.Millisecond {
		wait = 5 * time.Millisecond
	}
	return false, wait, nil
}

// Wait blocks until a slot is granted for the channel or ctx is cancelled.
func (rl *RedisRateLimiter) Wait(ctx context.Context, channel domain.Channel) error {
	for {
		allowed, wait, err := rl.Allow(ctx, channel)
		if err != nil {
			return err
		}
		if allowed {
			return nil
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
