package queue

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/redis/go-redis/v9"
)

func TestDistributedRateLimiterPerChannel(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	rl := NewRedisRateLimiter(client, map[domain.Channel]int{
		domain.ChannelSMS:   100,
		domain.ChannelEmail: 5,
		domain.ChannelPush:  100,
	})

	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	rl.clock = func() time.Time { return now }

	ctx := context.Background()

	// SMS should allow exactly 100 messages within 1 second
	for i := 0; i < 100; i++ {
		allowed, wait, err := rl.Allow(ctx, domain.ChannelSMS)
		if err != nil {
			t.Fatalf("allow %d failed: %v", i, err)
		}
		if !allowed {
			t.Fatalf("expected sms message %d to be allowed (wait=%v)", i, wait)
		}
	}

	// 101st SMS message in the same second must be rate-limited
	allowed, wait, err := rl.Allow(ctx, domain.ChannelSMS)
	if err != nil {
		t.Fatalf("allow 101 failed: %v", err)
	}
	if allowed {
		t.Fatal("expected 101st sms message in 1s window to be rejected")
	}
	if wait <= 0 {
		t.Fatalf("expected positive wait duration, got %v", wait)
	}

	// Independent channel (Email) is NOT blocked by SMS limit
	allowedEmail, _, err := rl.Allow(ctx, domain.ChannelEmail)
	if err != nil || !allowedEmail {
		t.Fatalf("expected email channel to remain independent, allowed=%v err=%v", allowedEmail, err)
	}

	// Advance clock by 1001ms — sliding window clears and SMS is allowed again
	now = now.Add(1001 * time.Millisecond)
	allowedAfter, _, err := rl.Allow(ctx, domain.ChannelSMS)
	if err != nil || !allowedAfter {
		t.Fatalf("expected sms allowed after 1s sliding window elapsed, allowed=%v err=%v", allowedAfter, err)
	}
}
