package worker

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/observability"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/provider"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/queue"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type fakeNotifRepo struct {
	notif      *domain.Notification
	claimNil   bool
	delivered  bool
	retrying   bool
	deferred   bool
	failed     bool
	lastErr    string
}

func (f *fakeNotifRepo) CreateWithOutbox(context.Context, *domain.Notification) error { return nil }
func (f *fakeNotifRepo) CreateBatchWithOutbox(context.Context, *domain.NotificationBatch, []*domain.Notification) error {
	return nil
}
func (f *fakeNotifRepo) GetByID(context.Context, uuid.UUID) (*domain.Notification, error) {
	return f.notif, nil
}
func (f *fakeNotifRepo) GetByIdempotencyKey(context.Context, string) (*domain.Notification, error) {
	return nil, domain.ErrNotFound
}
func (f *fakeNotifRepo) GetBatchByID(context.Context, uuid.UUID) (*domain.BatchSummary, error) {
	return nil, domain.ErrNotFound
}
func (f *fakeNotifRepo) GetBatchByIdempotencyKey(context.Context, string) (*domain.BatchSummary, error) {
	return nil, domain.ErrNotFound
}
func (f *fakeNotifRepo) List(context.Context, domain.NotificationFilter) (*domain.NotificationPage, error) {
	return nil, nil
}
func (f *fakeNotifRepo) Cancel(context.Context, uuid.UUID) (*domain.Notification, error) {
	return nil, nil
}
func (f *fakeNotifRepo) Replay(context.Context, uuid.UUID) (*domain.Notification, error) {
	return nil, nil
}
func (f *fakeNotifRepo) PromoteScheduled(context.Context, int) ([]domain.Notification, error) {
	return nil, nil
}
func (f *fakeNotifRepo) ReclaimStaleProcessing(context.Context, time.Duration, int) (int64, error) {
	return 0, nil
}

func (f *fakeNotifRepo) ClaimForProcessing(_ context.Context, _ uuid.UUID) (*domain.Notification, error) {
	if f.claimNil {
		return nil, nil
	}
	f.notif.AttemptCount++
	f.notif.Status = domain.StatusProcessing
	cp := *f.notif
	return &cp, nil
}

func (f *fakeNotifRepo) MarkDelivered(_ context.Context, _ uuid.UUID, msgID string) (*domain.Notification, error) {
	f.delivered = true
	f.notif.Status = domain.StatusDelivered
	f.notif.ProviderMessageID = &msgID
	cp := *f.notif
	return &cp, nil
}

func (f *fakeNotifRepo) MarkRetrying(_ context.Context, _ uuid.UUID, next time.Time, lastErr string) (*domain.Notification, error) {
	f.retrying = true
	f.lastErr = lastErr
	f.notif.Status = domain.StatusRetrying
	f.notif.NextRetryAt = &next
	cp := *f.notif
	return &cp, nil
}

func (f *fakeNotifRepo) MarkDeferred(_ context.Context, _ uuid.UUID, next time.Time, lastErr string) (*domain.Notification, error) {
	f.deferred = true
	f.lastErr = lastErr
	f.notif.AttemptCount--
	f.notif.Status = domain.StatusRetrying
	f.notif.NextRetryAt = &next
	cp := *f.notif
	return &cp, nil
}

func (f *fakeNotifRepo) MarkFailed(_ context.Context, _ uuid.UUID, lastErr string) (*domain.Notification, error) {
	f.failed = true
	f.lastErr = lastErr
	f.notif.Status = domain.StatusFailed
	cp := *f.notif
	return &cp, nil
}

type fakeAttemptRepo struct {
	recorded []*domain.DeliveryAttempt
}

func (f *fakeAttemptRepo) Record(_ context.Context, a *domain.DeliveryAttempt) error {
	f.recorded = append(f.recorded, a)
	return nil
}
func (f *fakeAttemptRepo) ListByNotificationID(context.Context, uuid.UUID) ([]domain.DeliveryAttempt, error) {
	return nil, nil
}

type stubDeliveryProvider struct {
	called int
	resp   *provider.DeliveryResponse
	err    error
}

func (s *stubDeliveryProvider) Deliver(_ context.Context, _ provider.DeliveryRequest) (*provider.DeliveryResponse, error) {
	s.called++
	return s.resp, s.err
}

func newWorkerTestQueue(t *testing.T) *queue.RedisQueue {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return queue.NewRedisQueue(client)
}

func TestProcessorDeliveryRetryPermanentFailAndCircuitOpen(t *testing.T) {
	t.Run("successful delivery records attempt and marks delivered", func(t *testing.T) {
		q := newWorkerTestQueue(t)
		id := uuid.New()
		repo := &fakeNotifRepo{notif: &domain.Notification{ID: id, Recipient: "+905551234567", Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, MaxAttempts: 3}}
		attempts := &fakeAttemptRepo{}
		prov := &stubDeliveryProvider{resp: &provider.DeliveryResponse{MessageID: "prov-1", Status: "accepted", StatusCode: 202}}
		metrics := observability.NewMetrics()

		proc := NewProcessor(repo, attempts, q, nil, prov, nil, metrics, NewBackoffPolicy(time.Second, 10*time.Second), nil)
		if err := proc.Process(context.Background(), queue.Message{NotificationID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}); err != nil {
			t.Fatalf("process failed: %v", err)
		}
		if !repo.delivered || len(attempts.recorded) != 1 || prov.called != 1 {
			t.Fatalf("expected delivered with 1 attempt; delivered=%v attempts=%d called=%d", repo.delivered, len(attempts.recorded), prov.called)
		}
	})

	t.Run("cancelled notification skipped without provider call", func(t *testing.T) {
		q := newWorkerTestQueue(t)
		repo := &fakeNotifRepo{claimNil: true}
		prov := &stubDeliveryProvider{}
		proc := NewProcessor(repo, nil, q, nil, prov, nil, nil, NewBackoffPolicy(time.Second, 10*time.Second), nil)
		if err := proc.Process(context.Background(), queue.Message{NotificationID: uuid.New(), Channel: domain.ChannelSMS}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if prov.called != 0 {
			t.Fatalf("expected 0 provider calls for cancelled item, got %d", prov.called)
		}
	})

	t.Run("retryable 503 schedules retry in Redis ZSET", func(t *testing.T) {
		q := newWorkerTestQueue(t)
		id := uuid.New()
		repo := &fakeNotifRepo{notif: &domain.Notification{ID: id, Channel: domain.ChannelEmail, Priority: domain.PriorityNormal, MaxAttempts: 3}}
		prov := &stubDeliveryProvider{err: &provider.DeliveryError{StatusCode: 503, Message: "unavailable", Retryable: true}}
		proc := NewProcessor(repo, &fakeAttemptRepo{}, q, nil, prov, nil, observability.NewMetrics(), NewBackoffPolicy(time.Second, 10*time.Second), nil)

		if err := proc.Process(context.Background(), queue.Message{NotificationID: id, Channel: domain.ChannelEmail, Priority: domain.PriorityNormal}); err != nil {
			t.Fatalf("process failed: %v", err)
		}
		if !repo.retrying {
			t.Fatal("expected notification marked retrying")
		}
		depths, _ := q.Depths(context.Background())
		if depths["email"].Retry != 1 {
			t.Fatalf("expected 1 entry in email retry ZSET, got %+v", depths["email"])
		}
	})

	t.Run("permanent 400 routes immediately to DLQ", func(t *testing.T) {
		q := newWorkerTestQueue(t)
		id := uuid.New()
		repo := &fakeNotifRepo{notif: &domain.Notification{ID: id, Channel: domain.ChannelPush, Priority: domain.PriorityLow, MaxAttempts: 5}}
		prov := &stubDeliveryProvider{err: &provider.DeliveryError{StatusCode: 400, Message: "invalid token", Retryable: false}}
		proc := NewProcessor(repo, &fakeAttemptRepo{}, q, nil, prov, nil, observability.NewMetrics(), NewBackoffPolicy(time.Second, 10*time.Second), nil)

		if err := proc.Process(context.Background(), queue.Message{NotificationID: id, Channel: domain.ChannelPush, Priority: domain.PriorityLow}); err != nil {
			t.Fatalf("process failed: %v", err)
		}
		if !repo.failed {
			t.Fatal("expected permanent 400 to mark failed immediately")
		}
		depths, _ := q.Depths(context.Background())
		if depths["push"].DLQ != 1 {
			t.Fatalf("expected 1 entry in push DLQ, got %+v", depths["push"])
		}
	})

	t.Run("open circuit breaker defers without burning an attempt", func(t *testing.T) {
		q := newWorkerTestQueue(t)
		id := uuid.New()
		repo := &fakeNotifRepo{notif: &domain.Notification{ID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh, MaxAttempts: 3}}
		prov := &stubDeliveryProvider{err: &provider.CircuitOpenError{Channel: "sms", RetryAfter: 5 * time.Second}}
		proc := NewProcessor(repo, &fakeAttemptRepo{}, q, nil, prov, nil, observability.NewMetrics(), NewBackoffPolicy(time.Second, 10*time.Second), nil)

		if err := proc.Process(context.Background(), queue.Message{NotificationID: id, Channel: domain.ChannelSMS, Priority: domain.PriorityHigh}); err != nil {
			t.Fatalf("process failed: %v", err)
		}
		if !repo.deferred || repo.notif.AttemptCount != 0 {
			t.Fatalf("expected deferred with attempt_count restored to 0, deferred=%v attempts=%d", repo.deferred, repo.notif.AttemptCount)
		}
	})
}
