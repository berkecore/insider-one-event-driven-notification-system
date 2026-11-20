package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/api"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/config"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/observability"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/provider"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/queue"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/realtime"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/service"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/worker"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

// inMemStore implements NotificationRepository, OutboxRepository, AttemptRepository, and TemplateRepository
// so the full async pipeline (API -> Outbox -> Redis Priority Queue -> Rate Limiter -> Circuit Breaker ->
// Webhook -> Retry/DLQ -> WebSocket Hub) can be tested end-to-end with `go test -race ./...`.
type inMemStore struct {
	mu           sync.Mutex
	notifs       map[uuid.UUID]*domain.Notification
	byIdem       map[string]*domain.Notification
	batches      map[uuid.UUID]*domain.NotificationBatch
	batchByIdem  map[string]*domain.NotificationBatch
	outbox       []domain.OutboxEvent
	attempts     map[uuid.UUID][]domain.DeliveryAttempt
	templates    map[uuid.UUID]*domain.Template
	nextOutboxID int64
}

func newInMemStore() *inMemStore {
	return &inMemStore{
		notifs:      make(map[uuid.UUID]*domain.Notification),
		byIdem:      make(map[string]*domain.Notification),
		batches:     make(map[uuid.UUID]*domain.NotificationBatch),
		batchByIdem: make(map[string]*domain.NotificationBatch),
		attempts:    make(map[uuid.UUID][]domain.DeliveryAttempt),
		templates:   make(map[uuid.UUID]*domain.Template),
	}
}

func (s *inMemStore) CreateWithOutbox(_ context.Context, n *domain.Notification) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n.IdempotencyKey != nil {
		if _, ok := s.byIdem[*n.IdempotencyKey]; ok {
			return domain.ErrConflict
		}
	}
	cp := *n
	s.notifs[n.ID] = &cp
	if n.IdempotencyKey != nil {
		s.byIdem[*n.IdempotencyKey] = &cp
	}
	if n.Status == domain.StatusPending {
		s.nextOutboxID++
		s.outbox = append(s.outbox, domain.OutboxEvent{
			ID:             s.nextOutboxID,
			NotificationID: n.ID,
			Channel:        n.Channel,
			Priority:       n.Priority,
			CorrelationID:  n.CorrelationID,
			TraceContext:   n.TraceContext,
			CreatedAt:      time.Now().UTC(),
		})
	}
	return nil
}

func (s *inMemStore) CreateBatchWithOutbox(_ context.Context, b *domain.NotificationBatch, items []*domain.Notification) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b.IdempotencyKey != nil {
		if _, ok := s.batchByIdem[*b.IdempotencyKey]; ok {
			return domain.ErrConflict
		}
	}
	bc := *b
	s.batches[b.ID] = &bc
	if b.IdempotencyKey != nil {
		s.batchByIdem[*b.IdempotencyKey] = &bc
	}
	for _, n := range items {
		cp := *n
		s.notifs[n.ID] = &cp
		if n.Status == domain.StatusPending {
			s.nextOutboxID++
			s.outbox = append(s.outbox, domain.OutboxEvent{
				ID:             s.nextOutboxID,
				NotificationID: n.ID,
				Channel:        n.Channel,
				Priority:       n.Priority,
				CorrelationID:  n.CorrelationID,
				CreatedAt:      time.Now().UTC(),
			})
		}
	}
	return nil
}

func (s *inMemStore) GetByID(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.notifs[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	cp := *n
	cp.Attempts = append([]domain.DeliveryAttempt(nil), s.attempts[id]...)
	return &cp, nil
}

func (s *inMemStore) GetByIdempotencyKey(_ context.Context, key string) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.byIdem[key]
	if !ok {
		return nil, domain.ErrNotFound
	}
	cp := *n
	return &cp, nil
}

func (s *inMemStore) GetBatchByID(_ context.Context, id uuid.UUID) (*domain.BatchSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.batches[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	counts := make(map[domain.Status]int)
	var list []domain.Notification
	for _, n := range s.notifs {
		if n.BatchID != nil && *n.BatchID == id {
			counts[n.Status]++
			list = append(list, *n)
		}
	}
	return &domain.BatchSummary{Batch: *b, StatusCounts: counts, Notifications: list}, nil
}

func (s *inMemStore) GetBatchByIdempotencyKey(_ context.Context, key string) (*domain.BatchSummary, error) {
	s.mu.Lock()
	b, ok := s.batchByIdem[key]
	s.mu.Unlock()
	if !ok {
		return nil, domain.ErrNotFound
	}
	return s.GetBatchByID(context.Background(), b.ID)
}

func (s *inMemStore) List(_ context.Context, f domain.NotificationFilter) (*domain.NotificationPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var items []domain.Notification
	for _, n := range s.notifs {
		if f.Status != nil && n.Status != *f.Status {
			continue
		}
		if f.Channel != nil && n.Channel != *f.Channel {
			continue
		}
		items = append(items, *n)
	}
	return &domain.NotificationPage{Items: items, Total: int64(len(items)), Page: f.Page, PageSize: f.PageSize, TotalPages: 1}, nil
}

func (s *inMemStore) Cancel(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.notifs[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	if !n.Status.CanCancel() {
		return nil, domain.ErrNotCancellable
	}
	n.Status = domain.StatusCancelled
	now := time.Now().UTC()
	n.CancelledAt = &now
	cp := *n
	return &cp, nil
}

func (s *inMemStore) Replay(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.notifs[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	if n.Status != domain.StatusFailed {
		return nil, domain.ErrNotReplayable
	}
	n.Status = domain.StatusPending
	n.AttemptCount = 0
	s.nextOutboxID++
	s.outbox = append(s.outbox, domain.OutboxEvent{
		ID:             s.nextOutboxID,
		NotificationID: n.ID,
		Channel:        n.Channel,
		Priority:       n.Priority,
		CorrelationID:  n.CorrelationID,
	})
	cp := *n
	return &cp, nil
}

func (s *inMemStore) ClaimForProcessing(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.notifs[id]
	if !ok || (n.Status != domain.StatusQueued && n.Status != domain.StatusRetrying) {
		return nil, nil
	}
	n.Status = domain.StatusProcessing
	n.AttemptCount++
	now := time.Now().UTC()
	n.ProcessingStartedAt = &now
	cp := *n
	return &cp, nil
}

func (s *inMemStore) MarkDelivered(_ context.Context, id uuid.UUID, msgID string) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.notifs[id]
	n.Status = domain.StatusDelivered
	n.ProviderMessageID = &msgID
	now := time.Now().UTC()
	n.DeliveredAt = &now
	cp := *n
	return &cp, nil
}

func (s *inMemStore) MarkRetrying(_ context.Context, id uuid.UUID, next time.Time, lastErr string) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.notifs[id]
	n.Status = domain.StatusRetrying
	n.NextRetryAt = &next
	n.LastError = &lastErr
	cp := *n
	return &cp, nil
}

func (s *inMemStore) MarkDeferred(_ context.Context, id uuid.UUID, next time.Time, lastErr string) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.notifs[id]
	n.Status = domain.StatusRetrying
	if n.AttemptCount > 0 {
		n.AttemptCount--
	}
	n.NextRetryAt = &next
	n.LastError = &lastErr
	cp := *n
	return &cp, nil
}

func (s *inMemStore) MarkFailed(_ context.Context, id uuid.UUID, lastErr string) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.notifs[id]
	n.Status = domain.StatusFailed
	n.LastError = &lastErr
	cp := *n
	return &cp, nil
}

func (s *inMemStore) PromoteScheduled(_ context.Context, _ int) ([]domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	var promoted []domain.Notification
	for _, n := range s.notifs {
		if n.Status == domain.StatusScheduled && n.ScheduledAt != nil && !n.ScheduledAt.After(now) {
			n.Status = domain.StatusPending
			s.nextOutboxID++
			s.outbox = append(s.outbox, domain.OutboxEvent{
				ID:             s.nextOutboxID,
				NotificationID: n.ID,
				Channel:        n.Channel,
				Priority:       n.Priority,
				CorrelationID:  n.CorrelationID,
			})
			promoted = append(promoted, *n)
		}
	}
	return promoted, nil
}

func (s *inMemStore) ReclaimStaleProcessing(_ context.Context, _ time.Duration, _ int) (int64, error) {
	return 0, nil
}

func (s *inMemStore) ProcessPending(_ context.Context, limit int, fn func([]domain.OutboxEvent) error) ([]domain.OutboxEvent, error) {
	s.mu.Lock()
	var batch []domain.OutboxEvent
	for _, ev := range s.outbox {
		if ev.PublishedAt == nil {
			batch = append(batch, ev)
			if len(batch) >= limit {
				break
			}
		}
	}
	s.mu.Unlock()
	if len(batch) == 0 {
		return nil, nil
	}
	if err := fn(batch); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	publishedIDs := make(map[int64]struct{}, len(batch))
	for _, ev := range batch {
		publishedIDs[ev.ID] = struct{}{}
		if n, ok := s.notifs[ev.NotificationID]; ok && n.Status == domain.StatusPending {
			n.Status = domain.StatusQueued
		}
	}
	for i := range s.outbox {
		if _, ok := publishedIDs[s.outbox[i].ID]; ok {
			s.outbox[i].PublishedAt = &now
		}
	}
	return batch, nil
}

func (s *inMemStore) Record(_ context.Context, a *domain.DeliveryAttempt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts[a.NotificationID] = append(s.attempts[a.NotificationID], *a)
	return nil
}

func (s *inMemStore) ListByNotificationID(_ context.Context, id uuid.UUID) ([]domain.DeliveryAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.DeliveryAttempt(nil), s.attempts[id]...), nil
}

func (s *inMemStore) Create(_ context.Context, tpl *domain.Template) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *tpl
	s.templates[tpl.ID] = &cp
	return nil
}
func (s *inMemStore) GetByIDTemplate(_ context.Context, id uuid.UUID) (*domain.Template, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.templates[id]
	if !ok {
		return nil, domain.ErrTemplateNotFound
	}
	cp := *t
	return &cp, nil
}

type tplAdapter struct{ *inMemStore }

func (a tplAdapter) GetByID(ctx context.Context, id uuid.UUID) (*domain.Template, error) {
	return a.GetByIDTemplate(ctx, id)
}
func (a tplAdapter) List(_ context.Context, _ *domain.Channel) ([]domain.Template, error) {
	return nil, nil
}
func (a tplAdapter) Update(_ context.Context, tpl *domain.Template) error { return a.Create(context.Background(), tpl) }
func (a tplAdapter) Delete(_ context.Context, id uuid.UUID) error {
	a.mu.Lock()
	delete(a.templates, id)
	a.mu.Unlock()
	return nil
}

func TestEndToEndAsyncPipelineWithRetriesAndWebSocket(t *testing.T) {
	mr := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })

	// Simulate an external webhook provider that fails once on content containing "flaky" and returns 202 otherwise.
	var flakyHits atomic.Int32
	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req provider.DeliveryRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if strings.Contains(req.Content, "flaky") && flakyHits.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"temporary outage"}`))
			return
		}
		if strings.Contains(req.Content, "bad-request") {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid recipient payload"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(provider.DeliveryResponse{
			MessageID: uuid.NewString(),
			Status:    "accepted",
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
	}))
	defer webhookSrv.Close()

	store := newInMemStore()
	metrics := observability.NewMetrics()
	redisQueue := queue.NewRedisQueue(redisClient)
	rateLimiter := queue.NewRedisRateLimiter(redisClient, map[domain.Channel]int{
		domain.ChannelSMS:   100,
		domain.ChannelEmail: 100,
		domain.ChannelPush:  100,
	})
	pub := realtime.NewEventPublisher(redisClient)
	hub := realtime.NewHub(redisClient, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = hub.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)

	prov := provider.NewCircuitBreakerProvider(
		provider.NewWebhookProvider(webhookSrv.URL, 2*time.Second),
		10, 5*time.Second, 2, nil,
	)

	processor := worker.NewProcessor(
		store, store, redisQueue, rateLimiter, prov, pub, metrics,
		worker.NewBackoffPolicy(50*time.Millisecond, 200*time.Millisecond), nil,
	)
	outboxRelay := worker.NewOutboxRelay(store, redisQueue, pub, nil, 25*time.Millisecond, 50)
	scheduler := worker.NewScheduler(store, pub, nil, 50*time.Millisecond, 30*time.Second)

	engine := worker.NewEngine(config.WorkerConfig{
		WorkersPerChannel:     2,
		RetryPromoterInterval: 50 * time.Millisecond,
	}, redisQueue, processor, outboxRelay, scheduler, metrics, nil)
	engine.Start(ctx)
	t.Cleanup(func() {
		cancel()
		engine.Wait()
	})

	notifSvc := service.NewNotificationService(store, tplAdapter{store}, pub, metrics, nil, 3)
	tplSvc := service.NewTemplateService(tplAdapter{store})
	router := api.NewRouter(api.RouterDeps{
		NotificationSvc: notifSvc,
		TemplateSvc:     tplSvc,
		Metrics:         metrics,
		QueueDepths:     redisQueue,
		Hub:             hub,
	})
	apiSrv := httptest.NewServer(router)
	defer apiSrv.Close()

	// Connect WebSocket client watching SMS channel events
	wsURL := "ws" + strings.TrimPrefix(apiSrv.URL, "http") + "/ws/notifications?channel=sms"
	wsConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("ws dial failed: %v", err)
	}
	defer func() { _ = wsConn.Close() }()

	wsStatuses := make(chan domain.StatusEvent, 32)
	go func() {
		for {
			_, data, err := wsConn.ReadMessage()
			if err != nil {
				return
			}
			var ev domain.StatusEvent
			if json.Unmarshal(data, &ev) == nil {
				wsStatuses <- ev
			}
		}
	}()

	// 1. Create a template & send a flaky SMS notification (fails once with 503, then retries and delivers)
	tplResp, err := http.Post(apiSrv.URL+"/api/v1/templates", "application/json",
		strings.NewReader(`{"name":"promo","channel":"sms","body":"Hi {{name}}, flaky test ok!"}`))
	if err != nil || tplResp.StatusCode != http.StatusCreated {
		t.Fatalf("create template failed: err=%v status=%v", err, tplResp)
	}
	var tpl domain.Template
	_ = json.NewDecoder(tplResp.Body).Decode(&tpl)
	_ = tplResp.Body.Close()

	createBody, _ := json.Marshal(map[string]any{
		"recipient":     "+905551234567",
		"channel":       "sms",
		"template_id":   tpl.ID,
		"template_vars": map[string]string{"name": "Insider"},
		"priority":      "high",
	})
	resp, err := http.Post(apiSrv.URL+"/api/v1/notifications", "application/json", bytes.NewReader(createBody))
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("create sms failed: err=%v resp=%v", err, resp)
	}
	var smsNotif domain.Notification
	_ = json.NewDecoder(resp.Body).Decode(&smsNotif)
	_ = resp.Body.Close()

	// 2. Create a permanently failing email notification (400 -> immediate DLQ)
	failResp, err := http.Post(apiSrv.URL+"/api/v1/notifications", "application/json",
		strings.NewReader(`{"recipient":"bad@example.com","channel":"email","content":"bad-request payload","priority":"low"}`))
	if err != nil || failResp.StatusCode != http.StatusCreated {
		t.Fatalf("create bad email failed: err=%v", err)
	}
	var failNotif domain.Notification
	_ = json.NewDecoder(failResp.Body).Decode(&failNotif)
	_ = failResp.Body.Close()

	// Poll until SMS is delivered (after 1 retry) and Email is permanently failed (in DLQ)
	deadline := time.Now().Add(4 * time.Second)
	var finalSMS, finalEmail *domain.Notification
	for time.Now().Before(deadline) {
		finalSMS, _ = store.GetByID(context.Background(), smsNotif.ID)
		finalEmail, _ = store.GetByID(context.Background(), failNotif.ID)
		if finalSMS.Status == domain.StatusDelivered && finalEmail.Status == domain.StatusFailed {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if finalSMS.Status != domain.StatusDelivered {
		t.Fatalf("expected flaky SMS to recover and be delivered, got status=%s attempts=%d", finalSMS.Status, finalSMS.AttemptCount)
	}
	if len(finalSMS.Attempts) != 2 {
		t.Fatalf("expected 2 delivery attempts for flaky SMS (1 fail + 1 success), got %d", len(finalSMS.Attempts))
	}
	if finalEmail.Status != domain.StatusFailed || len(finalEmail.Attempts) != 1 {
		t.Fatalf("expected permanent 400 email to fail immediately in 1 attempt, got status=%s attempts=%d", finalEmail.Status, len(finalEmail.Attempts))
	}

	// Verify WebSocket received a delivered event for SMS
	sawDelivered := false
	wsDeadline := time.After(1 * time.Second)
	for !sawDelivered {
		select {
		case ev := <-wsStatuses:
			if ev.NotificationID == smsNotif.ID && ev.Status == domain.StatusDelivered {
				sawDelivered = true
			}
		case <-wsDeadline:
			t.Fatal("timed out waiting for WebSocket delivered event")
		}
	}

	// Verify real-time JSON metrics reflect deliveries, retries, and DLQ
	mResp, err := http.Get(apiSrv.URL + "/api/v1/metrics")
	if err != nil || mResp.StatusCode != http.StatusOK {
		t.Fatalf("metrics request failed: %v", err)
	}
	var snap observability.MetricsSnapshot
	_ = json.NewDecoder(mResp.Body).Decode(&snap)
	_ = mResp.Body.Close()

	if snap.Delivery.TotalDelivered < 1 || snap.Delivery.TotalFailed < 1 || snap.Delivery.TotalRetried < 1 {
		t.Fatalf("unexpected delivery metrics snapshot: %+v", snap.Delivery)
	}
	if snap.Queues["email"].DLQ != 1 {
		t.Fatalf("expected email DLQ depth = 1, got %+v", snap.Queues["email"])
	}
}
