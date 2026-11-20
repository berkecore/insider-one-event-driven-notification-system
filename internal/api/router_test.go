package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/api"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/api/handler"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/observability"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/service"
	"github.com/google/uuid"
)

type stubNotifRepo struct {
	mu          sync.Mutex
	byID        map[uuid.UUID]*domain.Notification
	byIdem      map[string]*domain.Notification
	batches     map[uuid.UUID]*domain.BatchSummary
	batchByIdem map[string]*domain.BatchSummary
}

func newStubNotifRepo() *stubNotifRepo {
	return &stubNotifRepo{
		byID:        make(map[uuid.UUID]*domain.Notification),
		byIdem:      make(map[string]*domain.Notification),
		batches:     make(map[uuid.UUID]*domain.BatchSummary),
		batchByIdem: make(map[string]*domain.BatchSummary),
	}
}

func (s *stubNotifRepo) CreateWithOutbox(_ context.Context, n *domain.Notification) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *n
	s.byID[n.ID] = &cp
	if n.IdempotencyKey != nil {
		s.byIdem[*n.IdempotencyKey] = &cp
	}
	return nil
}

func (s *stubNotifRepo) CreateBatchWithOutbox(_ context.Context, b *domain.NotificationBatch, items []*domain.Notification) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := make([]domain.Notification, len(items))
	counts := make(map[domain.Status]int)
	for i, n := range items {
		cp := *n
		s.byID[n.ID] = &cp
		list[i] = cp
		counts[n.Status]++
	}
	sum := &domain.BatchSummary{Batch: *b, StatusCounts: counts, Notifications: list}
	s.batches[b.ID] = sum
	if b.IdempotencyKey != nil {
		s.batchByIdem[*b.IdempotencyKey] = sum
	}
	return nil
}

func (s *stubNotifRepo) GetByID(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	cp := *n
	return &cp, nil
}

func (s *stubNotifRepo) GetByIdempotencyKey(_ context.Context, key string) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.byIdem[key]
	if !ok {
		return nil, domain.ErrNotFound
	}
	cp := *n
	return &cp, nil
}

func (s *stubNotifRepo) GetBatchByID(_ context.Context, id uuid.UUID) (*domain.BatchSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.batches[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return b, nil
}

func (s *stubNotifRepo) GetBatchByIdempotencyKey(_ context.Context, key string) (*domain.BatchSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.batchByIdem[key]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return b, nil
}

func (s *stubNotifRepo) List(_ context.Context, f domain.NotificationFilter) (*domain.NotificationPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var items []domain.Notification
	for _, n := range s.byID {
		if f.Channel != nil && n.Channel != *f.Channel {
			continue
		}
		if f.Status != nil && n.Status != *f.Status {
			continue
		}
		items = append(items, *n)
	}
	return &domain.NotificationPage{
		Items:      items,
		Total:      int64(len(items)),
		Page:       f.Page,
		PageSize:   f.PageSize,
		TotalPages: 1,
	}, nil
}

func (s *stubNotifRepo) Cancel(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	if !n.Status.CanCancel() {
		return nil, domain.ErrNotCancellable
	}
	n.Status = domain.StatusCancelled
	cp := *n
	return &cp, nil
}

func (s *stubNotifRepo) Replay(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	return nil, domain.ErrNotReplayable
}
func (s *stubNotifRepo) ClaimForProcessing(context.Context, uuid.UUID) (*domain.Notification, error) {
	return nil, nil
}
func (s *stubNotifRepo) MarkDelivered(context.Context, uuid.UUID, string) (*domain.Notification, error) {
	return nil, nil
}
func (s *stubNotifRepo) MarkRetrying(context.Context, uuid.UUID, time.Time, string) (*domain.Notification, error) {
	return nil, nil
}
func (s *stubNotifRepo) MarkDeferred(context.Context, uuid.UUID, time.Time, string) (*domain.Notification, error) {
	return nil, nil
}
func (s *stubNotifRepo) MarkFailed(context.Context, uuid.UUID, string) (*domain.Notification, error) {
	return nil, nil
}
func (s *stubNotifRepo) PromoteScheduled(context.Context, int) ([]domain.Notification, error) {
	return nil, nil
}
func (s *stubNotifRepo) ReclaimStaleProcessing(context.Context, time.Duration, int) (int64, error) {
	return 0, nil
}

func newTestRouter() http.Handler {
	repo := newStubNotifRepo()
	metrics := observability.NewMetrics()
	svc := service.NewNotificationService(repo, nil, nil, metrics, nil, 5)
	return api.NewRouter(api.RouterDeps{
		NotificationSvc: svc,
		Metrics:         metrics,
		HealthChecks: map[string]handler.DependencyCheck{
			"postgres": func(context.Context) error { return nil },
			"redis":    func(context.Context) error { return nil },
		},
	})
}

func TestNotificationEndpointsAndObservability(t *testing.T) {
	router := newTestRouter()

	// 1. Health & Readiness
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /health, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /ready, got %d", rec.Code)
	}

	// 2. Create Notification with Idempotency-Key and Correlation ID
	body := `{"recipient":"+905551234567","channel":"sms","content":"Hello World","priority":"high"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/notifications", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "idem-1")
	req.Header.Set("X-Correlation-ID", "corr-test-1")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-Correlation-ID") != "corr-test-1" {
		t.Fatalf("expected echoed correlation ID, got %q", rec.Header().Get("X-Correlation-ID"))
	}
	var created domain.Notification
	if err := json.NewDecoder(rec.Body).Decode(&created); err != nil {
		t.Fatalf("decode created: %v", err)
	}

	// 3. Idempotent replay returns 200 OK with X-Idempotent-Replayed: true
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/notifications", strings.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Idempotency-Key", "idem-1")
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK || rec2.Header().Get("X-Idempotent-Replayed") != "true" {
		t.Fatalf("expected 200 replay, got status=%d header=%q", rec2.Code, rec2.Header().Get("X-Idempotent-Replayed"))
	}

	// 4. Cancel pending notification
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/notifications/"+created.ID.String()+"/cancel", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on cancel, got %d: %s", rec.Code, rec.Body.String())
	}

	// 5. Batch creation & batch query
	batchBody := `{"notifications":[
		{"recipient":"+905551234567","channel":"sms","content":"Msg 1","priority":"high"},
		{"recipient":"user@example.com","channel":"email","content":"Msg 2","priority":"normal"}
	]}`
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/notifications/batch", bytes.NewBufferString(batchBody)))
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 on batch create, got %d: %s", rec.Code, rec.Body.String())
	}
	var batchSum domain.BatchSummary
	_ = json.NewDecoder(rec.Body).Decode(&batchSum)

	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/notifications/batches/"+batchSum.Batch.ID.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on get batch, got %d", rec.Code)
	}

	// 6. Real-time JSON metrics, Prometheus metrics, and OpenAPI docs
	for _, path := range []string{"/api/v1/metrics", "/metrics", "/openapi.yaml", "/docs"} {
		rec = httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 on %s, got %d", path, rec.Code)
		}
	}
}
