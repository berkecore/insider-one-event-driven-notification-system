package realtime

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

// EventsChannel is the Redis Pub/Sub channel used to broadcast live status updates.
const EventsChannel = "notif:events"

// EventPublisher publishes notification status transitions onto Redis Pub/Sub.
type EventPublisher struct {
	client *redis.Client
}

func NewEventPublisher(client *redis.Client) *EventPublisher {
	return &EventPublisher{client: client}
}

// Publish serializes ev and publishes it to Redis Pub/Sub.
func (p *EventPublisher) Publish(ctx context.Context, ev domain.StatusEvent) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return p.client.Publish(ctx, EventsChannel, payload).Err()
}

// SubscriptionFilter restricts which status events a WebSocket client receives.
type SubscriptionFilter struct {
	NotificationID *uuid.UUID
	BatchID        *uuid.UUID
	Channel        *domain.Channel
}

func (f SubscriptionFilter) Matches(ev domain.StatusEvent) bool {
	if f.NotificationID != nil && ev.NotificationID != *f.NotificationID {
		return false
	}
	if f.BatchID != nil && (ev.BatchID == nil || *ev.BatchID != *f.BatchID) {
		return false
	}
	if f.Channel != nil && ev.Channel != *f.Channel {
		return false
	}
	return true
}

// Subscriber represents an active WebSocket or in-process event stream consumer.
type Subscriber struct {
	Filter SubscriptionFilter
	Ch     chan []byte
}

// Hub subscribes to Redis Pub/Sub and fans out matching status events to connected WebSocket clients.
type Hub struct {
	client   *redis.Client
	logger   *slog.Logger
	upgrader websocket.Upgrader

	mu          sync.RWMutex
	subscribers map[*Subscriber]struct{}
}

func NewHub(client *redis.Client, logger *slog.Logger) *Hub {
	if logger == nil {
		logger = slog.Default()
	}
	return &Hub{
		client:      client,
		logger:      logger,
		subscribers: make(map[*Subscriber]struct{}),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 4096,
			CheckOrigin:     func(_ *http.Request) bool { return true },
		},
	}
}

// Subscribe registers a new event consumer with the given filter.
func (h *Hub) Subscribe(filter SubscriptionFilter) *Subscriber {
	sub := &Subscriber{
		Filter: filter,
		Ch:     make(chan []byte, 64),
	}
	h.mu.Lock()
	h.subscribers[sub] = struct{}{}
	h.mu.Unlock()
	return sub
}

// Unsubscribe removes a subscriber and closes its buffered channel.
func (h *Hub) Unsubscribe(sub *Subscriber) {
	h.mu.Lock()
	if _, ok := h.subscribers[sub]; ok {
		delete(h.subscribers, sub)
		close(sub.Ch)
	}
	h.mu.Unlock()
}

// SubscriberCount returns the number of active connected subscribers.
func (h *Hub) SubscriberCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subscribers)
}

// Run subscribes to Redis Pub/Sub and fans out messages until ctx is cancelled.
func (h *Hub) Run(ctx context.Context) error {
	pubsub := h.client.Subscribe(ctx, EventsChannel)
	defer func() { _ = pubsub.Close() }()

	// Wait for subscription confirmation.
	if _, err := pubsub.Receive(ctx); err != nil {
		return err
	}

	msgCh := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			h.closeAll()
			return nil
		case msg, ok := <-msgCh:
			if !ok {
				h.closeAll()
				return nil
			}
			h.BroadcastRaw([]byte(msg.Payload))
		}
	}
}

// BroadcastRaw parses a status event payload and delivers it to matching subscribers without blocking.
func (h *Hub) BroadcastRaw(raw []byte) {
	var ev domain.StatusEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		return
	}

	h.mu.RLock()
	defer h.mu.RUnlock()
	for sub := range h.subscribers {
		if !sub.Filter.Matches(ev) {
			continue
		}
		select {
		case sub.Ch <- raw:
		default:
			// Slow consumer: drop oldest-style overflow to avoid blocking the hub.
		}
	}
}

func (h *Hub) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for sub := range h.subscribers {
		delete(h.subscribers, sub)
		close(sub.Ch)
	}
}

// ServeWS upgrades an HTTP connection to WebSocket and streams matching notification events.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request, filter SubscriptionFilter) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Warn("websocket upgrade failed", slog.Any("error", err))
		return
	}
	defer func() { _ = conn.Close() }()

	sub := h.Subscribe(filter)
	defer h.Unsubscribe(sub)

	const (
		writeWait  = 10 * time.Second
		pongWait   = 60 * time.Second
		pingPeriod = 30 * time.Second
	)

	conn.SetReadLimit(1024)
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	pingTicker := time.NewTicker(pingPeriod)
	defer pingTicker.Stop()

	for {
		select {
		case <-done:
			return
		case <-r.Context().Done():
			return
		case payload, ok := <-sub.Ch:
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}
		case <-pingTicker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
