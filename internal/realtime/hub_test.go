package realtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/berkecore/insider-one-event-driven-notification-system/internal/domain"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

func TestHubPubSubFilteringAndWebSocketStream(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	hub := NewHub(client, nil)
	pub := NewEventPublisher(client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hubReady := make(chan struct{})
	go func() {
		close(hubReady)
		_ = hub.Run(ctx)
	}()
	<-hubReady
	time.Sleep(50 * time.Millisecond)

	targetID := uuid.New()
	otherID := uuid.New()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hub.ServeWS(w, r, SubscriptionFilter{NotificationID: &targetID})
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("ws dial failed: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Publish one non-matching event and one matching event
	_ = pub.Publish(ctx, domain.StatusEvent{
		NotificationID: otherID,
		Channel:        domain.ChannelEmail,
		Status:         domain.StatusDelivered,
		Timestamp:      time.Now().UTC(),
	})
	_ = pub.Publish(ctx, domain.StatusEvent{
		NotificationID: targetID,
		Channel:        domain.ChannelSMS,
		Status:         domain.StatusDelivered,
		Timestamp:      time.Now().UTC(),
	})

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ws read failed: %v", err)
	}

	var got domain.StatusEvent
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal ws event: %v", err)
	}
	if got.NotificationID != targetID || got.Status != domain.StatusDelivered {
		t.Fatalf("unexpected ws event: %+v", got)
	}
}
