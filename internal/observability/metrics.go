package observability

import (
	"math"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const latencyWindowSize = 1000

// QueueDepthSnapshot holds real-time Redis queue depths for a single channel.
type QueueDepthSnapshot struct {
	High   int64 `json:"high"`
	Normal int64 `json:"normal"`
	Low    int64 `json:"low"`
	Retry  int64 `json:"retry"`
	DLQ    int64 `json:"dlq"`
	Total  int64 `json:"total"`
}

// ChannelDeliverySnapshot holds delivery counters and latency stats for one channel.
type ChannelDeliverySnapshot struct {
	Created            int64   `json:"created"`
	Delivered          int64   `json:"delivered"`
	Failed             int64   `json:"failed"`
	Retried            int64   `json:"retried"`
	Cancelled          int64   `json:"cancelled"`
	SuccessRatePercent float64 `json:"success_rate_percent"`
	FailureRatePercent float64 `json:"failure_rate_percent"`
	AvgLatencyMs       float64 `json:"avg_latency_ms"`
	P95LatencyMs       float64 `json:"p95_latency_ms"`
}

// DeliverySnapshot aggregates global and per-channel delivery statistics.
type DeliverySnapshot struct {
	TotalCreated       int64                              `json:"total_created"`
	TotalDelivered     int64                              `json:"total_delivered"`
	TotalFailed        int64                              `json:"total_failed"`
	TotalRetried       int64                              `json:"total_retried"`
	TotalCancelled     int64                              `json:"total_cancelled"`
	SuccessRatePercent float64                            `json:"success_rate_percent"`
	FailureRatePercent float64                            `json:"failure_rate_percent"`
	AvgLatencyMs       float64                            `json:"avg_latency_ms"`
	P95LatencyMs       float64                            `json:"p95_latency_ms"`
	ByChannel          map[string]ChannelDeliverySnapshot `json:"by_channel"`
}

// MetricsSnapshot is returned by the real-time JSON metrics endpoint (/api/v1/metrics).
type MetricsSnapshot struct {
	Timestamp       time.Time                     `json:"timestamp"`
	UptimeSeconds   float64                       `json:"uptime_seconds"`
	Queues          map[string]QueueDepthSnapshot `json:"queues"`
	Delivery        DeliverySnapshot              `json:"delivery"`
	CircuitBreakers map[string]string             `json:"circuit_breakers"`
}

type latencyRing struct {
	mu      sync.Mutex
	samples []float64
	pos     int
	full    bool
}

func newLatencyRing(size int) *latencyRing {
	return &latencyRing{samples: make([]float64, size)}
}

func (r *latencyRing) add(ms float64) {
	r.mu.Lock()
	r.samples[r.pos] = ms
	r.pos++
	if r.pos >= len(r.samples) {
		r.pos = 0
		r.full = true
	}
	r.mu.Unlock()
}

func (r *latencyRing) stats() (avg, p95 float64) {
	r.mu.Lock()
	n := r.pos
	if r.full {
		n = len(r.samples)
	}
	if n == 0 {
		r.mu.Unlock()
		return 0, 0
	}
	cp := make([]float64, n)
	copy(cp, r.samples[:n])
	r.mu.Unlock()

	var sum float64
	for _, v := range cp {
		sum += v
	}
	sort.Float64s(cp)
	idx := int(math.Ceil(0.95*float64(n))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return round2(sum / float64(n)), round2(cp[idx])
}

type channelCounters struct {
	created   atomic.Int64
	delivered atomic.Int64
	failed    atomic.Int64
	retried   atomic.Int64
	cancelled atomic.Int64
	latencies *latencyRing
}

// Metrics wraps both Prometheus instrumentation and a fast in-memory snapshot aggregator.
type Metrics struct {
	startTime time.Time
	registry  *prometheus.Registry

	createdTotal     *prometheus.CounterVec
	deliveredTotal   *prometheus.CounterVec
	failedTotal      *prometheus.CounterVec
	retriedTotal     *prometheus.CounterVec
	cancelledTotal   *prometheus.CounterVec
	deliveryDuration *prometheus.HistogramVec
	queueDepth       *prometheus.GaugeVec
	cbState          *prometheus.GaugeVec
	httpDuration     *prometheus.HistogramVec
	httpRequests     *prometheus.CounterVec

	mu        sync.RWMutex
	channels  map[string]*channelCounters
	cbStates  map[string]string
	globalLat *latencyRing
}

// NewMetrics initializes an isolated Prometheus registry and real-time metrics trackers.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		startTime: time.Now(),
		registry:  reg,
		createdTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_created_total",
			Help: "Total notifications created.",
		}, []string{"channel", "priority"}),
		deliveredTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_delivered_total",
			Help: "Total notifications successfully delivered.",
		}, []string{"channel"}),
		failedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_failed_total",
			Help: "Total notifications permanently failed.",
		}, []string{"channel"}),
		retriedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_retried_total",
			Help: "Total notification delivery retries scheduled.",
		}, []string{"channel"}),
		cancelledTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_cancelled_total",
			Help: "Total notifications cancelled.",
		}, []string{"channel"}),
		deliveryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "notification_delivery_duration_seconds",
			Help:    "Duration of external provider delivery requests.",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"channel", "outcome"}),
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "notification_queue_depth",
			Help: "Current number of notifications waiting in queues.",
		}, []string{"channel", "queue"}),
		cbState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "notification_circuit_breaker_state",
			Help: "Circuit breaker state (0=closed, 1=half-open, 2=open).",
		}, []string{"channel"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route", "status"}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total HTTP requests served.",
		}, []string{"method", "route", "status"}),
		channels:  make(map[string]*channelCounters),
		cbStates:  map[string]string{"sms": "closed", "email": "closed", "push": "closed"},
		globalLat: newLatencyRing(latencyWindowSize),
	}

	for _, ch := range []string{"sms", "email", "push"} {
		m.channels[ch] = &channelCounters{latencies: newLatencyRing(latencyWindowSize)}
	}

	reg.MustRegister(
		m.createdTotal,
		m.deliveredTotal,
		m.failedTotal,
		m.retriedTotal,
		m.cancelledTotal,
		m.deliveryDuration,
		m.queueDepth,
		m.cbState,
		m.httpDuration,
		m.httpRequests,
	)

	return m
}

// Handler returns the Prometheus HTTP scrape handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) getChannel(channel string) *channelCounters {
	m.mu.RLock()
	cc, ok := m.channels[channel]
	m.mu.RUnlock()
	if ok {
		return cc
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if cc, ok = m.channels[channel]; !ok {
		cc = &channelCounters{latencies: newLatencyRing(latencyWindowSize)}
		m.channels[channel] = cc
	}
	return cc
}

func (m *Metrics) IncCreated(channel, priority string) {
	m.createdTotal.WithLabelValues(channel, priority).Inc()
	m.getChannel(channel).created.Add(1)
}

func (m *Metrics) RecordDelivered(channel string, d time.Duration) {
	m.deliveredTotal.WithLabelValues(channel).Inc()
	m.deliveryDuration.WithLabelValues(channel, "delivered").Observe(d.Seconds())
	cc := m.getChannel(channel)
	cc.delivered.Add(1)
	ms := float64(d.Microseconds()) / 1000.0
	cc.latencies.add(ms)
	m.globalLat.add(ms)
}

func (m *Metrics) RecordFailed(channel string, d time.Duration) {
	m.failedTotal.WithLabelValues(channel).Inc()
	m.deliveryDuration.WithLabelValues(channel, "failed").Observe(d.Seconds())
	cc := m.getChannel(channel)
	cc.failed.Add(1)
	if d > 0 {
		ms := float64(d.Microseconds()) / 1000.0
		cc.latencies.add(ms)
		m.globalLat.add(ms)
	}
}

func (m *Metrics) RecordRetried(channel string, d time.Duration) {
	m.retriedTotal.WithLabelValues(channel).Inc()
	if d > 0 {
		m.deliveryDuration.WithLabelValues(channel, "retried").Observe(d.Seconds())
	}
	m.getChannel(channel).retried.Add(1)
}

func (m *Metrics) IncCancelled(channel string) {
	m.cancelledTotal.WithLabelValues(channel).Inc()
	m.getChannel(channel).cancelled.Add(1)
}

func (m *Metrics) SetQueueDepths(depths map[string]QueueDepthSnapshot) {
	for ch, d := range depths {
		m.queueDepth.WithLabelValues(ch, "high").Set(float64(d.High))
		m.queueDepth.WithLabelValues(ch, "normal").Set(float64(d.Normal))
		m.queueDepth.WithLabelValues(ch, "low").Set(float64(d.Low))
		m.queueDepth.WithLabelValues(ch, "retry").Set(float64(d.Retry))
		m.queueDepth.WithLabelValues(ch, "dlq").Set(float64(d.DLQ))
	}
}

func (m *Metrics) SetCircuitBreakerState(channel, state string) {
	var val float64
	switch state {
	case "half-open":
		val = 1
	case "open":
		val = 2
	default:
		val = 0
	}
	m.cbState.WithLabelValues(channel).Set(val)
	m.mu.Lock()
	m.cbStates[channel] = state
	m.mu.Unlock()
}

func (m *Metrics) RecordHTTPRequest(method, route string, status int, d time.Duration) {
	code := strconv.Itoa(status)
	m.httpRequests.WithLabelValues(method, route, code).Inc()
	m.httpDuration.WithLabelValues(method, route, code).Observe(d.Seconds())
}

// Snapshot builds a real-time JSON-serializable view of queue depths, rates, and latency.
func (m *Metrics) Snapshot(queues map[string]QueueDepthSnapshot) MetricsSnapshot {
	m.mu.RLock()
	byChannel := make(map[string]ChannelDeliverySnapshot, len(m.channels))
	var totalCreated, totalDelivered, totalFailed, totalRetried, totalCancelled int64

	for ch, cc := range m.channels {
		created := cc.created.Load()
		delivered := cc.delivered.Load()
		failed := cc.failed.Load()
		retried := cc.retried.Load()
		cancelled := cc.cancelled.Load()

		totalCreated += created
		totalDelivered += delivered
		totalFailed += failed
		totalRetried += retried
		totalCancelled += cancelled

		avg, p95 := cc.latencies.stats()
		succRate, failRate := calcRates(delivered, failed)
		byChannel[ch] = ChannelDeliverySnapshot{
			Created:            created,
			Delivered:          delivered,
			Failed:             failed,
			Retried:            retried,
			Cancelled:          cancelled,
			SuccessRatePercent: succRate,
			FailureRatePercent: failRate,
			AvgLatencyMs:       avg,
			P95LatencyMs:       p95,
		}
	}

	cbs := make(map[string]string, len(m.cbStates))
	for k, v := range m.cbStates {
		cbs[k] = v
	}
	m.mu.RUnlock()

	if queues == nil {
		queues = map[string]QueueDepthSnapshot{}
	} else {
		m.SetQueueDepths(queues)
	}

	gAvg, gP95 := m.globalLat.stats()
	gSucc, gFail := calcRates(totalDelivered, totalFailed)

	return MetricsSnapshot{
		Timestamp:     time.Now().UTC(),
		UptimeSeconds: round2(time.Since(m.startTime).Seconds()),
		Queues:        queues,
		Delivery: DeliverySnapshot{
			TotalCreated:       totalCreated,
			TotalDelivered:     totalDelivered,
			TotalFailed:        totalFailed,
			TotalRetried:       totalRetried,
			TotalCancelled:     totalCancelled,
			SuccessRatePercent: gSucc,
			FailureRatePercent: gFail,
			AvgLatencyMs:       gAvg,
			P95LatencyMs:       gP95,
			ByChannel:          byChannel,
		},
		CircuitBreakers: cbs,
	}
}

func calcRates(delivered, failed int64) (successRate, failureRate float64) {
	completed := delivered + failed
	if completed == 0 {
		return 0, 0
	}
	successRate = round2(float64(delivered) / float64(completed) * 100)
	failureRate = round2(float64(failed) / float64(completed) * 100)
	return successRate, failureRate
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}
