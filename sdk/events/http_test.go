package events_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sdk/events"
)

// captureRequest collects the most recent payload + auth header the
// mock yggdrasil-core saw, so tests can assert on wire shape.
type captureRequest struct {
	body  []byte
	auth  string
	path  string
	calls int32
}

func newCaptureServer(t *testing.T, status int) (*httptest.Server, *captureRequest) {
	t.Helper()
	cap := &captureRequest{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&cap.calls, 1)
		body, _ := io.ReadAll(r.Body)
		cap.body = body
		cap.auth = r.Header.Get("Authorization")
		cap.path = r.URL.Path
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, cap
}

func sampleEvent() events.MutationEvent {
	return events.MutationEvent{
		EventType:   "stripe.customer.ensured",
		Provider:    "stripe",
		Resource:    "customer",
		Verb:        events.VerbEnsured,
		ResourceID:  "cus_1234abc",
		InstanceID:  "stripe-acme",
		Idempotency: "ensure_customer_acme_abc",
		Observed:    json.RawMessage(`{"id":"cus_1234abc"}`),
	}
}

func TestHTTPEmitter_Emit_HappyPath(t *testing.T) {
	srv, cap := newCaptureServer(t, http.StatusAccepted)

	em := events.NewHTTPEmitter(
		events.WithCoreURL(srv.URL),
		events.WithToken("test-token"),
	)
	if err := em.Emit(context.Background(), sampleEvent()); err != nil {
		t.Fatalf("Emit returned err: %v", err)
	}

	if got := atomic.LoadInt32(&cap.calls); got != 1 {
		t.Fatalf("expected 1 HTTP call, got %d", got)
	}
	if cap.path != "/api/v1/events" {
		t.Fatalf("expected path /api/v1/events, got %s", cap.path)
	}
	if cap.auth != "Bearer test-token" {
		t.Fatalf("expected Authorization=Bearer test-token, got %q", cap.auth)
	}
	if !strings.Contains(string(cap.body), `"event_type":"stripe.customer.ensured"`) {
		t.Fatalf("expected payload to contain event_type; got %s", cap.body)
	}
	// emitted_at must be filled in by the emitter even though sampleEvent
	// leaves it zero.
	var decoded events.MutationEvent
	if err := json.Unmarshal(cap.body, &decoded); err != nil {
		t.Fatalf("decode posted body: %v", err)
	}
	if decoded.EmittedAt.IsZero() {
		t.Fatal("expected emitter to fill EmittedAt when zero")
	}
}

func TestHTTPEmitter_Emit_RetriesTransient5xx(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(srv.Close)

	em := events.NewHTTPEmitter(
		events.WithCoreURL(srv.URL),
		events.WithToken("t"),
		events.WithMaxRetries(4),
		events.WithRetryBackoff(1*time.Millisecond),
	)
	if err := em.Emit(context.Background(), sampleEvent()); err != nil {
		t.Fatalf("Emit returned err after retries: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("expected 3 HTTP calls (2 transient + 1 success), got %d", got)
	}
}

func TestHTTPEmitter_Emit_TerminalOn4xx(t *testing.T) {
	srv, cap := newCaptureServer(t, http.StatusBadRequest)

	em := events.NewHTTPEmitter(
		events.WithCoreURL(srv.URL),
		events.WithToken("t"),
		events.WithMaxRetries(5),
		events.WithRetryBackoff(1*time.Millisecond),
	)
	err := em.Emit(context.Background(), sampleEvent())
	if err == nil {
		t.Fatal("expected terminal 4xx error, got nil")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Fatalf("expected error mentioning 400, got %v", err)
	}
	if got := atomic.LoadInt32(&cap.calls); got != 1 {
		t.Fatalf("expected exactly 1 HTTP call on terminal 4xx, got %d", got)
	}
}

func TestHTTPEmitter_Emit_TerminalOnAuth401(t *testing.T) {
	srv, cap := newCaptureServer(t, http.StatusUnauthorized)

	em := events.NewHTTPEmitter(
		events.WithCoreURL(srv.URL),
		events.WithToken("bad-token"),
		events.WithMaxRetries(5),
		events.WithRetryBackoff(1*time.Millisecond),
	)
	err := em.Emit(context.Background(), sampleEvent())
	if err == nil {
		t.Fatal("expected 401 to be terminal")
	}
	if got := atomic.LoadInt32(&cap.calls); got != 1 {
		t.Fatalf("expected exactly 1 HTTP call on 401, got %d", got)
	}
}

func TestHTTPEmitter_Emit_ExhaustsRetriesOnPersistent5xx(t *testing.T) {
	srv, cap := newCaptureServer(t, http.StatusBadGateway)

	em := events.NewHTTPEmitter(
		events.WithCoreURL(srv.URL),
		events.WithToken("t"),
		events.WithMaxRetries(3),
		events.WithRetryBackoff(1*time.Millisecond),
	)
	err := em.Emit(context.Background(), sampleEvent())
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	// 1 initial + 2 retries = 3 total when MaxRetries=3 (interpreted as
	// total attempts).
	if got := atomic.LoadInt32(&cap.calls); got != 3 {
		t.Fatalf("expected 3 HTTP calls total, got %d", got)
	}
}

func TestHTTPEmitter_Emit_HonorsCustomHTTPClient(t *testing.T) {
	srv, _ := newCaptureServer(t, http.StatusAccepted)

	called := false
	rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		called = true
		return http.DefaultTransport.RoundTrip(req)
	})
	client := &http.Client{Transport: rt}

	em := events.NewHTTPEmitter(
		events.WithCoreURL(srv.URL),
		events.WithToken("t"),
		events.WithHTTPClient(client),
	)
	if err := em.Emit(context.Background(), sampleEvent()); err != nil {
		t.Fatalf("Emit returned err: %v", err)
	}
	if !called {
		t.Fatal("expected custom HTTP client to be used")
	}
}

func TestHTTPEmitter_Emit_RespectsContextCancellation(t *testing.T) {
	// Server that always 500s slowly.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(50 * time.Millisecond):
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	em := events.NewHTTPEmitter(
		events.WithCoreURL(srv.URL),
		events.WithToken("t"),
		events.WithMaxRetries(100),
		events.WithRetryBackoff(10*time.Millisecond),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := em.Emit(ctx, sampleEvent())
	if err == nil {
		t.Fatal("expected error when context is cancelled mid-retry")
	}
}

func TestNewHTTPEmitter_DefaultsFromEnv(t *testing.T) {
	srv, cap := newCaptureServer(t, http.StatusAccepted)

	t.Setenv("YGGDRASIL_CORE_URL", srv.URL)
	t.Setenv("YGGDRASIL_RUN_TOKEN", "env-token")

	em := events.NewHTTPEmitter()
	if err := em.Emit(context.Background(), sampleEvent()); err != nil {
		t.Fatalf("Emit returned err: %v", err)
	}
	if cap.auth != "Bearer env-token" {
		t.Fatalf("expected env-token bearer, got %q", cap.auth)
	}
	if atomic.LoadInt32(&cap.calls) != 1 {
		t.Fatal("expected 1 call against env-configured URL")
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
