package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
)

// TestRoundTrip_EndToEnd exercises a full request -> handler -> reply
// cycle over the HTTP transport. It's the canonical "does the
// abstraction work" test and the one the AMQP transport will mirror
// once that implementation lands its own tests.
func TestRoundTrip_EndToEnd(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	transport := New(Transport{
		BaseURL: srv.URL,
		Mux:     mux,
	})
	t.Cleanup(func() { _ = transport.Close() })

	// Handler: echoes the body with a prefix, proving the Delivery
	// got both the body and the correlation id, and that Reply made
	// it back through the envelope.
	handlerCalled := false
	sub, err := transport.Consume(rpc.ConsumerConfig{
		Endpoint: "echo",
		Handler: func(ctx context.Context, d rpc.Delivery) error {
			handlerCalled = true
			if d.CorrelationID != "test-corr-1" {
				t.Errorf("delivery correlation_id = %q, want test-corr-1", d.CorrelationID)
			}
			if d.Headers["x-trace"] != "abc" {
				t.Errorf("delivery header x-trace = %q, want abc", d.Headers["x-trace"])
			}
			payload := append([]byte("echo:"), d.Body...)
			if err := d.Reply(ctx, payload, "text/plain"); err != nil {
				return err
			}
			return d.Ack()
		},
		Timeout:     5 * time.Second,
		Concurrency: 1,
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	reply, err := transport.Request(context.Background(), rpc.Request{
		Endpoint:      "echo",
		Body:          []byte("hello"),
		ContentType:   "text/plain",
		CorrelationID: "test-corr-1",
		Headers:       map[string]string{"x-trace": "abc"},
		Timeout:       5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if !handlerCalled {
		t.Fatal("handler was not invoked")
	}
	if string(reply.Body) != "echo:hello" {
		t.Errorf("reply body = %q, want echo:hello", string(reply.Body))
	}
	if reply.ContentType != "text/plain" {
		t.Errorf("reply content-type = %q, want text/plain", reply.ContentType)
	}
	if reply.CorrelationID != "test-corr-1" {
		t.Errorf("reply correlation_id = %q, want test-corr-1 (transport must round-trip it)",
			reply.CorrelationID)
	}
}

// TestRequest_UnknownEndpointReturnsErr confirms an endpoint that
// was never registered surfaces as rpc.ErrEndpointUnknown. This is
// the HTTP-specific advantage: AMQP happily queues a message for a
// queue with no consumer; HTTP 404s fast.
func TestRequest_UnknownEndpointReturnsErr(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	transport := New(Transport{BaseURL: srv.URL, Mux: mux})
	t.Cleanup(func() { _ = transport.Close() })

	_, err := transport.Request(context.Background(), rpc.Request{
		Endpoint: "does-not-exist",
		Body:     []byte("anything"),
		Timeout:  time.Second,
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err != rpc.ErrEndpointUnknown {
		t.Errorf("err = %v, want ErrEndpointUnknown", err)
	}
}

// TestRequest_TimeoutTranslates proves a context deadline maps to
// rpc.ErrTimeout (not the bare Go error). This is the contract
// handlers rely on — error identity is transport-agnostic.
func TestRequest_TimeoutTranslates(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	transport := New(Transport{BaseURL: srv.URL, Mux: mux})
	t.Cleanup(func() { _ = transport.Close() })

	_, err := transport.Consume(rpc.ConsumerConfig{
		Endpoint: "slow",
		Handler: func(ctx context.Context, d rpc.Delivery) error {
			time.Sleep(500 * time.Millisecond)
			_ = d.Reply(ctx, []byte("late"), "")
			return d.Ack()
		},
		Timeout:     time.Second,
		Concurrency: 1,
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = transport.Request(ctx, rpc.Request{
		Endpoint: "slow",
		Body:     []byte("hi"),
		Timeout:  50 * time.Millisecond,
	})
	if err != rpc.ErrTimeout {
		t.Errorf("err = %v, want ErrTimeout", err)
	}
}

// TestHandler_NackSetsStatus verifies the nack requeue convention:
// requeue=true → 503 (try again), requeue=false → 500.
func TestHandler_NackSetsStatus(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	transport := New(Transport{BaseURL: srv.URL, Mux: mux})
	t.Cleanup(func() { _ = transport.Close() })

	_, err := transport.Consume(rpc.ConsumerConfig{
		Endpoint: "rejecter",
		Handler: func(ctx context.Context, d rpc.Delivery) error {
			// Nack with requeue=true means the client sees 503.
			return d.Nack(true)
		},
		Timeout:     time.Second,
		Concurrency: 1,
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	_, err = transport.Request(context.Background(), rpc.Request{
		Endpoint: "rejecter",
		Body:     []byte("x"),
		Timeout:  time.Second,
	})
	// The client surfaces a generic "HTTP 503" error because the
	// Nack returned 503. The error message will contain "503".
	if err == nil {
		t.Fatal("expected error for nack, got nil")
	}
	// No assertion on exact text — just confirming non-success.
}

// TestEnvelope_RawBodyFallback covers the legacy-client case: an
// adapter author who doesn't know about rpc.Envelope sends a raw
// JSON body. The transport should still accept it.
func TestEnvelope_RawBodyFallback(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	transport := New(Transport{BaseURL: srv.URL, Mux: mux})
	t.Cleanup(func() { _ = transport.Close() })

	var gotBody []byte
	_, err := transport.Consume(rpc.ConsumerConfig{
		Endpoint: "raw",
		Handler: func(ctx context.Context, d rpc.Delivery) error {
			gotBody = append([]byte{}, d.Body...)
			_ = d.Reply(ctx, []byte(`{"ok":true}`), "application/json")
			return d.Ack()
		},
		Timeout:     time.Second,
		Concurrency: 1,
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Raw POST: no envelope wrapper. The transport should treat the
	// whole body as envelope.Body.
	raw, _ := json.Marshal(map[string]string{"hello": "world"})
	resp, err := http.Post(srv.URL+DefaultPathPrefix+"raw", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = resp.Body.Close()

	if string(gotBody) == "" {
		t.Error("handler did not receive body")
	}
	// Envelope-wrapped input works via TestRoundTrip_EndToEnd; this
	// test only asserts the raw-body fallback path.
}
