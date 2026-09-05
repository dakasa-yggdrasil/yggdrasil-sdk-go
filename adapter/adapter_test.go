package adapter_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/adapter"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
	sdkhttp "github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc/http"
)

// TestAdapter_RunRejectsMissingTransport verifies the SDK surfaces a
// clear error when an adapter is built without a transport. The
// alternative — a silent hang, or a nil deref — is what plugin
// authors debug for hours; making it a plain error is load-bearing UX.
func TestAdapter_RunRejectsMissingTransport(t *testing.T) {
	a := adapter.New(adapter.Config{Provider: "test"}).
		Register("execute", func(ctx context.Context, d rpc.Delivery) ([]byte, string, error) {
			return nil, "", nil
		})

	err := a.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "no transport") {
		t.Fatalf("expected missing-transport error, got %v", err)
	}
}

// TestAdapter_RunRejectsNoHandlers guards that an adapter with a
// transport but no Register calls fails loud. Silent boot of a
// zero-handler adapter is almost always a bug.
func TestAdapter_RunRejectsNoHandlers(t *testing.T) {
	mux := http.NewServeMux()
	t0 := sdkhttp.New(&sdkhttp.Transport{Mux: mux})
	a := adapter.New(adapter.Config{Provider: "test"}).Transport(t0)

	err := a.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "no handlers") {
		t.Fatalf("expected missing-handlers error, got %v", err)
	}
}

// TestAdapter_RunReturnsTerminalSubscriptionError ensures a consumer that
// becomes permanently invalid after startup tears down the adapter runtime.
// Without this propagation an AMQP queue deleted or recreated with the wrong
// type would leave the process healthy-looking with zero consumers.
func TestAdapter_RunReturnsTerminalSubscriptionError(t *testing.T) {
	cause := errors.New("fixed queue topology mismatch")
	terminalErrors := make(chan error, 1)
	fake := &fakeTransport{
		handlers:       map[string]rpc.Handler{},
		done:           make(chan struct{}),
		terminalErrors: terminalErrors,
	}
	a := adapter.New(adapter.Config{Provider: "test"}).
		Register("execute", func(context.Context, rpc.Delivery) ([]byte, string, error) {
			return nil, "", nil
		}).
		Transport(fake)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		fake.mu.Lock()
		registered := fake.handlers["execute"] != nil
		fake.mu.Unlock()
		if registered {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	terminalErrors <- cause

	select {
	case err := <-runErr:
		if !errors.Is(err, cause) {
			t.Fatalf("Run error = %v, want wrapped cause %v", err, cause)
		}
		if !strings.Contains(err.Error(), "terminal consumer failure") {
			t.Fatalf("Run error = %q, want terminal consumer context", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after terminal subscription error")
	}
}

// TestAdapter_HTTPRoundTrip wires an adapter to a shared mux (client
// and server on the same mux), dispatches a Request, and asserts the
// handler's output comes back. This is the SDK's canonical happy path
// and exercises wrap/envelope/Ack/Reply in one pass.
func TestAdapter_HTTPRoundTrip(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	transport := sdkhttp.New(&sdkhttp.Transport{Mux: mux, BaseURL: srv.URL})

	a := adapter.New(adapter.Config{Provider: "test"}).
		Register("execute", func(ctx context.Context, d rpc.Delivery) ([]byte, string, error) {
			// Echo the input with an exclamation; tiny so the
			// assertion is obvious.
			return append([]byte("ack:"), d.Body...), "application/json", nil
		}).
		Transport(transport)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	// Wait until Consume has registered (at most ~50ms in practice).
	// A tight busy-loop that stops on the first success is simplest;
	// avoids sleep tuning.
	deadline := time.Now().Add(2 * time.Second)
	var reply rpc.Reply
	var err error
	for time.Now().Before(deadline) {
		reply, err = transport.Request(context.Background(), rpc.Request{
			Endpoint: "execute",
			Body:     []byte("hello"),
			Timeout:  500 * time.Millisecond,
		})
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Request never succeeded: %v", err)
	}
	if got := string(reply.Body); got != "ack:hello" {
		t.Errorf("reply body = %q, want %q", got, "ack:hello")
	}

	cancel()
	<-runErr
}

// TestAdapter_HandlerErrorNacksWithEnvelope asserts that a handler
// returning a non-nil error makes the SDK (a) reply with a JSON
// {"error": ...} envelope and (b) Nack the delivery. Plugin authors
// rely on this so their handlers can just `return err` without
// threading Ack/Nack themselves.
func TestAdapter_HandlerErrorNacksWithEnvelope(t *testing.T) {
	// We drive the handler directly through a synthetic Delivery so
	// the test does not need a live transport; the SDK's wrap()
	// contract is what matters here.
	var (
		replyBody []byte
		replyCT   string
		acks      int
		nacks     int
		mu        sync.Mutex
	)
	d := rpc.Delivery{
		Endpoint: "execute",
		Body:     []byte(`{"bad":"input"}`),
		AckFn:    func() error { mu.Lock(); defer mu.Unlock(); acks++; return nil },
		NackFn:   func(requeue bool) error { mu.Lock(); defer mu.Unlock(); nacks++; return nil },
		ReplyFn: func(_ context.Context, body []byte, ct string) error {
			mu.Lock()
			defer mu.Unlock()
			replyBody = body
			replyCT = ct
			return nil
		},
	}

	a := adapter.New(adapter.Config{Provider: "test"}).
		Register("execute", func(ctx context.Context, d rpc.Delivery) ([]byte, string, error) {
			return nil, "", errors.New("synthetic boom")
		})
	// Exercise the wrap path via an injected fake transport that
	// just hands us back the rpc.Handler the SDK installed.
	handler := captureHandler(t, a)
	if err := handler(context.Background(), d); err != nil {
		t.Fatalf("wrapped handler error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if acks != 0 {
		t.Errorf("acks = %d, want 0 on error", acks)
	}
	if nacks != 1 {
		t.Errorf("nacks = %d, want 1", nacks)
	}
	if !strings.Contains(string(replyBody), "synthetic boom") {
		t.Errorf("reply body = %q, want error envelope containing cause", replyBody)
	}
	if replyCT != "application/json" {
		t.Errorf("reply content-type = %q, want application/json", replyCT)
	}
}

// captureHandler drives the adapter's Run long enough to let it
// Consume one registered handler on a fake transport, captures the
// installed rpc.Handler, and tears down. It's a bit ceremonious for
// what it tests (just the wrap shape) but keeps the
// HandlerErrorNacksWithEnvelope test DB-free and isolated.
func captureHandler(t *testing.T, a *adapter.Adapter) rpc.Handler {
	t.Helper()
	fake := &fakeTransport{handlers: map[string]rpc.Handler{}, done: make(chan struct{})}
	a.Transport(fake)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(ctx) }()

	// Wait until the fake transport records a Consume.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		fake.mu.Lock()
		h := fake.handlers["execute"]
		fake.mu.Unlock()
		if h != nil {
			cancel()
			<-runErr
			return h
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-runErr
	t.Fatal("adapter did not register a Consume for 'execute' within 1s")
	return nil
}

type fakeTransport struct {
	mu             sync.Mutex
	handlers       map[string]rpc.Handler
	done           chan struct{}
	terminalErrors chan error
}

func (f *fakeTransport) Consume(cfg rpc.ConsumerConfig) (rpc.Subscription, error) {
	f.mu.Lock()
	f.handlers[cfg.Endpoint] = cfg.Handler
	f.mu.Unlock()
	if f.terminalErrors != nil {
		return &fakeTerminalErrorSubscription{
			endpoint:       cfg.Endpoint,
			terminalErrors: f.terminalErrors,
		}, nil
	}
	return &fakeSubscription{endpoint: cfg.Endpoint}, nil
}

func (f *fakeTransport) Request(context.Context, rpc.Request) (rpc.Reply, error) {
	return rpc.Reply{}, errors.New("fakeTransport does not support Request")
}
func (f *fakeTransport) Publish(context.Context, rpc.Request) error {
	return errors.New("fakeTransport does not support Publish")
}
func (f *fakeTransport) Close() error { close(f.done); return nil }

type fakeSubscription struct{ endpoint string }

func (s *fakeSubscription) Endpoint() string { return s.endpoint }
func (s *fakeSubscription) Close() error     { return nil }

type fakeTerminalErrorSubscription struct {
	endpoint       string
	terminalErrors <-chan error
}

func (s *fakeTerminalErrorSubscription) Endpoint() string { return s.endpoint }
func (s *fakeTerminalErrorSubscription) Close() error     { return nil }
func (s *fakeTerminalErrorSubscription) TerminalErrors() <-chan error {
	return s.terminalErrors
}
