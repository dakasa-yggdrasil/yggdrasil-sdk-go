// Package adapter provides high-level helpers for building yggdrasil
// integration adapters without reimplementing RPC plumbing. An adapter
// typically:
//
//  1. constructs an Adapter (Config with name, version, capabilities),
//  2. registers Handlers for each capability (`describe`, `execute`,
//     optional `health`, etc.) via Register,
//  3. picks a transport — ListenHTTP, ListenAMQP, or passes a custom
//     rpc.Transport via Transport,
//  4. calls Run (blocks until ctx is cancelled).
//
// Conceptually:
//
//	adapter.New(Config{...}).
//	    Register("execute", executeHandler).
//	    Register("describe", describeHandler).
//	    ListenHTTP(":8080").
//	    Run(ctx)
//
// This isolates plugin authors from: envelope framing, graceful
// shutdown, signal handling, error normalization, and the choice of
// transport backend.
package adapter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
)

// Config carries identity + runtime knobs used by every adapter. An
// adapter's main() populates this once at startup.
type Config struct {
	// Provider is the integration_type FAMILY identifier (e.g. "rabbitmq",
	// "grafana"). Must match the integration_type manifest's spec.provider
	// field — yggdrasil-core sends this in the describe handshake and the
	// adapter's describe handler typically validates against it. NOT used
	// for queue/path naming.
	Provider string

	// IntegrationType is the integration_type ID (e.g. "rabbitmq-topology",
	// "grafana"). When non-empty, the AMQP transport prefixes consumer
	// queue names with "yggdrasil.adapter.<integration_type>." to match
	// what yggdrasil-core publishes to per integration_type.adapter.queues.
	// Single-provider integrations where IntegrationType == Provider can
	// leave this empty and the SDK falls back to Provider.
	IntegrationType string

	// Version identifies the adapter binary build. Surfaced on the
	// describe handshake so the core can detect drift.
	Version string

	// DefaultTimeout bounds every Handler invocation when the
	// per-Consume ConsumerConfig.Timeout is zero. 30s when unset.
	DefaultTimeout time.Duration

	// Concurrency caps the number of in-flight handler invocations
	// per endpoint. Defaults to 1 (serial). Tune higher when the
	// adapter's handlers are IO-bound.
	Concurrency int
}

// Handler processes one RPC delivery. Unlike rpc.Handler, adapter
// handlers return a response body + content type and the SDK handles
// Ack/Nack/Reply automatically — an error return Nacks + replies with
// a wire-level error envelope; a nil error Acks + replies with the
// returned body.
type Handler func(ctx context.Context, d rpc.Delivery) (body []byte, contentType string, err error)

// Adapter is the builder + runtime exposed to plugin authors.
type Adapter struct {
	config    Config
	transport rpc.Transport
	handlers  map[string]Handler
	mu        sync.Mutex

	// beforeRun is invoked by Run before Consume. Transport helpers
	// (ListenHTTP, ListenAMQP) use it to start servers / dial
	// brokers lazily so that construction stays side-effect-free.
	beforeRun func(context.Context) error
	// afterRun is deferred by Run for cleanup (server Shutdown,
	// connection Close). Set alongside beforeRun by the transport
	// helpers.
	afterRun func()
}

// New constructs a fresh Adapter. Call chain is:
//
//	New(cfg).Register(cap, handler).Listen*(...).Run(ctx)
//
// Any step may return the same Adapter to enable chaining; errors are
// surfaced lazily on Run.
func New(cfg Config) *Adapter {
	if cfg.DefaultTimeout == 0 {
		cfg.DefaultTimeout = 30 * time.Second
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	return &Adapter{
		config:   cfg,
		handlers: map[string]Handler{},
	}
}

// Register binds a Handler to one capability name (describe, execute,
// health, etc.). The endpoint string the transport receives is the
// capability; the transport-specific mapping (AMQP queue name, HTTP
// path) is done by the transport when Consume is called.
//
// Duplicate registrations overwrite; this is deliberate so that tests
// can swap a mock handler in.
func (a *Adapter) Register(capability string, handler Handler) *Adapter {
	a.mu.Lock()
	defer a.mu.Unlock()
	capability = strings.ToLower(strings.TrimSpace(capability))
	a.handlers[capability] = handler
	return a
}

// Transport sets a custom rpc.Transport. Mutually exclusive with
// ListenHTTP / ListenAMQP — the last call wins, so the usual pattern
// is to use exactly one of these three. Useful when the adapter needs
// a transport configuration the convenience helpers do not expose, or
// for tests that inject a fake transport.
func (a *Adapter) Transport(t rpc.Transport) *Adapter {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.transport = t
	return a
}

// Run wires the registered handlers to the configured transport and
// blocks until ctx is cancelled or SIGINT/SIGTERM arrives.
//
// Consume is called once per Register'd handler. Signal handling is
// opt-in via the exported WithSignalHandler; Run alone does not
// install SIGINT/SIGTERM traps, so callers can compose their own
// cancellation.
func (a *Adapter) Run(ctx context.Context) error {
	a.mu.Lock()
	handlers := make(map[string]Handler, len(a.handlers))
	for k, v := range a.handlers {
		handlers[k] = v
	}
	beforeRun := a.beforeRun
	a.mu.Unlock()

	if len(handlers) == 0 {
		return errors.New("adapter: no handlers registered — call Register before Run")
	}

	if beforeRun != nil {
		if err := beforeRun(ctx); err != nil {
			return err
		}
	}

	a.mu.Lock()
	transport := a.transport
	afterRun := a.afterRun
	a.mu.Unlock()

	if transport == nil {
		return errors.New("adapter: no transport configured — call ListenHTTP, ListenAMQP, or Transport before Run")
	}
	if afterRun != nil {
		defer afterRun()
	}

	subs := make([]rpc.Subscription, 0, len(handlers))
	defer func() {
		for _, sub := range subs {
			_ = sub.Close()
		}
	}()

	for capability, handler := range handlers {
		handler := handler
		cfg := rpc.ConsumerConfig{
			Endpoint:    capability,
			Handler:     a.wrap(handler),
			Timeout:     a.config.DefaultTimeout,
			Concurrency: a.config.Concurrency,
		}
		sub, err := transport.Consume(cfg)
		if err != nil {
			return fmt.Errorf("adapter: consume %q: %w", capability, err)
		}
		subs = append(subs, sub)
	}

	<-ctx.Done()
	return nil
}

// WithSignalHandler returns a context cancelled on SIGINT or SIGTERM.
// Opt-in helper for binaries that want the conventional behavior
// without reimplementing signal trap:
//
//	ctx := adapter.WithSignalHandler(context.Background())
//	return a.Run(ctx)
func WithSignalHandler(parent context.Context) context.Context {
	ctx, cancel := context.WithCancel(parent)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-sig:
			cancel()
		case <-ctx.Done():
		}
		signal.Stop(sig)
	}()
	return ctx
}

// wrap bridges adapter.Handler semantics (return body+contentType+err)
// to the lower-level rpc.Handler shape (return err; drive
// Ack/Nack/Reply via Delivery methods). This is where error
// normalization happens so plugin authors do not think about it.
func (a *Adapter) wrap(handler Handler) rpc.Handler {
	return func(ctx context.Context, d rpc.Delivery) error {
		body, contentType, err := handler(ctx, d)
		if err != nil {
			// Transport Reply is best-effort — the originator may
			// have given up. Log via the caller's logger; here we
			// only surface through Nack.
			_ = d.Reply(ctx, errorEnvelope(err), "application/json")
			return d.Nack(false)
		}
		if replyErr := d.Reply(ctx, body, contentType); replyErr != nil {
			return d.Nack(true) // transient: the originator may still be waiting.
		}
		return d.Ack()
	}
}

// errorEnvelope produces the JSON body the SDK replies with when a
// handler returns a non-nil error. The shape is intentionally
// minimal — plugin-specific error codes belong in the business-level
// response, not here.
func errorEnvelope(err error) []byte {
	msg := strings.ReplaceAll(err.Error(), `"`, `\"`)
	return []byte(`{"error":"` + msg + `"}`)
}
