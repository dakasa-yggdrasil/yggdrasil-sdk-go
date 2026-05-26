package adapter

import (
	"context"
	"errors"
	"fmt"
	nethttp "net/http"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	sdkamqp "github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc/amqp"
	sdkhttp "github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc/http"
)

// ListenHTTP configures the adapter to serve its registered handlers
// over HTTP on addr. Handlers register at paths relative to the
// HTTP transport's DefaultPathPrefix ("/rpc/<capability>"). The HTTP
// server is owned by the adapter — Run starts it and shuts it down on
// ctx cancellation.
//
// Typical usage:
//
//	a.Register("execute", h).ListenHTTP(":8080")
//
// Kubernetes Services / Deployments point their `integration_type`
// manifest at `/rpc/execute`, `/rpc/describe`, etc.
func (a *Adapter) ListenHTTP(addr string) *Adapter {
	mux := nethttp.NewServeMux()
	server := &nethttp.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	a.transport = sdkhttp.New(&sdkhttp.Transport{Mux: mux})

	a.beforeRun = func(_ context.Context) error {
		errCh := make(chan error, 1)
		go func() {
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, nethttp.ErrServerClosed) {
				errCh <- err
			}
			close(errCh)
		}()
		// Give ListenAndServe a tick to surface bind errors before we
		// hand control to Consume. A tiny sleep is the simplest way
		// to catch "address already in use" early; if the process is
		// still up after a few ms, the listen succeeded.
		select {
		case err := <-errCh:
			return fmt.Errorf("adapter: http listen on %q: %w", addr, err)
		case <-time.After(25 * time.Millisecond):
			return nil
		}
	}
	a.afterRun = func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}
	return a
}

// ListenAMQP configures the adapter to consume from an AMQP broker
// reachable at url. The dial is deferred until Run so adapter
// construction does not require a live broker (important for unit
// tests and for binaries that retry on startup).
//
// The dial loop retries on connection-refused / network errors with
// exponential backoff (capped at 30s, up to 30 attempts ≈ 5 min) so
// adapter pods that boot before RabbitMQ is ready (a common race in
// Kubernetes when both come up at the same time, or when the broker
// is mid-restart) do not flap between CrashLoopBackOff and Running
// while the cluster catches up. After the budget is exhausted the
// error is returned so the pod fails loud — that is the right signal
// for an alert, not silent degradation.
//
// Once the initial dial succeeds, the transport is built with
// NewWithDial so subsequent broker-side disconnects (rabbit pod
// restart, network blip, idle close) are recovered automatically: the
// transport's watchdog re-dials the connection in the background and
// every subscription's channel-reconnect loop picks up the new
// connection on its next setupConsumer retry. Without that, an adapter
// would stay Running 1/1 after a rabbit restart with zero AMQP
// consumers — the failure mode this SDK was bumped to v0.3.0 to fix.
//
// The AMQP transport prefixes consumer queue names with
// "yggdrasil.adapter.<integration_type>." to match what
// yggdrasil-core publishes to per integration_type.adapter.queues.
// Prefers Config.IntegrationType (which can differ from Provider when
// a family hosts multiple integration types — e.g. provider="rabbitmq"
// with integration types "rabbitmq-topology" / "rabbitmq-runtime"),
// falling back to Config.Provider for single-type adapters where the
// two are equal. Without a prefix the adapter would consume from a
// bare queue name nobody publishes to.
func (a *Adapter) ListenAMQP(url string) *Adapter {
	a.beforeRun = func(ctx context.Context) error {
		// Initial dial: blocks with the full retry budget so the pod
		// only reports Ready after rabbit is reachable.
		conn, err := dialAMQPWithRetry(ctx, url)
		if err != nil {
			return err
		}
		// reconnectDial is what the transport calls to re-establish
		// the connection after a broker drop. The reconnect path uses
		// a tighter retry budget (single-attempt; the transport
		// watchdog handles its own backoff loop) so the watchdog
		// stays responsive — it owns the outer loop, the dial is just
		// "open a fresh connection now."
		reconnectDial := func() (*amqp.Connection, error) {
			return amqp.Dial(url)
		}
		// Reuse the connection dialAMQPWithRetry just opened by
		// constructing the transport with New + SetDialFunc instead
		// of NewWithDial (which would re-dial immediately). That keeps
		// the boot path single-dial while still wiring reconnect for
		// the lifetime of the transport.
		transport := sdkamqp.New(conn)
		transport.SetDialFunc(reconnectDial)
		queueOwner := a.config.IntegrationType
		if queueOwner == "" {
			queueOwner = a.config.Provider
		}
		if queueOwner != "" {
			transport.SetEndpointPrefix("yggdrasil.adapter." + queueOwner + ".")
		}
		a.transport = transport
		a.afterRun = func() {
			// transport.Close also closes the underlying connection,
			// so no separate conn.Close needed (and a double Close
			// would error harmlessly anyway).
			_ = transport.Close()
		}
		return nil
	}
	return a
}

// dialAMQPWithRetry dials url with exponential backoff (1s → 2s → 4s →
// ... capped at 30s, max 30 attempts). Honors ctx cancellation so
// SIGTERM / Stop during the retry loop exits promptly. Wraps the final
// failure with the attempt count so the operator can tell "broker is
// flat-out down" from "broker is taking forever to come up."
func dialAMQPWithRetry(ctx context.Context, url string) (*amqp.Connection, error) {
	const (
		maxAttempts    = 30
		initialBackoff = 1 * time.Second
		maxBackoff     = 30 * time.Second
	)
	backoff := initialBackoff
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn, err := amqp.Dial(url)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if attempt == maxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("adapter: dial AMQP %q cancelled after %d attempts: %w", url, attempt, ctx.Err())
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
	return nil, fmt.Errorf("adapter: dial AMQP %q failed after %d attempts: %w", url, maxAttempts, lastErr)
}
