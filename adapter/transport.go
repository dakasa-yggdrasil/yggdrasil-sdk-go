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
	a.transport = sdkhttp.New(sdkhttp.Transport{Mux: mux})

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
func (a *Adapter) ListenAMQP(url string) *Adapter {
	a.beforeRun = func(_ context.Context) error {
		conn, err := amqp.Dial(url)
		if err != nil {
			return fmt.Errorf("adapter: dial AMQP %q: %w", url, err)
		}
		transport := sdkamqp.New(conn)
		a.transport = transport
		a.afterRun = func() {
			_ = transport.Close()
			_ = conn.Close()
		}
		return nil
	}
	return a
}
