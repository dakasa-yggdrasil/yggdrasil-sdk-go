package reconcile

import (
	"context"
	"fmt"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/adapter"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
)

// Dispatch routes an inbound execute envelope through the adapter's
// reconcile dispatch table, invoking the registered Reconciler for the
// requested operation and emitting §6.5 mutation events on success
// when an emitter is wired (via WithEmitter at registration time).
//
// # Production wiring (v0.7.0+)
//
// Adapters wire Dispatch into their controllers/message ExecuteHandler
// AFTER calling RegisterReconciler (or WireReconcilers) so production
// traffic flows through the SDK dispatch path — activating §6.5
// auto-emission for every operator request, not just tests:
//
//	// In main.go:
//	a := adapter.New(adapter.Config{Provider: "stripe", ...})
//	reconcile.RegisterReconciler(a, "customer", "customers", customerR,
//	    reconcile.WithEmitter(emitter),
//	    reconcile.WithProvider("stripe"),
//	)
//	// ...register all resources...
//
//	// In controllers/message/execute.go:
//	func ExecuteHandler(...) Handler {
//	    return func(ctx context.Context, d rpc.Delivery) ([]byte, string, error) {
//	        // ...auth, logging, capability normalize...
//	        return reconcile.Dispatch(ctx, a, d)  // ← SDK auto-emits here
//	    }
//	}
//
// # IMPORTANT — registration order
//
// adapter.Adapter.Register is last-write-wins ("Duplicate registrations
// overwrite; this is deliberate so that tests can swap a mock handler in"
// — see adapter/adapter.go). RegisterReconciler auto-installs an
// "execute" handler on the adapter the FIRST time it runs per adapter;
// adapters that ALSO call a.Register("execute", customHandler) AFTER
// RegisterReconciler will clobber the SDK's auto-installed handler.
//
// The supported wiring patterns are therefore:
//
//   - Register a SINGLE custom execute handler that internally calls
//     reconcile.Dispatch (recommended — preserves auth / logging / capability
//     normalization while delegating routing + §6.5 emission to the SDK).
//   - Skip a.Register("execute", ...) entirely and rely on the SDK's
//     auto-installed handler.
//
// The pattern that does NOT work is calling RegisterReconciler AND
// a.Register("execute", legacyHandler) — the legacy handler wins
// (last-write-wins) and §6.5 emission silently stops.
//
// # Return values
//
// Returns the response body bytes, content-type
// ("application/json"), and any error from envelope parsing, op
// lookup, or the underlying Reconciler call. Returns a clear error
// when the adapter has no registered Reconciler or the requested
// operation is not in the dispatch table.
func Dispatch(ctx context.Context, a *adapter.Adapter, d rpc.Delivery) ([]byte, string, error) {
	dispatchTablesMu.Lock()
	t, ok := dispatchTables[a]
	dispatchTablesMu.Unlock()
	if !ok {
		return nil, "", fmt.Errorf("reconcile: adapter has no registered Reconciler (call reconcile.RegisterReconciler before reconcile.Dispatch)")
	}
	return buildExecuteHandler(t)(ctx, d)
}
