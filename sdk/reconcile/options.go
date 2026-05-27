package reconcile

import (
	"context"
	"log"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sdk/events"
)

// options carries the variadic Option flags RegisterReconciler
// consumes. Internal so adapter authors don't poke at it directly.
type options struct {
	legacyNames []string
	warnLogger  func(format string, args ...any)

	// emitter is the optional MutationEvent sink wired in v0.6.0.
	// When nil, RegisterReconciler logs a WARN once at startup and
	// the auto-emission path is skipped (v0.5.0 behavior).
	emitter events.Emitter

	// provider is the integration family identifier used to build
	// MutationEvent.EventType ("<provider>.<resource>.<verb>").
	// Defaults to the adapter's Config.Provider when unset, but
	// the option exists so adapters can override per Reconciler
	// (rare; mainly for testing).
	provider string

	// instanceID is the multi-tenant scope written into
	// MutationEvent.InstanceID. Adapters typically set this from
	// the integration_instance label resolved at startup.
	instanceID string
}

// Option mutates an options value during RegisterReconciler setup.
type Option func(*options)

func warnShim(
	legacy, canonical string,
	canonicalFn func(context.Context, executeRequest) ([]byte, error),
	logger func(format string, args ...any),
) func(context.Context, executeRequest) ([]byte, error) {
	if logger == nil {
		logger = func(format string, args ...any) {
			log.Printf("WARN "+format, args...)
		}
	}
	return func(ctx context.Context, env executeRequest) ([]byte, error) {
		logger("reconcile: deprecated capability name %q invoked; use %q (compat shim, removed in v0.7.0)", legacy, canonical)
		return canonicalFn(ctx, env)
	}
}

func pickCanonicalForLegacy(legacy, ensureName, observeName, destroyName string) string {
	// Heuristic: name choice based on legacy verb prefix.
	// Tests in Task 9 lock the table; this default rule keeps the
	// common Tier C renames (create_X→ensure_X, get_X/list_X→observe_X,
	// delete_X/unregister_X/cancel_X→destroy_X) cheap.
	switch {
	case startsWithAny(legacy, "create_", "update_", "upsert_", "register_", "set_", "apply_", "issue_"):
		return ensureName
	case startsWithAny(legacy, "get_", "list_", "describe_", "lookup_", "retrieve_"):
		return observeName
	case startsWithAny(legacy, "delete_", "unregister_", "remove_", "teardown_", "revoke_", "cancel_", "archive_"):
		return destroyName
	default:
		return ensureName
	}
}

func startsWithAny(s string, prefixes ...string) bool {
	for _, p := range prefixes {
		if len(s) >= len(p) && s[:len(p)] == p {
			return true
		}
	}
	return false
}

// WithLegacyNames declares pre-convention capability names this
// Reconciler should also accept. The first time a legacy name is
// invoked, the shim logs a WARN and routes the call to the
// canonical handler determined by verb prefix:
//
//	create_* / update_* / register_* / set_* / apply_* / issue_*
//	                                                    → ensure_
//	get_* / list_* / describe_* / lookup_* / retrieve_* → observe_
//	delete_* / unregister_* / cancel_* / archive_*      → destroy_
//
// The shim's removal target is SDK v0.7.0 (moved from v0.6.0 to make
// room for the v0.6.0 events package rollout). Adapters MUST drop
// WithLegacyNames before bumping to v0.7.x.
func WithLegacyNames(names ...string) Option {
	return func(o *options) {
		o.legacyNames = append(o.legacyNames, names...)
	}
}

// WithWarnLogger overrides the default log.Printf-based WARN emitter
// the compat shim uses. Tests inject a capturing logger; production
// adapters typically leave the default in place.
func WithWarnLogger(logger func(format string, args ...any)) Option {
	return func(o *options) {
		o.warnLogger = logger
	}
}

// WithEmitter wires a MutationEvent sink for auto-emission. When set,
// every successful Ensure() and Destroy() invocation produces a
// MutationEvent that the SDK posts via emitter.Emit on the caller's
// behalf — the adapter author writes zero emission boilerplate.
//
// Emission is best-effort by design: a failure to emit logs a WARN
// but does NOT fail the capability call. Adapter latency and
// availability MUST NOT depend on event bus health. The downstream
// event_log table absorbs the missed event on the next reconciliation
// tick when the bus recovers.
//
// When WithEmitter is omitted (backward-compat with v0.5.0), the SDK
// logs a single WARN per adapter startup so the missing wiring is
// visible. v0.5.0 adapter binaries keep building unchanged.
func WithEmitter(em events.Emitter) Option {
	return func(o *options) {
		o.emitter = em
	}
}

// WithProvider overrides the integration family identifier the SDK
// uses to build MutationEvent.EventType. Defaults to the adapter's
// Config.Provider when unset.
func WithProvider(provider string) Option {
	return func(o *options) {
		o.provider = provider
	}
}

// WithInstanceID sets MutationEvent.InstanceID — the multi-tenant
// scope (e.g. "stripe-acme" vs "stripe-corp"). Adapters typically
// resolve this from the integration_instance label at startup.
func WithInstanceID(instanceID string) Option {
	return func(o *options) {
		o.instanceID = instanceID
	}
}
