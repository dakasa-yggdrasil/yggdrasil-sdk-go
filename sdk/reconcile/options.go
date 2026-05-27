package reconcile

import (
	"context"
	"encoding/json"
	"log"
)

// options carries the variadic Option flags RegisterReconciler
// consumes. Internal so adapter authors don't poke at it directly.
type options struct {
	legacyNames []string
	warnLogger  func(format string, args ...any)
}

// Option mutates an options value during RegisterReconciler setup.
type Option func(*options)

func warnShim(
	legacy, canonical string,
	canonicalFn func(context.Context, json.RawMessage) ([]byte, error),
	logger func(format string, args ...any),
) func(context.Context, json.RawMessage) ([]byte, error) {
	if logger == nil {
		logger = func(format string, args ...any) {
			log.Printf("WARN "+format, args...)
		}
	}
	return func(ctx context.Context, raw json.RawMessage) ([]byte, error) {
		logger("reconcile: deprecated capability name %q invoked; use %q (compat shim, removed in v0.6.0)", legacy, canonical)
		return canonicalFn(ctx, raw)
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
// The shim is removed in SDK v0.6.0. Adapters MUST drop
// WithLegacyNames before bumping to v0.6.x.
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
