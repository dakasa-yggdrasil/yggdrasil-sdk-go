package reconcile

import "context"

// Reconciler is the canonical interface every managed-resource type
// implements. It expresses the convention's ensure_/observe_/destroy_
// triple as a single Go-level contract.
//
// D is the desired-state payload (input to Ensure). O is the
// observed-state payload (output of Ensure, element of Observe).
// They are typically distinct: the desired state lacks
// provider-generated fields like ID and observed timestamps; the
// observed state lacks adapter-only metadata like "ensure_managed".
type Reconciler[D any, O any] interface {
	// Ensure brings the resource to the desired state. MUST be
	// idempotent — repeated calls with the same D produce the same
	// O. Returns the observed state after the operation.
	Ensure(ctx context.Context, desired D) (O, error)

	// Observe lists resources of this type matching filter.
	// Filter is provider-specific; an empty filter returns all
	// resources the caller is authorized to see. Returns a page of
	// results and an opaque cursor; cursor "" means complete.
	Observe(ctx context.Context, filter map[string]any) ([]O, string, error)

	// Destroy removes the resource named by ref. MUST treat a
	// "not found" response from the provider as success — the
	// caller's desired state was "this resource should not exist,"
	// and it does not.
	Destroy(ctx context.Context, ref string) error
}

// DestroyWithDesired is the optional sister interface for reconcilers
// that need access to the full desired payload (not just the ref
// string) during destruction.
//
// # Motivation
//
// The base Reconciler[D, O].Destroy(ctx, ref) signature is the right
// shape for the canonical convention — destroy is identified by a
// stable provider-side ref. However, the SDK's controllers/message
// bridge (in every adapter pinning SDK v0.7.0+) stashes per-request
// integration context under reserved input keys
// (__instance_credentials / __instance_config / __request_auth) so
// the dispatched reconciler can rebuild auth + provider URLs
// per-call. Those reserved keys live in the parsed desired payload —
// NOT on the ref string. A reconciler that only sees the ref cannot
// resolve auth, breaking destroy_* operations for any adapter that
// loads credentials per-request (the dominant pattern across the
// ecosystem).
//
// DestroyWithDesired closes this gap: when a Reconciler[D, O]
// ALSO implements DestroyWithDesired[D], the SDK's dispatch path
// prefers DestroyWithDesired and passes the FULL parsed desired
// payload — letting the reconciler extract the reserved bridge keys
// and forward them through its `Execute`-equivalent dispatch helper.
//
// # Backward compatibility
//
// This interface is OPT-IN. Reconcilers that don't need credentials
// in destroy may continue to implement only the base Destroy(ctx, ref)
// method — v0.7.0 behavior is preserved verbatim. SDK v0.8.0 does NOT
// require any change to adapters that don't need this.
//
// # Wire shape
//
// The desired payload arrives unmarshalled from the same env.Input
// bytes the legacy Destroy reads `{"ref": "..."}` from. It is the
// same shape Ensure receives — including the reserved bridge keys
// the controllers/message bridge stashes per
// INTEGRATION_CONTRACT.md §5.b (op-echo cycle).
//
// # Example
//
//	type repoReconciler struct{ dispatch dispatchFn; instanceID string }
//
//	// Legacy — kept for backward compat / direct callers.
//	func (r *repoReconciler) Destroy(ctx context.Context, ref string) error {
//	    _, err := r.dispatch(OperationDestroyRepository, r.instanceID, payload{"ref": ref})
//	    return err
//	}
//
//	// Env-aware — preferred by SDK v0.8.0 dispatch.
//	func (r *repoReconciler) DestroyWithDesired(ctx context.Context, ref string, desired payload) error {
//	    if desired == nil { desired = payload{} }
//	    desired["ref"] = ref
//	    _, err := r.dispatch(OperationDestroyRepository, instanceFromPayload(desired, r.instanceID), desired)
//	    return err
//	}
type DestroyWithDesired[D any] interface {
	DestroyWithDesired(ctx context.Context, ref string, desired D) error
}

// Discoverer is the optional sister interface for resources the
// adapter should walk on the provider side — finding resources
// that exist in the provider's account/namespace including
// resources the adapter did not create.
//
// This is distinct from Observe: Observe retrieves resources by
// known identity or known filter; Discover enumerates without
// prior knowledge. Reconciliation workflows that need to import
// existing provider-side state implement this.
type Discoverer[O any] interface {
	Discover(ctx context.Context, scope map[string]any) ([]O, error)
}

// DriftReporter reports whether observed and desired diverge.
// Helper for reconciliation workflows that want to know "does the
// current state match what I asked for?" without re-running Ensure.
//
// Adapters that always Ensure on every reconciliation tick may
// skip this interface; workflows that branch on drift detection
// (e.g. emit-on-drift telemetry) require it.
type DriftReporter[D any, O any] interface {
	Drift(desired D, observed O) bool
}
