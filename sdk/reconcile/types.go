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
