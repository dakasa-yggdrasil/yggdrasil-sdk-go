package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/adapter"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
)

// dispatchEntry holds one operation→handler binding installed by
// RegisterReconciler. The package keeps a per-adapter dispatch
// table keyed by *adapter.Adapter so multiple Reconcilers on the
// same adapter compose without colliding.
type dispatchEntry struct {
	fn func(ctx context.Context, input json.RawMessage) ([]byte, error)
}

type adapterDispatch struct {
	mu      sync.RWMutex
	entries map[string]dispatchEntry // operation name → handler
}

var (
	dispatchTablesMu sync.Mutex
	dispatchTables   = map[*adapter.Adapter]*adapterDispatch{}
)

// executeRequest is the wire-shape the synthesized execute handler
// expects. Matches the existing AdapterExecuteIntegrationRequest
// envelope adapters in tree already speak.
type executeRequest struct {
	Operation  string          `json:"operation,omitempty"`
	Capability string          `json:"capability,omitempty"`
	Input      json.RawMessage `json:"input,omitempty"`
}

func tableFor(a *adapter.Adapter) *adapterDispatch {
	dispatchTablesMu.Lock()
	defer dispatchTablesMu.Unlock()
	t, ok := dispatchTables[a]
	if !ok {
		t = &adapterDispatch{entries: map[string]dispatchEntry{}}
		dispatchTables[a] = t
		// Install the synthesized execute handler exactly once
		// per adapter. RegisterReconciler may be called multiple
		// times (one per resource type); only the first install
		// wires the top-level capability.
		a.Register("execute", buildExecuteHandler(t))
	}
	return t
}

// buildExecuteHandler returns the adapter.Handler that ranges the
// dispatch table on every inbound delivery, picking the function
// keyed by the request's `operation` field. Unknown operations
// fail with a clear error so callers see the canonical capability
// name they meant to invoke.
func buildExecuteHandler(t *adapterDispatch) adapter.Handler {
	return func(ctx context.Context, d rpc.Delivery) ([]byte, string, error) {
		var req executeRequest
		if len(d.Body) > 0 {
			if err := json.Unmarshal(d.Body, &req); err != nil {
				return nil, "", fmt.Errorf("reconcile: parse execute request: %w", err)
			}
		}
		op := strings.TrimSpace(req.Operation)
		if op == "" {
			op = strings.TrimSpace(req.Capability)
		}
		if op == "" {
			return nil, "", fmt.Errorf("reconcile: execute request missing operation")
		}

		t.mu.RLock()
		entry, ok := t.entries[op]
		t.mu.RUnlock()
		if !ok {
			return nil, "", fmt.Errorf("reconcile: unsupported operation %q", op)
		}
		body, err := entry.fn(ctx, req.Input)
		if err != nil {
			return nil, "", err
		}
		return body, "application/json", nil
	}
}

// RegisterReconciler wires r into a's dispatch table under three
// canonical operation names:
//
//   - "ensure_"   + resource       → r.Ensure
//   - "observe_"  + resourceType   → r.Observe
//   - "destroy_"  + resource       → r.Destroy
//
// resource is the singular suffix (e.g. "user", "s3_bucket");
// resourceType is the plural form used by Observe (e.g. "users",
// "s3_buckets"). The adapter's hand-authored Describe() catalog
// still owns metadata (description, input_schema, idempotent flag)
// — this function only owns the runtime dispatch boilerplate.
//
// Opts is variadic so v0.5.x compat shims (WithLegacyNames) can be
// attached without changing the call signature for the common case.
func RegisterReconciler[D, O any](
	a *adapter.Adapter,
	resource string,
	resourceType string,
	r Reconciler[D, O],
	opts ...Option,
) {
	resource = strings.ToLower(strings.TrimSpace(resource))
	resourceType = strings.ToLower(strings.TrimSpace(resourceType))
	if resource == "" || resourceType == "" {
		panic("reconcile.RegisterReconciler: resource and resourceType are required")
	}

	cfg := options{}
	for _, opt := range opts {
		opt(&cfg)
	}

	t := tableFor(a)

	ensureName := "ensure_" + resource
	observeName := "observe_" + resourceType
	destroyName := "destroy_" + resource

	t.mu.Lock()
	t.entries[ensureName] = dispatchEntry{fn: makeEnsureFn[D, O](r)}
	t.entries[observeName] = dispatchEntry{fn: makeObserveFn[D, O](r)}
	t.entries[destroyName] = dispatchEntry{fn: makeDestroyFn[D, O](r)}
	for _, legacy := range cfg.legacyNames {
		legacy = strings.ToLower(strings.TrimSpace(legacy))
		if legacy == "" {
			continue
		}
		canonical := pickCanonicalForLegacy(legacy, ensureName, observeName, destroyName)
		canonicalFn := t.entries[canonical].fn
		t.entries[legacy] = dispatchEntry{fn: warnShim(legacy, canonical, canonicalFn, cfg.warnLogger)}
	}
	t.mu.Unlock()
}

func makeEnsureFn[D, O any](r Reconciler[D, O]) func(context.Context, json.RawMessage) ([]byte, error) {
	return func(ctx context.Context, raw json.RawMessage) ([]byte, error) {
		var desired D
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &desired); err != nil {
				return nil, fmt.Errorf("reconcile.Ensure: parse desired: %w", err)
			}
		}
		observed, err := r.Ensure(ctx, desired)
		if err != nil {
			return nil, err
		}
		return json.Marshal(observed)
	}
}

func makeObserveFn[D, O any](r Reconciler[D, O]) func(context.Context, json.RawMessage) ([]byte, error) {
	return func(ctx context.Context, raw json.RawMessage) ([]byte, error) {
		filter := map[string]any{}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &filter); err != nil {
				return nil, fmt.Errorf("reconcile.Observe: parse filter: %w", err)
			}
		}
		items, cursor, err := r.Observe(ctx, filter)
		if err != nil {
			return nil, err
		}
		return json.Marshal(map[string]any{"items": items, "cursor": cursor})
	}
}

func makeDestroyFn[D, O any](r Reconciler[D, O]) func(context.Context, json.RawMessage) ([]byte, error) {
	return func(ctx context.Context, raw json.RawMessage) ([]byte, error) {
		var in struct {
			Ref string `json:"ref"`
		}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &in); err != nil {
				return nil, fmt.Errorf("reconcile.Destroy: parse ref: %w", err)
			}
		}
		if err := r.Destroy(ctx, in.Ref); err != nil {
			return nil, err
		}
		return []byte(`{"deleted":true}`), nil
	}
}

// ExecuteForTest invokes the adapter's synthesized execute handler
// directly so package-external tests can verify dispatch without
// booting a transport. NOT for production use — adapters call
// adapter.Run which wires the transport.
func ExecuteForTest(ctx context.Context, a *adapter.Adapter, d rpc.Delivery) ([]byte, string, error) {
	dispatchTablesMu.Lock()
	t, ok := dispatchTables[a]
	dispatchTablesMu.Unlock()
	if !ok {
		return nil, "", fmt.Errorf("reconcile: adapter has no registered Reconciler")
	}
	return buildExecuteHandler(t)(ctx, d)
}
