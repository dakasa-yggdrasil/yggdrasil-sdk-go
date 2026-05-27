package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/adapter"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sdk/events"
)

// dispatchEntry holds one operation→handler binding installed by
// RegisterReconciler. The package keeps a per-adapter dispatch
// table keyed by *adapter.Adapter so multiple Reconcilers on the
// same adapter compose without colliding.
//
// The handler receives the raw input plus the parsed envelope so it
// can pull metadata (idempotency key, instance id) onto an emitted
// MutationEvent when emission is wired.
type dispatchEntry struct {
	fn func(ctx context.Context, env executeRequest) ([]byte, error)
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
// envelope adapters in tree already speak. The Idempotency and
// InstanceID fields are optional metadata the SDK forwards onto
// emitted MutationEvents.
type executeRequest struct {
	Operation   string          `json:"operation,omitempty"`
	Capability  string          `json:"capability,omitempty"`
	Idempotency string          `json:"idempotency,omitempty"`
	InstanceID  string          `json:"instance_id,omitempty"`
	Input       json.RawMessage `json:"input,omitempty"`
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
		body, err := entry.fn(ctx, req)
		if err != nil {
			return nil, "", err
		}
		return body, "application/json", nil
	}
}

// emitContext captures the per-resource emission wiring threaded into
// every dispatch handler. It exists so the SDK can call emitter.Emit
// after Ensure/Destroy returns success without leaking emission
// concerns into the generic makeFn helpers' signatures.
type emitContext struct {
	emitter    events.Emitter
	provider   string
	resource   string
	instanceID string
	warn       func(format string, args ...any)
}

func (ec *emitContext) effectiveProvider(env executeRequest) string {
	// Future-proof: if any path ever wants per-call provider override
	// it lives here. For now, the registered provider wins.
	_ = env
	return ec.provider
}

func (ec *emitContext) effectiveInstanceID(env executeRequest) string {
	if env.InstanceID != "" {
		return env.InstanceID
	}
	return ec.instanceID
}

// emit posts a MutationEvent best-effort. emit errors are WARN-logged
// and swallowed — never bubbled up to the capability caller.
func (ec *emitContext) emit(ctx context.Context, env executeRequest, verb events.Verb, resourceID string, observed json.RawMessage) {
	if ec == nil || ec.emitter == nil {
		return
	}
	provider := ec.effectiveProvider(env)
	ev := events.MutationEvent{
		EventType:   events.BuildEventType(provider, ec.resource, verb),
		Provider:    provider,
		Resource:    ec.resource,
		Verb:        verb,
		ResourceID:  resourceID,
		InstanceID:  ec.effectiveInstanceID(env),
		Idempotency: env.Idempotency,
		Observed:    observed,
	}
	if err := ec.emitter.Emit(ctx, ev); err != nil {
		ec.warn("reconcile: emit %q failed (best-effort, not blocking): %v", ev.EventType, err)
	}
}

// missingEmitterWarned tracks one-shot WARN logs so backward-compat
// adapters get exactly one warning per adapter, not one per call.
var (
	missingEmitterWarnedMu sync.Mutex
	missingEmitterWarned   = map[*adapter.Adapter]bool{}
)

func warnMissingEmitterOnce(a *adapter.Adapter, resource string, logger func(format string, args ...any)) {
	missingEmitterWarnedMu.Lock()
	defer missingEmitterWarnedMu.Unlock()
	if missingEmitterWarned[a] {
		return
	}
	missingEmitterWarned[a] = true
	if logger == nil {
		logger = func(format string, args ...any) {
			fmt.Printf("WARN "+format+"\n", args...)
		}
	}
	logger("reconcile: RegisterReconciler for resource %q has no emitter wired (call reconcile.WithEmitter to enable §6.5 auto-emission)", resource)
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
// Opts is variadic so additive options (WithLegacyNames, WithEmitter,
// WithProvider, WithInstanceID, WithWarnLogger) can be attached
// without changing the call signature for the common case.
//
// When WithEmitter is supplied (v0.6.0+), the SDK auto-emits a
// MutationEvent after every successful Ensure() and Destroy() call —
// satisfying the INTEGRATION_CONTRACT.md §6.5 Golden Rule without
// adapter-author boilerplate. When omitted, the SDK logs one WARN
// per adapter at first registration (backward-compat with v0.5.0).
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

	ec := &emitContext{
		emitter:    cfg.emitter,
		provider:   cfg.provider,
		resource:   resource,
		instanceID: cfg.instanceID,
		warn:       resolveWarnLogger(cfg.warnLogger),
	}
	if cfg.emitter == nil {
		// Missing-emitter WARN goes to the stdlib log sink directly
		// — NOT through cfg.warnLogger — because the user-supplied
		// logger is dedicated to legacy-shim warnings and tests
		// assert on its content.
		warnMissingEmitterOnce(a, resource, nil)
	}

	t := tableFor(a)

	ensureName := "ensure_" + resource
	observeName := "observe_" + resourceType
	destroyName := "destroy_" + resource

	t.mu.Lock()
	t.entries[ensureName] = dispatchEntry{fn: makeEnsureFn[D, O](r, ec)}
	t.entries[observeName] = dispatchEntry{fn: makeObserveFn[D, O](r)}
	t.entries[destroyName] = dispatchEntry{fn: makeDestroyFn[D, O](r, ec)}
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

func resolveWarnLogger(logger func(format string, args ...any)) func(format string, args ...any) {
	if logger != nil {
		return logger
	}
	return func(format string, args ...any) {
		fmt.Printf("WARN "+format+"\n", args...)
	}
}

func makeEnsureFn[D, O any](r Reconciler[D, O], ec *emitContext) func(context.Context, executeRequest) ([]byte, error) {
	return func(ctx context.Context, env executeRequest) ([]byte, error) {
		var desired D
		if len(env.Input) > 0 {
			if err := json.Unmarshal(env.Input, &desired); err != nil {
				return nil, fmt.Errorf("reconcile.Ensure: parse desired: %w", err)
			}
		}
		observed, err := r.Ensure(ctx, desired)
		if err != nil {
			return nil, err
		}
		body, err := json.Marshal(observed)
		if err != nil {
			return nil, fmt.Errorf("reconcile.Ensure: marshal observed: %w", err)
		}
		resourceID := inferResourceID(observed, body)
		ec.emit(ctx, env, events.VerbEnsured, resourceID, body)
		return body, nil
	}
}

func makeObserveFn[D, O any](r Reconciler[D, O]) func(context.Context, executeRequest) ([]byte, error) {
	return func(ctx context.Context, env executeRequest) ([]byte, error) {
		filter := map[string]any{}
		if len(env.Input) > 0 {
			if err := json.Unmarshal(env.Input, &filter); err != nil {
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

func makeDestroyFn[D, O any](r Reconciler[D, O], ec *emitContext) func(context.Context, executeRequest) ([]byte, error) {
	return func(ctx context.Context, env executeRequest) ([]byte, error) {
		var in struct {
			Ref string `json:"ref"`
		}
		if len(env.Input) > 0 {
			if err := json.Unmarshal(env.Input, &in); err != nil {
				return nil, fmt.Errorf("reconcile.Destroy: parse ref: %w", err)
			}
		}

		// v0.8.1: when "ref" was not present in the inbound input
		// shape, scan the input map for an alternate identifier.
		// Adapters in tree send a variety of destroy payload shapes —
		// {"channel_id":"C123"} (slack), {"customer_id":"cus_..."}
		// (stripe), {"owner":"x","repo":"y"} (github composite),
		// {"id":"..."} (grafana). Without this fallback, ec.emit
		// receives ref="" and §6.5 events fail with
		// "HTTP 400: resource_id is required" at yggdrasil-core.
		// See destroy_resource_id_test.go for the full matrix.
		if in.Ref == "" && len(env.Input) > 0 {
			in.Ref = inferRefFromInput(env.Input, ec.resource)
		}

		// v0.8.0: prefer DestroyWithDesired[D] when the reconciler
		// implements it. That path receives the FULL parsed desired
		// payload — including the reserved bridge keys stashed by
		// adapter ExecuteHandlers (__instance_credentials etc) — so
		// destroy can resolve auth context the same way ensure_* and
		// observe_* do. Adapters that don't need credentials in
		// destroy keep the legacy Destroy(ctx, ref) signature and
		// continue working unchanged. See types.go DestroyWithDesired
		// docstring for the full rationale.
		if dwd, ok := any(r).(DestroyWithDesired[D]); ok {
			var desired D
			if len(env.Input) > 0 {
				if err := json.Unmarshal(env.Input, &desired); err != nil {
					return nil, fmt.Errorf("reconcile.DestroyWithDesired: parse desired: %w", err)
				}
			}
			if err := dwd.DestroyWithDesired(ctx, in.Ref, desired); err != nil {
				return nil, err
			}
		} else {
			if err := r.Destroy(ctx, in.Ref); err != nil {
				return nil, err
			}
		}
		// Observed for destroy is a minimal envelope — provider may
		// not return any state once the resource is gone.
		observed := json.RawMessage(fmt.Sprintf(`{"ref":%q,"deleted":true}`, in.Ref))
		ec.emit(ctx, env, events.VerbDestroyed, in.Ref, observed)
		return []byte(`{"deleted":true}`), nil
	}
}

// inferRefFromInput scans the destroy input payload for a stable
// resource identifier. Real-world adapter destroys NEVER send
// `{"ref": ...}` — they send provider-shaped payloads. Without this
// inference the SDK emits §6.5 events with empty resource_id and
// yggdrasil-core rejects with HTTP 400.
//
// Lookup order:
//
//  1. {"ref": "..."}                (explicit canonical)
//  2. {"<resource>_id": "..."}      (e.g. channel_id, customer_id)
//  3. {"id": "..."}                 (generic)
//  4. {"owner": "x", "repo": "y"}   (composite — joined as "x/y", github)
//  5. {"<resource>": "..."}         (named-after-resource — e.g. repository="owner/repo")
//
// Returns "" if no identifier can be derived; the emit attempt then
// fails at yggdrasil-core's validator (400 resource_id required) —
// surfacing the gap to maintainers rather than silently emitting a
// malformed event.
func inferRefFromInput(input []byte, resource string) string {
	var m map[string]any
	if err := json.Unmarshal(input, &m); err != nil {
		return ""
	}
	if v, ok := m["ref"].(string); ok && v != "" {
		return v
	}
	if resource != "" {
		if v, ok := m[resource+"_id"].(string); ok && v != "" {
			return v
		}
	}
	if v, ok := m["id"].(string); ok && v != "" {
		return v
	}
	// Composite owner+repo (github pattern).
	if owner, ok := m["owner"].(string); ok && owner != "" {
		if repo, ok := m["repo"].(string); ok && repo != "" {
			return owner + "/" + repo
		}
	}
	// Named-after-resource (e.g. repository="owner/repo").
	if resource != "" {
		if v, ok := m[resource].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

// inferResourceID extracts the provider-side ID from the observed
// state. Strategy:
//
//  1. If observed is a struct with an "ID" / "Id" / "id" field, use it.
//  2. Otherwise, scan the marshalled JSON for a top-level "id" key
//     (matches the conventional Yggdrasil observed-state shape).
//  3. Returns "" when no ID can be derived — the resulting event will
//     carry an empty ResourceID, which is still better than no event
//     at all and lets downstream consumers detect the misconfiguration.
func inferResourceID(observed any, body []byte) string {
	if id := reflectID(observed); id != "" {
		return id
	}
	var probe map[string]any
	if err := json.Unmarshal(body, &probe); err == nil {
		for _, k := range []string{"id", "ID", "Id"} {
			if v, ok := probe[k]; ok {
				if s, ok := v.(string); ok {
					return s
				}
			}
		}
	}
	return ""
}

func reflectID(v any) string {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return ""
	}
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return ""
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return ""
	}
	for _, name := range []string{"ID", "Id"} {
		f := rv.FieldByName(name)
		if f.IsValid() && f.Kind() == reflect.String {
			return f.String()
		}
	}
	return ""
}

// ExecuteForTest is the v0.5.0 / v0.6.0 entry point into the
// per-adapter dispatch table. Promoted to a public production API as
// reconcile.Dispatch in v0.7.0; this alias remains for one minor
// cycle so adapters mid-migration keep building.
//
// Deprecated: use reconcile.Dispatch — semantically identical. Will
// be removed at v1.0.0.
func ExecuteForTest(ctx context.Context, a *adapter.Adapter, d rpc.Delivery) ([]byte, string, error) {
	return Dispatch(ctx, a, d)
}
