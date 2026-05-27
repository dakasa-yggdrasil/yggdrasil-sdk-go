package reconcile_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/adapter"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sdk/reconcile"
)

// destroyMapPayload mirrors the `reconcilePayload = map[string]any`
// shape every in-tree adapter uses today: the desired payload that
// flows through the SDK's dispatch envelope and lands at the
// reconciler. Tests below exercise the new DestroyWithDesired
// interface against this shape because that's what every adapter
// will adopt at Phase B.
type destroyMapPayload map[string]any

// captureCall records every invocation a reconciler observes so the
// test can assert on which method the SDK chose to call and what
// payload that method received.
type captureCall struct {
	mu              sync.Mutex
	destroyCalls    []destroyInvocation
	withDesiredCall []destroyInvocation
}

type destroyInvocation struct {
	ref     string
	desired destroyMapPayload
}

func (c *captureCall) recordDestroy(ref string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.destroyCalls = append(c.destroyCalls, destroyInvocation{ref: ref})
}

func (c *captureCall) recordWithDesired(ref string, desired destroyMapPayload) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := destroyMapPayload{}
	for k, v := range desired {
		cp[k] = v
	}
	c.withDesiredCall = append(c.withDesiredCall, destroyInvocation{ref: ref, desired: cp})
}

// legacyOnlyReconciler implements ONLY the canonical Reconciler[D,O]
// interface (Ensure/Observe/Destroy). It does NOT implement
// DestroyWithDesired — the SDK MUST fall back to the legacy
// Destroy(ctx, ref) path. Backward-compat guarantee.
type legacyOnlyReconciler struct {
	cap *captureCall
}

func (r *legacyOnlyReconciler) Ensure(ctx context.Context, d destroyMapPayload) (destroyMapPayload, error) {
	return destroyMapPayload{"id": "fake"}, nil
}

func (r *legacyOnlyReconciler) Observe(ctx context.Context, filter map[string]any) ([]destroyMapPayload, string, error) {
	return nil, "", nil
}

func (r *legacyOnlyReconciler) Destroy(ctx context.Context, ref string) error {
	r.cap.recordDestroy(ref)
	return nil
}

// envAwareReconciler implements BOTH Reconciler[D,O] and the new
// DestroyWithDesired[D] interface. The SDK MUST prefer the
// DestroyWithDesired call when both are available so the reconciler
// can see reserved bridge keys (__instance_credentials etc).
type envAwareReconciler struct {
	cap *captureCall
}

func (r *envAwareReconciler) Ensure(ctx context.Context, d destroyMapPayload) (destroyMapPayload, error) {
	return destroyMapPayload{"id": "fake"}, nil
}

func (r *envAwareReconciler) Observe(ctx context.Context, filter map[string]any) ([]destroyMapPayload, string, error) {
	return nil, "", nil
}

func (r *envAwareReconciler) Destroy(ctx context.Context, ref string) error {
	// Legacy path — should NOT be called when DestroyWithDesired is
	// also implemented.
	r.cap.recordDestroy(ref)
	return nil
}

func (r *envAwareReconciler) DestroyWithDesired(ctx context.Context, ref string, desired destroyMapPayload) error {
	r.cap.recordWithDesired(ref, desired)
	return nil
}

func callDestroy(t *testing.T, a *adapter.Adapter, input map[string]any) ([]byte, error) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"operation": "destroy_widget",
		"input":     input,
	})
	if err != nil {
		t.Fatalf("marshal destroy envelope: %v", err)
	}
	resp, _, derr := reconcile.Dispatch(context.Background(), a, rpc.Delivery{Body: body, ContentType: "application/json"})
	return resp, derr
}

// TestDestroyWithDesired_LegacyOnly_FallsThroughToDestroy verifies the
// SDK keeps calling the legacy Destroy(ctx, ref) signature when a
// reconciler does NOT implement DestroyWithDesired. Backward-compat is
// the load-bearing guarantee for v0.8.0.
func TestDestroyWithDesired_LegacyOnly_FallsThroughToDestroy(t *testing.T) {
	cap := &captureCall{}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &legacyOnlyReconciler{cap: cap}
	reconcile.RegisterReconciler[destroyMapPayload, destroyMapPayload](a, "widget", "widgets", rec)

	body, err := callDestroy(t, a, map[string]any{"ref": "w-123"})
	if err != nil {
		t.Fatalf("destroy_widget legacy path failed: %v", err)
	}
	if !strings.Contains(string(body), `"deleted":true`) {
		t.Fatalf("expected deleted:true in response, got %s", body)
	}
	if got := len(cap.destroyCalls); got != 1 {
		t.Fatalf("expected legacy Destroy called exactly once, got %d", got)
	}
	if cap.destroyCalls[0].ref != "w-123" {
		t.Fatalf("expected legacy Destroy ref 'w-123', got %q", cap.destroyCalls[0].ref)
	}
	if len(cap.withDesiredCall) != 0 {
		t.Fatalf("expected no DestroyWithDesired calls on legacy reconciler, got %d", len(cap.withDesiredCall))
	}
}

// TestDestroyWithDesired_EnvAware_ReceivesFullDesired verifies the
// SDK calls DestroyWithDesired(ctx, ref, desired) with the FULL parsed
// payload — including reserved bridge keys like __instance_credentials
// — so the reconciler can resolve auth context. This is the canonical
// fix for the destroy-credential bug.
func TestDestroyWithDesired_EnvAware_ReceivesFullDesired(t *testing.T) {
	cap := &captureCall{}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &envAwareReconciler{cap: cap}
	reconcile.RegisterReconciler[destroyMapPayload, destroyMapPayload](a, "widget", "widgets", rec)

	input := map[string]any{
		"ref":                     "w-456",
		"instance_id":             "test-instance",
		"__instance_credentials":  map[string]any{"token": "ghp_secret_value_must_reach_destroy"},
		"__instance_config":       map[string]any{"api_base_url": "https://api.test"},
		"__request_auth":          map[string]any{"actor": "alice"},
	}
	body, err := callDestroy(t, a, input)
	if err != nil {
		t.Fatalf("destroy_widget env-aware path failed: %v", err)
	}
	if !strings.Contains(string(body), `"deleted":true`) {
		t.Fatalf("expected deleted:true in response, got %s", body)
	}

	if got := len(cap.withDesiredCall); got != 1 {
		t.Fatalf("expected DestroyWithDesired called exactly once, got %d", got)
	}
	if cap.withDesiredCall[0].ref != "w-456" {
		t.Fatalf("expected DestroyWithDesired ref 'w-456', got %q", cap.withDesiredCall[0].ref)
	}
	creds, ok := cap.withDesiredCall[0].desired["__instance_credentials"].(map[string]any)
	if !ok {
		t.Fatalf("expected __instance_credentials in desired map, got %#v", cap.withDesiredCall[0].desired)
	}
	if creds["token"] != "ghp_secret_value_must_reach_destroy" {
		t.Fatalf("expected credentials.token to round-trip through SDK, got %#v", creds)
	}
	cfg, ok := cap.withDesiredCall[0].desired["__instance_config"].(map[string]any)
	if !ok {
		t.Fatalf("expected __instance_config in desired map, got %#v", cap.withDesiredCall[0].desired)
	}
	if cfg["api_base_url"] != "https://api.test" {
		t.Fatalf("expected config.api_base_url to round-trip, got %#v", cfg)
	}
}

// TestDestroyWithDesired_PreferredOverLegacy verifies that when a
// reconciler implements BOTH Destroy(ctx, ref) AND DestroyWithDesired,
// the SDK ALWAYS picks the env-aware method. Prevents the legacy
// Destroy from being silently invoked and dropping credentials.
func TestDestroyWithDesired_PreferredOverLegacy(t *testing.T) {
	cap := &captureCall{}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &envAwareReconciler{cap: cap}
	reconcile.RegisterReconciler[destroyMapPayload, destroyMapPayload](a, "widget", "widgets", rec)

	_, err := callDestroy(t, a, map[string]any{"ref": "w-789"})
	if err != nil {
		t.Fatalf("destroy_widget envelope dispatch failed: %v", err)
	}
	if got := len(cap.destroyCalls); got != 0 {
		t.Fatalf("expected legacy Destroy NOT called when DestroyWithDesired exists, got %d calls", got)
	}
	if got := len(cap.withDesiredCall); got != 1 {
		t.Fatalf("expected DestroyWithDesired exactly once, got %d", got)
	}
}

// TestDestroyWithDesired_ReservedKeysReachReconciler verifies the
// reserved bridge keys defined in INTEGRATION_CONTRACT §5.b
// (__instance_credentials, __instance_config, __request_auth) ALL
// arrive in the desired map of DestroyWithDesired. This is the
// invariant the controllers/message bridge relies on across every
// adapter that pins SDK v0.8.0+.
func TestDestroyWithDesired_ReservedKeysReachReconciler(t *testing.T) {
	cap := &captureCall{}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &envAwareReconciler{cap: cap}
	reconcile.RegisterReconciler[destroyMapPayload, destroyMapPayload](a, "widget", "widgets", rec)

	input := map[string]any{
		"ref":                    "w-abc",
		"__instance_credentials": map[string]any{"k": "v"},
		"__instance_config":      map[string]any{"k": "v"},
		"__request_auth":         map[string]any{"k": "v"},
	}
	if _, err := callDestroy(t, a, input); err != nil {
		t.Fatalf("destroy_widget dispatch failed: %v", err)
	}
	if len(cap.withDesiredCall) != 1 {
		t.Fatalf("expected DestroyWithDesired called once, got %d", len(cap.withDesiredCall))
	}
	desired := cap.withDesiredCall[0].desired
	for _, key := range []string{"__instance_credentials", "__instance_config", "__request_auth"} {
		if _, ok := desired[key]; !ok {
			t.Fatalf("expected reserved key %q in DestroyWithDesired payload, keys=%v", key, mapKeys(desired))
		}
	}
}

func mapKeys(m destroyMapPayload) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
