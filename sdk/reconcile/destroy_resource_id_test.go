package reconcile

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/adapter"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sdk/events"
)

// TestInferRefFromInput_ExplicitRef verifies the canonical {"ref":"..."}
// shape is preferred and resolves verbatim. Backward compat with the
// pre-v0.8.1 SDK behavior (where every destroy payload carried "ref").
func TestInferRefFromInput_ExplicitRef(t *testing.T) {
	got := inferRefFromInput([]byte(`{"ref": "C123"}`), "channel")
	if got != "C123" {
		t.Fatalf("expected ref=C123, got %q", got)
	}
}

// TestInferRefFromInput_ResourceScopedID verifies {"<resource>_id":"..."}
// resolution for the dominant adapter pattern — slack channel_id,
// stripe customer_id, nfeio service_invoice_id, etc.
func TestInferRefFromInput_ResourceScopedID(t *testing.T) {
	got := inferRefFromInput([]byte(`{"channel_id": "C123"}`), "channel")
	if got != "C123" {
		t.Fatalf("expected channel_id=C123 → ref=C123, got %q", got)
	}
}

// TestInferRefFromInput_GenericID verifies {"id":"..."} resolution
// (grafana folder/dashboard pattern).
func TestInferRefFromInput_GenericID(t *testing.T) {
	got := inferRefFromInput([]byte(`{"id": "uuid-abc"}`), "folder")
	if got != "uuid-abc" {
		t.Fatalf("expected id=uuid-abc → ref=uuid-abc, got %q", got)
	}
}

// TestInferRefFromInput_OwnerRepoComposite verifies the github
// composite {"owner":"...","repo":"..."} resolves to "owner/repo".
func TestInferRefFromInput_OwnerRepoComposite(t *testing.T) {
	got := inferRefFromInput(
		[]byte(`{"owner": "dakasa-yggdrasil", "repo": "x"}`),
		"repository",
	)
	if got != "dakasa-yggdrasil/x" {
		t.Fatalf("expected owner+repo composite=dakasa-yggdrasil/x, got %q", got)
	}
}

// TestInferRefFromInput_NamedAfterResource verifies the
// {"<resource>": "..."} fallback for repository="owner/repo" callers
// that send the full identity as a single string.
func TestInferRefFromInput_NamedAfterResource(t *testing.T) {
	got := inferRefFromInput(
		[]byte(`{"repository": "owner/repo"}`),
		"repository",
	)
	if got != "owner/repo" {
		t.Fatalf("expected repository=owner/repo, got %q", got)
	}
}

// TestInferRefFromInput_EmptyInput returns "" — the resulting
// MutationEvent will then carry an empty ResourceID, which makes the
// gap visible at yggdrasil-core's POST /api/v1/events validator.
func TestInferRefFromInput_EmptyInput(t *testing.T) {
	got := inferRefFromInput(nil, "user")
	if got != "" {
		t.Fatalf("expected empty ref on nil input, got %q", got)
	}
	got = inferRefFromInput([]byte(`{}`), "user")
	if got != "" {
		t.Fatalf("expected empty ref on empty object, got %q", got)
	}
}

// TestInferRefFromInput_GarbageInput exercises malformed JSON. Returns
// "" without panicking. Defensive: the upstream Unmarshal already
// failed by this point in real dispatch, but inferRefFromInput must
// be safe to call on any byte slice.
func TestInferRefFromInput_GarbageInput(t *testing.T) {
	got := inferRefFromInput([]byte(`not json {{`), "user")
	if got != "" {
		t.Fatalf("expected empty ref on garbage input, got %q", got)
	}
}

// TestInferRefFromInput_PrefersRefOverScopedID verifies precedence:
// when both "ref" and "channel_id" are present, "ref" wins. This
// keeps the canonical shape as the source of truth and lets adapters
// that send both (e.g. during transitional periods) get the explicit
// value.
func TestInferRefFromInput_PrefersRefOverScopedID(t *testing.T) {
	got := inferRefFromInput(
		[]byte(`{"ref": "explicit", "channel_id": "from_scoped"}`),
		"channel",
	)
	if got != "explicit" {
		t.Fatalf("expected ref to win over channel_id, got %q", got)
	}
}

// TestInferRefFromInput_PrefersScopedOverGenericID verifies the
// scoped {"<resource>_id":"..."} beats {"id":"..."} when both exist —
// preserves provider-specific identity over Yggdrasil-generic.
func TestInferRefFromInput_PrefersScopedOverGenericID(t *testing.T) {
	got := inferRefFromInput(
		[]byte(`{"channel_id": "scoped", "id": "generic"}`),
		"channel",
	)
	if got != "scoped" {
		t.Fatalf("expected channel_id to win over id, got %q", got)
	}
}

// TestInferRefFromInput_EmptyStringScopedID skips empty-string values
// so the lookup falls through to the next candidate. Without this,
// {"channel_id":""} would short-circuit on the empty key and produce
// ref="" even when later fields had real values.
func TestInferRefFromInput_EmptyStringScopedID(t *testing.T) {
	got := inferRefFromInput(
		[]byte(`{"channel_id": "", "id": "fallback"}`),
		"channel",
	)
	if got != "fallback" {
		t.Fatalf("expected empty channel_id to fall through to id, got %q", got)
	}
}

// TestInferRefFromInput_OwnerWithoutRepo verifies the composite path
// requires BOTH owner and repo. If only owner is present, we don't
// invent "owner/" — fall through to the next candidate.
func TestInferRefFromInput_OwnerWithoutRepo(t *testing.T) {
	got := inferRefFromInput(
		[]byte(`{"owner": "dakasa", "id": "fallback"}`),
		"repository",
	)
	if got != "fallback" {
		t.Fatalf("expected owner-without-repo to fall through to id, got %q", got)
	}
}

// captureMutationEmitter is an Emitter that snapshots every emit so
// the dispatch-path tests below can assert what ResourceID the SDK
// actually puts on the wire.
type captureMutationEmitter struct{ events []events.MutationEvent }

func (c *captureMutationEmitter) Emit(_ context.Context, ev events.MutationEvent) error {
	c.events = append(c.events, ev)
	return nil
}

// silentReconciler implements Reconciler[map[string]any, map[string]any]
// and accepts whatever ref/desired the SDK threads through. It is
// intentionally minimal — we care about what the SDK emits, not what
// the reconciler does.
type silentReconciler struct{}

func (silentReconciler) Ensure(_ context.Context, _ map[string]any) (map[string]any, error) {
	return map[string]any{}, nil
}
func (silentReconciler) Observe(_ context.Context, _ map[string]any) ([]map[string]any, string, error) {
	return nil, "", nil
}
func (silentReconciler) Destroy(_ context.Context, _ string) error { return nil }

// dispatchDestroy is a helper that drives the SDK's full execute
// pipeline (matching how a real adapter sees a destroy envelope).
func dispatchDestroy(t *testing.T, resource, resourceType string, input map[string]any) []events.MutationEvent {
	t.Helper()
	em := &captureMutationEmitter{}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	RegisterReconciler[map[string]any, map[string]any](
		a, resource, resourceType, silentReconciler{},
		WithEmitter(em),
		WithProvider("test"),
	)
	body, _ := json.Marshal(map[string]any{
		"operation": "destroy_" + resource,
		"input":     input,
	})
	if _, _, err := Dispatch(context.Background(), a, rpc.Delivery{Body: body, ContentType: "application/json"}); err != nil {
		t.Fatalf("destroy dispatch failed: %v", err)
	}
	return em.events
}

// TestDestroyDispatch_EmitsResourceID_FromExplicitRef verifies the
// happy-path canonical {"ref":"..."} keeps emitting the right
// ResourceID — backward compat with pre-v0.8.1 callers.
func TestDestroyDispatch_EmitsResourceID_FromExplicitRef(t *testing.T) {
	ev := dispatchDestroy(t, "widget", "widgets", map[string]any{"ref": "w-123"})
	if len(ev) != 1 {
		t.Fatalf("expected 1 emit, got %d", len(ev))
	}
	if ev[0].ResourceID != "w-123" {
		t.Fatalf("expected ResourceID=w-123, got %q", ev[0].ResourceID)
	}
}

// TestDestroyDispatch_EmitsResourceID_FromChannelID exercises the
// slack pattern — destroy_channel sends {"channel_id":"C123"}. Before
// the v0.8.1 fix this emitted ResourceID="" and yggdrasil-core
// rejected with HTTP 400.
func TestDestroyDispatch_EmitsResourceID_FromChannelID(t *testing.T) {
	ev := dispatchDestroy(t, "channel", "channels", map[string]any{"channel_id": "C123"})
	if len(ev) != 1 {
		t.Fatalf("expected 1 emit, got %d", len(ev))
	}
	if ev[0].ResourceID != "C123" {
		t.Fatalf("expected ResourceID=C123 (inferred from channel_id), got %q", ev[0].ResourceID)
	}
}

// TestDestroyDispatch_EmitsResourceID_FromCustomerID exercises the
// stripe pattern — destroy_customer sends {"customer_id":"cus_abc"}.
func TestDestroyDispatch_EmitsResourceID_FromCustomerID(t *testing.T) {
	ev := dispatchDestroy(t, "customer", "customers", map[string]any{"customer_id": "cus_abc"})
	if len(ev) != 1 {
		t.Fatalf("expected 1 emit, got %d", len(ev))
	}
	if ev[0].ResourceID != "cus_abc" {
		t.Fatalf("expected ResourceID=cus_abc (inferred from customer_id), got %q", ev[0].ResourceID)
	}
}

// TestDestroyDispatch_EmitsResourceID_FromOwnerRepo exercises the
// github pattern — destroy_repository sends {"owner":"...","repo":"..."}.
func TestDestroyDispatch_EmitsResourceID_FromOwnerRepo(t *testing.T) {
	ev := dispatchDestroy(t, "repository", "repositories", map[string]any{
		"owner": "dakasa-yggdrasil", "repo": "x",
	})
	if len(ev) != 1 {
		t.Fatalf("expected 1 emit, got %d", len(ev))
	}
	if ev[0].ResourceID != "dakasa-yggdrasil/x" {
		t.Fatalf("expected ResourceID=dakasa-yggdrasil/x (inferred from owner+repo), got %q", ev[0].ResourceID)
	}
}

// TestDestroyDispatch_EmitsResourceID_FromID exercises the grafana
// pattern — destroy_folder sends {"id":"uuid"}.
func TestDestroyDispatch_EmitsResourceID_FromID(t *testing.T) {
	ev := dispatchDestroy(t, "folder", "folders", map[string]any{"id": "uuid-abc"})
	if len(ev) != 1 {
		t.Fatalf("expected 1 emit, got %d", len(ev))
	}
	if ev[0].ResourceID != "uuid-abc" {
		t.Fatalf("expected ResourceID=uuid-abc (inferred from id), got %q", ev[0].ResourceID)
	}
}
