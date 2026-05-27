package reconcile_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/adapter"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sdk/events"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sdk/reconcile"
)

// dispatchEnvelope builds an execute-shaped JSON delivery so tests can
// drive reconcile.Dispatch end-to-end without booting a transport.
func dispatchEnvelope(t *testing.T, op string, input map[string]any) rpc.Delivery {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"operation": op,
		"input":     input,
	})
	if err != nil {
		t.Fatalf("marshal dispatch envelope: %v", err)
	}
	return rpc.Delivery{Body: body, ContentType: "application/json"}
}

// TestDispatch_RoutesToEnsure exercises the production wiring an
// adapter's ExecuteHandler will use: call reconcile.Dispatch with an
// ensure_<resource> op and verify both the response body and the
// auto-emitted §6.5 MutationEvent are produced. Dispatch is the public
// successor of ExecuteForTest — adapters wire it directly into their
// controllers/message ExecuteHandler so production traffic flows
// through the SDK dispatch path.
func TestDispatch_RoutesToEnsure(t *testing.T) {
	em := &captureEmitter{}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec,
		reconcile.WithEmitter(em),
		reconcile.WithProvider("stripe"),
		reconcile.WithInstanceID("stripe-acme"),
	)

	body, ct, err := reconcile.Dispatch(context.Background(), a,
		dispatchEnvelope(t, "ensure_user", map[string]any{
			"login": "alice", "email": "alice@dakasa.me", "role": "Editor",
		}))
	if err != nil {
		t.Fatalf("Dispatch ensure_user: %v", err)
	}
	if ct != "application/json" {
		t.Errorf("content-type: got %q, want application/json", ct)
	}
	if !strings.Contains(string(body), "u_alice") {
		t.Fatalf("expected u_alice in body, got %s", body)
	}
	emitted := em.snapshot()
	if len(emitted) != 1 {
		t.Fatalf("expected 1 §6.5 event from ensure_user, got %d", len(emitted))
	}
	if emitted[0].EventType != "stripe.user.ensured" {
		t.Errorf("event_type: got %q, want stripe.user.ensured", emitted[0].EventType)
	}
}

// TestDispatch_RoutesToObserve confirms observe_<resource_type> flows
// through Dispatch and produces a paged response without emitting any
// §6.5 event (observe is read-only by contract).
func TestDispatch_RoutesToObserve(t *testing.T) {
	em := &captureEmitter{}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{
		"u_a": {ID: "u_a", Login: "a"},
	}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec,
		reconcile.WithEmitter(em),
		reconcile.WithProvider("stripe"),
	)

	body, _, err := reconcile.Dispatch(context.Background(), a,
		dispatchEnvelope(t, "observe_users", map[string]any{}))
	if err != nil {
		t.Fatalf("Dispatch observe_users: %v", err)
	}
	if !strings.Contains(string(body), "u_a") {
		t.Fatalf("expected u_a in observe body, got %s", body)
	}
	if got := em.snapshot(); len(got) != 0 {
		t.Fatalf("observe MUST NOT emit; got %d events", len(got))
	}
}

// TestDispatch_RoutesToDestroy confirms destroy_<resource> flows
// through Dispatch and emits the §6.5 destroyed event.
func TestDispatch_RoutesToDestroy(t *testing.T) {
	em := &captureEmitter{}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{
		"u_a": {ID: "u_a", Login: "a"},
	}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec,
		reconcile.WithEmitter(em),
		reconcile.WithProvider("stripe"),
		reconcile.WithInstanceID("stripe-acme"),
	)

	body, _, err := reconcile.Dispatch(context.Background(), a,
		dispatchEnvelope(t, "destroy_user", map[string]any{"ref": "u_a"}))
	if err != nil {
		t.Fatalf("Dispatch destroy_user: %v", err)
	}
	if !strings.Contains(string(body), `"deleted":true`) {
		t.Fatalf("expected deleted:true, got %s", body)
	}
	emitted := em.snapshot()
	if len(emitted) != 1 {
		t.Fatalf("expected 1 §6.5 event from destroy_user, got %d", len(emitted))
	}
	if emitted[0].Verb != events.VerbDestroyed {
		t.Errorf("verb: got %q, want destroyed", emitted[0].Verb)
	}
	if emitted[0].ResourceID != "u_a" {
		t.Errorf("resource_id: got %q, want u_a", emitted[0].ResourceID)
	}
}

// TestDispatch_LegacyNameRoutes verifies the legacy WithLegacyNames
// shim still works when invoked through the public Dispatch path —
// adapters mid-migration must keep working under the new API.
func TestDispatch_LegacyNameRoutes(t *testing.T) {
	em := &captureEmitter{}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec,
		reconcile.WithEmitter(em),
		reconcile.WithProvider("stripe"),
		reconcile.WithLegacyNames("create_user"),
		reconcile.WithWarnLogger(func(format string, args ...any) {}),
	)

	body, _, err := reconcile.Dispatch(context.Background(), a,
		dispatchEnvelope(t, "create_user", map[string]any{
			"login": "alice", "email": "a@x", "role": "Editor",
		}))
	if err != nil {
		t.Fatalf("Dispatch create_user (legacy): %v", err)
	}
	if !strings.Contains(string(body), "u_alice") {
		t.Fatalf("expected u_alice through legacy alias, got %s", body)
	}
	emitted := em.snapshot()
	if len(emitted) != 1 {
		t.Fatalf("expected legacy alias to still emit §6.5 event, got %d", len(emitted))
	}
	if emitted[0].EventType != "stripe.user.ensured" {
		t.Errorf("legacy alias event_type: got %q, want stripe.user.ensured", emitted[0].EventType)
	}
}

// TestDispatch_UnknownOperation surfaces a clear error when the
// operation field is not in the dispatch table — the caller sees the
// exact name they tried so they can diagnose typos / wrong rename.
func TestDispatch_UnknownOperation(t *testing.T) {
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec)

	_, _, err := reconcile.Dispatch(context.Background(), a,
		dispatchEnvelope(t, "ensure_nonexistent_thing", map[string]any{}))
	if err == nil {
		t.Fatal("expected error for unknown operation, got nil")
	}
	if !strings.Contains(err.Error(), "ensure_nonexistent_thing") {
		t.Errorf("error should name the missing op; got %v", err)
	}
}

// TestDispatch_NoReconcilerRegistered fails clearly when Dispatch is
// called against an adapter that has never wired a Reconciler —
// otherwise we'd silently nil-deref or hang.
func TestDispatch_NoReconcilerRegistered(t *testing.T) {
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})

	_, _, err := reconcile.Dispatch(context.Background(), a,
		dispatchEnvelope(t, "ensure_user", map[string]any{}))
	if err == nil {
		t.Fatal("expected error when no Reconciler registered, got nil")
	}
	if !strings.Contains(err.Error(), "no registered Reconciler") {
		t.Errorf("error should mention missing Reconciler; got %v", err)
	}
}

// TestExecuteForTest_StillWorks is the backward-compat guarantee for
// the v0.7.0 transition window: ExecuteForTest is deprecated but MUST
// continue to delegate to Dispatch with identical semantics for one
// minor cycle. Removed at v1.0.0.
func TestExecuteForTest_StillWorks(t *testing.T) {
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec)

	body, _, err := reconcile.ExecuteForTest(context.Background(), a,
		dispatchEnvelope(t, "ensure_user", map[string]any{
			"login": "alice", "email": "a@x", "role": "Editor",
		}))
	if err != nil {
		t.Fatalf("ExecuteForTest (deprecated alias) should still work: %v", err)
	}
	if !strings.Contains(string(body), "u_alice") {
		t.Fatalf("expected u_alice through deprecated alias, got %s", body)
	}
}
