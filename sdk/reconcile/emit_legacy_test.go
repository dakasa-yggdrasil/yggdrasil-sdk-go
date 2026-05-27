package reconcile_test

import (
	"strings"
	"testing"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/adapter"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sdk/events"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sdk/reconcile"
)

// Legacy capability invocations route through the canonical handler
// — so they MUST also trigger auto-emission. Otherwise adapters
// migrating away from old names would silently break their audit
// trail during the cut-over window.
func TestRegisterReconciler_LegacyAliasAlsoEmits(t *testing.T) {
	em := &captureEmitter{}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec,
		reconcile.WithEmitter(em),
		reconcile.WithProvider("stripe"),
		reconcile.WithInstanceID("stripe-acme"),
		reconcile.WithLegacyNames("create_user"),
		reconcile.WithWarnLogger(func(format string, args ...any) {}),
	)

	body, err := callExecuteHandler(t, a, "create_user", map[string]any{
		"login": "alice", "email": "a@x", "role": "Viewer",
	})
	if err != nil {
		t.Fatalf("legacy create_user dispatch failed: %v", err)
	}
	if !strings.Contains(string(body), "u_alice") {
		t.Fatalf("expected legacy invocation to produce 'u_alice', got %s", body)
	}

	emitted := em.snapshot()
	if len(emitted) != 1 {
		t.Fatalf("expected exactly 1 emit through legacy alias, got %d", len(emitted))
	}
	if emitted[0].EventType != "stripe.user.ensured" {
		t.Fatalf("expected canonical event_type from legacy alias, got %q", emitted[0].EventType)
	}
	if emitted[0].Verb != events.VerbEnsured {
		t.Fatalf("expected verb=ensured, got %q", emitted[0].Verb)
	}
}

// Verify the legacy DELETE alias also fires the destroy emission.
func TestRegisterReconciler_LegacyDestroyAliasEmits(t *testing.T) {
	em := &captureEmitter{}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{
		"u_a": {ID: "u_a", Login: "a"},
	}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec,
		reconcile.WithEmitter(em),
		reconcile.WithProvider("stripe"),
		reconcile.WithLegacyNames("delete_user"),
		reconcile.WithWarnLogger(func(format string, args ...any) {}),
	)

	if _, err := callExecuteHandler(t, a, "delete_user", map[string]any{"ref": "u_a"}); err != nil {
		t.Fatalf("legacy delete_user dispatch failed: %v", err)
	}

	emitted := em.snapshot()
	if len(emitted) != 1 {
		t.Fatalf("expected exactly 1 emit through legacy destroy alias, got %d", len(emitted))
	}
	if emitted[0].EventType != "stripe.user.destroyed" {
		t.Fatalf("expected stripe.user.destroyed, got %q", emitted[0].EventType)
	}
	if emitted[0].ResourceID != "u_a" {
		t.Fatalf("expected ResourceID=u_a, got %q", emitted[0].ResourceID)
	}
}
