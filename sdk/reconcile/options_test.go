package reconcile_test

import (
	"strings"
	"testing"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/adapter"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sdk/reconcile"
)

func TestWithLegacyNames_RoutesAndWarns(t *testing.T) {
	var warnings []string
	logger := func(format string, args ...any) {
		warnings = append(warnings, format)
	}

	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{}}

	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec,
		reconcile.WithLegacyNames("create_user"),
		reconcile.WithWarnLogger(logger),
	)

	body, err := callExecuteHandler(t, a, "create_user", map[string]any{
		"login": "alice", "email": "alice@dakasa.me", "role": "Editor",
	})
	if err != nil {
		t.Fatalf("legacy create_user dispatch failed: %v", err)
	}
	if !strings.Contains(string(body), "u_alice") {
		t.Fatalf("expected legacy invocation to produce 'u_alice', got %s", body)
	}
	if len(warnings) != 1 {
		t.Fatalf("expected exactly 1 WARN entry, got %d (entries=%v)", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "deprecated") {
		t.Fatalf("expected WARN message to mention 'deprecated', got %q", warnings[0])
	}
}

func TestWithLegacyNames_DestroyAliasRoutes(t *testing.T) {
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{
		"u_a": {ID: "u_a", Login: "a"},
	}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec,
		reconcile.WithLegacyNames("delete_user"),
		reconcile.WithWarnLogger(func(format string, args ...any) {}),
	)

	body, err := callExecuteHandler(t, a, "delete_user", map[string]any{"ref": "u_a"})
	if err != nil {
		t.Fatalf("legacy delete_user dispatch failed: %v", err)
	}
	if !strings.Contains(string(body), `"deleted":true`) {
		t.Fatalf("expected deleted:true, got %s", body)
	}
}
