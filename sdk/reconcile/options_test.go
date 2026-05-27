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

func TestWithLegacyNames_CanonicalMappingMatrix(t *testing.T) {
	tests := []struct {
		legacy     string
		invokeWith map[string]any
		expectIn   string // substring expected in JSON response
	}{
		{"create_user", map[string]any{"login": "alice", "email": "a@x", "role": "Viewer"}, "u_alice"},
		{"update_user", map[string]any{"login": "alice", "email": "a@x", "role": "Editor"}, "u_alice"},
		{"upsert_user", map[string]any{"login": "alice", "email": "a@x", "role": "Editor"}, "u_alice"},
		{"register_user", map[string]any{"login": "alice", "email": "a@x", "role": "Viewer"}, "u_alice"},
		{"list_users", map[string]any{}, `"items"`},
		{"get_users", map[string]any{}, `"items"`},
		{"delete_user", map[string]any{"ref": "u_a"}, `"deleted":true`},
		{"unregister_user", map[string]any{"ref": "u_a"}, `"deleted":true`},
		{"cancel_user", map[string]any{"ref": "u_a"}, `"deleted":true`},
	}
	for _, tc := range tests {
		t.Run(tc.legacy, func(t *testing.T) {
			a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
			rec := &fakeUserReconciler{users: map[string]userObserved{
				"u_a": {ID: "u_a", Login: "a"},
			}}
			reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec,
				reconcile.WithLegacyNames(tc.legacy),
				reconcile.WithWarnLogger(func(format string, args ...any) {}),
			)
			body, err := callExecuteHandler(t, a, tc.legacy, tc.invokeWith)
			if err != nil {
				t.Fatalf("%s dispatch failed: %v", tc.legacy, err)
			}
			if !strings.Contains(string(body), tc.expectIn) {
				t.Fatalf("%s: expected substring %q in response, got %s", tc.legacy, tc.expectIn, body)
			}
		})
	}
}
