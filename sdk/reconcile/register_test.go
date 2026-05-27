package reconcile_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/adapter"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sdk/reconcile"
)

// callExecuteHandler invokes the adapter's registered "execute"
// capability handler with an operation field + input payload,
// returning the JSON response body so tests can assert on it.
func callExecuteHandler(t *testing.T, a *adapter.Adapter, operation string, input map[string]any) ([]byte, error) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"operation": operation,
		"input":     input,
	})
	d := rpc.Delivery{Body: body, ContentType: "application/json"}
	// reconcile.ExecuteForTest is the test-only export the package
	// provides so tests can invoke the synthesized dispatch path
	// without booting a transport.
	respBody, _, err := reconcile.ExecuteForTest(context.Background(), a, d)
	return respBody, err
}

func TestRegisterReconciler_EnsureDispatch(t *testing.T) {
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec)

	body, err := callExecuteHandler(t, a, "ensure_user", map[string]any{
		"login": "alice", "email": "alice@dakasa.me", "role": "Editor",
	})
	if err != nil {
		t.Fatalf("ensure_user dispatch failed: %v", err)
	}
	if !strings.Contains(string(body), "u_alice") {
		t.Fatalf("expected ID 'u_alice' in response, got %s", body)
	}
}

func TestRegisterReconciler_ObserveDispatch(t *testing.T) {
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{
		"u_a": {ID: "u_a", Login: "a"},
	}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec)

	body, err := callExecuteHandler(t, a, "observe_users", map[string]any{})
	if err != nil {
		t.Fatalf("observe_users dispatch failed: %v", err)
	}
	if !strings.Contains(string(body), "u_a") {
		t.Fatalf("expected 'u_a' in observe response, got %s", body)
	}
}

func TestRegisterReconciler_DestroyDispatch(t *testing.T) {
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{
		"u_a": {ID: "u_a", Login: "a"},
	}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec)

	body, err := callExecuteHandler(t, a, "destroy_user", map[string]any{"ref": "u_a"})
	if err != nil {
		t.Fatalf("destroy_user dispatch failed: %v", err)
	}
	if !strings.Contains(string(body), `"deleted":true`) {
		t.Fatalf("expected deleted:true in response, got %s", body)
	}
	if _, present := rec.users["u_a"]; present {
		t.Fatal("expected user 'u_a' to be removed from fake store")
	}
}
