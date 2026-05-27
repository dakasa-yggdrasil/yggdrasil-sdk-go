package reconcile_test

import (
	"context"
	"testing"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sdk/reconcile"
)

// userDesired is the desired-state payload used by the test
// Reconciler fixture. It models the minimal Grafana user payload
// the convention spec uses as its running example.
type userDesired struct {
	Login string `json:"login"`
	Email string `json:"email"`
	Role  string `json:"role"`
}

// userObserved is the observed-state payload — the provider's view
// of the user, including a stable ID.
type userObserved struct {
	ID    string `json:"id"`
	Login string `json:"login"`
	Email string `json:"email"`
	Role  string `json:"role"`
}

// fakeUserReconciler is the in-memory implementation used to verify
// the Reconciler[D, O] interface is satisfiable from outside the
// package (the public API surface).
type fakeUserReconciler struct {
	users map[string]userObserved
}

func (r *fakeUserReconciler) Ensure(ctx context.Context, d userDesired) (userObserved, error) {
	obs := userObserved{ID: "u_" + d.Login, Login: d.Login, Email: d.Email, Role: d.Role}
	r.users[obs.ID] = obs
	return obs, nil
}

func (r *fakeUserReconciler) Observe(ctx context.Context, filter map[string]any) ([]userObserved, string, error) {
	out := make([]userObserved, 0, len(r.users))
	for _, u := range r.users {
		out = append(out, u)
	}
	return out, "", nil
}

func (r *fakeUserReconciler) Destroy(ctx context.Context, ref string) error {
	delete(r.users, ref)
	return nil
}

func TestReconciler_InterfaceShape(t *testing.T) {
	// This file must compile — that alone proves Reconciler[D, O]
	// is exported with the expected method set.
	var _ reconcile.Reconciler[userDesired, userObserved] = &fakeUserReconciler{users: map[string]userObserved{}}
}

// fakeUserDiscoverer adds discover_ semantics to fakeUserReconciler.
type fakeUserDiscoverer struct{ *fakeUserReconciler }

func (d *fakeUserDiscoverer) Discover(ctx context.Context, scope map[string]any) ([]userObserved, error) {
	out := make([]userObserved, 0, len(d.users))
	for _, u := range d.users {
		out = append(out, u)
	}
	return out, nil
}

func TestDiscoverer_InterfaceShape(t *testing.T) {
	var _ reconcile.Discoverer[userObserved] = &fakeUserDiscoverer{
		fakeUserReconciler: &fakeUserReconciler{users: map[string]userObserved{}},
	}
}
