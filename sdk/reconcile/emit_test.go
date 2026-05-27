package reconcile_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/adapter"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sdk/events"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sdk/reconcile"
)

func newDelivery(body []byte) rpc.Delivery {
	return rpc.Delivery{Body: body, ContentType: "application/json"}
}

// captureEmitter records every MutationEvent emit-attempt so tests can
// assert auto-emission behavior.
type captureEmitter struct {
	mu       sync.Mutex
	events   []events.MutationEvent
	failWith error
}

func (c *captureEmitter) Emit(_ context.Context, e events.MutationEvent) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
	return c.failWith
}

func (c *captureEmitter) snapshot() []events.MutationEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]events.MutationEvent, len(c.events))
	copy(out, c.events)
	return out
}

// failingReconciler exercises the "no-emit-on-error" path.
type failingReconciler struct{ *fakeUserReconciler }

func (f *failingReconciler) Ensure(ctx context.Context, d userDesired) (userObserved, error) {
	return userObserved{}, errors.New("provider exploded")
}

func TestRegisterReconciler_WithEmitter_EmitsOnEnsureSuccess(t *testing.T) {
	em := &captureEmitter{}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec,
		reconcile.WithEmitter(em),
		reconcile.WithProvider("stripe"),
		reconcile.WithInstanceID("stripe-acme"),
	)

	if _, err := callExecuteHandler(t, a, "ensure_user", map[string]any{
		"login": "alice", "email": "alice@dakasa.me", "role": "Editor",
	}); err != nil {
		t.Fatalf("ensure_user dispatch failed: %v", err)
	}

	emitted := em.snapshot()
	if len(emitted) != 1 {
		t.Fatalf("expected exactly 1 emitted event, got %d", len(emitted))
	}
	got := emitted[0]
	if got.EventType != "stripe.user.ensured" {
		t.Errorf("EventType: got %q, want stripe.user.ensured", got.EventType)
	}
	if got.Provider != "stripe" {
		t.Errorf("Provider: got %q, want stripe", got.Provider)
	}
	if got.Resource != "user" {
		t.Errorf("Resource: got %q, want user", got.Resource)
	}
	if got.Verb != events.VerbEnsured {
		t.Errorf("Verb: got %q, want ensured", got.Verb)
	}
	if got.InstanceID != "stripe-acme" {
		t.Errorf("InstanceID: got %q, want stripe-acme", got.InstanceID)
	}
	if len(got.Observed) == 0 {
		t.Fatal("expected Observed to be populated with marshalled state")
	}
	var observed map[string]any
	if err := json.Unmarshal(got.Observed, &observed); err != nil {
		t.Fatalf("Observed JSON: %v", err)
	}
	if observed["id"] != "u_alice" {
		t.Errorf("Observed.id: got %v, want u_alice", observed["id"])
	}
	if id, _ := observed["id"].(string); id != got.ResourceID {
		t.Errorf("ResourceID %q != Observed.id %q — auto-inference broken", got.ResourceID, id)
	}
}

func TestRegisterReconciler_WithEmitter_EmitsOnDestroySuccess(t *testing.T) {
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

	if _, err := callExecuteHandler(t, a, "destroy_user", map[string]any{"ref": "u_a"}); err != nil {
		t.Fatalf("destroy_user dispatch failed: %v", err)
	}

	emitted := em.snapshot()
	if len(emitted) != 1 {
		t.Fatalf("expected exactly 1 emitted event, got %d", len(emitted))
	}
	got := emitted[0]
	if got.EventType != "stripe.user.destroyed" {
		t.Errorf("EventType: got %q, want stripe.user.destroyed", got.EventType)
	}
	if got.Verb != events.VerbDestroyed {
		t.Errorf("Verb: got %q, want destroyed", got.Verb)
	}
	if got.ResourceID != "u_a" {
		t.Errorf("ResourceID: got %q, want u_a (destroy ref)", got.ResourceID)
	}
}

func TestRegisterReconciler_WithEmitter_NoEmitOnEnsureFailure(t *testing.T) {
	em := &captureEmitter{}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &failingReconciler{fakeUserReconciler: &fakeUserReconciler{users: map[string]userObserved{}}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec,
		reconcile.WithEmitter(em),
		reconcile.WithProvider("stripe"),
	)

	_, err := callExecuteHandler(t, a, "ensure_user", map[string]any{
		"login": "alice", "email": "a@x", "role": "Viewer",
	})
	if err == nil {
		t.Fatal("expected ensure_user to surface reconciler error")
	}
	emitted := em.snapshot()
	if len(emitted) != 0 {
		t.Fatalf("expected NO events on failure; got %d (events=%+v)", len(emitted), emitted)
	}
}

func TestRegisterReconciler_WithEmitter_NoEmitOnObserve(t *testing.T) {
	em := &captureEmitter{}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{
		"u_a": {ID: "u_a", Login: "a"},
	}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec,
		reconcile.WithEmitter(em),
		reconcile.WithProvider("stripe"),
	)

	if _, err := callExecuteHandler(t, a, "observe_users", map[string]any{}); err != nil {
		t.Fatalf("observe dispatch failed: %v", err)
	}
	if got := em.snapshot(); len(got) != 0 {
		t.Fatalf("observe MUST NOT emit; got %d events", len(got))
	}
}

func TestRegisterReconciler_WithEmitter_BestEffortEmitFailureDoesNotBlockCall(t *testing.T) {
	em := &captureEmitter{failWith: errors.New("event bus down")}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{}}

	var warns []string
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec,
		reconcile.WithEmitter(em),
		reconcile.WithProvider("stripe"),
		reconcile.WithWarnLogger(func(format string, args ...any) {
			warns = append(warns, format)
		}),
	)

	body, err := callExecuteHandler(t, a, "ensure_user", map[string]any{
		"login": "alice", "email": "a@x", "role": "Viewer",
	})
	if err != nil {
		t.Fatalf("expected ensure_user to succeed despite emit failure: %v", err)
	}
	if !strings.Contains(string(body), "u_alice") {
		t.Fatalf("expected canonical response despite emit failure; got %s", body)
	}
	// Emit was attempted (event captured) and failed; the failure
	// should have surfaced as a WARN log line.
	if len(em.snapshot()) != 1 {
		t.Fatalf("expected exactly 1 emit attempt, got %d", len(em.snapshot()))
	}
	hasWarn := false
	for _, w := range warns {
		if strings.Contains(w, "emit") {
			hasWarn = true
			break
		}
	}
	if !hasWarn {
		t.Fatalf("expected WARN log mentioning emit failure; got %v", warns)
	}
}

func TestRegisterReconciler_NoEmitter_BackwardCompat(t *testing.T) {
	// v0.5.0 adapters that don't pass WithEmitter MUST keep working;
	// no panic, no error. Optionally a single WARN per adapter
	// startup is acceptable.
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec)

	body, err := callExecuteHandler(t, a, "ensure_user", map[string]any{
		"login": "alice", "email": "a@x", "role": "Viewer",
	})
	if err != nil {
		t.Fatalf("expected ensure_user to succeed without emitter: %v", err)
	}
	if !strings.Contains(string(body), "u_alice") {
		t.Fatalf("expected canonical response without emitter; got %s", body)
	}
}

func TestRegisterReconciler_WithEmitter_IdempotencyKeyFromRequest(t *testing.T) {
	// When the input includes an "idempotency" field, the emitted
	// event MUST carry it forward so the downstream event_log can
	// dedup re-emissions across retries.
	em := &captureEmitter{}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec,
		reconcile.WithEmitter(em),
		reconcile.WithProvider("stripe"),
	)

	body, _ := json.Marshal(map[string]any{
		"operation":   "ensure_user",
		"idempotency": "ensure_user_alice_abc123",
		"input": map[string]any{
			"login": "alice", "email": "a@x", "role": "Viewer",
		},
	})
	// We need to drive the execute handler with a richer envelope so
	// the idempotency key on the request travels into the event. Use
	// the same callExecuteHandler wrapper indirectly by reaching into
	// the test exposure.
	respBody, _, err := reconcile.ExecuteForTest(context.Background(),
		a, newDelivery(body))
	if err != nil {
		t.Fatalf("dispatch err: %v", err)
	}
	if !strings.Contains(string(respBody), "u_alice") {
		t.Fatalf("expected u_alice in response; got %s", respBody)
	}
	emitted := em.snapshot()
	if len(emitted) != 1 {
		t.Fatalf("expected 1 emit, got %d", len(emitted))
	}
	if emitted[0].Idempotency != "ensure_user_alice_abc123" {
		t.Fatalf("expected Idempotency carried forward; got %q", emitted[0].Idempotency)
	}
}

// TestRegisterReconciler_WithEmitter_IdempotencyKeySynthesizedWhenAbsent
// covers v0.8.2: when the inbound execute envelope omits the
// idempotency field (the realistic shape — adapter-initiated destroys
// have no caller-supplied dedup key), the SDK MUST synthesize one
// rather than emit with an empty Idempotency.
//
// Yggdrasil-core's validator at controllers/httpapi/events_publish.go:247
// rejects mutation events with empty idempotency (HTTP 400). The
// synthesized key MUST be deterministic-enough that an at-least-once
// retry within the same nanosecond hashes identically (the downstream
// event_log dedups on it).
func TestRegisterReconciler_WithEmitter_IdempotencyKeySynthesizedWhenAbsent(t *testing.T) {
	em := &captureEmitter{}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec,
		reconcile.WithEmitter(em),
		reconcile.WithProvider("stripe"),
	)

	// Realistic destroy envelope: no idempotency key, just the ref.
	body, _ := json.Marshal(map[string]any{
		"operation": "destroy_user",
		"input": map[string]any{
			"ref": "u_alice",
		},
	})
	if _, _, err := reconcile.ExecuteForTest(context.Background(), a, newDelivery(body)); err != nil {
		t.Fatalf("dispatch err: %v", err)
	}
	emitted := em.snapshot()
	if len(emitted) != 1 {
		t.Fatalf("expected 1 emit, got %d", len(emitted))
	}
	got := emitted[0].Idempotency
	if got == "" {
		t.Fatalf("Idempotency was empty; the SDK MUST synthesize one when the envelope omits it (yggdrasil-core rejects empty)")
	}
	// Shape sanity: <provider>.<resource>.<verb>.<resource_id>.<hash>
	wantPrefix := "stripe.user.destroyed.u_alice."
	if !strings.HasPrefix(got, wantPrefix) {
		t.Fatalf("Idempotency %q does not start with %q (provider.resource.verb.resource_id.hash)", got, wantPrefix)
	}
	if len(got) <= len(wantPrefix) {
		t.Fatalf("Idempotency %q has no hash suffix", got)
	}
}

// TestRegisterReconciler_WithEmitter_IdempotencyKeySynthesizedForEnsure
// mirrors the destroy synthesis test for the ensure path so the fix
// covers BOTH verbs (the bug was reported on destroy because the
// adapter-initiated path is more common there, but the ensure path
// is equally affected when callers don't pass idempotency).
func TestRegisterReconciler_WithEmitter_IdempotencyKeySynthesizedForEnsure(t *testing.T) {
	em := &captureEmitter{}
	a := adapter.New(adapter.Config{Provider: "test", IntegrationType: "test"})
	rec := &fakeUserReconciler{users: map[string]userObserved{}}
	reconcile.RegisterReconciler[userDesired, userObserved](a, "user", "users", rec,
		reconcile.WithEmitter(em),
		reconcile.WithProvider("stripe"),
	)
	body, _ := json.Marshal(map[string]any{
		"operation": "ensure_user",
		"input": map[string]any{
			"login": "alice", "email": "a@x", "role": "Viewer",
		},
	})
	if _, _, err := reconcile.ExecuteForTest(context.Background(), a, newDelivery(body)); err != nil {
		t.Fatalf("dispatch err: %v", err)
	}
	emitted := em.snapshot()
	if len(emitted) != 1 {
		t.Fatalf("expected 1 emit, got %d", len(emitted))
	}
	if emitted[0].Idempotency == "" {
		t.Fatalf("ensure path also requires synthesized idempotency key when caller omits one")
	}
	if !strings.HasPrefix(emitted[0].Idempotency, "stripe.user.ensured.") {
		t.Fatalf("Idempotency %q lacks expected ensured-verb prefix", emitted[0].Idempotency)
	}
}
