package events_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sdk/events"
)

func TestMutationEvent_SerializesToConventionalShape(t *testing.T) {
	e := events.MutationEvent{
		EventType:   "stripe.customer.ensured",
		Provider:    "stripe",
		Resource:    "customer",
		Verb:        events.VerbEnsured,
		ResourceID:  "cus_1234abc",
		InstanceID:  "stripe-acme",
		Idempotency: "ensure_customer_acme_abc",
		Observed:    json.RawMessage(`{"id":"cus_1234abc","email":"a@x"}`),
		EmittedAt:   time.Date(2026, 5, 27, 10, 30, 0, 0, time.UTC),
	}

	body, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out := string(body)

	for _, want := range []string{
		`"event_type":"stripe.customer.ensured"`,
		`"provider":"stripe"`,
		`"resource":"customer"`,
		`"verb":"ensured"`,
		`"resource_id":"cus_1234abc"`,
		`"instance_id":"stripe-acme"`,
		`"idempotency":"ensure_customer_acme_abc"`,
		`"observed":{"id":"cus_1234abc","email":"a@x"}`,
		`"emitted_at":"2026-05-27T10:30:00Z"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("expected payload to contain %q; got %s", want, out)
		}
	}
}

func TestVerbConstants_StringValues(t *testing.T) {
	tests := []struct {
		verb events.Verb
		want string
	}{
		{events.VerbEnsured, "ensured"},
		{events.VerbDestroyed, "destroyed"},
		{events.VerbCreated, "created"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if string(tc.verb) != tc.want {
				t.Fatalf("expected verb %q, got %q", tc.want, string(tc.verb))
			}
		})
	}
}

func TestBuildEventType_Convention(t *testing.T) {
	tests := []struct {
		provider, resource string
		verb               events.Verb
		want               string
	}{
		{"stripe", "customer", events.VerbEnsured, "stripe.customer.ensured"},
		{"efi", "charge", events.VerbDestroyed, "efi.charge.destroyed"},
		{"efi", "payout", events.VerbCreated, "efi.payout.created"},
		{"github", "team_membership", events.VerbDestroyed, "github.team_membership.destroyed"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			got := events.BuildEventType(tc.provider, tc.resource, tc.verb)
			if got != tc.want {
				t.Fatalf("BuildEventType(%q,%q,%q) = %q; want %q",
					tc.provider, tc.resource, tc.verb, got, tc.want)
			}
		})
	}
}

func TestNoopEmitter_LogsWarn(t *testing.T) {
	var warnings []string
	logger := func(format string, args ...any) {
		warnings = append(warnings, format)
	}
	n := &events.NoopEmitter{Logger: logger}
	err := n.Emit(context.Background(), events.MutationEvent{EventType: "stripe.customer.ensured"})
	if err != nil {
		t.Fatalf("NoopEmitter.Emit returned err: %v", err)
	}
	if len(warnings) != 1 {
		t.Fatalf("expected exactly 1 WARN log, got %d", len(warnings))
	}
	if !strings.Contains(warnings[0], "suppressed") && !strings.Contains(warnings[0], "noop") {
		t.Fatalf("expected WARN to mention noop/suppression, got %q", warnings[0])
	}
}

func TestNoopEmitter_DefaultLoggerSafe(t *testing.T) {
	// Nil Logger must not panic — fall back to log.Printf.
	n := &events.NoopEmitter{}
	if err := n.Emit(context.Background(), events.MutationEvent{EventType: "x"}); err != nil {
		t.Fatalf("default-logger NoopEmitter returned err: %v", err)
	}
}

func TestEmitter_InterfaceShape(t *testing.T) {
	// Compile-time proof that NoopEmitter satisfies events.Emitter.
	var _ events.Emitter = (*events.NoopEmitter)(nil)
}
