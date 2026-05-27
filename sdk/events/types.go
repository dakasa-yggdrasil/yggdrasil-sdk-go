package events

import (
	"context"
	"encoding/json"
	"log"
	"time"
)

// Verb is the past-tense action recorded in a MutationEvent. The
// universal capability convention uses past tense because the event
// records a fact that already happened — "ensured", "destroyed",
// "created" (for money-movement actions where idempotent ensure_
// semantics don't apply).
//
// See INTEGRATION_CONTRACT.md §6.5 for the naming convention.
type Verb string

const (
	// VerbEnsured marks the successful result of an ensure_<resource>
	// capability call. Idempotent declarative mutation.
	VerbEnsured Verb = "ensured"

	// VerbDestroyed marks the successful result of a destroy_<resource>
	// capability call. Idempotent terminal removal.
	VerbDestroyed Verb = "destroyed"

	// VerbCreated marks the successful result of a non-idempotent
	// money-movement action (create_payout, create_refund, ...).
	// Use VerbEnsured for declarative ensure_ ops; reserve VerbCreated
	// for the allowlisted action capabilities that aren't idempotent.
	VerbCreated Verb = "created"
)

// MutationEvent is the payload posted to yggdrasil-core's
// POST /api/v1/events endpoint after a successful ensure_*,
// destroy_*, or money-movement action capability returns.
//
// Field-tag layout matches INTEGRATION_CONTRACT.md §6.5 exactly so
// downstream consumers (event_log, MaterializeReactions, reactor
// dispatch) can parse a single canonical shape regardless of
// adapter source.
type MutationEvent struct {
	// EventType is the dotted "<provider>.<resource>.<verb>" tuple
	// (e.g. "stripe.customer.ensured"). Use BuildEventType to derive
	// it from its components.
	EventType string `json:"event_type"`

	// Provider is the integration family ("stripe", "efi", "github").
	Provider string `json:"provider"`

	// Resource is the singular resource type ("customer", "charge",
	// "team_membership").
	Resource string `json:"resource"`

	// Verb is the past-tense action — see the Verb constants above.
	Verb Verb `json:"verb"`

	// ResourceID is the provider-side stable identity (e.g.
	// "cus_1234abc" for Stripe customers).
	ResourceID string `json:"resource_id"`

	// InstanceID is the integration_instance scope (multi-tenant
	// boundary — "stripe-acme" vs "stripe-corp").
	InstanceID string `json:"instance_id"`

	// Idempotency is the dedup key for re-emissions. The downstream
	// event_log enforces uniqueness over a 24h window.
	Idempotency string `json:"idempotency"`

	// Observed is the full observed-state payload after the
	// mutation succeeded. JSON-encoded so the producer can stuff
	// any provider-specific struct in without forcing this package
	// to know its shape.
	Observed json.RawMessage `json:"observed"`

	// EmittedAt is the timestamp at which the SDK constructed this
	// event. When zero, the HTTP emitter fills it with time.Now().UTC()
	// at Emit time.
	EmittedAt time.Time `json:"emitted_at"`
}

// BuildEventType joins provider, resource, and verb into the dotted
// "<provider>.<resource>.<verb>" event_type per §6.5 of the contract.
func BuildEventType(provider, resource string, verb Verb) string {
	return provider + "." + resource + "." + string(verb)
}

// Emitter posts MutationEvents to yggdrasil-core (or any consumer
// honoring the §6.5 payload shape). Implementations:
//
//   - HTTPEmitter — production transport against yggdrasil-core's
//     POST /api/v1/events endpoint (NewHTTPEmitter).
//   - NoopEmitter — environments where emission is intentionally
//     disabled (local dev, tests of unrelated paths).
type Emitter interface {
	Emit(ctx context.Context, e MutationEvent) error
}

// NoopEmitter satisfies Emitter without posting anywhere. Every call
// logs at WARN so operators see the suppression rather than wondering
// why downstream reactors never fire.
type NoopEmitter struct {
	// Logger receives the WARN line. When nil, the emitter falls
	// back to log.Printf to keep zero-value NoopEmitter usable.
	Logger func(format string, args ...any)
}

// Emit logs a WARN noting that the event was suppressed and returns
// nil. Never blocks, never fails — by design.
func (n *NoopEmitter) Emit(_ context.Context, e MutationEvent) error {
	logger := n.Logger
	if logger == nil {
		logger = func(format string, args ...any) {
			log.Printf("WARN "+format, args...)
		}
	}
	logger("events: noop emitter suppressed mutation event %q (resource_id=%q instance_id=%q)",
		e.EventType, e.ResourceID, e.InstanceID)
	return nil
}
