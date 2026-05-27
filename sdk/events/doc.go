// Package events expresses the §6.5 Golden Rule of the Yggdrasil
// integration contract as a typed Go API: every ensure_<resource>
// and destroy_<resource> capability MUST emit a MutationEvent after
// the underlying operation succeeds. The event is what makes the
// mutation auditable beyond workflow_run and what allows other
// adapters and workflows to react.
//
// # Event naming
//
// MutationEvent.EventType is "<provider>.<resource>.<verb_past>",
// e.g. "stripe.customer.ensured", "efi.charge.destroyed",
// "github.team_membership.destroyed". BuildEventType helps callers
// derive the dotted form from its components.
//
// # Verbs
//
//   - VerbEnsured   — successful ensure_<resource> outcome.
//   - VerbDestroyed — successful destroy_<resource> outcome.
//   - VerbCreated   — successful money-movement action (create_payout,
//     create_refund). Reserved for non-idempotent allowlist actions —
//     ensure_ ops use VerbEnsured.
//
// # Wire
//
// HTTPEmitter posts the MutationEvent to yggdrasil-core's
// POST /api/v1/events endpoint, authenticated with the same bearer
// token adapter pods use for workflow runs (YGGDRASIL_RUN_TOKEN).
// Transient 5xx responses retry; 4xx responses are terminal.
//
// # Auto-emission via sdk/reconcile
//
// Adapter authors do NOT call Emit by hand. Pass an Emitter to
// reconcile.RegisterReconciler via reconcile.WithEmitter(emitter)
// and the SDK auto-emits the MutationEvent after each successful
// Ensure() / Destroy() invocation. Emission is best-effort: an
// emit error is logged but does NOT fail the capability call.
//
// # Disabling emission
//
// NoopEmitter satisfies Emitter without posting anywhere; every
// call logs at WARN so the suppression is visible. Useful for
// local dev or when an environment intentionally has no event bus.
//
// See INTEGRATION_CONTRACT.md §6.5 in the integration-template repo
// for the contract this package implements.
package events
