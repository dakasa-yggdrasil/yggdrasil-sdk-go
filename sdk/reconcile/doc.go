// Package reconcile expresses the Yggdrasil universal capability naming
// convention as a typed Go interface — one Reconciler per managed
// resource type, exposing the canonical ensure_/observe_/destroy_
// triple plus an optional discover_ enumeration.
//
// Adapter authors implement Reconciler[D, O] once per resource type
// (where D is the desired-state payload and O is the observed-state
// payload), then call RegisterReconciler to wire three named
// capability handlers into the existing *adapter.Adapter dispatch
// path. The wiring replaces the hand-written switch blocks every
// adapter currently maintains in its spec.go.
//
// # Naming convention (canonical prefixes)
//
//   - ensure_<resource>          mutating idempotent (collapses
//     create_*/update_*/upsert_*/set_*)
//   - observe_<resource_type>    read-only paged enumeration
//   - destroy_<resource>         terminal removal, 404-tolerant
//   - discover_<resource_type>   optional service-side traversal
//
// See docs/superpowers/specs/2026-05-27-yggdrasil-integration-capability-convention.md
// for the full convention.
//
// # Compat shim (v0.5.x)
//
// WithLegacyNames lets adapter authors keep the pre-v2.0.0 capability
// names callable alongside the canonical names. Each legacy
// invocation logs a WARN entry. The shim's removal target moved from
// v0.6.0 to v0.7.0 — keeping the door open for adapters still finishing
// the convention rollout.
//
// # Auto-emission (v0.6.0+)
//
// WithEmitter wires a sdk/events.Emitter into the dispatch path. The
// SDK auto-emits a MutationEvent after every successful Ensure() and
// Destroy() invocation, satisfying INTEGRATION_CONTRACT.md §6.5
// (the Golden Rule) without adapter-author boilerplate. Emission is
// best-effort: an emit error logs WARN but does NOT fail the
// capability call.
//
//	emitter := events.NewHTTPEmitter() // reads YGGDRASIL_CORE_URL + _RUN_TOKEN
//	reconcile.RegisterReconciler(a, "customer", "customers", customerR,
//	    reconcile.WithEmitter(emitter),
//	    reconcile.WithProvider("stripe"),
//	    reconcile.WithInstanceID(instanceID),
//	)
//
// Backward compatibility: v0.5.0 RegisterReconciler calls without
// WithEmitter keep working. A single WARN per adapter at startup
// points operators at the new option.
package reconcile
