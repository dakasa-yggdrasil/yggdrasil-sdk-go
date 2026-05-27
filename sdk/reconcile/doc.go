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
// Naming convention (canonical prefixes):
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
// Compat shim: WithLegacyNames lets adapter authors keep the
// pre-v2.0.0 capability names callable alongside the canonical
// names for one minor version cycle. Each legacy invocation logs
// a WARN entry. The shim is removed in SDK v0.6.0.
package reconcile
