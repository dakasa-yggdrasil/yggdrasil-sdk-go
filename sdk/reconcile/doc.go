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
//
// # Production wiring (v0.7.0+)
//
// reconcile.Dispatch is the public entry point adapters wire into
// their controllers/message ExecuteHandler so production traffic
// flows through the SDK dispatch path — activating §6.5 auto-emission
// for every operator request, not just tests:
//
//	// In main.go:
//	a := adapter.New(adapter.Config{Provider: "stripe", ...})
//	reconcile.RegisterReconciler(a, "customer", "customers", customerR,
//	    reconcile.WithEmitter(emitter),
//	    reconcile.WithProvider("stripe"),
//	    reconcile.WithInstanceID(instanceID),
//	)
//
//	// In controllers/message/execute.go::ExecuteHandler:
//	func ExecuteHandler(...) Handler {
//	    return func(ctx context.Context, d rpc.Delivery) ([]byte, string, error) {
//	        // ...auth, logging, capability normalize...
//	        return reconcile.Dispatch(ctx, a, d)  // ← SDK auto-emits here
//	    }
//	}
//
// adapter.Adapter.Register is last-write-wins. RegisterReconciler
// auto-installs an "execute" handler on first call per adapter; any
// subsequent a.Register("execute", legacyHandler) call clobbers the
// SDK's handler and silently disables §6.5 emission. The supported
// patterns are: (1) register a single custom execute handler that
// internally calls reconcile.Dispatch; or (2) skip a.Register("execute")
// entirely and rely on the auto-installed handler.
//
// ExecuteForTest remains as a deprecated alias delegating to Dispatch
// for one minor cycle; removed at v1.0.0.
//
// # DestroyWithDesired — env-aware destruction (v0.8.0+)
//
// The base Reconciler[D, O].Destroy(ctx, ref) signature receives only
// the ref string. That's the right canonical shape for destruction by
// stable provider-side identity — but it does NOT see the reserved
// bridge keys (__instance_credentials / __instance_config /
// __request_auth) that adapter ExecuteHandlers stash into the desired
// payload to forward per-request auth context through the SDK
// transport (per INTEGRATION_CONTRACT.md §5.b op-echo cycle). For
// adapters that load credentials per-request — the dominant pattern
// across the ecosystem — destroy_* operations therefore lose auth
// context unless the reconciler can see the full desired map.
//
// DestroyWithDesired[D] closes this gap. When a Reconciler[D, O]
// ALSO implements DestroyWithDesired[D], the SDK dispatch path
// prefers DestroyWithDesired over Destroy and passes the FULL
// unmarshalled desired payload to it:
//
//	type repoReconciler struct{ dispatch dispatchFn; instanceID string }
//
//	// Legacy — kept for backward compat / direct callers.
//	func (r *repoReconciler) Destroy(ctx context.Context, ref string) error {
//	    _, err := r.dispatch(OperationDestroyRepository, r.instanceID, payload{"ref": ref})
//	    return err
//	}
//
//	// Env-aware — preferred by SDK v0.8.0 dispatch. Sees reserved
//	// bridge keys (__instance_credentials etc) so the dispatch helper
//	// can resolve auth context the same way ensure_* and observe_* do.
//	func (r *repoReconciler) DestroyWithDesired(ctx context.Context, ref string, desired payload) error {
//	    if desired == nil { desired = payload{} }
//	    desired["ref"] = ref
//	    _, err := r.dispatch(OperationDestroyRepository, instanceFromPayload(desired, r.instanceID), desired)
//	    return err
//	}
//
// Backward compat: this interface is OPT-IN. Reconcilers that don't
// need credentials in destroy continue implementing only Destroy and
// keep building unchanged.
package reconcile
