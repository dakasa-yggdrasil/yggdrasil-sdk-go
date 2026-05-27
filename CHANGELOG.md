# Changelog

All notable changes to `yggdrasil-sdk-go` are documented here. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project adheres to [Semantic Versioning](https://semver.org/).

## [v0.8.5] - 2026-05-27

Patch release. Polishes the v0.8.4 numeric coercion: integer-valued
floats now format as integers (`"1234567890"`) instead of scientific
notation (`"1.234567890e+09"`). Same precedence and same set of
shapes are still accepted — only the rendered string changes for
large integer IDs.

### Fixed

- **`sdk/reconcile.coerceToNonEmptyString`** — float64 branch now
  formats integer-valued values via `%d`, falling back to `%g` only
  for true fractions. github.repository.ensured event_log
  `aggregate_id` is now `"1234567890"` instead of
  `"1.23456789e+09"`.

## [v0.8.4] - 2026-05-27

Patch release. Closes the final gap in the v0.8.x §6.5 emission
hardening cycle: github ensure_repository responses carry `id` as a
JSON number (the upstream GitHub API integer), not a string. The
v0.8.3 type assertion `v.(string)` returned false on the numeric
shape and inference fell through to "" — and
`github.repository.ensured` still emitted with empty resource_id,
rejected by yggdrasil-core's validator. Adds (a) numeric coercion
across the canonical and scoped rungs, plus (b) a `full_name`
fallback for the github-specific `{owner: {login}, full_name:
"x/y"}` response shape where the flat `owner+repo` composite
doesn't apply.

### Fixed

- **`sdk/reconcile.inferResourceID`** — new `coerceToNonEmptyString`
  helper tolerates string, json.Number, float64, int, int64. Applied
  to both the canonical `id`/`ID`/`Id` rung and the scoped
  `<resource>_id` rung. Empty / zero values fall through (the caller
  walks to the next rung).
- **New rung between composite owner+repo and named-after-resource**:
  `full_name` — covers the github upstream shape where `owner` is an
  object (`{login: "..."}`) so the rung-3 composite doesn't apply but
  `full_name` carries the canonical "owner/repo" identity.
- 5 new unit tests in `sdk/reconcile/ensure_resource_id_test.go`:
  - `TestInferResourceID_NumericID` (github int id)
  - `TestInferResourceID_GithubFullNameComposite` (full github shape)
  - `TestInferResourceID_FullNameWhenNoID` (full_name-only fallback)
  - `TestInferResourceID_StringIDWinsOverNumericFallback` (precedence pin)

### Migration notes

No adapter code change required. Bumping the SDK pin from `v0.8.3` →
`v0.8.4` is sufficient. Adapters that already returned `id` as a
string continue to work identically (rung 1 still wins). The new
coercion only fires when `id` is a JSON number; the `full_name`
rung only fires when rungs 1-3 find nothing.

## [v0.8.3] - 2026-05-27

Patch release. Extends the ensure-path resource_id inference to
match the v0.8.1 destroy-path fix. Pre-v0.8.3, ensure responses
shaped like `{"customer_id": "cus_X"}` (stripe), `{"channel_id":
"C..."}` (slack), or `{"owner": "x", "repo": "y"}` (github) ALL
resolved to `ResourceID: ""` because `inferResourceID` only checked
the canonical `id` / `ID` / `Id` keys — and yggdrasil-core's
validator rejected the emit with HTTP 400. Combined with v0.8.2's
idempotency synthesis, BOTH ensure and destroy auto-emissions now
land in `event_log` end-to-end without any caller-side
instrumentation.

### Fixed

- **`sdk/reconcile.inferResourceID`** — signature extended from
  `(observed any, body []byte)` to `(observed any, body []byte,
  resource string)`. Lookup precedence (mirrors `inferRefFromInput`
  for consistency):
  1. Struct field `ID` / `Id` via reflect (pre-v0.8.3 behavior, preserved).
  2. JSON top-level `id` / `ID` / `Id` (pre-v0.8.3 behavior, preserved).
  3. **NEW**: `<resource>_id` (e.g. `customer_id`, `channel_id`,
     `service_invoice_id`). Empty values fall through to the next rung.
  4. **NEW**: composite `{"owner": "x", "repo": "y"}` joined as `"x/y"`
     (github symmetry with destroy).
  5. **NEW**: named-after-resource (e.g. `repository="owner/repo"`).
  6. `""` if nothing resolves — the resulting event will still 400 at
     yggdrasil-core's validator, surfacing the gap rather than silently
     swallowing it.

- 9 new unit tests in `sdk/reconcile/ensure_resource_id_test.go`
  cover the precedence matrix + edge cases (empty body, garbage
  input, canonical `id` wins over scoped, explicitly empty scoped
  field falls through).

### Migration notes

No adapter code change required. Bumping the SDK pin from `v0.8.2` →
`v0.8.3` is sufficient. Adapters whose ensure responses already
expose a canonical `id` field continue to work identically (rung 2
still wins). The new rungs only fire when rung 2 finds nothing.

## [v0.8.2] - 2026-05-27

Patch release. Closes the second emission-validation gap surfaced
during the SDK v0.8.1 follow-up smoke: every `.destroyed` event the
SDK auto-emitted carried `Idempotency: ""` because the inbound
adapter execute envelope rarely supplies one — destroy payloads
typically carry only the resource ref and (optionally) credentials.
yggdrasil-core's POST /api/v1/events validator requires non-empty
`idempotency` for every mutation event (per INTEGRATION_CONTRACT
§6.5 — "the dedup key for safe retries"), so the response was a
clean `HTTP 400: idempotency is required for mutation events`.
Combined with v0.8.1's resource_id fix, the §6.5 wire is now whole:
adapter-initiated destroys land in `event_log` end-to-end without
any caller-side instrumentation.

### Fixed

- **`sdk/reconcile`** — `emitContext.emit` now synthesizes an
  idempotency key when `env.Idempotency == ""`. The synthesized key
  follows the deterministic shape
  `<provider>.<resource>.<verb>.<resource_id>.<sha256_8_of_unixnano>`,
  which preserves dedup safety: a retried emit within the same
  nanosecond hashes identically, so downstream `event_log`
  deduplicates on it. Caller-supplied keys (`env.Idempotency != ""`)
  flow through unchanged — backward-compat with v0.8.1.
- New unit tests in `sdk/reconcile/emit_test.go`:
  - `TestRegisterReconciler_WithEmitter_IdempotencyKeySynthesizedWhenAbsent`
    (destroy path)
  - `TestRegisterReconciler_WithEmitter_IdempotencyKeySynthesizedForEnsure`
    (ensure path — equally affected when callers omit the key)
- The existing `TestRegisterReconciler_WithEmitter_IdempotencyKeyFromRequest`
  still asserts the caller-supplied key flows through verbatim.

### Migration notes

No adapter code change required. Bumping the SDK pin from `v0.8.1` →
`v0.8.2` is sufficient — the synthesis happens inside the SDK on
auto-emission. Adapters that already plumbed an idempotency key
through their custom request shaper continue to win precedence; the
fallback only fires when the inbound envelope is empty.

## [v0.8.1] - 2026-05-27

Patch release. Closes a destroy emission gap discovered during the
SDK v0.8.0 cycle Phase C smoke: every `.destroyed` event the SDK
emitted carried `resource_id: ""` because `makeDestroyFn` only
extracted ref via `{"ref": "..."}`, while real adapters send
provider-shaped payloads — `{"channel_id": "C123"}` (slack),
`{"customer_id": "cus_abc"}` (stripe), `{"owner": "x", "repo": "y"}`
(github), `{"id": "uuid"}` (grafana), etc. With an empty
`resource_id`, yggdrasil-core's POST /api/v1/events rejects with
`HTTP 400: resource_id is required` — and the §6.5 mutation event
never lands in `event_log`.

### Fixed

- **`sdk/reconcile`** — `makeDestroyFn` now falls back to a flexible
  payload scan via the new internal helper `inferRefFromInput(input,
  resource)` when `{"ref": "..."}` is absent from the destroy
  envelope. Lookup precedence:
  1. `{"ref": "..."}` (explicit canonical — preserves pre-v0.8.1 behavior)
  2. `{"<resource>_id": "..."}` (slack `channel_id`, stripe
     `customer_id`, nfeio `service_invoice_id`, etc.)
  3. `{"id": "..."}` (grafana, generic shape)
  4. `{"owner": "x", "repo": "y"}` (github composite — joined as `"x/y"`)
  5. `{"<resource>": "..."}` (e.g. `repository="owner/repo"`)
  6. `""` if no identifier resolves — the resulting event will carry
     an empty `ResourceID` and yggdrasil-core's validator will return
     a clear 400, surfacing the gap to the adapter maintainer.

  Backward compat verified by `destroy_resource_id_test.go`: 16 unit
  tests cover the matrix plus precedence rules (explicit ref beats
  scoped, scoped beats generic, empty values fall through, garbage
  input doesn't panic). Existing destroy dispatch tests
  (`destroy_with_desired_test.go`, `register_test.go`,
  `emit_test.go::TestRegisterReconciler_WithEmitter_EmitsOnDestroySuccess`)
  continue to pass — the canonical `{"ref": "..."}` path is
  unchanged.

### Impact

- All 8 adapters pinning SDK v0.8.0 inherit the fix on the next
  rebuild; no adapter source changes required.
- Pre-existing `.destroyed` events that failed silently before now
  emit with the correct `resource_id` field on the wire.
- No API surface change — `inferRefFromInput` is unexported; the
  fallback is invisible to callers.

### Compat

- Purely additive at the binary level. No public API change.
- Tag: `v0.8.1`. Adapters bump `go.mod` from `v0.8.0` and rebuild.

## [v0.8.0] - 2026-05-27

Closes the latent destroy-credential bug across the entire Yggdrasil
adapter ecosystem. The legacy `Reconciler.Destroy(ctx, ref)` signature
receives only the ref string — silently dropping the reserved bridge
keys (`__instance_credentials` / `__instance_config` / `__request_auth`)
that adapter `ExecuteHandlers` stash into the desired payload per
INTEGRATION_CONTRACT.md §5.b. v0.8.0 adds an OPT-IN `DestroyWithDesired[D]`
interface; when a reconciler implements it, the SDK dispatch path
prefers it over `Destroy` and passes the FULL parsed desired payload —
letting destroy resolve auth the same way ensure / observe do.

### Added

- **`sdk/reconcile`** — new opt-in interface:
  - `DestroyWithDesired[D any] interface { DestroyWithDesired(ctx context.Context, ref string, desired D) error }`
    — when a `Reconciler[D, O]` ALSO implements this interface, the
    SDK's `makeDestroyFn` unmarshals the full desired payload (same
    shape `Ensure` receives) and invokes `DestroyWithDesired`. This
    allows destroy implementations to extract the reserved bridge keys
    and forward them through the same dispatch helper `ensure_*` and
    `observe_*` use.
  - Backward-compat verified by `destroy_with_desired_test.go`:
    - Legacy-only reconciler (no `DestroyWithDesired`) → SDK falls
      through to `Destroy(ctx, ref)`. Verbatim v0.7.0 behavior.
    - Env-aware reconciler (implements both) → SDK prefers
      `DestroyWithDesired` and the legacy `Destroy` is NOT called.
    - Reserved keys (`__instance_credentials`, `__instance_config`,
      `__request_auth`) round-trip through the SDK into the desired
      payload `DestroyWithDesired` receives.

### Why this matters (architectural rationale)

Pre-v0.8.0 architecture relied on adapters working around the
credential-drop by bypassing `reconcile.Dispatch` for destroy
operations (e.g. `integration-github` v2.4.3 had an
`if strings.HasPrefix(op, "destroy_")` short-circuit in its
`ExecuteHandler`). That workaround is a latent bug: every new adapter
adopting SDK reconcile inherits the same blind spot the moment a
destroy capability needs credentials. v0.8.0 fixes the root cause —
the SDK now has a typed shape that supports the canonical destroy +
the env-aware destroy side by side.

### Compat

- Purely additive at the binary level. v0.7.x adapters keep building
  unchanged — `DestroyWithDesired` is opt-in; adapters that don't need
  credentials in destroy continue to implement only the base `Destroy`
  signature.
- Adapters that adopt v0.8.0 + `DestroyWithDesired` MAY remove the
  destroy bypass workarounds from their `ExecuteHandlers`. The
  `integration-github` v2.5.0 release does so.

### Migration (per adapter)

```go
// Existing reconciler (works unchanged):
func (r *channelReconciler) Destroy(ctx context.Context, ref string) error {
    _, err := r.dispatch(OperationDestroyChannel, r.instanceID, payload{"channel_id": ref})
    return err
}

// Add this method to opt into env-aware destroy:
func (r *channelReconciler) DestroyWithDesired(ctx context.Context, ref string, desired payload) error {
    if desired == nil { desired = payload{} }
    desired["channel_id"] = ref
    _, err := r.dispatch(OperationDestroyChannel, instanceFromPayload(desired, r.instanceID), desired)
    return err
}
```

The SDK invokes whichever method is present — adapters can adopt
DestroyWithDesired per-reconciler as needed without a wholesale
migration.

## [v0.7.0] - 2026-05-27

Promotes the per-adapter reconcile dispatch table to a public
production API. Adapter `ExecuteHandler`s now delegate operation
routing to `reconcile.Dispatch`, activating INTEGRATION_CONTRACT.md
§6.5 auto-emission for every operator request — not just tests.

### Added

- **`sdk/reconcile`** — new public function:
  - `Dispatch(ctx, *adapter.Adapter, rpc.Delivery) ([]byte, string, error)`
    — routes an inbound execute envelope through the adapter's
    reconcile dispatch table, invoking the registered Reconciler for
    the requested operation and emitting §6.5 mutation events on
    success when an emitter is wired (via `WithEmitter` at
    registration time).
  - Returns a clear error when the adapter has no registered
    Reconciler or the requested operation is not in the dispatch
    table — the error message names the missing op so callers can
    diagnose typos / wrong rename.
  - Package doc gains a "Production wiring (v0.7.0+)" section
    documenting the recommended adapter wiring pattern:
    `controllers/message/execute.go::ExecuteHandler` returns
    `reconcile.Dispatch(ctx, a, d)` after auth/log/normalize.

### Behavior notes

- **`adapter.Register` is last-write-wins** (see
  `adapter/adapter.go::Register` — "Duplicate registrations overwrite;
  this is deliberate so that tests can swap a mock handler in").
  `RegisterReconciler` auto-installs an `execute` handler the FIRST
  time it runs per adapter; a subsequent
  `a.Register("execute", legacyHandler)` call clobbers the SDK's
  handler and silently disables §6.5 emission.
- Supported wiring patterns: (1) register a single custom execute
  handler that internally calls `reconcile.Dispatch`; or (2) skip
  `a.Register("execute", ...)` entirely and rely on the SDK's
  auto-installed handler.

### Deprecated

- **`reconcile.ExecuteForTest`** — kept as a thin alias delegating to
  `reconcile.Dispatch` with identical semantics. Will be removed at
  `v1.0.0`. Migration is a single-line rename
  (`reconcile.ExecuteForTest` → `reconcile.Dispatch`).

### Compat

- Purely additive at the binary level. v0.6.x adapters keep building
  unchanged — `ExecuteForTest` still works.
- The `WithLegacyNames` shim deadline stays at v0.7.0 conceptually,
  but the shim itself is preserved in this release; adapters
  finishing the convention rollout retain the shim warning for one
  more cycle.

### Dependencies

- No new external dependencies. Uses stdlib only.

## [v0.6.0] - 2026-05-27

Auto-emission of mutation events. The §6.5 Golden Rule of the
Yggdrasil Integration Contract — "every successful ensure_/destroy_
MUST emit a MutationEvent" — is now satisfied by the SDK on the
adapter's behalf. Adapter authors stop writing emission boilerplate.

### Added

- **`sdk/events`** — new package:
  - `MutationEvent` — payload matching INTEGRATION_CONTRACT.md §6.5
    exactly (event_type / provider / resource / verb / resource_id /
    instance_id / idempotency / observed / emitted_at).
  - `Verb` — typed string with constants `VerbEnsured`, `VerbDestroyed`,
    `VerbCreated` (the last for non-idempotent money-movement actions
    on the contract allowlist).
  - `BuildEventType(provider, resource, verb)` — derives the dotted
    `<provider>.<resource>.<verb>` event_type so callers don't reinvent
    the format.
  - `Emitter` interface — single `Emit(ctx, MutationEvent) error`.
  - `NewHTTPEmitter(opts...)` — production transport against
    yggdrasil-core's `POST /api/v1/events` endpoint. Reads
    `YGGDRASIL_CORE_URL` + `YGGDRASIL_RUN_TOKEN` from env, posts with
    `Authorization: Bearer <token>`, retries transient 5xx with fixed
    backoff (`WithMaxRetries`/`WithRetryBackoff`), treats 4xx as
    terminal, honors `context.Context` cancellation between retries.
  - Options: `WithCoreURL`, `WithToken`, `WithHTTPClient` (for tests),
    `WithMaxRetries`, `WithRetryBackoff`, `WithEventsPath`.
  - `NoopEmitter{Logger}` — satisfies `Emitter` without posting;
    every call logs at WARN so suppression is visible. Zero-value
    usable (falls back to `log.Printf`).

- **`sdk/reconcile`** — new options on `RegisterReconciler`:
  - `WithEmitter(events.Emitter)` — wires the auto-emission path.
    After every successful `Ensure()`, SDK calls
    `emitter.Emit(MutationEvent{Verb:VerbEnsured, ...})`. After
    every successful `Destroy()`, SDK calls
    `emitter.Emit(MutationEvent{Verb:VerbDestroyed, ...})`. `Observe`
    is read-only and never emits.
  - `WithProvider(string)` — overrides the integration family written
    into `MutationEvent.EventType` / `Provider`. Defaults to the
    adapter's `Config.Provider`.
  - `WithInstanceID(string)` — sets `MutationEvent.InstanceID`
    (multi-tenant scope; resolved from the integration_instance
    label at adapter startup).

### Behavior

- **Best-effort emission.** A failure from `emitter.Emit` logs a
  WARN but does NOT fail the capability call. Adapter latency and
  availability MUST NOT depend on event bus health.
- **No emit on failure.** Ensure() / Destroy() that return an error
  do not emit — the event records facts, never claims.
- **Idempotency forwarding.** The execute envelope's optional
  `idempotency` and `instance_id` fields travel onto the emitted
  MutationEvent so the downstream event_log can dedup re-emissions
  across retries.
- **ResourceID inference.** Looked up via reflection on the observed
  struct's `ID`/`Id` field, falling back to the marshalled JSON's
  top-level `id` key. Empty when neither resolves — better than
  no event at all.

### Compat

- Purely additive. v0.5.x adapters keep building unchanged.
- Adapters that don't pass `WithEmitter` get exactly one WARN per
  adapter at startup pointing at the new option. The warning is
  routed to the stdlib log sink (not `cfg.warnLogger`) so existing
  legacy-shim tests stay deterministic.
- `WithLegacyNames` shim removal target moved from v0.6.0 to v0.7.0
  to make room for the events rollout. Adapters mid-migration keep
  working; the shim WARN message updated accordingly.

### Dependencies

- No new external dependencies. Uses stdlib only (`net/http`,
  `encoding/json`, `log`, `context`, `time`, `os`, `reflect`).

## [v0.5.0] - 2026-05-27

Additive `sdk/reconcile` package — the Go-level expression of the
Yggdrasil universal capability naming convention. Adapter authors
implement Reconciler[D, O] per managed resource type and call
RegisterReconciler to replace hand-written Execute switch
boilerplate with three named dispatch entries
(ensure_/observe_/destroy_).

### Added

- **`sdk/reconcile`** — new package:
  - `Reconciler[D, O]` — interface with `Ensure(ctx, d) (O, error)`,
    `Observe(ctx, filter) ([]O, cursor, error)`, `Destroy(ctx, ref) error`.
  - `Discoverer[O]` — optional sister interface for resources the
    adapter walks on the provider side.
  - `DriftReporter[D, O]` — helper for workflows branching on drift.
  - `RegisterReconciler[D, O](adapter, resource, resourceType, r, opts...)`
    — wires the canonical triple into the adapter's execute dispatch.
  - `WithLegacyNames(names...)` — compat shim that accepts pre-v2.0.0
    capability names alongside the canonical ones; each legacy
    invocation logs a WARN entry. Removed in v0.6.0.
  - `WithWarnLogger(fn)` — overrides the shim's log emitter (tests).

### Migration notes

- Purely additive. v0.4.x adapter binaries continue to build against
  v0.5.0 without modification.
- Adapters adopting `RegisterReconciler` typically delete their
  hand-written Execute switch in spec.go in favor of one call per
  resource type. See `integration-efi` / `integration-nfeio` /
  `integration-stripe` v2.0.0 for worked examples (this rollout's
  Phases C-E).

## [v0.4.0] - 2026-05-26

Three additive packages so the three new public adapters
(`integration-efi`, `integration-nfeio`, `integration-stripe`) do not
each ship their own copy of the same primitives.

### Added

- **`sig/hmac`** — provider-specific webhook signature verifiers:
  - `VerifyStripe(payload, header, secret, toleranceSecs) (ts int64, err error)`
    — implements the `t=<unix>,v1=<hex>` scheme with a tolerance
    window. Supports multiple `v1=` components (key rotation).
  - `VerifyHubSignature256(payload, header, secret) error` — implements
    the `X-Hub-Signature-256: sha256=<hex>` scheme used by GitHub,
    NFe.io, EFI, and other webhook providers.
  - All verifiers use `crypto/subtle.ConstantTimeCompare`.
  - Typed errors: `ErrTimestampExpired`, `ErrSignatureMismatch`,
    `ErrMissingTimestamp`, `ErrMissingV1`, `ErrMalformedHeader`,
    `ErrMalformedHubSig`.
- **`mtls`** — loads `*tls.Config` from a PKCS#12 bundle:
  - `Load(cfg Config)` with `SourceFile`, `SourceBase64`, `SourceDisabled`.
  - `LoadFromEnv(prefix)` reading `{PREFIX}_MTLS_ENABLED`,
    `{PREFIX}_CERTIFICATE`, `{PREFIX}_CERTIFICATE_BASE64`,
    `{PREFIX}_CERTIFICATE_PASSWORD`.
  - Returns `(nil, nil)` when disabled so callers can construct a
    plain HTTP client without branching.
- **`webhookhttp`** — wraps `net/http.Server` with the framing every
  webhook receiver wants:
  - `Server`, `New(Config)`, `Handle(method, path, h, opts...)`,
    `ListenAndServe(ctx)`.
  - `HandlerOption`: `WithVerifyFunc`, `WithIdempotencyKey`,
    `WithMaxBodyBytes` (default 64KB).
  - Handler return → HTTP status mapping: `nil`→202, `ErrDuplicate`→200,
    `*TerminalError`→400, other error→500.
  - Helper primitives `ParseHMACSHA256Signature` and
    `VerifyHMACSHA256Header` re-export `sig/hmac.VerifyHubSignature256`
    for one-line verifier wiring.

### Dependencies

- Added `golang.org/x/crypto v0.52.0` (used by `mtls` for PKCS#12 decode).

### Migration notes

- Purely additive — no breaking changes to v0.3.x APIs.
- v0.3.x adapter binaries continue to build against v0.4.0 without
  modification.

## [v0.3.0] - 2026-05-26

### Added

- AMQP transport: connection-level reconnect watchdog
  (`rpc/amqp/amqp.go`) — re-dials the broker on disconnect so a
  RabbitMQ restart no longer wedges every adapter pod. See memory
  `project_amqp_cascade_root_cause_2026_05_26.md`.

## [v0.2.x and earlier]

See `git log` — pre-v0.3.0 history is small and undocumented.
