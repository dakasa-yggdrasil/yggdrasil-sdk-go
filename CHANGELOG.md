# Changelog

All notable changes to `yggdrasil-sdk-go` are documented here. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project adheres to [Semantic Versioning](https://semver.org/).

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
