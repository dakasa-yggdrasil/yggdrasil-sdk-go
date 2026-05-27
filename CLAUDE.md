# Claude Code Context: yggdrasil-sdk-go

## What this repo is

The Go SDK for **building Yggdrasil integration adapters**. Provides the
RPC transport contracts that `yggdrasil-core` speaks, plus the high-level
`adapter.Adapter` builder that handles envelope framing, transport choice
(AMQP or HTTP), lifecycle, and (since v0.3.0) broker-disconnect recovery.

Repo: `github.com/dakasa-yggdrasil/yggdrasil-sdk-go` (open source).
Consumed by **every** `dakasa-yggdrasil/integration-*` adapter (~14 today),
plus a handful of internal DaKasa adapters.

This repo ships **no binary** — it's a library. No Dockerfile, no CI
workflows. Releases are git tags; consumers pin via `go.mod`.

## Stack

- Go 1.25 (`go.mod`).
- `rabbitmq/amqp091-go v1.10.0` — AMQP transport.
- `google/uuid v1.6.0` — envelope IDs / correlation IDs.
- `gopkg.in/yaml.v3` — only used by `surface/loader.go` for manifest
  loading.
- **No** dependency on `yggdrasil-core`. The SDK speaks JSON over the
  wire; types are duplicated where necessary.

## Repo layout

```
adapter/                # High-level builder: New(Config), Register, ListenHTTP/AMQP, Run
rpc/                    # Transport interface (rpc.Transport, rpc.Delivery, rpc.Envelope)
rpc/amqp/               # AMQP 0-9-1 implementation (the production transport)
rpc/http/               # HTTP/JSON implementation (for adapters exposed as k8s Service)
surface/                # Optional console-ops surface contract (manifest, handlers, validator)
sig/hmac/               # v0.4.0: HMAC-SHA256 webhook signature verifiers (Stripe, GitHub/NFe.io/EFI)
mtls/                   # v0.4.0: Load *tls.Config from PKCS#12 bundle (file or base64)
webhookhttp/            # v0.4.0: Inbound webhook listener with HMAC + dedup + body-size primitives
sdk/reconcile/          # v0.5.0: Reconciler[D,O] interface + RegisterReconciler dispatch + WithLegacyNames compat shim
sdk/events/             # v0.6.0: MutationEvent + Emitter + NewHTTPEmitter + NoopEmitter (POST /api/v1/events)
examples/minimal/       # Minimal adapter demonstrating the SDK end-to-end
protocol/               # (currently empty)
```

## Key constructs

- `adapter.Adapter` — the builder consumers use. Returns the configured
  worker; call `.Run(ctx)` to block until ctx cancels.
- `adapter.Config{Provider, IntegrationType, Version}` — Provider is
  the family (e.g. `aws`); IntegrationType is the queue-prefix
  (e.g. `aws-iam`). See commit `25bdaea` for the rationale of
  separating them.
- `rpc.Transport` — interface every transport implements
  (`Consume`, `Publish`, `Close`). The contract `yggdrasil-core`
  publishes against. Implementations: `rpc/amqp` and `rpc/http`.
- `rpc/amqp.DialFunc` — function the transport calls to open a fresh
  AMQP connection. Wired automatically by `adapter.ListenAMQP`.
- `surface.RegisterHandlers(mux, manifest, handler)` — mounts the
  three console-ops surface endpoints (`GET /surface/manifest`,
  `GET /surface/data/{viewId}`, `POST /surface/action/{actionId}`)
  on the adapter's existing health-server mux.
- `surface.SchemaVersionCurrent` (`= 1`) — bump when extending
  surface manifest schema; the core MUST accept manifests with
  `SchemaVersion <= SchemaVersionCurrent`.

## Important behaviors

- **Provider namespace prefix** (commit `86e6833`). The AMQP transport
  prefixes consumer queue declarations with `yggdrasil.adapter.<provider>.`
  because `yggdrasil-core` publishes against that namespace. Without
  the prefix, the adapter sits on an empty queue while requests pile
  up on the real one. Wired automatically from `Config.Provider` by
  `adapter.ListenAMQP`.
- **Connection-level reconnect watchdog** (commit `7a10a9e`, v0.3.0).
  When the transport is constructed via `NewWithDial` (or after
  `SetDialFunc`), a watchdog goroutine re-dials the AMQP connection on
  broker-side disconnects (rabbit restart, network blip, idle close).
  Subscriptions notice on their next `setupConsumer` retry and rebind.
  Without this, a single rabbit restart kills every adapter until
  kubelet restarts the pod — the pattern we hit repeatedly in production.
  See memory `[AMQP cascade root cause RESOLVED 2026-05-26]`.
- **Subscription-level auto-reconnect** (commit `50d77ef`). Consumer
  goroutines reopen the channel + redeclare the queue on `channel.close`.
- **`adapter.ListenAMQP` retry/backoff** (commit `010e964`) — retries
  the initial AMQP dial with exponential backoff so a pod-startup
  race against rabbit doesn't crashloop.
- **v0.4.0 additive packages** (CHANGELOG entry 2026-05-26):
  - `sig/hmac` — provider-specific HMAC verifiers. `VerifyStripe`
    parses `t=<unix>,v1=<hex>`; `VerifyHubSignature256` parses
    `sha256=<hex>`. All use `crypto/subtle.ConstantTimeCompare`.
  - `mtls` — `Load(Config{Source, Path, Base64, Password})` plus
    `LoadFromEnv(prefix)`. Reads `{PREFIX}_MTLS_ENABLED`,
    `{PREFIX}_CERTIFICATE`, `{PREFIX}_CERTIFICATE_BASE64`.
  - `webhookhttp` — `Server.Handle(method, path, h, ...HandlerOption)`,
    `ListenAndServe(ctx)`. Return-value→status mapping:
    `nil`→202, `ErrDuplicate`→200, `*TerminalError`→400, other→500.
  - Consumed by `integration-stripe`, `integration-nfeio`,
    `integration-efi`.
- **v0.5.0 additive package** (CHANGELOG entry 2026-05-27):
  - `sdk/reconcile` — the Go expression of the universal capability
    naming convention. `Reconciler[D,O]`, `Discoverer[O]`,
    `DriftReporter[D,O]`. `RegisterReconciler(a, "user", "users", r)`
    wires three operations (`ensure_user`, `observe_users`,
    `destroy_user`) into the adapter's execute dispatch.
    `WithLegacyNames("create_user", ...)` keeps pre-convention names
    working with a WARN shim during the v0.5.x migration window;
    removal target moved to **v0.7.0** (was v0.6.0 — v0.6.0 is the
    events package rollout).
  - Consumed by `integration-efi` v2.0.0, `integration-nfeio` v2.0.0,
    `integration-stripe` v2.0.0 (this rollout's Phase C-E).
- **v0.6.0 additive package** (CHANGELOG entry 2026-05-27):
  - `sdk/events` — typed Go expression of INTEGRATION_CONTRACT.md
    §6.5 Golden Rule. `MutationEvent` payload matches the §6.5 wire
    shape exactly. `Verb` constants: `VerbEnsured`, `VerbDestroyed`,
    `VerbCreated` (the last reserved for non-idempotent
    money-movement allowlist actions). `Emitter` interface.
  - `NewHTTPEmitter(opts...)` — POSTs to `<YGGDRASIL_CORE_URL>/api/v1/events`
    with `Authorization: Bearer <YGGDRASIL_RUN_TOKEN>`. Retries
    transient 5xx with fixed backoff (`WithMaxRetries`,
    `WithRetryBackoff`); treats 4xx as terminal; honors `context.Context`
    cancellation. Options: `WithCoreURL`, `WithToken`, `WithHTTPClient`
    (tests), `WithEventsPath`.
  - `NoopEmitter` — satisfies `Emitter` without posting, WARNs on
    every call so suppression is visible.
  - `sdk/reconcile.RegisterReconciler` gains `WithEmitter`,
    `WithProvider`, `WithInstanceID` options. When wired, the SDK
    auto-emits a MutationEvent after every successful `Ensure()` /
    `Destroy()` invocation. **Emission is best-effort** — an emit
    error logs WARN but does NOT fail the capability call (adapter
    availability MUST NOT depend on event bus health).
  - **Backward compat**: v0.5.0 adapters keep building. Adapters
    without `WithEmitter` get one WARN per adapter at startup pointing
    at the new option; routed to stdlib log (not user logger) so
    existing tests stay deterministic.
  - Idempotency: when the inbound execute envelope carries an
    `idempotency` or `instance_id` field, those values forward onto
    the emitted MutationEvent so the downstream `event_log` table
    can dedup re-emissions across retries.
  - ResourceID inference: extracted via reflect on observed struct's
    `ID`/`Id` field, falling back to top-level `id` in the marshalled
    JSON. Empty string when neither resolves.
  - **Zero new external deps** — stdlib only.

## Recent commits (entire log — small repo)

```
<v0.6.0> sdk/events: MutationEvent + Emitter + HTTPEmitter + NoopEmitter
         sdk/reconcile: WithEmitter auto-emits on Ensure/Destroy success
<v0.5.0> sdk/reconcile: Reconciler[D,O] + RegisterReconciler + WithLegacyNames shim
<v0.4.0> sig/hmac + mtls + webhookhttp additive packages
7a10a9e 🛡️ amqp: connection-level auto-reconnect on broker disconnect   ← v0.3.0
b115c21 🐛 fix(rpc/http): take *Transport in New to stop sync.Mutex copy
3e7d407 feat(surface): extend ops surface metadata
6167b3a feat(surface): manifest types + validator + YAML loader + RegisterHandlers
50d77ef ✨ amqp: auto-reconnect consumer subscription on channel close
010e964 ✨ adapter: retry+backoff on AMQP dial in ListenAMQP
25bdaea ✨ separate Provider (family) from IntegrationType (queue prefix)
86e6833 🐛 prefix AMQP consumer queue names with provider namespace
1a755af chore: bootstrap yggdrasil-sdk-go with rpc + adapter packages
```

## Validation

```bash
go test ./...
go vet ./...
```

No CI configured (yet). Validation runs locally + on consumer-repo CI
when adapters bump.

## Mandatory contracts

- **Wire compatibility with yggdrasil-core**. Every adapter in the
  ecosystem pins this SDK; a breaking change in `rpc.Envelope`,
  `rpc.Delivery`, or the AMQP queue-naming convention breaks every
  adapter binary at the next rebuild.
- **No core type leakage**. The SDK MUST NOT import
  `yggdrasil-core/*`. Adapters define their own request/response
  structs against the JSON wire (see `integration-template` for the
  conventional shape).
- **Surface manifest schema is additive**. `v0.2.0 → v0.3.0` diff was
  additive only (`Endpoint`, `Method`, `Fields`, `SubmitLabel` on
  `ActionRequest` and related types). Bump `SchemaVersionCurrent`
  only when removing/renaming fields, and update
  `surface/validate.go` to keep accepting older versions.
- **`v0.x`** — public API may break with notice until v1.0. Tag
  releases via `git tag vX.Y.Z && git push --tags`; consumers
  pin pseudo-versions or tagged versions in their `go.mod`.

## Where things live

- Wire contract types → `rpc/transport.go`
- AMQP transport + watchdog → `rpc/amqp/amqp.go`
- HTTP/JSON transport → `rpc/http/*.go`
- Adapter builder + signal handler → `adapter/adapter.go` and
  `adapter/transport.go`
- Surface manifest types + validator → `surface/{types,validate}.go`
- Surface HTTP handlers → `surface/handler.go`
- Worked example → `examples/minimal/`
