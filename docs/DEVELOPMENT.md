# Development — test, release, compatibility

`yggdrasil-sdk-go` is a **library**: no binary or Dockerfile. Releases are git
tags; consumers pin via `go.mod`. The SDK Go tests workflow runs tests with the
race detector and vet on pull requests and Main, including an actual isolated
RabbitMQ service. No tests or builds run on the owner's local machine.

Back to the [README](../README.md) · [USAGE](USAGE.md) · [PACKAGES](PACKAGES.md) ·
[RECONCILE-AND-EVENTS](RECONCILE-AND-EVENTS.md).

---

## Toolchain

- **Go 1.25+** (`go.mod`: `go 1.25.0`).
- Dependencies (all stdlib otherwise):
  - `github.com/rabbitmq/amqp091-go` — AMQP transport.
  - `github.com/google/uuid` — RPC correlation ids.
  - `golang.org/x/crypto` — PKCS#12 decode (`mtls`).
  - `gopkg.in/yaml.v3` — surface manifest loading only.
- **No dependency on `yggdrasil-core`** — and it MUST stay that way (the wire is
  JSON; types are duplicated where needed).

---

## CI validation

The `SDK Go tests` workflow runs `go test -race -count=1 ./...` and `go vet ./...`
with `GOWORK=off`. Its RabbitMQ service supplies `YGGDRASIL_TEST_AMQP_URL`; the
new drain tests fail if that configured broker is unavailable. Local validation
on the owner's machine is limited to static checks such as formatting and diffs.
Inspect the pull request's GitHub Actions result for executable validation.

## Planned shutdown

The AMQP and HTTP subscriptions stop admitting handlers, wait for admitted work
including Reply and Ack/Nack to return, then release transport resources. AMQP
uses its own consumer tag for `basic.cancel`; deliveries not started before the
cutoff remain unacknowledged until the channel closes. Reconnect also joins the
old delivery loop before admitting work on its replacement channel. This does
not prevent redelivery caused by an unexpected broker or process failure.

Concrete transports/subscriptions additionally support `CloseContext(ctx)`;
`rpc.Transport` and `rpc.Subscription` interfaces remain unchanged. The context
bounds the caller's wait, not the active handler: expiry returns an error and
the original drain continues without a forced channel close. `Close()` waits for
actual drain. Adapter `Run` gives shutdown the existing DefaultTimeout plus one
second for settlement, separately from its cancelled signal context, and
returns drain/server shutdown failures instead of silently reporting success.

Runtime termination grace must allow that budget. A handler that ignores its
own deadline, a forced process kill, failed reply or failed settlement can still
leave an unknown result. Preserve the original run/idempotency identity; this is
not an exactly-once or no-effects guarantee. Merging this library change alone
does not qualify any deployed adapter for autoscaling: consumers still need a
reviewed pin, rebuilt image and actual drain receiving. No tags or runtime pins
are changed by this patch.

---

## Repository layout

```
adapter/          Builder: New(Config), Register, ListenHTTP/ListenAMQP, Run, WithSignalHandler
                  (transport.go holds the Listen* helpers + AMQP retry/watchdog wiring)
rpc/              Transport contract: Transport, Delivery, Request, Reply, Envelope, ConsumerConfig
rpc/amqp/         AMQP 0-9-1 transport + reconnect watchdog + per-subscription rebind
rpc/http/         HTTP/JSON transport (POST /rpc/<endpoint>)
surface/          Console-ops surface: Manifest, Validate, loader, RegisterHandlers, view/format constants
sig/hmac/         VerifyStripe, VerifyHubSignature256 (constant-time)
mtls/             Load / LoadFromEnv — *tls.Config from PKCS#12 (file or base64)
webhookhttp/      Inbound webhook Server: Handle, ListenAndServe, verify/dedup/body-size options
sdk/reconcile/    Reconciler[D,O], RegisterReconciler, Dispatch, DestroyWithDesired, options
sdk/events/       MutationEvent, Verb, Emitter, NewHTTPEmitter, NoopEmitter
examples/minimal/ (empty — placeholder for a worked example)
protocol/         (empty)
```

> `examples/minimal/` and `protocol/` are empty directories in this release (git
> does not track empty dirs, so they appear only on a fresh checkout that creates
> them). The canonical worked example lives in
> [`integration-template`](https://github.com/dakasa-yggdrasil/integration-template).
> Until `examples/minimal/` is populated, use [USAGE.md](USAGE.md) as the
> end-to-end reference.

---

## Tagging & releasing

Releases are plain semver git tags — no build artifact, no publish step.

```sh
# 1. land the change on the default branch with an updated CHANGELOG.md entry
# 2. tag and push
git tag v0.8.6
git push origin v0.8.6
```

Consumers then bump:

```sh
go get github.com/dakasa-yggdrasil/yggdrasil-sdk-go@v0.8.6
go mod tidy
```

Existing tags: `v0.1.0 … v0.8.5` (current). Keep `CHANGELOG.md` as the source of
truth for what each tag changed — it follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and is the version map
this doc set defers to.

### Version history at a glance (verified against code)

| Tag | Headline | Code anchor |
|---|---|---|
| v0.3.0 | AMQP connection-level reconnect watchdog | `rpc/amqp/amqp.go` (`watchdogLoop`, `reconnect`) |
| v0.4.0 | `sig/hmac` + `mtls` + `webhookhttp` | those three packages |
| v0.5.0 | `sdk/reconcile`: `Reconciler[D,O]` + `RegisterReconciler` + legacy shim | `sdk/reconcile/{types,register,options}.go` |
| v0.6.0 | `sdk/events` + `WithEmitter` auto-emission | `sdk/events/*`, `register.go` emit path |
| v0.7.0 | public `reconcile.Dispatch`; `ExecuteForTest` deprecated | `sdk/reconcile/dispatch.go` |
| v0.8.0 | `DestroyWithDesired[D]` env-aware destroy | `types.go`, `makeDestroyFn` |
| v0.8.1 | destroy `ref` inference from provider-shaped payloads | `inferRefFromInput` |
| v0.8.2 | idempotency-key synthesis when envelope omits it | `synthesizeIdempotencyKey` |
| v0.8.3 | ensure-path `resource_id` inference (`<resource>_id`, composite) | `inferResourceID` |
| v0.8.4 | numeric `id` coercion + GitHub `full_name` rung | `coerceToNonEmptyString` |
| v0.8.5 | integer-valued floats render as integers, not `1.2e+09` | `coerceToNonEmptyString` |

---

## Compatibility policy

- **`v0.x` may break with notice until `v1.0`.** Adapters pin a specific tag.
- **Wire compatibility with `yggdrasil-core` is mandatory.** A breaking change in
  `rpc.Envelope`, `rpc.Delivery`, or the AMQP queue-naming convention
  (`yggdrasil.adapter.<integration_type>.<capability>`) breaks **every** adapter
  at its next rebuild. Treat those as the frozen surface.
- **No core type leakage.** The SDK MUST NOT import `yggdrasil-core/*`.
- **Surface manifest schema is additive.** `surface.SchemaVersionCurrent == 1`;
  the validator accepts any manifest with `SchemaVersion <= SchemaVersionCurrent`.
  Bump the constant only when removing/renaming a field, and keep
  `surface/validate.go` accepting older versions.
- **`reconcile.ExecuteForTest`** is a deprecated alias for `reconcile.Dispatch`,
  scheduled for removal at `v1.0.0`. **`WithLegacyNames`** has a stated removal
  target of `v0.7.0`; treat it as on its way out.

### Pre-bump checklist for a consumer adapter

1. `go get github.com/dakasa-yggdrasil/yggdrasil-sdk-go@<new-tag> && go mod tidy`
2. `go build ./... && go test ./...` in the adapter repo.
3. If you adopt reconcile, confirm `WithProvider` is passed on **every**
   `RegisterReconciler` call (see
   [RECONCILE-AND-EVENTS.md#gotchas](RECONCILE-AND-EVENTS.md#gotchas)).
4. Confirm exactly one `execute` wiring pattern (don't clobber the SDK handler).
</content>
