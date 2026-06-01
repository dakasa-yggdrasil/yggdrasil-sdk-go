# Development — test, release, compatibility

`yggdrasil-sdk-go` is a **library**: no binary, no Dockerfile, no CI workflow in
this repo. Releases are git tags; consumers pin via `go.mod`. Validation runs
locally and on consumer-repo CI when an adapter bumps the pin.

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

## Build, test, vet

```sh
go test ./...
go vet  ./...
```

> **If a `go.work` file is present in a parent directory**, the multi-module
> workspace may reference sibling repos that aren't checked out, and bare
> `go test ./...` fails to load the workspace. Run with the workspace disabled to
> test this module standalone:
>
> ```sh
> GOWORK=off go test ./...
> GOWORK=off go vet  ./...
> ```

All ten buildable packages have tests (`rpc` is the only one with no test files —
it's pure type/contract definitions). The AMQP reconnect behavior is covered by
`rpc/amqp/amqp_reconnect_test.go` without a live broker.

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
