<div align="center">

# yggdrasil-sdk-go

**The Go SDK for building Yggdrasil integration adapters — the RPC transport
contract `yggdrasil-core` speaks, plus the lifecycle, webhook, and
reconcile/auto-emission helpers so plugin authors write business logic, not plumbing.**

[![Go](https://img.shields.io/badge/go-1.25-00ADD8.svg)](go.mod)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Release](https://img.shields.io/badge/release-v0.8.5-informational.svg)](CHANGELOG.md)

Library · [Usage](docs/USAGE.md) · [Packages](docs/PACKAGES.md) · [Reconcile & Events](docs/RECONCILE-AND-EVENTS.md) · [Development](docs/DEVELOPMENT.md)

</div>

---

## What it is

`yggdrasil-sdk-go` is the library every `dakasa-yggdrasil/integration-*` adapter
imports. It defines the **transport contract** (`rpc.Transport` / `rpc.Delivery`
/ `rpc.Envelope`) that `yggdrasil-core` uses to talk to adapters, ships two
production transports (**AMQP** and **HTTP/JSON**), and layers on the helpers an
adapter actually needs: a builder that owns envelope framing and graceful
shutdown, webhook signature verifiers, a PKCS#12 mTLS loader, a console-ops
surface contract, and the **reconcile + auto-emission** machinery that satisfies
the [Integration Contract](https://github.com/dakasa-yggdrasil/yggdrasil-core)
§6.5 Golden Rule on the adapter's behalf.

It ships **no binary** — no Dockerfile, no CI workflow. Releases are git tags
(`v0.8.5` today); consumers pin via `go.mod`.

> Part of **Yggdrasil** — the self-hosted control plane for declarative
> workflows + integrations. This SDK is the build-time dependency that makes an
> adapter wire-compatible with the core. See
> [`yggdrasil-core`](https://github.com/dakasa-yggdrasil/yggdrasil-core).

## Where it fits

```mermaid
flowchart LR
    core["yggdrasil-core<br/>(orchestrator + event_log)"]
    sdk["yggdrasil-sdk-go<br/>(this library)"]
    adapter["integration-&lt;name&gt;<br/>(adapter binary)"]
    backend["real backend<br/>(Stripe, GitHub, AWS, ...)"]

    core -- "RPC over AMQP / HTTP" --> adapter
    adapter -. "imports / built on" .-> sdk
    adapter -- "provider API calls" --> backend
    adapter -- "POST /api/v1/events<br/>(MutationEvent, via sdk/events)" --> core
```

The core publishes RPC requests onto a queue (or POSTs to an HTTP endpoint); the
adapter — built on this SDK — consumes them, calls the real backend, replies,
and (for `ensure_*` / `destroy_*` operations) emits a `MutationEvent` back to the
core's `/api/v1/events` so the mutation lands in `event_log`.

## Packages

| Package | What a plugin author uses it for |
|---|---|
| [`adapter`](docs/PACKAGES.md#adapter) | `adapter.New(Config).Register(cap, h).ListenHTTP/ListenAMQP(...).Run(ctx)` — the builder. Owns envelope framing, Ack/Nack/Reply, graceful shutdown, AMQP reconnect wiring. |
| [`rpc`](docs/PACKAGES.md#rpc) | The transport contract: `Transport`, `Delivery`, `Request`, `Reply`, `Envelope`, `ConsumerConfig`. Most adapters touch only `rpc.Delivery` (handler arg). |
| [`rpc/amqp`](docs/PACKAGES.md#rpcamqp) | AMQP 0-9-1 transport (the production backend) with a reconnect watchdog + per-subscription rebind. Wired automatically by `ListenAMQP`. |
| [`rpc/http`](docs/PACKAGES.md#rpchttp) | HTTP/JSON transport for broker-free / k8s-Service adapters. Registers `POST /rpc/<capability>`. Wired by `ListenHTTP`. |
| [`surface`](docs/PACKAGES.md#surface) | Console-ops surface contract: `Manifest`, `LoadManifestFromFS`, `RegisterHandlers(mux, manifest, handler)`. Lets an adapter contribute pages/widgets to the console. |
| [`sig/hmac`](docs/PACKAGES.md#sighmac) | Webhook signature verifiers: `VerifyStripe` (`t=,v1=`), `VerifyHubSignature256` (`sha256=` — GitHub/NFe.io/EFI). Constant-time. |
| [`mtls`](docs/PACKAGES.md#mtls) | `Load` / `LoadFromEnv(prefix)` — build a `*tls.Config` from a PKCS#12 bundle (file or base64). Returns `(nil, nil)` when disabled. |
| [`webhookhttp`](docs/PACKAGES.md#webhookhttp) | Inbound webhook listener: `New(Config).Handle(method, path, h, opts...).ListenAndServe(ctx)` with HMAC verify + dedup + body-size limits. |
| [`sdk/reconcile`](docs/PACKAGES.md#sdkreconcile) | `Reconciler[D,O]` + `RegisterReconciler` + `Dispatch` — express the `ensure_/observe_/destroy_` triple once per resource and route execute traffic through the SDK. |
| [`sdk/events`](docs/PACKAGES.md#sdkevents) | `MutationEvent`, `Emitter`, `NewHTTPEmitter`, `NoopEmitter` — the §6.5 mutation-event payload + transport against `POST /api/v1/events`. |

Full per-package reference with signatures and snippets: **[docs/PACKAGES.md](docs/PACKAGES.md)**.

## Install

```sh
go get github.com/dakasa-yggdrasil/yggdrasil-sdk-go@v0.8.5
```

Requires **Go 1.25+**. Dependencies: `rabbitmq/amqp091-go`, `google/uuid`,
`golang.org/x/crypto` (PKCS#12), `gopkg.in/yaml.v3` (surface manifests only).
**No dependency on `yggdrasil-core`** — the wire is JSON.

## Quick start — a minimal adapter

```go
package main

import (
    "context"
    "log"

    "github.com/dakasa-yggdrasil/yggdrasil-sdk-go/adapter"
    "github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
)

func main() {
    a := adapter.New(adapter.Config{
        Provider:        "datadog",
        IntegrationType: "datadog",
        Version:         "1.0.0",
    }).
        Register("describe", describe).
        Register("execute", execute).
        ListenHTTP(":8080") // or .ListenAMQP("amqp://guest:guest@rabbit:5672/")

    if err := a.Run(adapter.WithSignalHandler(context.Background())); err != nil {
        log.Fatalf("adapter: %v", err)
    }
}

func describe(ctx context.Context, d rpc.Delivery) (body []byte, contentType string, err error) {
    return []byte(`{"provider":"datadog","adapter":{"transport":"http_json","version":"1.0.0"}}`),
        "application/json", nil
}

func execute(ctx context.Context, d rpc.Delivery) (body []byte, contentType string, err error) {
    // d.Body is the request payload. Do the work, return the response.
    return []byte(`{"status":"succeeded"}`), "application/json", nil
}
```

Swap `ListenHTTP(":8080")` for `ListenAMQP("amqp://...")` and the handlers do not
change — the SDK abstracts the transport. The full end-to-end walkthrough
(including the reconcile path) is in **[docs/USAGE.md](docs/USAGE.md)**.

## Configuration

The SDK is a library; it reads no env vars itself **except** `sdk/events`'
`NewHTTPEmitter`, which falls back to these when its options are not supplied:

| Variable | Read by | Purpose | Default |
|---|---|---|---|
| `YGGDRASIL_CORE_URL` | `events.NewHTTPEmitter` (via `WithCoreURL`) | Base URL the emitter POSTs `MutationEvent`s to. | — (required for emission) |
| `YGGDRASIL_RUN_TOKEN` | `events.NewHTTPEmitter` (via `WithToken`) | Bearer token (`Authorization: Bearer <token>`). Same token adapter pods use for workflow runs. | — |

The `mtls.LoadFromEnv(prefix)` helper reads `<PREFIX>_MTLS_ENABLED`,
`<PREFIX>_CERTIFICATE`, `<PREFIX>_CERTIFICATE_BASE64`,
`<PREFIX>_CERTIFICATE_PASSWORD` — see [docs/PACKAGES.md#mtls](docs/PACKAGES.md#mtls).
Everything else (`adapter.Config`, transport addresses) is passed in code by the
adapter's `main()`.

## Transports & the AMQP queue namespace

`ListenAMQP` prefixes every consumer queue with
**`yggdrasil.adapter.<integration_type>.`** because `yggdrasil-core` publishes
RPC requests to that namespace. The prefix is taken from `Config.IntegrationType`
when set, falling back to `Config.Provider` otherwise. Get this wrong and the
adapter consumes from a bare queue name nobody publishes to — it sits idle while
requests pile up on the real queue.

`ListenAMQP` also wires a **reconnect watchdog**: after the initial retrying dial
succeeds, a background goroutine re-dials on broker-side disconnects (rabbit
restart, network blip), and each subscription rebinds on its next setup retry.
Without it, a single rabbit restart used to strand every adapter pod until the
kubelet restarted it — the bug this SDK was bumped to v0.3.0 to fix. Details in
[docs/PACKAGES.md#rpcamqp](docs/PACKAGES.md#rpcamqp).

## Reconcile & auto-emission (§6.5)

Implement `Reconciler[D,O]` per managed resource, register it, and route execute
traffic through `reconcile.Dispatch` — the SDK then auto-emits a `MutationEvent`
after every successful `Ensure()` / `Destroy()`:

```go
emitter := events.NewHTTPEmitter() // reads YGGDRASIL_CORE_URL + YGGDRASIL_RUN_TOKEN
reconcile.RegisterReconciler(a, "customer", "customers", customerR,
    reconcile.WithEmitter(emitter),
    reconcile.WithProvider("stripe"),   // REQUIRED for a correct event_type prefix
    reconcile.WithInstanceID(instanceID),
)
```

The complete contract — the `ensure_/observe_/destroy_` dispatch, `resource_id`
inference, idempotency-key synthesis, `DestroyWithDesired`, and the
execute → Dispatch → Ensure → emit → core sequence — is documented with diagrams
in **[docs/RECONCILE-AND-EVENTS.md](docs/RECONCILE-AND-EVENTS.md)**.

## What this SDK deliberately does NOT do

- **No business-logic helpers.** No Kubernetes/AWS/Stripe clients — adapters
  import those directly. The SDK's scope is the RPC contract + lifecycle +
  webhook/reconcile boilerplate.
- **No core type leakage.** It MUST NOT import `yggdrasil-core/*`. The wire is
  JSON; adapters define their own request/response structs.
- **No manifest schema validation** beyond `surface.Manifest.Validate()`. The
  core validates `integration_type` manifests.

## Operations

This is a library — it exposes no `/healthz`, no `/metrics`. Those belong to the
adapter binary that consumes the SDK (see `integration-template`). What the SDK
*does* give an adapter for operability:

- **AMQP self-healing** — reconnect watchdog + per-subscription rebind +
  retrying initial dial (no CrashLoopBackOff on a pod/broker startup race).
- **Best-effort emission** — an emit failure logs WARN, never fails the
  capability call. Adapter availability does not depend on event-bus health.
- **Diagnosable dispatch errors** — `reconcile.Dispatch` names the missing
  operation when an unregistered op is requested.

## Development

```sh
GOWORK=off go test ./...   # unit tests (all packages)
GOWORK=off go vet ./...
```

Build, test, tagging/release flow, and the compatibility policy are in
**[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)**.

## Compatibility

`v0.x` — the public API may break with notice until `v1.0`. Wire compatibility
with `yggdrasil-core` is mandatory: a breaking change in `rpc.Envelope`,
`rpc.Delivery`, or the AMQP queue-naming convention breaks every adapter at its
next rebuild. The `surface` manifest schema is additive
(`surface.SchemaVersionCurrent == 1`). Current release: **v0.8.5** (see
[CHANGELOG.md](CHANGELOG.md)).

## Related

- [`yggdrasil-core`](https://github.com/dakasa-yggdrasil/yggdrasil-core) — the
  server that talks to adapters using this SDK.
- [`integration-template`](https://github.com/dakasa-yggdrasil/integration-template)
  — scaffolded adapter repo; `yggdrasil new integration <name>` generates an
  adapter wired to this SDK.

## License

Apache-2.0 — see [LICENSE](LICENSE).
</content>
