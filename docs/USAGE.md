# Usage — build a minimal adapter end to end

This walks you from `go get` to a running adapter that `yggdrasil-core` can call,
twice: first a bare handler adapter, then the same adapter upgraded to the
**reconcile + auto-emission** path (the shape every production
`integration-*` adapter uses).

Back to the [README](../README.md) · See also
[PACKAGES](PACKAGES.md) · [RECONCILE-AND-EVENTS](RECONCILE-AND-EVENTS.md).

> Every claim here is derived from the source in this repo (`adapter/`,
> `rpc/`, `sdk/reconcile/`, `sdk/events/`). Verified against `v0.9.1`.

---

## 0. Where the adapter sits

```mermaid
flowchart LR
    core["yggdrasil-core"]
    bin["your adapter binary"]
    backend["provider API"]

    core -- "RPC: execute / describe<br/>(AMQP queue or POST /rpc/...)" --> bin
    bin -- "Reply (response body)" --> core
    bin -- "calls" --> backend
    bin -- "MutationEvent → POST /api/v1/events" --> core
```

The core sends RPC requests named by **capability** (`describe`, `execute`,
optional `health`). The adapter registers a `Handler` per capability; the SDK
owns framing and Ack/Nack/Reply.

---

## 1. Install

```sh
go get github.com/dakasa-yggdrasil/yggdrasil-sdk-go@v0.9.1
```

Go 1.25+. No `yggdrasil-core` dependency — the wire is JSON.

---

## 2. The minimal adapter (bare handlers)

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
        Provider:        "datadog", // integration_type FAMILY (matches manifest spec.provider)
        IntegrationType: "datadog", // queue-prefix owner; can differ from Provider
        Version:         "1.0.0",
    }).
        Register("describe", describe).
        Register("execute", execute).
        ListenHTTP(":8080")

    // WithSignalHandler cancels ctx on SIGINT/SIGTERM. Run alone installs no traps.
    if err := a.Run(adapter.WithSignalHandler(context.Background())); err != nil {
        log.Fatalf("adapter: %v", err)
    }
}

// A Handler returns (body, contentType, err). On err the SDK Nacks and replies
// with {"error":"..."}; on nil it Acks and replies with body.
func describe(ctx context.Context, d rpc.Delivery) ([]byte, string, error) {
    return []byte(`{"provider":"datadog","adapter":{"transport":"http_json","version":"1.0.0"}}`),
        "application/json", nil
}

func execute(ctx context.Context, d rpc.Delivery) ([]byte, string, error) {
    // d.Body holds the request payload. Parse it into your own struct.
    return []byte(`{"status":"succeeded"}`), "application/json", nil
}
```

What the SDK does for you here:

- **Framing & settlement** — your handler returns bytes; the SDK calls
  `Reply` + `Ack`, or replies an error envelope + `Nack(false)` on error.
- **Graceful shutdown** — `Run` blocks until ctx is cancelled, then shuts the
  HTTP server (10s drain) or closes the AMQP connection.
- **Lazy transport** — `ListenHTTP` / `ListenAMQP` defer the listen/dial to
  `Run`, so construction is side-effect-free (good for tests).

### Pick a transport

| Call | Backend | Endpoint shape | Use when |
|---|---|---|---|
| `.ListenHTTP(":8080")` | HTTP/JSON | `POST /rpc/<capability>` | broker-free deploy, k8s Service, local testing |
| `.ListenAMQP("amqp://…")` | RabbitMQ | queue `yggdrasil.adapter.<integration_type>.<capability>` | production (the default ecosystem transport) |
| `.Transport(custom)` | your own `rpc.Transport` | — | tests / a backend the helpers don't expose |

The handlers are identical across transports. For AMQP the SDK retries the
initial dial with backoff and runs a reconnect watchdog automatically — see
[PACKAGES.md#rpcamqp](PACKAGES.md#rpcamqp).

AMQP fixed queues must already exist as durable quorum queues. `Run` passively
checks that each queue exists and fails if the platform definitions have not
been imported; the SDK never creates `describe` or `execute` queues. Passive
AMQP declaration does not compare queue attributes, so the deployment gate must
verify durability and `x-queue-type=quorum` through RabbitMQ's management API.

> **Note — `Run` requires at least one `Register` and a transport.** It returns
> a clear error otherwise (`no handlers registered` / `no transport configured`).

---

## 3. Upgrade to reconcile + auto-emission

Real adapters don't hand-write an `execute` switch. They implement
`Reconciler[D,O]` per managed resource and let the SDK route and emit.

### 3a. Implement a Reconciler

`D` is the desired-state input; `O` is the observed-state output. Give `O` an
`ID` field (or a top-level `id` in its JSON) so the SDK can infer `resource_id`.

```go
package main

import "context"

type Customer struct {
    ID    string `json:"id"`
    Email string `json:"email"`
}

type DesiredCustomer struct {
    Email string `json:"email"`
}

type customerReconciler struct{ /* provider client */ }

func (r *customerReconciler) Ensure(ctx context.Context, d DesiredCustomer) (Customer, error) {
    // idempotent create-or-update against the provider
    return Customer{ID: "cus_123", Email: d.Email}, nil
}

func (r *customerReconciler) Observe(ctx context.Context, filter map[string]any) ([]Customer, string, error) {
    return []Customer{{ID: "cus_123", Email: "a@b.com"}}, "", nil // items, cursor("" = done)
}

func (r *customerReconciler) Destroy(ctx context.Context, ref string) error {
    // MUST treat "not found" as success
    return nil
}
```

### 3b. Wire it in `main()`

```go
import (
    "github.com/dakasa-yggdrasil/yggdrasil-sdk-go/adapter"
    "github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sdk/events"
    "github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sdk/reconcile"
)

a := adapter.New(adapter.Config{Provider: "stripe", IntegrationType: "stripe", Version: "2.0.0"})

emitter := events.NewHTTPEmitter() // reads YGGDRASIL_CORE_URL + YGGDRASIL_RUN_TOKEN

reconcile.RegisterReconciler(a, "customer", "customers", &customerReconciler{},
    reconcile.WithEmitter(emitter),
    reconcile.WithProvider("stripe"),    // see the warning below
    reconcile.WithInstanceID(instanceID),
)
// ...register more resources on the same adapter...

a.ListenAMQP("amqp://guest:guest@rabbit:5672/")
_ = a.Run(adapter.WithSignalHandler(context.Background()))
```

`RegisterReconciler(a, "customer", "customers", r, ...)` installs three
operations into the adapter's execute dispatch table:

| Operation | Reconciler method | Emits on success |
|---|---|---|
| `ensure_customer` | `Ensure` | `MutationEvent{verb: ensured}` |
| `observe_customers` | `Observe` | — (read-only, never emits) |
| `destroy_customer` | `Destroy` (or `DestroyWithDesired`) | `MutationEvent{verb: destroyed}` |

> ⚠️ **Always pass `WithProvider`.** Although the option's doc says it
> "defaults to `Config.Provider`", the dispatch code reads only the value set by
> `WithProvider` — there is no automatic fallback to the adapter's
> `Config.Provider`. Omit it and the emitted `event_type` becomes
> `.customer.ensured` (empty provider), which `yggdrasil-core`'s validator
> rejects. See [RECONCILE-AND-EVENTS.md](RECONCILE-AND-EVENTS.md#gotchas).

### 3c. Route production execute traffic through the SDK

`RegisterReconciler` auto-installs an `execute` handler the **first** time it
runs per adapter, so in the simplest case (3b) you're already done. If your
adapter needs custom auth/logging before dispatch, register **one** execute
handler that delegates to `reconcile.Dispatch`:

```go
a.Register("execute", func(ctx context.Context, d rpc.Delivery) ([]byte, string, error) {
    // ...auth, structured logging, capability normalization...
    return reconcile.Dispatch(ctx, a, d) // routes to the right reconciler + auto-emits
})
```

> ⚠️ **Do NOT** call `RegisterReconciler` **and then** register a *legacy*
> `execute` handler that bypasses `Dispatch`. `Register` is last-write-wins, so
> the legacy handler clobbers the SDK's and §6.5 emission silently stops.

---

## 4. Verify the run

**HTTP transport** — call a capability directly:

```sh
# describe
curl -s -X POST localhost:8080/rpc/describe -d '{}'

# an ensure_ operation (note the {"operation": ..., "input": ...} envelope the
# reconcile dispatcher parses)
curl -s -X POST localhost:8080/rpc/execute \
  -d '{"operation":"ensure_customer","instance_id":"stripe-acme","input":{"email":"a@b.com"}}'
```

A successful `ensure_customer` returns the observed JSON (`{"id":"cus_123",...}`)
and — when an emitter is wired and `YGGDRASIL_CORE_URL`/`YGGDRASIL_RUN_TOKEN` are
set — POSTs a `MutationEvent` to `<core>/api/v1/events`. Check the core's
`event_log` for a `stripe.customer.ensured` row.

**AMQP transport** — the core (or `yggdrasil orchestrator-stub`) publishes to
`yggdrasil.adapter.stripe.execute`; confirm the adapter logs the reply and the
queue's consumer count is 1 (a healthy adapter, not stranded after a reconnect).

**Local, no core** — pass `&events.NoopEmitter{}` instead of the HTTP emitter; it
WARN-logs each suppressed event so you can see emission firing without a bus.

---

## 5. Add a webhook receiver (optional)

Adapters that ingest provider webhooks (Stripe, GitHub, NFe.io, EFI) use
`webhookhttp` + `sig/hmac` instead of (or alongside) the RPC transport:

```go
import (
    "github.com/dakasa-yggdrasil/yggdrasil-sdk-go/webhookhttp"
    sdkhmac "github.com/dakasa-yggdrasil/yggdrasil-sdk-go/sig/hmac"
)

srv := webhookhttp.New(webhookhttp.Config{Addr: ":8082"}).
    Handle("POST", "/webhooks/stripe", handleStripe,
        webhookhttp.WithVerifyFunc(func(r *http.Request, body []byte) error {
            _, err := sdkhmac.VerifyStripe(body, r.Header.Get("Stripe-Signature"), secret, 300)
            return err // non-nil ⇒ HTTP 401 before handler runs
        }),
    )
go srv.ListenAndServe(ctx)
```

The `Handler` return value maps to status: `nil`→202, `ErrDuplicate`→200,
`*TerminalError`→400, any other error→500 (and a verify-func failure → 401).
Full reference: [PACKAGES.md#webhookhttp](PACKAGES.md#webhookhttp).

---

## Next

- Per-package API + signatures → [PACKAGES.md](PACKAGES.md)
- The full reconcile/emission contract + diagrams → [RECONCILE-AND-EVENTS.md](RECONCILE-AND-EVENTS.md)
- Test, tag, release, compat policy → [DEVELOPMENT.md](DEVELOPMENT.md)
</content>
