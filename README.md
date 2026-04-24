# yggdrasil-sdk-go

Go SDK for building Yggdrasil integration adapters. Provides the RPC
transport contracts the core talks, plus adapter helpers that handle
envelope framing, lifecycle, and transport choice so plugin authors
can focus on business logic.

## Install

```sh
go get github.com/dakasa-yggdrasil/yggdrasil-sdk-go
```

## Minimum adapter

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
        Provider: "datadog",
        Version:  "1.0.0",
    }).
        Register("describe", describe).
        Register("execute", execute).
        ListenHTTP(":8080")

    ctx := adapter.WithSignalHandler(context.Background())
    if err := a.Run(ctx); err != nil {
        log.Fatalf("adapter: %v", err)
    }
}

func describe(ctx context.Context, d rpc.Delivery) ([]byte, string, error) {
    return []byte(`{"provider":"datadog","adapter":{"transport":"http_json","version":"1.0.0"}}`), "application/json", nil
}

func execute(ctx context.Context, d rpc.Delivery) ([]byte, string, error) {
    // ... do the work, return the response body.
    return []byte(`{"status":"succeeded"}`), "application/json", nil
}
```

Swap `ListenHTTP(":8080")` for `ListenAMQP("amqp://...")` to consume
from a broker instead — the handlers do not change.

## Packages

| Package | Purpose |
|---|---|
| `rpc` | Transport interface (`rpc.Transport`, `rpc.Delivery`, `rpc.Envelope`). This is the contract the core speaks. |
| `rpc/amqp` | AMQP 0-9-1 implementation. Used when the adapter consumes from RabbitMQ. |
| `rpc/http` | HTTP/JSON implementation. Used when the adapter exposes a Kubernetes Service / HTTP endpoint. |
| `adapter` | High-level builder for adapter binaries: Config, Register, ListenHTTP / ListenAMQP, Run. Handles signal traps, envelope framing, Ack/Nack. |

## What this SDK deliberately does not do

- **Does not bundle business logic helpers** (Kubernetes client, AWS
  SDK, etc.). Adapters import those directly. The SDK's scope is the
  RPC contract + lifecycle.
- **Does not vendor the core's internal types.** The wire is JSON;
  adapters define their own request/response structs as they see fit.
  See `integration-template` for the conventional shape.
- **Does not include schema validation.** The core validates
  integration_type manifests; the SDK trusts that validation.

## Stability

`v0.x` — public API may break with notice until `v1.0`. Version pins
in adapter repos document the core version they were built against.

## Related

- [`yggdrasil-core`](https://github.com/dakasa-yggdrasil/yggdrasil-core)
  — the server that talks to adapters using this SDK.
- [`integration-template`](https://github.com/dakasa-yggdrasil/integration-template)
  — scaffolded adapter repo. `yggdrasil new integration <name>`
  generates an adapter wired to this SDK.
