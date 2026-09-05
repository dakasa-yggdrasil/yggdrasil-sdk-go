# Packages — per-package reference

Every signature below was read from the `.go` source in this repo and verified
against `v0.9.1`. Import path root:
`github.com/dakasa-yggdrasil/yggdrasil-sdk-go`.

Back to the [README](../README.md) · [USAGE](USAGE.md) ·
[RECONCILE-AND-EVENTS](RECONCILE-AND-EVENTS.md) · [DEVELOPMENT](DEVELOPMENT.md).

## Index

| Package | One line | Added |
|---|---|---|
| [`adapter`](#adapter) | The builder: `New → Register → Listen* → Run`. | v0.1.0 |
| [`rpc`](#rpc) | Transport contract types (`Transport`, `Delivery`, `Envelope`). | v0.1.0 |
| [`rpc/amqp`](#rpcamqp) | AMQP 0-9-1 transport + reconnect watchdog. | v0.1.0 (watchdog v0.3.0) |
| [`rpc/http`](#rpchttp) | HTTP/JSON transport (`POST /rpc/<endpoint>`). | v0.1.0 |
| [`surface`](#surface) | Console-ops surface manifest + handlers. | v0.x |
| [`sig/hmac`](#sighmac) | Webhook HMAC verifiers (Stripe, GitHub/NFe.io/EFI). | v0.4.0 |
| [`mtls`](#mtls) | `*tls.Config` from a PKCS#12 bundle. | v0.4.0 |
| [`webhookhttp`](#webhookhttp) | Inbound webhook server with verify + dedup. | v0.4.0 |
| [`sdk/reconcile`](#sdkreconcile) | `Reconciler[D,O]` + dispatch + auto-emission. | v0.5.0 |
| [`sdk/events`](#sdkevents) | `MutationEvent` + `Emitter` + HTTP/Noop emitters. | v0.6.0 |

---

## adapter

The high-level builder. Construct once in `main()`, register a handler per
capability, choose a transport, `Run`.

**Types**

```go
type Config struct {
    Provider        string        // integration_type FAMILY (matches manifest spec.provider). NOT used for queue naming.
    IntegrationType string        // queue-prefix owner. When set, AMQP queues are "yggdrasil.adapter.<IntegrationType>.<cap>".
    Version         string        // adapter build id, surfaced on describe.
    DefaultTimeout  time.Duration // per-Handler timeout when ConsumerConfig.Timeout is 0. Default 30s.
    Concurrency     int           // max in-flight handlers per endpoint. Default 1 (serial).
}

// Handler returns a response body + content type; the SDK drives Ack/Nack/Reply.
type Handler func(ctx context.Context, d rpc.Delivery) (body []byte, contentType string, err error)

type Adapter struct{ /* unexported */ }
```

**Functions / methods**

```go
func New(cfg Config) *Adapter
func (a *Adapter) Register(capability string, handler Handler) *Adapter   // lowercased+trimmed; last-write-wins
func (a *Adapter) Transport(t rpc.Transport) *Adapter                     // inject a custom transport
func (a *Adapter) ListenHTTP(addr string) *Adapter                        // serve POST /rpc/<cap> on addr
func (a *Adapter) ListenAMQP(url string) *Adapter                         // consume from a broker (retry + watchdog)
func (a *Adapter) Run(ctx context.Context) error                          // block until ctx cancelled

func WithSignalHandler(parent context.Context) context.Context            // ctx cancelled on SIGINT/SIGTERM
```

**Behavioral notes (from source):**

- `Register` lowercases and trims the capability name; duplicate registrations
  **overwrite** (deliberate, so tests can swap mocks — and the reason the
  reconcile/legacy-execute gotcha exists).
- On handler error the SDK replies `{"error":"<msg>"}` then `Nack(false)`. On
  success it `Reply`s the body then `Ack`s; a failed reply does `Nack(true)`.
- `Run` errors if **no handler** is registered or **no transport** is set.
- `Run` installs **no** signal traps by itself — opt in with
  `WithSignalHandler`.

```go
a := adapter.New(adapter.Config{Provider: "datadog", IntegrationType: "datadog", Version: "1.0.0"}).
    Register("describe", describe).
    Register("execute", execute).
    ListenHTTP(":8080")
_ = a.Run(adapter.WithSignalHandler(context.Background()))
```

---

## rpc

The transport-agnostic contract. Most adapters touch only `rpc.Delivery` (their
handler's second argument); the rest matters when implementing a transport or
calling one as a client.

**Core types**

```go
type Transport interface {
    Consume(cfg ConsumerConfig) (Subscription, error)
    Request(ctx context.Context, req Request) (Reply, error)   // RPC: blocks for a Reply
    Publish(ctx context.Context, req Request) error            // fire-and-forget
    Close() error
}

type Subscription interface {
    Endpoint() string
    Close() error
}

type TerminalErrorSubscription interface { // optional asynchronous-failure contract
    Subscription
    TerminalErrors() <-chan error
}

type ConsumerConfig struct {
    Endpoint    string         // queue / route / topic. Required.
    Handler     Handler        // func(ctx, Delivery) error — MUST Ack or Nack.
    Timeout     time.Duration  // per-invocation bound
    Concurrency int            // max in-flight; default 1
}

type Handler func(ctx context.Context, d Delivery) error

type Request struct {
    Endpoint, ContentType, CorrelationID string
    Body                                 []byte
    Headers                              map[string]string
    Timeout                              time.Duration
}

type Reply struct {
    Body          []byte
    ContentType   string
    Headers       map[string]string
    CorrelationID string
}

type Delivery struct {
    Endpoint, ContentType, CorrelationID, ReplyTo string
    Body                                          []byte
    Headers                                       map[string]string
    AckFn   func() error
    NackFn  func(requeue bool) error
    ReplyFn func(ctx context.Context, body []byte, contentType string) error
}
func (d Delivery) Ack() error
func (d Delivery) Nack(requeue bool) error
func (d Delivery) Reply(ctx context.Context, body []byte, contentType string) error

type Envelope struct { // JSON framing for transports without native correlation/headers (HTTP uses it; AMQP does not)
    CorrelationID, ReplyTo, ContentType string
    Headers                             map[string]string
    Body                                []byte
}
```

**Sentinel errors:** `ErrClosed`, `ErrTimeout`, `ErrEndpointUnknown`
(use `errors.Is`).

> `rpc.Delivery` is a **struct**, not an interface — `d.Body`, `d.ReplyTo`,
> `d.CorrelationID` are direct field reads; `Ack`/`Nack`/`Reply` are methods
> that dispatch through the transport-populated function fields. Default content
> type is `application/json` when empty.

---

## rpc/amqp

`rpc.Transport` over RabbitMQ / AMQP 0-9-1 (`rabbitmq/amqp091-go`). Wired
automatically by `adapter.ListenAMQP`; you rarely construct it directly.

```go
type DialFunc func() (*amqp091.Connection, error)
type Transport struct{ /* unexported */ }

func New(conn *amqp091.Connection) *Transport               // wrap an open conn; NO reconnect
func NewWithDial(dialFn DialFunc) (*Transport, error)       // owns conn + reconnect watchdog
func (t *Transport) SetDialFunc(dialFn DialFunc)            // enable reconnect on a New() transport
func (t *Transport) SetEndpointPrefix(prefix string)        // queue-name namespace
func (t *Transport) Connection() *amqp091.Connection        // escape hatch (stale after reconnect!)
func (t *Transport) Consume(cfg rpc.ConsumerConfig) (rpc.Subscription, error)
func (t *Transport) Request(ctx, req) (rpc.Reply, error)
func (t *Transport) Publish(ctx, req) error
func (t *Transport) Close() error
```

**Queue namespace — verified behavior.** `ListenAMQP` calls
`SetEndpointPrefix("yggdrasil.adapter." + owner + ".")`, where `owner` is
`Config.IntegrationType` and **falls back to `Config.Provider`** only when
`IntegrationType` is empty. So a handler registered as `execute` consumes from
queue `yggdrasil.adapter.<owner>.execute`. The prefix exists because
`yggdrasil-core` publishes to that namespace; without it the adapter sits on a
queue nobody writes to.

**Fixed topology ownership (v0.9.1).** `Consume` passively requires each fixed
queue to exist and does not create it. RabbitMQ passive declaration ignores
durable, auto-delete, exclusive, and arguments; it cannot distinguish classic
from quorum. Import the canonical platform definitions and verify
`durable=true`, `auto_delete=false`, and `x-queue-type=quorum` through the
management API before starting the adapter. Retry queues, TTL and dead-letter
routing are also platform-owned and are never synthesized by this SDK. A
permanent 403, 404, 405, or 406 while re-binding is delivered through
`rpc.TerminalErrorSubscription`; `adapter.Run` returns that error so the process
cannot remain ready with no consumer.

**Reconnect watchdog — verified behavior (v0.3.0).** A transport with a
`DialFunc` (which `ListenAMQP` always sets via `SetDialFunc`) spawns **one**
watchdog goroutine (`watchdogOnce`). It listens for `NotifyClose` on the current
connection and re-dials on broker drops (rabbit restart, network blip, idle
close), re-attaching its listener to each new connection so it survives any
number of restarts. Dial failures back off `1s → 30s` (capped). A concurrent
"reconnect storm" from N subscriptions collapses to a single dial via `dialMu` +
a check-after-lock. Each subscription independently rebinds on its next
`setupConsumer` retry (also `1s → 30s` backoff); the passive existence check is
safe to repeat. Publish resolves/reconnects the connection before taking the
publish-channel mutex, avoiding a self-deadlock when it wins the recovery race.
`ListenAMQP`'s **initial** dial retries `1s → 30s` up to 30
attempts (~5 min) so a pod that boots before rabbit doesn't CrashLoop.

> `New(conn)` (no DialFunc) has **no** connection-level reconnect — fine for
> tests, brittle in prod. Production always goes through `ListenAMQP`.

---

## rpc/http

`rpc.Transport` over HTTP. Broker-free; each `Consume` registers a
`POST /rpc/<endpoint>` handler on a shared mux. Wired by `adapter.ListenHTTP`.

```go
const DefaultPathPrefix = "/rpc/"

type Transport struct {
    BaseURL    string         // origin for outbound Request/Publish (client side)
    Mux        *http.ServeMux // where Consume registers handlers (server side)
    Client     *http.Client   // outbound client; default 30s timeout
    PathPrefix string         // overrides "/rpc/"
}

func New(opts *Transport) *Transport   // takes a *pointer* to avoid copying the embedded mutexes
func (t *Transport) Consume(cfg rpc.ConsumerConfig) (rpc.Subscription, error)
func (t *Transport) Request(ctx, req) (rpc.Reply, error)
func (t *Transport) Publish(ctx, req) error
func (t *Transport) Close() error
```

**Notes:** request/response bodies are `rpc.Envelope` JSON, but the server
**also accepts a raw JSON body** (no envelope) and treats the whole body as
`Body` — supporting cURL/legacy callers. `Request` maps HTTP 404 →
`rpc.ErrEndpointUnknown`, ≥400 → an error with the status. `Close` cannot
unregister mux handlers, so closed subscriptions answer `503`.

```go
mux := http.NewServeMux()
t := sdkhttp.New(&sdkhttp.Transport{Mux: mux}) // ← pointer literal, per the New() contract
```

---

## surface

The contract by which an adapter contributes console-ops pages/widgets. An
adapter declares a static `Manifest` (usually embedded YAML), implements
`DataHandler`, and mounts three endpoints on its existing health mux.

```go
const SchemaVersionCurrent = 1   // core MUST accept manifests with SchemaVersion <= this
const MaxActionBodyBytes  = 512 * 1024

type Manifest struct {
    Surface, SurfaceVersion, DisplayName, Icon, Description string
    SchemaVersion int
    Permissions []Permission
    Pages       []Page    // ≥1 required
    Widgets     []Widget
}
func (m *Manifest) Validate() error

type DataRequest   struct { ViewID, RawQuery string; Header http.Header }
type ActionRequest struct { ActionID string; Body []byte; Header http.Header }

type DataHandler interface {
    HandleData(ctx context.Context, req DataRequest) (any, error)
    HandleAction(ctx context.Context, req ActionRequest) (any, error)
}

func LoadManifestFromBytes(raw []byte) (Manifest, error)        // parse YAML/JSON + Validate
func LoadManifestFromFS(f fs.FS, path string) (Manifest, error) // embed.FS convenience
func RegisterHandlers(mux *http.ServeMux, m Manifest, h DataHandler)
func WriteJSON(w http.ResponseWriter, status int, v any)
func ReadActionBody(r *http.Request) ([]byte, error)
func SupportedViewKinds() map[string]struct{}
func SupportedFormFieldKinds() map[string]struct{}
```

`RegisterHandlers` mounts:

```
GET  /surface/manifest          → the manifest (by value)
GET  /surface/data/{viewId}     → DataHandler.HandleData
POST /surface/action/{actionId} → DataHandler.HandleAction
```

View kinds (`ViewKind*`), formats (`Format*`), filter kinds (`FilterKind*`),
form-field kinds (`FormFieldKind*`), and widget slots (`SlotOps*`) are exported
constants in `views.go`. `Validate` enforces: non-empty `surface` /
`surface_version` / `display_name`, `0 < schema_version ≤ SchemaVersionCurrent`,
≥1 page, unique page/widget/permission ids, page paths starting with `/`, known
view kinds, and form-specific rules (`kind=form` needs ≥1 field; `kind=custom`
needs a component; `kind=select` field needs options).

```go
//go:embed surface/manifest.yaml
var manifestFS embed.FS

m, err := surface.LoadManifestFromFS(manifestFS, "surface/manifest.yaml")
if err != nil { log.Fatal(err) }
surface.RegisterHandlers(healthMux, m, mySurfaceHandler{})
```

---

## sig/hmac

Constant-time webhook signature verifiers
(`crypto/subtle.ConstantTimeCompare`).

```go
// Stripe scheme "t=<unix>,v1=<hex>". Returns the signing timestamp.
// toleranceSecs bounds |now - ts| (pass 300 for the Stripe default; <=0 disables it).
// Supports multiple v1= components (key rotation).
func VerifyStripe(payload []byte, header string, secret []byte, toleranceSecs int64) (ts int64, err error)

// "X-Hub-Signature-256: sha256=<hex>" — GitHub, NFe.io, EFI.
func VerifyHubSignature256(payload []byte, header string, secret []byte) error
```

**Typed errors** (use `errors.Is`): `ErrTimestampExpired`,
`ErrSignatureMismatch`, `ErrMissingTimestamp`, `ErrMissingV1`,
`ErrMalformedHeader` (Stripe); `ErrMalformedHubSig` (`VerifyHubSignature256`,
which requires an exact `sha256.Size*2` hex length).

```go
ts, err := sdkhmac.VerifyStripe(body, r.Header.Get("Stripe-Signature"), secret, 300)
err     := sdkhmac.VerifyHubSignature256(body, r.Header.Get("X-Hub-Signature-256"), secret)
```

---

## mtls

Build a `*tls.Config` from a PKCS#12 bundle (decoded via
`golang.org/x/crypto/pkcs12`; the key must be `*rsa.PrivateKey`).

```go
type Source int
const ( SourceDisabled Source = iota; SourceFile; SourceBase64 )

type Config struct {
    Source   Source
    Path     string // SourceFile
    Base64   string // SourceBase64
    Password string // optional
}

func Load(cfg Config) (*tls.Config, error)
func LoadFromEnv(prefix string) (*tls.Config, error)
```

`SourceDisabled` (and any disabled env state) returns **`(nil, nil)`** so callers
build a plain client without branching on TLS. `LoadFromEnv(prefix)` reads:

| Env var | Effect |
|---|---|
| `<PREFIX>_MTLS_ENABLED` | `""` / `false` / `0` / `False` / `FALSE` → disabled `(nil,nil)`; anything else → continue |
| `<PREFIX>_CERTIFICATE` | when set → `SourceFile` (takes precedence) |
| `<PREFIX>_CERTIFICATE_BASE64` | when set (and no `_CERTIFICATE`) → `SourceBase64` |
| `<PREFIX>_CERTIFICATE_PASSWORD` | optional password for either source |

Enabled but neither cert var set ⇒ a clear error. Convention: `integration-efi`
uses prefix `EFI`, `integration-stripe` uses `STRIPE`, etc.

```go
tlsCfg, err := mtls.LoadFromEnv("EFI")        // nil when EFI_MTLS_ENABLED=false
client := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}}
```

---

## webhookhttp

A `net/http.Server` wrapper for inbound webhooks: per-route HMAC verify,
idempotency extraction, body-size limits, and a return-value → HTTP-status map.

```go
const DefaultMaxBodyBytes int64 = 65_536 // 64 KB

type Config struct {
    Addr         string        // required (e.g. ":8082")
    TLSConfig    *tls.Config   // non-nil ⇒ TLS / mTLS
    MaxBodyBytes int64         // 0 ⇒ 64 KB
    ReadTimeout, WriteTimeout, IdleTimeout time.Duration // 0 ⇒ 10s/10s/30s
}

type Delivery struct { Body []byte; Headers http.Header; IdempotencyKey string }
type Handler  func(ctx context.Context, d Delivery) error

var ErrDuplicate error               // ⇒ HTTP 200
type TerminalError struct{ Cause error } // ⇒ HTTP 400 (implements Error/Unwrap)

type HandlerOption func(*handlerConfig)
func WithVerifyFunc(fn func(r *http.Request, body []byte) error) HandlerOption  // failure ⇒ 401, before handler
func WithIdempotencyKey(fn func(r *http.Request, body []byte) string) HandlerOption
func WithMaxBodyBytes(n int64) HandlerOption

func New(cfg Config) *Server
func (s *Server) Handle(method, path string, h Handler, opts ...HandlerOption) *Server
func (s *Server) ListenAndServe(ctx context.Context) error // graceful shutdown on ctx cancel

// helpers
func ParseHMACSHA256Signature(header string) (hex string, ok bool)
func VerifyHMACSHA256Header(body []byte, header string, secret []byte) error // wraps sig/hmac
```

**Handler return → HTTP status (verified in `server.go`):**

| Outcome | Status |
|---|---|
| verify func returns non-nil | **401 Unauthorized** (before the handler runs) |
| handler returns `nil` | 202 Accepted |
| handler returns `ErrDuplicate` | 200 OK |
| handler returns `*TerminalError` | 400 Bad Request |
| handler returns any other error | 500 Internal Server Error |
| body exceeds the limit | 413 Request Entity Too Large |
| unregistered method on a known path | 405 Method Not Allowed (with `Allow`) |

> The old docs' return-table omitted the **401 verify-failure** and the 413/405
> paths — they're in the source and documented here.

```go
srv := webhookhttp.New(webhookhttp.Config{Addr: ":8082"}).
    Handle("POST", "/webhooks/stripe", handleStripe,
        webhookhttp.WithVerifyFunc(func(r *http.Request, body []byte) error {
            _, err := sdkhmac.VerifyStripe(body, r.Header.Get("Stripe-Signature"), secret, 300)
            return err
        }),
    )
go srv.ListenAndServe(ctx)
```

---

## sdk/reconcile

Expresses the universal capability convention (`ensure_/observe_/destroy_`) as a
typed interface, wires it into the adapter's execute dispatch, and (with an
emitter) auto-emits §6.5 `MutationEvent`s. **This package is the heart of a
production adapter — its full contract is in
[RECONCILE-AND-EVENTS.md](RECONCILE-AND-EVENTS.md).**

```go
type Reconciler[D any, O any] interface {
    Ensure(ctx context.Context, desired D) (O, error)                       // idempotent
    Observe(ctx context.Context, filter map[string]any) ([]O, string, error) // items, cursor("" = done)
    Destroy(ctx context.Context, ref string) error                          // 404-tolerant
}

// opt-in: when a Reconciler ALSO implements this, dispatch prefers it and passes
// the FULL desired payload (so destroy can read reserved __instance_credentials etc.)
type DestroyWithDesired[D any] interface {
    DestroyWithDesired(ctx context.Context, ref string, desired D) error
}
type Discoverer[O any]      interface { Discover(ctx, scope map[string]any) ([]O, error) }
type DriftReporter[D, O any] interface { Drift(desired D, observed O) bool }

func RegisterReconciler[D, O any](a *adapter.Adapter, resource, resourceType string,
    r Reconciler[D, O], opts ...Option)
func Dispatch(ctx context.Context, a *adapter.Adapter, d rpc.Delivery) ([]byte, string, error)

// Deprecated: alias for Dispatch; removed at v1.0.0.
func ExecuteForTest(ctx context.Context, a *adapter.Adapter, d rpc.Delivery) ([]byte, string, error)

// Options
func WithEmitter(em events.Emitter) Option   // wire §6.5 auto-emission
func WithProvider(provider string) Option     // sets the event_type provider prefix (PASS THIS — no auto-default)
func WithInstanceID(instanceID string) Option // multi-tenant scope
func WithLegacyNames(names ...string) Option   // pre-convention name shim (WARN; removal target v0.7.0)
func WithWarnLogger(fn func(format string, args ...any)) Option
```

`RegisterReconciler(a, "user", "users", r)` installs `ensure_user`,
`observe_users`, `destroy_user`. It auto-installs the adapter's `execute` handler
on the first call per adapter. See the reconcile doc for the dispatch table,
`resource_id` inference, idempotency synthesis, and the registration-order
gotcha.

---

## sdk/events

The §6.5 `MutationEvent` payload + the transport that POSTs it to
`yggdrasil-core`.

```go
type Verb string
const ( VerbEnsured Verb = "ensured"; VerbDestroyed Verb = "destroyed"; VerbCreated Verb = "created" )

type MutationEvent struct {
    EventType   string          `json:"event_type"`   // "<provider>.<resource>.<verb>"
    Provider    string          `json:"provider"`
    Resource    string          `json:"resource"`
    Verb        Verb            `json:"verb"`
    ResourceID  string          `json:"resource_id"`
    InstanceID  string          `json:"instance_id"`
    Idempotency string          `json:"idempotency"`
    Observed    json.RawMessage `json:"observed"`
    EmittedAt   time.Time       `json:"emitted_at"`   // emitter fills if zero
}
func BuildEventType(provider, resource string, verb Verb) string

type Emitter interface { Emit(ctx context.Context, e MutationEvent) error }

func NewHTTPEmitter(opts ...Option) Emitter // POSTs to <core>/api/v1/events; reads env fallbacks
type NoopEmitter struct{ Logger func(format string, args ...any) } // WARN-logs, never posts

// constants + options
const ( EnvCoreURL = "YGGDRASIL_CORE_URL"; EnvRunToken = "YGGDRASIL_RUN_TOKEN"
        DefaultEventsPath = "/api/v1/events"; DefaultMaxRetries = 3
        DefaultRetryBackoff = 250 * time.Millisecond; DefaultHTTPTimeout = 5 * time.Second )
func WithCoreURL(url string) Option
func WithToken(token string) Option
func WithHTTPClient(c *http.Client) Option
func WithMaxRetries(n int) Option
func WithRetryBackoff(d time.Duration) Option
func WithEventsPath(path string) Option
```

`NewHTTPEmitter` falls back to `YGGDRASIL_CORE_URL` / `YGGDRASIL_RUN_TOKEN` when
`WithCoreURL` / `WithToken` aren't passed; sends `Authorization: Bearer <token>`;
**retries transient 5xx** (flat backoff, total `maxRetries` attempts), treats
**4xx as terminal**, and honors `context` cancellation between retries.
`NoopEmitter`'s zero value is usable (falls back to `log.Printf`).
</content>
