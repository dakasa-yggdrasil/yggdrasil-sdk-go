# Waiting for the original shutdown

`Adapter.Run` still waits at most `DefaultTimeout + 1 second` for shutdown.
If that wait expires, it returns a deadline error while the original admitted
work and settlement remain owned. The request timeout, queue names, delivery
envelopes, Reply/Ack/Nack behavior and event contracts are unchanged.

`Adapter.WaitDrained(ctx)` is an additive API for joining that exact original
shutdown. Its completion includes every original subscription's close result
and the original `afterRun` callback (HTTP server shutdown or AMQP connection
closure). It does not create another subscription, invoke the provider again,
reset credentials or infer that an interrupted operation had no effects.

The completion handle is created at the beginning of the first `Run`, before
startup. `WaitDrained` before that returns `ErrRunNotStarted`. Concurrent or
repeated `Run` returns `ErrRunAlreadyStarted` and never replaces the handle.
Every waiter observes the same completion channel and final cleanup error.
A cancelled or expired waiter returns its own context error and leaves the
original owner running. A permanently incomplete provider or settlement stays
incomplete; another wait budget cannot turn it into a successful drain.

The error from `Run` describes its own bounded wait and startup state. The
later result from `WaitDrained` describes actual original completion. A
deadline from the earlier wait is not inserted into a later successful final
result. Actual startup and cleanup failures remain in that final result.
Cleanup installed by a partially failed startup is included as well.

Built-in HTTP and AMQP subscriptions additionally implement synchronous
`StopAdmission()`. All original endpoints stop admitting new invocations
before any original endpoint join begins. HTTP rejects new calls even before
the server's deferred `Shutdown`; AMQP preserves the original handler channel
through Reply/Ack/Nack. The public `rpc.Subscription` interface remains
unchanged. Legacy custom subscriptions still close concurrently and are joined
according to their existing `Close` contract; that interface does not provide
a separate synchronous admission phase or prove more than its implementation.

A caller that needs eventual original completion after a `Run` error can call
`worker.WaitDrained(context.Background())` without replaying work. Only a nil
result qualifies successful original cleanup. Any error leaves the result
unqualified: applications that admitted work must retain their original
process in their existing HOLD path, rather than exit because a failed cleanup
callback returned. Kustomize and Helm retain their existing indefinite HOLD
until a separately qualified consumer pin adopts this API.

Panics are contained only at original stop-admission, subscription-close and
`afterRun` callback boundaries. The error identifies the fixed phase and says
completion is unqualified; the panic value is never dumped or formatted.
Other original endpoints still receive their stop and join calls. A stop/close
panic defers global `afterRun` cleanup under that unknown settlement, so no
transport is closed as though that failed callback had joined its resource.
Nothing is retried. `afterRun`'s own panic likewise remains an unqualified
error. An external goroutine is not joined merely because its callback
panicked and returned to the containment boundary.

Using an uncancelled context deliberately retains the original process for an
unbounded, honestly incomplete operation. A caller may instead bound its wait,
but must retain that unknown lifetime and avoid claiming an ordinary successful
exit, safe replay or absence of provider effects.

CI tests use actual HTTP response/endpoint refusal and an isolated RabbitMQ
service with the exact original Ack held across `Run` budget expiry. They also
cover concurrent waiters, cancellation of one waiter, startup failure,
`afterRun` held after handler completion, and fixed callback-panic errors while
a separate original endpoint and an external fixture goroutine remain active. Race tests and vet run only in CI.
This Source candidate does not update existing SDK098 consumers, publish a
tag or image, or change a live deployment. Consumers need a separately reviewed
pin and runtime adoption before using this API.
