// Package rpc defines the generic transport contract the core uses to
// exchange RPC messages with its consumers and with integration
// adapters. Implementations live in subpackages (rpc/amqp, rpc/http,
// …). Adding a new backend — Kafka, NATS, gRPC, SQS, Pub/Sub — is a
// matter of implementing Transport and registering it under a string
// name for YGGDRASIL_RPC_TRANSPORT.
//
// Shape:
//
//	Transport — the broker-side handle. Produces deliveries into
//	            handlers (Consume) and dispatches Requests that expect
//	            a Reply (Request) or not (Publish).
//	Delivery  — one inbound message. Carries body + correlation +
//	            reply-to metadata plus Reply/Ack/Nack methods that
//	            abstract the transport-specific response semantics.
//	Request   — one outbound message. Endpoint + body + optional
//	            correlation/reply-to/timeout.
//	Reply     — the response to a Request. Body + headers +
//	            content-type.
//
// The abstraction is intentionally thin: it wraps the AMQP mental
// model (publish / reply-to / correlation) because every
// request/response broker can express the same shape — but we only
// expose what the core actually uses.
package rpc

import (
	"context"
	"errors"
	"time"
)

// ErrClosed is returned by transport operations after Close has
// been called. Callers can use errors.Is to test for it.
var ErrClosed = errors.New("rpc: transport closed")

// ErrTimeout indicates a Request did not receive a reply within the
// configured timeout.
var ErrTimeout = errors.New("rpc: request timed out")

// ErrEndpointUnknown is returned when Consume is called for an
// endpoint the transport does not recognize, or when Request is
// dispatched to an endpoint no consumer has registered for (on
// transports that can tell — e.g. HTTP).
var ErrEndpointUnknown = errors.New("rpc: endpoint unknown")

// Transport is the generic RPC handle.
//
// Implementations must be safe for concurrent use. Consume may be
// called multiple times for different endpoints; each call registers
// an independent handler. Request and Publish are stateless beyond
// the open underlying connection.
type Transport interface {
	// Consume registers a handler for the given endpoint. The
	// returned Subscription's Close unregisters the handler and
	// drains any in-flight deliveries.
	Consume(cfg ConsumerConfig) (Subscription, error)

	// Request sends req and blocks until a Reply arrives, the
	// context is cancelled, or the request times out.
	Request(ctx context.Context, req Request) (Reply, error)

	// Publish sends req as fire-and-forget. ReplyTo and correlation
	// are ignored if set.
	Publish(ctx context.Context, req Request) error

	// Close releases the underlying connection and all
	// subscriptions. Safe to call multiple times.
	Close() error
}

// Subscription represents one active consumer. Closing it stops new
// deliveries and waits for any in-flight handler to finish.
type Subscription interface {
	Endpoint() string
	Close() error
}

// ConsumerConfig configures one Consume call.
type ConsumerConfig struct {
	// Endpoint names the message path (AMQP queue, HTTP route, Kafka
	// topic, NATS subject). Required.
	Endpoint string

	// Handler processes each delivery. Must call Ack or Nack on the
	// Delivery before returning; the transport may retry if it does
	// not.
	Handler Handler

	// Timeout bounds each handler invocation. When the handler
	// returns before the timeout, its value wins; otherwise the
	// transport cancels the handler's context and Nacks the
	// delivery.
	Timeout time.Duration

	// Concurrency is the max number of handlers in flight at once.
	// Defaults to 1 (serial). Maps to AMQP prefetch, HTTP worker
	// pool, or the equivalent on other transports.
	Concurrency int
}

// Handler processes one inbound delivery.
type Handler func(ctx context.Context, d Delivery) error

// Request is the outbound message shape shared by Request and
// Publish. Transports translate between this and their native format
// (AMQP Publishing, HTTP request, Kafka record, etc.).
type Request struct {
	// Endpoint is the target path (queue / route / topic). Required.
	Endpoint string

	// Body is the payload. Transports do not interpret it.
	Body []byte

	// ContentType defaults to "application/json" when empty.
	ContentType string

	// Headers are optional key/value metadata propagated to the
	// receiving Delivery.Headers() map. Transports with no native
	// header support encode them in the body framing.
	Headers map[string]string

	// CorrelationID is set by the caller when it wants the
	// corresponding Reply to carry the same id. Transports that do
	// not support correlation (fire-and-forget only) pass it
	// through in the body framing.
	CorrelationID string

	// Timeout bounds a Request call. Zero uses the transport's
	// default. Ignored by Publish.
	Timeout time.Duration
}

// Reply is the response to a Request.
type Reply struct {
	Body          []byte
	ContentType   string
	Headers       map[string]string
	CorrelationID string
}

// Delivery is one inbound message handed to a Consume handler.
//
// The struct shape lets handlers keep concise field access
// (`d.Body`, `d.ReplyTo`, `d.CorrelationID`) regardless of the
// backing transport. Per-message actions (Ack, Nack, Reply) are
// methods that dispatch through the function-valued fields the
// transport populates when it builds the Delivery. AMQP, HTTP,
// gRPC, Kafka, NATS each construct a Delivery from their native
// message without requiring handlers to know which backend
// delivered it.
type Delivery struct {
	// Endpoint is the endpoint this delivery was consumed from.
	Endpoint string

	// Body is the raw payload.
	Body []byte

	// ContentType is the body MIME type.
	ContentType string

	// Headers are the delivery metadata (may be nil).
	Headers map[string]string

	// CorrelationID is the correlation id of the originating
	// Request, or "" when not set.
	CorrelationID string

	// ReplyTo is where Reply should send the response.
	ReplyTo string

	// AckFn is called by Ack(). Transports populate it when
	// constructing the Delivery.
	AckFn func() error

	// NackFn is called by Nack(requeue).
	NackFn func(requeue bool) error

	// ReplyFn is called by Reply(ctx, body, contentType).
	ReplyFn func(ctx context.Context, body []byte, contentType string) error
}

// Ack acknowledges successful processing.
func (d Delivery) Ack() error {
	if d.AckFn == nil {
		return nil
	}
	return d.AckFn()
}

// Nack signals the handler failed. When requeue is true, the
// transport should redeliver (if supported); otherwise the delivery
// is discarded / dead-lettered per transport policy.
func (d Delivery) Nack(requeue bool) error {
	if d.NackFn == nil {
		return nil
	}
	return d.NackFn(requeue)
}

// Reply sends a response to the originator. Safe to call at most
// once per Delivery.
func (d Delivery) Reply(ctx context.Context, body []byte, contentType string) error {
	if d.ReplyFn == nil {
		return errors.New("rpc: delivery has no reply function (transport did not populate it)")
	}
	if contentType == "" {
		contentType = "application/json"
	}
	return d.ReplyFn(ctx, body, contentType)
}

// Envelope is the optional JSON framing shared by all transports that
// lack native correlation/headers. AMQP does not use it (it has
// native AMQP.Publishing fields). HTTP uses it so handlers see the
// same Headers() / CorrelationID() semantics regardless of the
// backend.
type Envelope struct {
	CorrelationID string            `json:"correlation_id,omitempty"`
	ReplyTo       string            `json:"reply_to,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	ContentType   string            `json:"content_type,omitempty"`
	Body          []byte            `json:"body"`
}
