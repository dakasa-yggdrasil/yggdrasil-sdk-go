// Package amqp implements rpc.Transport over RabbitMQ / AMQP 0-9-1
// using streadway's amqp091-go. This is the backend every deployment
// used before the rpc abstraction landed; the wrapper preserves the
// exact wire semantics (queue per endpoint, reply_to + correlation_id
// for RPC, manual ack) while exposing a transport-agnostic interface.
package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
	"github.com/google/uuid"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

// Transport is an rpc.Transport backed by one AMQP connection.
// The connection is expected to outlive the transport; Close closes
// it along with the shared publish channel.
type Transport struct {
	conn *amqp091.Connection

	pubMu sync.Mutex
	pubCh *amqp091.Channel

	closed   chan struct{}
	closeErr error

	subsMu sync.Mutex
	subs   []*subscription

	// endpointPrefix is prepended to ConsumerConfig.Endpoint when
	// declaring/consuming a queue. It exists because integration_type
	// manifests in yggdrasil-core declare AMQP queues under a
	// "yggdrasil.adapter.<provider>." namespace, but adapter authors
	// register handlers by bare capability name ("describe", "execute").
	// Without the prefix, the consumer queue would not match what
	// yggdrasil-core publishes to and the adapter would silently sit
	// on an empty queue while requests piled up on the real one.
	// Set via SetEndpointPrefix; ListenAMQP wires it from Config.Provider.
	endpointPrefix string
}

// New wraps an open AMQP connection. Callers provision the connection
// (via amqp091.Dial or a managed pool) and hand ownership to the
// transport; Close will close the underlying connection.
func New(conn *amqp091.Connection) *Transport {
	return &Transport{
		conn:   conn,
		closed: make(chan struct{}),
	}
}

// Connection returns the underlying amqp connection. Exposed so
// legacy code that still expects *amqp091.Connection can reach it
// during the migration. New code should not depend on this.
func (t *Transport) Connection() *amqp091.Connection {
	return t.conn
}

// SetEndpointPrefix configures a string prepended to every consumer
// endpoint name. Used by ListenAMQP to apply the
// "yggdrasil.adapter.<provider>." namespace expected by
// yggdrasil-core integration_type manifests.
func (t *Transport) SetEndpointPrefix(prefix string) {
	t.endpointPrefix = prefix
}

// Consume declares the endpoint queue (durable) and starts a
// consumer that dispatches each delivery to cfg.Handler.
//
// The consumer auto-recovers when the AMQP channel closes (broker
// restart, connection blip, channel reset). Without that, a single
// transient close would silently strand the subscription — the
// goroutine reading deliveries would exit when the channel range loop
// drained, the queue would still exist, but consumer count drops to 0
// and messages pile up unread. Recovery is event-driven via
// ch.NotifyClose: each time the channel signals close while sub.done
// has not been closed by the caller, the subscription tears down the
// dead channel and re-runs setupConsumer. setupConsumer is idempotent
// (QueueDeclare with the same params is a no-op) so this is safe to
// retry. Backoff doubles up to 30s to ride out broker restart windows
// without thrashing.
func (t *Transport) Consume(cfg rpc.ConsumerConfig) (rpc.Subscription, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("amqp consume: endpoint is required")
	}
	if cfg.Handler == nil {
		return nil, fmt.Errorf("amqp consume: handler is required")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}

	queueName := t.endpointPrefix + cfg.Endpoint

	// Validate that we can set up the consumer once before returning to
	// the caller. After the initial success the subscription owns the
	// reconnect loop; the caller doesn't need to know recovery is
	// happening.
	ch, deliveries, err := t.setupConsumer(queueName, cfg.Concurrency)
	if err != nil {
		return nil, err
	}

	sub := &subscription{
		transport: t,
		endpoint:  queueName,
		ch:        ch,
		done:      make(chan struct{}),
	}

	go sub.runWithReconnect(deliveries, cfg)

	t.subsMu.Lock()
	t.subs = append(t.subs, sub)
	t.subsMu.Unlock()

	return sub, nil
}

// setupConsumer opens a channel, declares the queue (idempotent on
// re-runs because QueueDeclare with identical parameters is a no-op),
// sets the prefetch QoS and starts the basic.consume. Factored out of
// Consume so the subscription's reconnect loop can call it again to
// rebind after a broker channel close without re-running input
// validation. Caller owns the returned channel and is responsible for
// closing it when done.
func (t *Transport) setupConsumer(queueName string, concurrency int) (*amqp091.Channel, <-chan amqp091.Delivery, error) {
	ch, err := t.conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("amqp consume: open channel: %w", err)
	}
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("amqp consume: declare queue %q: %w", queueName, err)
	}
	if err := ch.Qos(concurrency, 0, false); err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("amqp consume: set qos: %w", err)
	}
	deliveries, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("amqp consume: start consumer %q: %w", queueName, err)
	}
	return ch, deliveries, nil
}

// Publish sends a fire-and-forget message to req.Endpoint. Headers,
// correlation, and reply-to are ignored; Publish is for producers
// that do not expect a reply.
func (t *Transport) Publish(ctx context.Context, req rpc.Request) error {
	if req.Endpoint == "" {
		return fmt.Errorf("amqp publish: endpoint is required")
	}

	ch, err := t.publishChannel()
	if err != nil {
		return err
	}

	contentType := req.ContentType
	if contentType == "" {
		contentType = "application/json"
	}

	return ch.PublishWithContext(ctx, "", req.Endpoint, false, false, amqp091.Publishing{
		ContentType: contentType,
		Headers:     stringHeadersToTable(req.Headers),
		Body:        req.Body,
	})
}

// Request performs an RPC: publishes with a temporary reply queue,
// then waits for the matching correlation id. Translates
// timeouts/cancellations to rpc.ErrTimeout / context.Canceled.
func (t *Transport) Request(ctx context.Context, req rpc.Request) (rpc.Reply, error) {
	if req.Endpoint == "" {
		return rpc.Reply{}, fmt.Errorf("amqp request: endpoint is required")
	}

	ch, err := t.conn.Channel()
	if err != nil {
		return rpc.Reply{}, fmt.Errorf("amqp request: open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	replyQueue, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return rpc.Reply{}, fmt.Errorf("amqp request: declare reply queue: %w", err)
	}

	deliveries, err := ch.Consume(replyQueue.Name, "", true, true, false, false, nil)
	if err != nil {
		return rpc.Reply{}, fmt.Errorf("amqp request: consume reply queue: %w", err)
	}

	correlationID := req.CorrelationID
	if correlationID == "" {
		correlationID = uuid.NewString()
	}
	contentType := req.ContentType
	if contentType == "" {
		contentType = "application/json"
	}

	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}

	if err := ch.PublishWithContext(ctx, "", req.Endpoint, false, false, amqp091.Publishing{
		ContentType:   contentType,
		CorrelationId: correlationID,
		ReplyTo:       replyQueue.Name,
		Headers:       stringHeadersToTable(req.Headers),
		Body:          req.Body,
	}); err != nil {
		return rpc.Reply{}, fmt.Errorf("amqp request: publish: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return rpc.Reply{}, rpc.ErrTimeout
			}
			return rpc.Reply{}, ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				return rpc.Reply{}, fmt.Errorf("amqp request: reply channel closed")
			}
			if delivery.CorrelationId != correlationID {
				// Stale reply from a previous call; skip.
				continue
			}
			return rpc.Reply{
				Body:          delivery.Body,
				ContentType:   delivery.ContentType,
				Headers:       tableToStringHeaders(delivery.Headers),
				CorrelationID: delivery.CorrelationId,
			}, nil
		}
	}
}

// Close tears down the shared publish channel, every subscription,
// and the underlying connection. Safe to call multiple times.
func (t *Transport) Close() error {
	select {
	case <-t.closed:
		return t.closeErr
	default:
	}

	t.subsMu.Lock()
	subs := append([]*subscription(nil), t.subs...)
	t.subs = nil
	t.subsMu.Unlock()

	for _, sub := range subs {
		_ = sub.Close()
	}

	t.pubMu.Lock()
	if t.pubCh != nil {
		_ = t.pubCh.Close()
		t.pubCh = nil
	}
	t.pubMu.Unlock()

	err := t.conn.Close()
	t.closeErr = err
	close(t.closed)
	return err
}

func (t *Transport) publishChannel() (*amqp091.Channel, error) {
	t.pubMu.Lock()
	defer t.pubMu.Unlock()

	if t.pubCh != nil && !t.pubCh.IsClosed() {
		return t.pubCh, nil
	}
	ch, err := t.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("amqp: open publish channel: %w", err)
	}
	t.pubCh = ch
	return ch, nil
}

// subscription wraps one active consumer.
type subscription struct {
	transport *Transport
	endpoint  string
	chMu      sync.Mutex // guards ch — swapped on reconnect
	ch        *amqp091.Channel
	done      chan struct{}
	closeOnce sync.Once
}

func (s *subscription) Endpoint() string { return s.endpoint }

func (s *subscription) currentChannel() *amqp091.Channel {
	s.chMu.Lock()
	defer s.chMu.Unlock()
	return s.ch
}

func (s *subscription) swapChannel(ch *amqp091.Channel) {
	s.chMu.Lock()
	defer s.chMu.Unlock()
	s.ch = ch
}

func (s *subscription) Close() error {
	var err error
	s.closeOnce.Do(func() {
		ch := s.currentChannel()
		if ch != nil {
			err = ch.Close()
		}
		close(s.done)
	})
	return err
}

// runWithReconnect is the outer loop that re-runs setupConsumer when
// the AMQP channel closes unexpectedly. The first iteration consumes
// the deliveries channel handed in by Consume; subsequent iterations
// rebind via subscription.transport.setupConsumer. Exits cleanly when
// sub.Close() is called (s.done closed). Each reconnect attempt waits
// an exponentially backing-off interval (1s → 30s, capped) so a broker
// in extended downtime does not get hammered.
func (s *subscription) runWithReconnect(initialDeliveries <-chan amqp091.Delivery, cfg rpc.ConsumerConfig) {
	deliveries := initialDeliveries
	backoff := time.Second
	for {
		// Deliver-loop returns when the underlying channel closes
		// (range exit on closed deliveries chan). We then check
		// whether this was a caller-initiated Close (sub.done) or a
		// broker-initiated drop (need to reconnect).
		s.deliverLoop(deliveries, cfg)

		select {
		case <-s.done:
			return
		default:
		}

		// Channel died unexpectedly. Discard the dead one and try to
		// rebind. Loop until success or sub.Close() arrives.
		for {
			select {
			case <-s.done:
				return
			case <-time.After(backoff):
			}
			ch, newDeliveries, err := s.transport.setupConsumer(s.endpoint, cfg.Concurrency)
			if err == nil {
				s.swapChannel(ch)
				deliveries = newDeliveries
				backoff = time.Second // reset on successful rebind
				break
			}
			if backoff < 30*time.Second {
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			}
		}
	}
}

// deliverLoop fans each delivery into a handler, respecting concurrency
// and the per-call timeout. It builds a rpc.Delivery struct wiring its
// Ack/Nack/Reply function fields to the amqp091 primitives on the
// subscription's channel. Returns when the deliveries channel closes,
// at which point the outer runWithReconnect decides whether to rebind.
func (s *subscription) deliverLoop(deliveries <-chan amqp091.Delivery, cfg rpc.ConsumerConfig) {
	sem := make(chan struct{}, cfg.Concurrency)
	for raw := range deliveries {
		raw := raw
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()

			ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
			defer cancel()

			// settleOnce guards Ack/Nack — they are mutually
			// exclusive and must only fire once per delivery.
			var settleOnce sync.Once
			// replyOnce guards Reply — Publish to reply_to once
			// per delivery. Multiple reply attempts would violate
			// RPC semantics.
			var replyOnce sync.Once

			ack := func() error {
				var err error
				settleOnce.Do(func() { err = raw.Ack(false) })
				return err
			}
			nack := func(requeue bool) error {
				var err error
				settleOnce.Do(func() { err = raw.Nack(false, requeue) })
				return err
			}
			reply := func(ctx context.Context, body []byte, contentType string) error {
				if raw.ReplyTo == "" {
					return fmt.Errorf("amqp delivery: no reply_to set")
				}
				if contentType == "" {
					contentType = "application/json"
				}
				var replyErr error
				replyOnce.Do(func() {
					ch := s.currentChannel()
					if ch == nil {
						replyErr = fmt.Errorf("amqp delivery: subscription has no active channel")
						return
					}
					replyErr = ch.PublishWithContext(ctx, "", raw.ReplyTo, false, false, amqp091.Publishing{
						ContentType:   contentType,
						CorrelationId: raw.CorrelationId,
						Body:          body,
					})
				})
				return replyErr
			}

			d := rpc.Delivery{
				Endpoint:      s.endpoint,
				Body:          raw.Body,
				ContentType:   raw.ContentType,
				Headers:       tableToStringHeaders(raw.Headers),
				CorrelationID: raw.CorrelationId,
				ReplyTo:       raw.ReplyTo,
				AckFn:         ack,
				NackFn:        nack,
				ReplyFn:       reply,
			}

			if err := cfg.Handler(ctx, d); err != nil {
				// Fallback settle if the handler forgot.
				settleOnce.Do(func() { _ = raw.Nack(false, false) })
				return
			}
			settleOnce.Do(func() { _ = raw.Ack(false) })
		}()
	}
}

// stringHeadersToTable converts the rpc-agnostic map to the AMQP
// Table type, preserving string values.
func stringHeadersToTable(m map[string]string) amqp091.Table {
	if len(m) == 0 {
		return nil
	}
	t := amqp091.Table{}
	for k, v := range m {
		t[k] = v
	}
	return t
}

func tableToStringHeaders(t amqp091.Table) map[string]string {
	if len(t) == 0 {
		return nil
	}
	m := make(map[string]string, len(t))
	for k, v := range t {
		switch typed := v.(type) {
		case string:
			m[k] = typed
		case []byte:
			m[k] = string(typed)
		default:
			// Best-effort JSON for anything unexpected. Transports
			// with richer native types may lose fidelity here, but
			// the generic abstraction is string-only.
			b, err := json.Marshal(typed)
			if err == nil {
				m[k] = string(b)
			}
		}
	}
	return m
}
