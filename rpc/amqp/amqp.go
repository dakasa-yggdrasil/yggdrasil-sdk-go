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

// DialFunc opens a fresh AMQP connection. Used by the transport's
// reconnect path to replace a broker-dropped connection without the
// caller having to wire it up. Implementations should retry their own
// internal backoff if the broker is temporarily unreachable, or return
// the error and let the transport's outer loop retry with backoff.
type DialFunc func() (*amqp091.Connection, error)

// Transport is an rpc.Transport backed by one AMQP connection.
// The connection is expected to outlive the transport; Close closes
// it along with the shared publish channel.
//
// When constructed via NewWithDial (or after SetDialFunc), the
// transport runs a watchdog goroutine that re-dials the connection on
// broker-side disconnects (rabbit restart, network blip, idle close).
// Subscriptions notice the new connection on their next setupConsumer
// retry and rebind. Without a dial function, the transport falls back
// to the legacy single-connection mode — sufficient for tests but
// brittle in production because once the connection dies, no
// subscription can recover.
type Transport struct {
	connMu sync.RWMutex
	conn   *amqp091.Connection

	dialFn DialFunc

	pubMu sync.Mutex
	pubCh *amqp091.Channel

	closed   chan struct{}
	closeErr error
	closeMu  sync.Mutex

	subsMu sync.Mutex
	subs   []*subscription

	// watchdogOnce ensures we only spawn the connection watchdog
	// goroutine once even if SetDialFunc is called multiple times or
	// raced with Consume.
	watchdogOnce sync.Once

	// dialMu serializes calls to dialFn so that N concurrent
	// goroutines all hitting a dead connection at once trigger ONE
	// dial, not N. Without this, an adapter with M subscriptions
	// would open M sockets at the broker on every reconnect, then
	// close M-1 of them — wasteful, and on a fully-down broker would
	// stack M error-handling backoff loops in lockstep.
	dialMu sync.Mutex

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
//
// Transports built with New have NO connection-level reconnect. If the
// broker drops the connection, every subscription's setupConsumer will
// keep failing with "connection closed" forever. Use NewWithDial in
// production; New stays available for tests and for callers that
// manage connections externally.
func New(conn *amqp091.Connection) *Transport {
	return &Transport{
		conn:   conn,
		closed: make(chan struct{}),
	}
}

// NewWithDial constructs a Transport that owns its connection and can
// re-dial when the broker drops it. dialFn is invoked once eagerly to
// open the initial connection, then again whenever the watchdog
// observes a NotifyClose without Close having been called. Returns the
// dial error if the initial connection cannot be opened — at that
// point retrying is the caller's responsibility (typical pattern:
// dialAMQPWithRetry around the dialFn before handing it here).
func NewWithDial(dialFn DialFunc) (*Transport, error) {
	if dialFn == nil {
		return nil, errors.New("amqp: NewWithDial requires a non-nil DialFunc")
	}
	conn, err := dialFn()
	if err != nil {
		return nil, fmt.Errorf("amqp: initial dial: %w", err)
	}
	t := &Transport{
		conn:   conn,
		dialFn: dialFn,
		closed: make(chan struct{}),
	}
	t.startWatchdog()
	return t, nil
}

// SetDialFunc enables connection-level reconnect on a Transport built
// with New. Useful when the dial function is not available at
// construction time but the caller wants reconnect semantics anyway.
// Safe to call from any goroutine; only the first call wires the
// watchdog.
func (t *Transport) SetDialFunc(dialFn DialFunc) {
	if dialFn == nil {
		return
	}
	t.connMu.Lock()
	t.dialFn = dialFn
	t.connMu.Unlock()
	t.startWatchdog()
}

// Connection returns the underlying amqp connection. Exposed so
// legacy code that still expects *amqp091.Connection can reach it
// during the migration. New code should not depend on this.
//
// IMPORTANT: when the transport has a DialFunc, the returned pointer
// becomes stale after a reconnect. Callers that hold the value across
// any duration MUST re-fetch via Connection() before each use, or
// expect "connection closed" errors when the broker restarts.
func (t *Transport) Connection() *amqp091.Connection {
	t.connMu.RLock()
	defer t.connMu.RUnlock()
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
//
// When the underlying connection is dropped (rabbit pod restart), the
// transport's watchdog re-dials and replaces t.conn; the subscription's
// next setupConsumer attempt picks up the new connection automatically.
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
//
// If the connection itself is closed (broker restart), this attempts a
// single inline reconnect via the transport's DialFunc before opening
// the channel. That keeps the failure path one error deep — callers
// (the subscription reconnect loop) just retry on any error and the
// backoff handles repeated dial failures.
func (t *Transport) setupConsumer(queueName string, concurrency int) (*amqp091.Channel, <-chan amqp091.Delivery, error) {
	conn, err := t.connectionForUse()
	if err != nil {
		return nil, nil, fmt.Errorf("amqp consume: get connection: %w", err)
	}
	ch, err := conn.Channel()
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

// connectionForUse returns a live connection, attempting a single
// inline reconnect if the current one is closed and a DialFunc is
// available. Returns ErrClosed if Close has been called on the
// transport. Callers must NOT cache the returned pointer beyond the
// immediate operation — the watchdog may replace it concurrently.
func (t *Transport) connectionForUse() (*amqp091.Connection, error) {
	t.connMu.RLock()
	conn := t.conn
	dialFn := t.dialFn
	t.connMu.RUnlock()

	if conn != nil && !conn.IsClosed() {
		return conn, nil
	}

	select {
	case <-t.closed:
		return nil, rpc.ErrClosed
	default:
	}

	if dialFn == nil {
		return conn, nil // legacy mode — return the (probably dead) conn and let the caller error out
	}

	return t.reconnect(dialFn)
}

// reconnect dials a fresh connection and swaps it in under connMu.
// dialMu serializes the dial itself so that N concurrent callers
// against a dead connection produce one socket, not N.
//
// The check-after-lock pattern is critical: by the time a goroutine
// acquires dialMu, the previous holder may have already replaced t.conn
// with a live one — in that case we return it instead of opening yet
// another socket. This is the cheap path; the dial only fires when
// nobody else has reconnected yet.
func (t *Transport) reconnect(dialFn DialFunc) (*amqp091.Connection, error) {
	t.dialMu.Lock()
	defer t.dialMu.Unlock()

	// After acquiring dialMu, re-check whether a previous dial already
	// installed a live connection. This collapses the M-goroutine
	// reconnect storm into a single dial.
	t.connMu.RLock()
	if t.conn != nil && !t.conn.IsClosed() {
		conn := t.conn
		t.connMu.RUnlock()
		return conn, nil
	}
	t.connMu.RUnlock()

	// Also short-circuit if Close happened while we were waiting.
	select {
	case <-t.closed:
		return nil, rpc.ErrClosed
	default:
	}

	newConn, err := dialFn()
	if err != nil {
		return nil, err
	}

	t.connMu.Lock()
	// Close stale connection (may already be closed; tolerant).
	if t.conn != nil {
		_ = t.conn.Close()
	}
	t.conn = newConn
	t.connMu.Unlock()

	// Reset the publish channel so the next Publish call opens a fresh
	// one on the new connection. The old pubCh is bound to the dead
	// connection and would error on every publish.
	t.pubMu.Lock()
	if t.pubCh != nil {
		_ = t.pubCh.Close()
		t.pubCh = nil
	}
	t.pubMu.Unlock()

	return newConn, nil
}

// startWatchdog spawns a goroutine that listens for NotifyClose on the
// current connection and re-dials when the broker drops it. Idempotent
// — only the first call actually spawns the goroutine.
//
// The watchdog reattaches its NotifyClose listener to each new
// connection so it survives any number of broker restarts. Subscriptions
// pick up the new connection on their own reconnect cycles; the
// watchdog only owns the connection itself.
//
// Without this, even with a DialFunc set, the transport would only
// reconnect lazily on the next setupConsumer call. The watchdog makes
// recovery proactive so publish-only flows (which don't trigger
// setupConsumer) also see a live connection promptly after a blip.
func (t *Transport) startWatchdog() {
	t.watchdogOnce.Do(func() {
		go t.watchdogLoop()
	})
}

func (t *Transport) watchdogLoop() {
	backoff := time.Second
	for {
		select {
		case <-t.closed:
			return
		default:
		}

		t.connMu.RLock()
		conn := t.conn
		dialFn := t.dialFn
		t.connMu.RUnlock()

		if dialFn == nil {
			return // no way to reconnect; nothing to watch
		}

		if conn == nil || conn.IsClosed() {
			if _, err := t.reconnect(dialFn); err != nil {
				// Dial failed; back off and retry. Cap so a fully-down
				// broker doesn't push the retry interval into hours.
				select {
				case <-t.closed:
					return
				case <-time.After(backoff):
				}
				if backoff < 30*time.Second {
					backoff *= 2
					if backoff > 30*time.Second {
						backoff = 30 * time.Second
					}
				}
				continue
			}
			backoff = time.Second
			continue
		}

		// Connection is live; wait for it to close.
		notifyClose := conn.NotifyClose(make(chan *amqp091.Error, 1))
		select {
		case <-t.closed:
			return
		case <-notifyClose:
			// Connection dropped. Loop body will reconnect.
			backoff = time.Second
		}
	}
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

	conn, err := t.connectionForUse()
	if err != nil {
		return rpc.Reply{}, fmt.Errorf("amqp request: get connection: %w", err)
	}
	ch, err := conn.Channel()
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
	t.closeMu.Lock()
	select {
	case <-t.closed:
		t.closeMu.Unlock()
		return t.closeErr
	default:
	}
	close(t.closed)
	t.closeMu.Unlock()

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

	t.connMu.Lock()
	conn := t.conn
	t.conn = nil
	t.connMu.Unlock()

	var err error
	if conn != nil {
		err = conn.Close()
	}
	t.closeErr = err
	return err
}

func (t *Transport) publishChannel() (*amqp091.Channel, error) {
	t.pubMu.Lock()
	defer t.pubMu.Unlock()

	if t.pubCh != nil && !t.pubCh.IsClosed() {
		return t.pubCh, nil
	}

	conn, err := t.connectionForUse()
	if err != nil {
		return nil, fmt.Errorf("amqp: get connection for publish: %w", err)
	}
	ch, err := conn.Channel()
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
//
// When the underlying connection (not just the channel) is dropped,
// setupConsumer routes through Transport.connectionForUse which
// triggers a re-dial via the transport's DialFunc. From the
// subscription's point of view that is just "the next setupConsumer
// retry happened to succeed" — no extra plumbing needed here.
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
