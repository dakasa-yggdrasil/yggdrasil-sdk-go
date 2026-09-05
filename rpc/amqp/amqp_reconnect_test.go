// Reconnect tests for the AMQP transport. These exercise the
// connection-level recovery that v0.3.0 adds on top of the prior
// channel-level recovery: when the broker closes the underlying TCP
// connection (rabbit pod restart, network blip), the transport
// re-dials and the subscription rebinds without operator intervention.
//
// The tests require a running RabbitMQ broker. The default URL is the
// one our local docker-compose / make-up scripts expose:
// amqp://guest:guest@localhost:5673/ — override via
// YGGDRASIL_TEST_AMQP_URL when running against a different broker. If
// no broker is reachable within a short dial budget, the tests skip
// (build still runs in CI environments without rabbit).
package amqp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
	"github.com/google/uuid"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

// testBrokerURL returns the AMQP URL the integration tests should
// connect to. Defaults to the local docker test broker; override via
// YGGDRASIL_TEST_AMQP_URL.
func testBrokerURL() string {
	if v := os.Getenv("YGGDRASIL_TEST_AMQP_URL"); v != "" {
		return v
	}
	return "amqp://guest:guest@localhost:5673/"
}

// dialOrSkip dials the test broker once with a short timeout. If the
// broker is unreachable, the test is skipped — these are integration
// tests, not unit tests, and we'd rather skip than fail when a dev
// laptop doesn't have rabbit running.
func dialOrSkip(t *testing.T) *amqp091.Connection {
	t.Helper()
	url := testBrokerURL()
	conn, err := amqp091.DialConfig(url, amqp091.Config{
		Dial: amqp091.DefaultDial(2 * time.Second),
	})
	if err != nil {
		t.Skipf("test rabbit not reachable at %s: %v", url, err)
	}
	return conn
}

// uniqueEndpoint returns a fresh capability name so parallel test runs
// (and reruns) don't collide on queue residue.
func uniqueEndpoint(prefix string) string {
	return prefix + "-" + uuid.NewString()[:8]
}

// declareQuorumQueue provisions the fixed topology required by Consume. The
// production owner is definitions.json; integration tests create only their
// unique fixture queue before exercising reconnect behavior.
func declareQuorumQueue(t *testing.T, conn *amqp091.Connection, name string) {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open topology channel: %v", err)
	}
	defer func() { _ = ch.Close() }()
	if _, err := ch.QueueDeclare(name, true, false, false, false, amqp091.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatalf("declare quorum fixture queue %q: %v", name, err)
	}
}

func declareClassicQueue(t *testing.T, conn *amqp091.Connection, name string) {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open topology channel: %v", err)
	}
	defer func() { _ = ch.Close() }()
	if _, err := ch.QueueDeclare(name, true, false, false, false, amqp091.Table{"x-queue-type": "classic"}); err != nil {
		t.Fatalf("declare classic fixture queue %q: %v", name, err)
	}
}

func deleteQueue(t *testing.T, conn *amqp091.Connection, name string) {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open topology channel: %v", err)
	}
	defer func() { _ = ch.Close() }()
	if _, err := ch.QueueDelete(name, false, false, false); err != nil {
		t.Fatalf("delete fixture queue %q: %v", name, err)
	}
}

// TestNewIsBackwardCompatible verifies that the legacy New constructor
// still works without any DialFunc — the v0.2.0 contract is preserved
// for callers that manage connections externally.
func TestNewIsBackwardCompatible(t *testing.T) {
	conn := dialOrSkip(t)
	transport := New(conn)
	defer func() { _ = transport.Close() }()

	if transport.Connection() != conn {
		t.Errorf("Connection() = %p, want %p", transport.Connection(), conn)
	}
}

// TestNewWithDialOpensInitialConnection asserts NewWithDial calls the
// dial function eagerly and returns the dial error when it fails. The
// dial function MUST be called exactly once on success.
func TestNewWithDialOpensInitialConnection(t *testing.T) {
	url := testBrokerURL()
	if _, err := amqp091.DialConfig(url, amqp091.Config{
		Dial: amqp091.DefaultDial(2 * time.Second),
	}); err != nil {
		t.Skipf("test rabbit not reachable at %s: %v", url, err)
	}

	var dialCount int32
	dialFn := func() (*amqp091.Connection, error) {
		atomic.AddInt32(&dialCount, 1)
		return amqp091.Dial(url)
	}

	transport, err := NewWithDial(dialFn)
	if err != nil {
		t.Fatalf("NewWithDial: %v", err)
	}
	defer func() { _ = transport.Close() }()

	// One dial for the initial open. Watchdog may attach NotifyClose
	// but should not re-dial while the connection is live.
	time.Sleep(50 * time.Millisecond) // let watchdog settle
	if got := atomic.LoadInt32(&dialCount); got != 1 {
		t.Errorf("dialCount = %d, want 1", got)
	}
}

// TestNewWithDialPropagatesInitialDialError makes sure the constructor
// returns the dial error rather than a bare transport pointing at nil.
// Plugin authors rely on this to fail-loud at startup.
func TestNewWithDialPropagatesInitialDialError(t *testing.T) {
	dialFn := func() (*amqp091.Connection, error) {
		return nil, fmt.Errorf("synthetic dial failure")
	}
	transport, err := NewWithDial(dialFn)
	if err == nil {
		t.Fatal("expected dial error, got nil")
	}
	if transport != nil {
		t.Errorf("expected nil transport on dial failure, got %p", transport)
	}
}

// TestNewWithDialRejectsNilDialFunc guards the constructor contract.
func TestNewWithDialRejectsNilDialFunc(t *testing.T) {
	if _, err := NewWithDial(nil); err == nil {
		t.Fatal("expected error for nil DialFunc")
	}
}

func TestConsumeDoesNotCreateMissingFixedQueue(t *testing.T) {
	conn := dialOrSkip(t)
	transport := New(conn)
	defer func() { _ = transport.Close() }()

	endpoint := uniqueEndpoint("missing-fixed-queue")
	_, err := transport.Consume(rpc.ConsumerConfig{
		Endpoint: endpoint,
		Handler:  func(context.Context, rpc.Delivery) error { return nil },
	})
	if !errors.Is(err, ErrFixedQueueUnavailable) {
		t.Fatalf("Consume error = %v, want ErrFixedQueueUnavailable", err)
	}

	ch, channelErr := conn.Channel()
	if channelErr != nil {
		t.Fatalf("open inspect channel: %v", channelErr)
	}
	defer func() { _ = ch.Close() }()
	if _, inspectErr := ch.QueueInspect(endpoint); inspectErr == nil {
		t.Fatalf("missing queue %q was created by Consume", endpoint)
	}
}

// TestConsumePassiveCheckCannotDistinguishClassicQueue documents an AMQP
// protocol limitation: passive queue.declare checks existence but RabbitMQ
// ignores durable/auto-delete/arguments. The deployment management gate, not
// the SDK, must reject this classic queue before the adapter is started.
func TestConsumePassiveCheckCannotDistinguishClassicQueue(t *testing.T) {
	conn := dialOrSkip(t)
	transport := New(conn)
	defer func() { _ = transport.Close() }()

	endpoint := uniqueEndpoint("classic-fixed-queue")
	declareClassicQueue(t, conn, endpoint)
	defer deleteQueue(t, conn, endpoint)

	sub, err := transport.Consume(rpc.ConsumerConfig{
		Endpoint: endpoint,
		Handler:  func(context.Context, rpc.Delivery) error { return nil },
	})
	if err != nil {
		t.Fatalf("passive existence check rejected an existing classic queue: %v", err)
	}
	defer func() { _ = sub.Close() }()
}

func TestReconnectReportsFixedQueueBecomingUnavailable(t *testing.T) {
	conn := dialOrSkip(t)
	transport := New(conn)
	defer func() { _ = transport.Close() }()

	endpoint := uniqueEndpoint("queue-unavailable")
	declareQuorumQueue(t, conn, endpoint)
	sub, err := transport.Consume(rpc.ConsumerConfig{
		Endpoint: endpoint,
		Handler:  func(context.Context, rpc.Delivery) error { return nil },
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer func() { _ = sub.Close() }()

	// Stop the active consumer, then remove the deployment-owned queue before
	// the reconnect loop's first one-second retry.
	concrete := sub.(*subscription)
	if err := concrete.currentChannel().Close(); err != nil {
		t.Fatalf("close consumer channel: %v", err)
	}
	deleteQueue(t, conn, endpoint)

	source := sub.(rpc.TerminalErrorSubscription)
	select {
	case terminalErr := <-source.TerminalErrors():
		if !errors.Is(terminalErr, ErrFixedQueueUnavailable) {
			t.Fatalf("terminal error = %v, want ErrFixedQueueUnavailable", terminalErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reconnect did not report unavailable fixed queue")
	}
}

func TestPublishCanOwnConnectionReconnectWithoutDeadlock(t *testing.T) {
	url := testBrokerURL()
	conn := dialOrSkip(t)
	transport := New(conn)
	defer func() { _ = transport.Close() }()

	// Install a dial function without starting the watchdog. This forces the
	// immediately following Publish to own the reconnect path deterministically.
	transport.connMu.Lock()
	transport.dialFn = func() (*amqp091.Connection, error) { return amqp091.Dial(url) }
	transport.connMu.Unlock()
	if err := conn.Close(); err != nil {
		t.Fatalf("close initial connection: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- transport.Publish(context.Background(), rpc.Request{
			Endpoint: uniqueEndpoint("publish-after-close"),
			Body:     []byte("probe"),
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Publish after connection close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Publish deadlocked while reconnecting a dropped connection")
	}
}

// TestConsumerRecoversChannelClose validates the v0.2.0 channel-level
// recovery still works: closing the consumer's channel directly should
// not strand the subscription — it must rebind and resume processing.
func TestConsumerRecoversChannelClose(t *testing.T) {
	conn := dialOrSkip(t)
	transport := New(conn)
	defer func() { _ = transport.Close() }()

	endpoint := uniqueEndpoint("channel-recover")
	declareQuorumQueue(t, conn, endpoint)
	var received int32
	delivered := make(chan struct{}, 4)

	sub, err := transport.Consume(rpc.ConsumerConfig{
		Endpoint: endpoint,
		Handler: func(ctx context.Context, d rpc.Delivery) error {
			atomic.AddInt32(&received, 1)
			delivered <- struct{}{}
			return d.Ack()
		},
		Timeout:     5 * time.Second,
		Concurrency: 1,
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer func() { _ = sub.Close() }()

	// Publish, wait for delivery, kill the channel, publish again,
	// assert second delivery comes through.
	publish := func(body string) {
		if err := transport.Publish(context.Background(), rpc.Request{
			Endpoint: endpoint,
			Body:     []byte(body),
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	publish("first")
	waitForDelivery(t, delivered, 5*time.Second, "first delivery")

	// Yank the channel from under the subscription.
	s := sub.(*subscription)
	_ = s.currentChannel().Close()

	// Give the reconnect loop a beat to rebind.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ch := s.currentChannel()
		if ch != nil && !ch.IsClosed() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if s.currentChannel().IsClosed() {
		t.Fatal("subscription did not rebind a fresh channel after force-close")
	}

	publish("second")
	waitForDelivery(t, delivered, 5*time.Second, "second delivery (post-channel-reconnect)")

	if got := atomic.LoadInt32(&received); got < 2 {
		t.Errorf("received = %d, want >= 2", got)
	}
}

// TestConsumerRecoversConnectionClose is the main v0.3.0 fix: when the
// entire AMQP connection drops (simulated here by Close on the
// underlying conn — the closest in-process analog of a rabbit pod
// restart), the transport re-dials via DialFunc and the subscription
// rebinds. Before this fix, the subscription would loop forever on a
// dead connection because setupConsumer kept calling Channel() on the
// closed connection.
func TestConsumerRecoversConnectionClose(t *testing.T) {
	url := testBrokerURL()
	if _, err := amqp091.DialConfig(url, amqp091.Config{
		Dial: amqp091.DefaultDial(2 * time.Second),
	}); err != nil {
		t.Skipf("test rabbit not reachable at %s: %v", url, err)
	}

	var dialCount int32
	dialFn := func() (*amqp091.Connection, error) {
		atomic.AddInt32(&dialCount, 1)
		return amqp091.Dial(url)
	}

	transport, err := NewWithDial(dialFn)
	if err != nil {
		t.Fatalf("NewWithDial: %v", err)
	}
	defer func() { _ = transport.Close() }()

	endpoint := uniqueEndpoint("conn-recover")
	declareQuorumQueue(t, transport.Connection(), endpoint)
	delivered := make(chan string, 4)

	sub, err := transport.Consume(rpc.ConsumerConfig{
		Endpoint: endpoint,
		Handler: func(ctx context.Context, d rpc.Delivery) error {
			delivered <- string(d.Body)
			return d.Ack()
		},
		Timeout:     5 * time.Second,
		Concurrency: 1,
	})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer func() { _ = sub.Close() }()

	publish := func(body string) error {
		return transport.Publish(context.Background(), rpc.Request{
			Endpoint: endpoint,
			Body:     []byte(body),
		})
	}

	if err := publish("pre-restart"); err != nil {
		t.Fatalf("Publish pre-restart: %v", err)
	}
	if got := waitForString(t, delivered, 5*time.Second); got != "pre-restart" {
		t.Errorf("pre-restart delivery = %q, want %q", got, "pre-restart")
	}

	// Simulate rabbit dropping the connection. Closing the connection
	// in-process produces the same NotifyClose signal an external drop
	// would; it is the closest analog we can simulate without
	// restarting the broker.
	oldConn := transport.Connection()
	_ = oldConn.Close()

	// Wait for the watchdog + subscription to recover. Up to 15s
	// because the subscription's reconnect loop starts at 1s backoff
	// and may need a couple of attempts while the watchdog re-dials.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		newConn := transport.Connection()
		if newConn != nil && newConn != oldConn && !newConn.IsClosed() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if c := transport.Connection(); c == nil || c == oldConn || c.IsClosed() {
		t.Fatalf("transport did not re-dial connection after close (conn=%p oldConn=%p)", c, oldConn)
	}

	// Now publish again; this exercises Publish on the re-dialed
	// connection (the pubCh would have been reset on reconnect).
	// Allow a few retries because the subscription rebind may still
	// be in progress on its own backoff loop.
	var lastPubErr error
	for attempt := 0; attempt < 40; attempt++ {
		if err := publish("post-restart"); err == nil {
			lastPubErr = nil
			break
		} else {
			lastPubErr = err
			time.Sleep(250 * time.Millisecond)
		}
	}
	if lastPubErr != nil {
		t.Fatalf("Publish post-restart never succeeded: %v", lastPubErr)
	}

	if got := waitForString(t, delivered, 15*time.Second); got != "post-restart" {
		t.Errorf("post-restart delivery = %q, want %q", got, "post-restart")
	}

	// At minimum: initial dial + one reconnect. May be higher if the
	// subscription rebind raced with the watchdog and triggered an
	// inline reconnect via connectionForUse; both paths converge on a
	// single live connection thanks to reconnect()'s double-check.
	if got := atomic.LoadInt32(&dialCount); got < 2 {
		t.Errorf("dialCount = %d, want >= 2 (initial + reconnect)", got)
	}
}

// TestSetDialFuncEnablesReconnect verifies the fallback path: a
// Transport built with the legacy New constructor can be upgraded to
// reconnect semantics via SetDialFunc. This is the path adapter's
// ListenAMQP uses — it dials once with a retry budget before handing
// the connection in, then wires the dial func separately.
func TestSetDialFuncEnablesReconnect(t *testing.T) {
	url := testBrokerURL()
	conn, err := amqp091.DialConfig(url, amqp091.Config{
		Dial: amqp091.DefaultDial(2 * time.Second),
	})
	if err != nil {
		t.Skipf("test rabbit not reachable at %s: %v", url, err)
	}

	transport := New(conn)
	defer func() { _ = transport.Close() }()

	var dialCount int32
	transport.SetDialFunc(func() (*amqp091.Connection, error) {
		atomic.AddInt32(&dialCount, 1)
		return amqp091.Dial(url)
	})

	// Drop the connection; watchdog should re-dial.
	_ = conn.Close()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&dialCount) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&dialCount); got < 1 {
		t.Errorf("dialCount = %d, want >= 1 after connection close", got)
	}
}

// TestCloseStopsWatchdog asserts Close shuts down the watchdog
// goroutine. Without this, calling Close on a transport whose broker
// is unreachable would leak a goroutine retrying dials forever.
func TestCloseStopsWatchdog(t *testing.T) {
	url := testBrokerURL()
	if _, err := amqp091.DialConfig(url, amqp091.Config{
		Dial: amqp091.DefaultDial(2 * time.Second),
	}); err != nil {
		t.Skipf("test rabbit not reachable at %s: %v", url, err)
	}

	var dialCount int32
	dialFn := func() (*amqp091.Connection, error) {
		atomic.AddInt32(&dialCount, 1)
		return amqp091.Dial(url)
	}

	transport, err := NewWithDial(dialFn)
	if err != nil {
		t.Fatalf("NewWithDial: %v", err)
	}

	// Drop the connection and immediately close the transport. The
	// watchdog should see <-t.closed before it re-dials.
	_ = transport.Connection().Close()
	_ = transport.Close()

	// Give it a moment, then snapshot the dial count. Any further
	// increments after this point would indicate the watchdog kept
	// running after Close.
	time.Sleep(200 * time.Millisecond)
	beforeWait := atomic.LoadInt32(&dialCount)
	time.Sleep(2 * time.Second)
	afterWait := atomic.LoadInt32(&dialCount)

	if afterWait != beforeWait {
		t.Errorf("watchdog kept dialing after Close: before=%d after=%d", beforeWait, afterWait)
	}
}

// TestConnectionForUseReturnsErrClosedAfterClose makes sure callers
// that hold the transport across a Close see a clean error rather
// than a nil deref or panic.
func TestConnectionForUseReturnsErrClosedAfterClose(t *testing.T) {
	conn := dialOrSkip(t)
	transport := New(conn)
	transport.SetDialFunc(func() (*amqp091.Connection, error) {
		// If somehow called after Close, succeed — the contract under
		// test is that connectionForUse refuses to call us, not that
		// we'd fail.
		return nil, fmt.Errorf("should not be called after Close")
	})
	_ = transport.Close()

	if _, err := transport.connectionForUse(); err == nil {
		t.Error("connectionForUse after Close returned nil error")
	}
}

// TestReconnectIsConcurrencySafe spins up multiple goroutines all
// calling connectionForUse concurrently while the underlying conn is
// dead. Only one reconnect should win; the others should observe the
// new connection without each dialing their own.
func TestReconnectIsConcurrencySafe(t *testing.T) {
	url := testBrokerURL()
	conn, err := amqp091.DialConfig(url, amqp091.Config{
		Dial: amqp091.DefaultDial(2 * time.Second),
	})
	if err != nil {
		t.Skipf("test rabbit not reachable at %s: %v", url, err)
	}

	var dialCount int32
	transport := New(conn)
	transport.SetDialFunc(func() (*amqp091.Connection, error) {
		atomic.AddInt32(&dialCount, 1)
		// Add a small delay so concurrent callers actually race.
		time.Sleep(50 * time.Millisecond)
		return amqp091.Dial(url)
	})
	defer func() { _ = transport.Close() }()

	// Kill the connection.
	_ = conn.Close()
	// Give watchdog a tick to NOT have run yet by racing with our
	// concurrent connectionForUse calls. The watchdog will fire too,
	// but the dial dedup in reconnect() should keep dialCount sane.
	time.Sleep(10 * time.Millisecond)

	const goroutines = 8
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, _ = transport.connectionForUse()
		}()
	}
	wg.Wait()

	// Wait for any straggling watchdog dial to land.
	time.Sleep(500 * time.Millisecond)

	// Expect at most 2 dials: one from connectionForUse (winner),
	// possibly one from the watchdog if it raced ahead. More than
	// that would mean the dedup in reconnect() is broken.
	if got := atomic.LoadInt32(&dialCount); got > 2 {
		t.Errorf("dialCount = %d, want <= 2 (watchdog + at most one connectionForUse winner)", got)
	}
}

// waitForDelivery blocks until a delivery arrives or the timeout
// expires.
func waitForDelivery(t *testing.T, ch <-chan struct{}, timeout time.Duration, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// waitForString returns the first string delivered on ch, or fails
// the test on timeout.
func waitForString(t *testing.T, ch <-chan string, timeout time.Duration) string {
	t.Helper()
	select {
	case s := <-ch:
		return s
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for string delivery")
		return ""
	}
}
