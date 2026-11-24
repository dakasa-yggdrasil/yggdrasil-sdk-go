package adapter

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
	sdkamqp "github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc/amqp"
	"github.com/google/uuid"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

type originalAckTransport struct {
	*sdkamqp.Transport
	ackStarted chan struct{}
	ackRelease <-chan struct{}
	ackResult  chan error
	calls      atomic.Int32
	terminal   chan error
}

func (t *originalAckTransport) Consume(cfg rpc.ConsumerConfig) (rpc.Subscription, error) {
	handler := cfg.Handler
	cfg.Handler = func(ctx context.Context, d rpc.Delivery) error {
		t.calls.Add(1)
		originalAck := d.AckFn
		d.AckFn = func() error {
			close(t.ackStarted)
			<-t.ackRelease // Hold only the exact original settlement, not a replay.
			err := originalAck()
			t.ackResult <- err
			return err
		}
		return handler(ctx, d)
	}
	sub, err := t.Transport.Consume(cfg)
	if err != nil || t.terminal == nil {
		return sub, err
	}
	return &originalTerminalAckSub{Subscription: sub, terminal: t.terminal}, nil

}

type originalTerminalAckSub struct {
	rpc.Subscription
	terminal <-chan error
}

func (s *originalTerminalAckSub) TerminalErrors() <-chan error { return s.terminal }
func (s *originalTerminalAckSub) StopAdmission() {
	s.Subscription.(interface{ StopAdmission() }).StopAdmission()
}
func (s *originalTerminalAckSub) CloseContext(ctx context.Context) error {
	return s.Subscription.(interface{ CloseContext(context.Context) error }).CloseContext(ctx)
}

func TestOriginalAMQPDrainOverrunWaitsForOriginalAckAndConnection(t *testing.T) {
	testOriginalAMQPDrain(t, nil)
}
func TestTerminalRunFailurePreservesOriginalAckOwnerAndFinalError(t *testing.T) {
	testOriginalAMQPDrain(t, errors.New("original fixed queue unavailable"))
}
func testOriginalAMQPDrain(t *testing.T, terminalCause error) {
	url := os.Getenv("YGGDRASIL_TEST_AMQP_URL")
	if url == "" {
		t.Skip("requires configured isolated CI RabbitMQ")
	}
	dial := func() *amqp091.Connection {
		conn, err := amqp091.DialConfig(url, amqp091.Config{Dial: amqp091.DefaultDial(3 * time.Second)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	serverConn, clientConn := dial(), dial()
	inner, client := sdkamqp.New(serverConn), sdkamqp.New(clientConn)
	admin, err := clientConn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()
	queue := "original-drain-" + uuid.NewString()
	if _, err := admin.QueueDeclare(queue, true, false, false, false, amqp091.Table{"x-queue-type": "quorum"}); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.QueueDelete(queue, false, false, false) }()
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	tr := &originalAckTransport{Transport: inner, ackStarted: make(chan struct{}), ackRelease: release, ackResult: make(chan error, 1)}
	if terminalCause != nil {
		tr.terminal = make(chan error, 1)
	}
	a := New(Config{DefaultTimeout: 250 * time.Millisecond}).Transport(tr).Register(queue, func(context.Context, rpc.Delivery) ([]byte, string, error) {
		return []byte("original broker reply"), "text/plain", nil
	})
	a.afterRun = func(ctx context.Context) error { return inner.CloseContext(ctx) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- a.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		q, e := admin.QueueDeclarePassive(queue, true, false, false, false, nil)
		if e == nil && q.Consumers == 1 {
			break
		}
		// A missing passive declaration closes its channel. Reopen only this
		// CI metadata client; it never reissues the original RPC message.
		_ = admin.Close()
		admin, e = clientConn.Channel()
		if e != nil {
			t.Fatal(e)
		}
		if time.Now().After(deadline) {
			t.Fatal("actual original consumer not ready")
		}
		time.Sleep(5 * time.Millisecond)
	}
	replyDone := make(chan error, 1)
	go func() {
		reply, e := client.Request(context.Background(), rpc.Request{Endpoint: queue, Body: []byte("one original"), Timeout: 5 * time.Second})
		if e == nil && string(reply.Body) != "original broker reply" {
			e = errors.New("original reply differs")
		}
		replyDone <- e
	}()
	originalDrainSignal(t, tr.ackStarted)
	if err := originalDrainResult(t, replyDone); err != nil {
		t.Fatal(err)
	}
	if terminalCause == nil {
		cancel()
	} else {
		tr.terminal <- terminalCause
	}
	if err := originalDrainResult(t, runDone); !errors.Is(err, context.DeadlineExceeded) || (terminalCause != nil && !errors.Is(err, terminalCause)) {
		t.Fatal(err)
	}
	if serverConn.IsClosed() {
		t.Fatal("original connection closed under pending Ack")
	}
	q, err := admin.QueueInspect(queue)
	if err != nil || q.Consumers != 0 {
		t.Fatalf("admission did not stop: %+v %v", q, err)
	}
	cancelled, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	if err := a.WaitDrained(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	joined := make(chan error, 2)
	for range 2 {
		go func() { joined <- a.WaitDrained(context.Background()) }()
	}
	select {
	case err := <-joined:
		t.Fatalf("joined before original Ack: %v", err)
	default:
	}
	once.Do(func() { close(release) })
	if err := originalDrainResult(t, tr.ackResult); err != nil {
		t.Fatalf("original Ack failed: %v", err)
	}
	for range 2 {
		err := originalDrainResult(t, joined)
		if (terminalCause == nil && err != nil) || (terminalCause != nil && (!errors.Is(err, terminalCause) || errors.Is(err, context.DeadlineExceeded))) {
			t.Fatalf("final original cause/settlement changed: %v", err)
		}
	}
	if !serverConn.IsClosed() || tr.calls.Load() != 1 {
		t.Fatal("original connection join missing or original call replayed")
	}
	q, err = admin.QueueInspect(queue)
	if err != nil || q.Messages != 0 || q.Consumers != 0 {
		t.Fatalf("original message not settled: %+v %v", q, err)
	}
}
