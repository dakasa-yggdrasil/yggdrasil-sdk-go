package amqp

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

func shutdownBroker(t *testing.T) *amqp091.Connection {
	t.Helper()
	url := os.Getenv("YGGDRASIL_TEST_AMQP_URL")
	if url == "" {
		t.Skip("requires the CI RabbitMQ service")
	}
	conn, err := amqp091.DialConfig(url, amqp091.Config{Dial: amqp091.DefaultDial(3 * time.Second)})
	if err != nil {
		t.Fatalf("configured CI RabbitMQ unavailable: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
func shutdownWait(t *testing.T, ready <-chan struct{}) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown handshake timed out")
	}
}
func waitNoConsumer(t *testing.T, ch *amqp091.Channel, queue string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		q, err := ch.QueueInspect(queue)
		if err != nil {
			t.Fatal(err)
		}
		if q.Consumers == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("broker consumer did not cancel")
}

func TestSubscriptionCloseDrainsOriginalReplyAckBeforeChannelClose(t *testing.T) {
	transport := New(shutdownBroker(t))
	client := New(shutdownBroker(t))
	admin, err := client.Connection().Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	queue := uniqueEndpoint("shutdown-drain")
	defer func() { _, _ = admin.QueueDelete(queue, false, false, false) }()
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	handlerErr := make(chan error, 1)
	sub, err := transport.Consume(rpc.ConsumerConfig{Endpoint: queue, Timeout: 5 * time.Second, Handler: func(ctx context.Context, d rpc.Delivery) error {
		close(started)
		<-release
		if err := ctx.Err(); err != nil {
			handlerErr <- err
			return err
		}
		err := d.Reply(ctx, []byte("original reply"), "application/json")
		if err == nil {
			err = d.Ack()
		}
		handlerErr <- err
		return err
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { once.Do(func() { close(release) }); _ = transport.Close() }()
	s := sub.(*subscription)
	type result struct {
		reply rpc.Reply
		err   error
	}
	replied := make(chan result, 1)
	go func() {
		r, e := client.Request(context.Background(), rpc.Request{Endpoint: queue, Body: []byte("original"), Timeout: 8 * time.Second})
		replied <- result{r, e}
	}()
	shutdownWait(t, started)
	closed := make(chan error, 2)
	go func() { closed <- sub.Close() }()
	shutdownWait(t, s.done)
	go func() { closed <- sub.Close() }()
	waitNoConsumer(t, admin, queue)
	if s.currentChannel().IsClosed() {
		t.Fatal("channel closed under the original handler")
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned before handler: %v", err)
	default:
	}
	if err := client.Publish(context.Background(), rpc.Request{Endpoint: queue, Body: []byte("not admitted")}); err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	select {
	case r := <-replied:
		if r.err != nil || string(r.reply.Body) != "original reply" {
			t.Fatalf("original reply=%q err=%v", r.reply.Body, r.err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("original reply lost")
	}
	if err := <-handlerErr; err != nil {
		t.Fatalf("original settlement: %v", err)
	}
	for range 2 {
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not join original handler")
		}
	}
	q, err := admin.QueueInspect(queue)
	if err != nil {
		t.Fatal(err)
	}
	if q.Messages != 1 || q.Consumers != 0 {
		t.Fatalf("queued after drain: ready=%d consumers=%d; original must not be redelivered", q.Messages, q.Consumers)
	}
}

func TestTransportCloseContextDeadlineLeavesOriginalHandlerChannelOpen(t *testing.T) {
	transport := New(shutdownBroker(t))
	client := New(shutdownBroker(t))
	admin, err := client.Connection().Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	queue := uniqueEndpoint("shutdown-deadline")
	defer func() { _, _ = admin.QueueDelete(queue, false, false, false) }()
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	originalContext := make(chan error, 1)
	sub, err := transport.Consume(rpc.ConsumerConfig{Endpoint: queue, Timeout: 5 * time.Second, Handler: func(ctx context.Context, d rpc.Delivery) error {
		close(started)
		<-release
		originalContext <- ctx.Err()
		return d.Ack()
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Publish(context.Background(), rpc.Request{Endpoint: queue, Body: []byte("original")}); err != nil {
		t.Fatal(err)
	}
	shutdownWait(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := transport.CloseContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain timeout must remain failure: %v", err)
	}
	s := sub.(*subscription)
	if s.currentChannel().IsClosed() || transport.Connection().IsClosed() {
		t.Fatal("deadline force-closed transport under active handler")
	}
	once.Do(func() { close(release) })
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-originalContext; err != nil {
		t.Fatalf("planned shutdown cancelled original context: %v", err)
	}
	q, err := admin.QueueInspect(queue)
	if err != nil {
		t.Fatal(err)
	}
	if q.Messages != 0 || q.Consumers != 0 {
		t.Fatalf("original settlement not retained: %+v", q)
	}
}
