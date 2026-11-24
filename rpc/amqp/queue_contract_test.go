package amqp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

type recordingPassiveQueueDeclarer struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
	args       amqp091.Table
	err        error
}

func (r *recordingPassiveQueueDeclarer) QueueDeclarePassive(
	name string,
	durable, autoDelete, exclusive, noWait bool,
	args amqp091.Table,
) (amqp091.Queue, error) {
	r.name = name
	r.durable = durable
	r.autoDelete = autoDelete
	r.exclusive = exclusive
	r.noWait = noWait
	r.args = args
	return amqp091.Queue{}, r.err
}

func TestRequireFixedConsumerQueueUsesPassiveExistenceCheck(t *testing.T) {
	declarer := &recordingPassiveQueueDeclarer{}

	if err := requireFixedConsumerQueue(declarer, "yggdrasil.adapter.stripe.execute"); err != nil {
		t.Fatalf("requireFixedConsumerQueue: %v", err)
	}

	if declarer.name != "yggdrasil.adapter.stripe.execute" {
		t.Fatalf("queue name = %q", declarer.name)
	}
	if !declarer.durable || declarer.autoDelete || declarer.exclusive || declarer.noWait {
		t.Fatalf(
			"queue flags = durable:%t autoDelete:%t exclusive:%t noWait:%t",
			declarer.durable,
			declarer.autoDelete,
			declarer.exclusive,
			declarer.noWait,
		)
	}
	if declarer.args != nil {
		t.Fatalf("passive arguments = %#v, want nil existence-only check", declarer.args)
	}
}

func TestRequireFixedConsumerQueuePropagatesBrokerError(t *testing.T) {
	want := errors.New("queue does not exist")
	declarer := &recordingPassiveQueueDeclarer{err: want}

	if err := requireFixedConsumerQueue(declarer, "missing"); !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func TestRequireFixedConsumerQueueMarksPermanentAvailabilityErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
	}{
		{name: "queue access refused", code: amqp091.AccessRefused},
		{name: "missing queue", code: amqp091.NotFound},
		{name: "queue locked", code: amqp091.ResourceLocked},
		{name: "broker precondition failed", code: amqp091.PreconditionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			brokerErr := &amqp091.Error{Code: tc.code, Reason: tc.name, Server: true}
			declarer := &recordingPassiveQueueDeclarer{err: brokerErr}

			err := requireFixedConsumerQueue(declarer, "yggdrasil.adapter.stripe.execute")
			if !errors.Is(err, ErrFixedQueueUnavailable) {
				t.Fatalf("error = %v, want ErrFixedQueueUnavailable", err)
			}
			var gotBrokerErr *amqp091.Error
			if !errors.As(err, &gotBrokerErr) || gotBrokerErr.Code != tc.code {
				t.Fatalf("error = %v, want broker code %d", err, tc.code)
			}
		})
	}
}

func TestReconnectReportsPermanentQueueErrorWithoutRetryingForever(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
	}{
		{name: "queue access revoked", code: amqp091.AccessRefused},
		{name: "queue removed", code: amqp091.NotFound},
		{name: "queue became locked", code: amqp091.ResourceLocked},
		{name: "broker rejected passive queue check", code: amqp091.PreconditionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			initialDeliveries := make(chan amqp091.Delivery)
			close(initialDeliveries)
			attempts := 0
			sub := &subscription{
				endpoint: "yggdrasil.adapter.stripe.execute",
				runDone:  make(chan struct{}),
				done:     make(chan struct{}),
				setupConsumer: func(string, int, string) (*amqp091.Channel, <-chan amqp091.Delivery, error) {
					attempts++
					return nil, nil, fmt.Errorf("passive declare: %w", &amqp091.Error{
						Code:   tc.code,
						Reason: tc.name,
						Server: true,
					})
				},
				retryAfter: func(time.Duration) <-chan time.Time {
					ready := make(chan time.Time, 1)
					ready <- time.Now()
					return ready
				},
				terminalErrors: make(chan error, 1),
			}

			sub.runWithReconnect(initialDeliveries, rpc.ConsumerConfig{Concurrency: 1})

			if attempts != 1 {
				t.Fatalf("reconnect attempts = %d, want 1 for permanent queue error", attempts)
			}
			select {
			case err := <-sub.TerminalErrors():
				if !errors.Is(err, ErrFixedQueueUnavailable) {
					t.Fatalf("terminal error = %v, want ErrFixedQueueUnavailable", err)
				}
			default:
				t.Fatal("permanent reconnect error was not reported")
			}
		})
	}
}

func TestReconnectRetriesTransientFailure(t *testing.T) {
	conn := dialOrSkip(t)
	defer conn.Close()
	reboundChannel, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer reboundChannel.Close()
	initialDeliveries := make(chan amqp091.Delivery)
	close(initialDeliveries)
	reboundDeliveries := make(chan amqp091.Delivery)
	close(reboundDeliveries)
	attempts := 0
	sub := &subscription{
		endpoint:       "yggdrasil.adapter.stripe.execute",
		runDone:        make(chan struct{}),
		done:           make(chan struct{}),
		terminalErrors: make(chan error, 1),
		retryAfter: func(time.Duration) <-chan time.Time {
			ready := make(chan time.Time, 1)
			ready <- time.Now()
			return ready
		},
	}
	sub.setupConsumer = func(string, int, string) (*amqp091.Channel, <-chan amqp091.Delivery, error) {
		attempts++
		if attempts == 1 {
			return nil, nil, errors.New("connection temporarily unavailable")
		}
		close(sub.done)
		return reboundChannel, reboundDeliveries, nil
	}

	sub.runWithReconnect(initialDeliveries, rpc.ConsumerConfig{Concurrency: 1})

	if attempts != 2 {
		t.Fatalf("reconnect attempts = %d, want transient retry then success", attempts)
	}
	select {
	case err := <-sub.TerminalErrors():
		t.Fatalf("unexpected terminal error for transient failure: %v", err)
	default:
	}
}

func TestPublishChannelReconnectDoesNotSelfDeadlock(t *testing.T) {
	transport := &Transport{
		closed: make(chan struct{}),
		dialFn: func() (*amqp091.Connection, error) {
			// A nil connection is invalid but intentionally sufficient to
			// drive reconnect through its pubCh reset without a real broker.
			return nil, nil
		},
	}
	done := make(chan error, 1)
	go func() {
		_, err := transport.publishChannel()
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("publishChannel returned nil error with no connection")
		}
	case <-time.After(time.Second):
		t.Fatal("publishChannel deadlocked while reconnect reset the publish channel")
	}
}

// The configured CI-only user can connect/configure but cannot read queues.
// This reaches the real broker's basic.consume refusal after a passive check.
func TestConsumeAccessDeniedPreservesFixedQueueAndReportsPermanentError(t *testing.T) {
	url := os.Getenv("YGGDRASIL_TEST_AMQP_DENIED_URL")
	if url == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("missing CI restricted-user fixture")
		}
		t.Skip("requires isolated CI restricted RabbitMQ user")
	}
	adminConn := dialOrSkip(t)
	queue := uniqueEndpoint("fixed-denied")
	declareQuorumQueue(t, adminConn, queue)
	defer deleteQueue(t, adminConn, queue)
	conn, err := amqp091.DialConfig(url, amqp091.Config{Dial: amqp091.DefaultDial(3 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	tr := New(conn)
	defer tr.Close()
	_, err = tr.Consume(rpc.ConsumerConfig{Endpoint: queue, Handler: func(context.Context, rpc.Delivery) error { t.Error("denied handler invoked"); return nil }})
	var brokerErr *amqp091.Error
	if !errors.Is(err, ErrFixedQueueUnavailable) || !errors.As(err, &brokerErr) || brokerErr.Code != amqp091.AccessRefused {
		t.Fatalf("expected real access refusal, got %v", err)
	}
	ch, err := adminConn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	q, err := ch.QueueInspect(queue)
	if err != nil || q.Messages != 0 || q.Consumers != 0 {
		t.Fatalf("fixture queue changed: %+v %v", q, err)
	}
}
