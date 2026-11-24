package adapter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
)

type drainProbeSubscription struct {
	started chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (s *drainProbeSubscription) Endpoint() string { return "probe" }
func (s *drainProbeSubscription) Close() error     { return s.CloseContext(context.Background()) }
func (s *drainProbeSubscription) CloseContext(ctx context.Context) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type drainProbeTransport struct {
	subs     []*drainProbeSubscription
	consumed chan struct{}
	calls    int
}

func (t *drainProbeTransport) Consume(rpc.ConsumerConfig) (rpc.Subscription, error) {
	s := t.subs[t.calls]
	t.calls++
	if t.calls == len(t.subs) {
		close(t.consumed)
	}
	return s, nil
}
func (*drainProbeTransport) Publish(context.Context, rpc.Request) error { return nil }
func (*drainProbeTransport) Request(context.Context, rpc.Request) (rpc.Reply, error) {
	return rpc.Reply{}, nil
}
func (*drainProbeTransport) Close() error { return nil }

func TestRunStopsEveryEndpointBeforeWaitingAndReportsDrainDeadline(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	first := &drainProbeSubscription{started: make(chan struct{}), release: release}
	second := &drainProbeSubscription{started: make(chan struct{}), release: release}
	transport := &drainProbeTransport{subs: []*drainProbeSubscription{first, second}, consumed: make(chan struct{})}
	a := New(Config{DefaultTimeout: 10 * time.Millisecond}).Register("one", func(context.Context, rpc.Delivery) ([]byte, string, error) { return nil, "", nil }).Register("two", func(context.Context, rpc.Delivery) ([]byte, string, error) { return nil, "", nil }).Transport(transport)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan error, 1)
	go func() { returned <- a.Run(ctx) }()
	select {
	case <-transport.consumed:
	case <-time.After(time.Second):
		t.Fatal("subscriptions not registered")
	}
	cancel()
	for _, s := range []*drainProbeSubscription{first, second} {
		select {
		case <-s.started:
		case <-time.After(300 * time.Millisecond):
			t.Fatal("shutdown waited for one endpoint before stopping the other")
		}
	}
	select {
	case err := <-returned:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("incomplete drain reported success: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown exceeded its request-timeout budget")
	}
}
