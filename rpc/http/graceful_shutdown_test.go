package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
)

type blockedReplyWriter struct {
	*httptest.ResponseRecorder
	writing chan struct{}
	release chan struct{}
}

func (w *blockedReplyWriter) Write(p []byte) (int, error) {
	close(w.writing)
	<-w.release
	return w.ResponseRecorder.Write(p)
}
func awaitShutdown(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown handshake timed out")
	}
}
func noCloseReturn(t *testing.T, returned <-chan error) {
	t.Helper()
	select {
	case err := <-returned:
		t.Fatalf("Close returned before settlement: %v", err)
	default:
	}
}

func TestSubscriptionCloseWaitsForHandlerReplyAndConcurrentClose(t *testing.T) {
	transport := New(&Transport{Mux: http.NewServeMux()})
	started, release, acknowledged := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	sub, err := transport.Consume(rpc.ConsumerConfig{Endpoint: "execute", Timeout: 5 * time.Second, Handler: func(ctx context.Context, d rpc.Delivery) error {
		close(started)
		<-release
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := d.Reply(ctx, []byte("original reply"), "application/json"); err != nil {
			return err
		}
		if err := d.Ack(); err != nil {
			return err
		}
		close(acknowledged)
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	s := sub.(*subscription)
	writer := &blockedReplyWriter{ResponseRecorder: httptest.NewRecorder(), writing: make(chan struct{}), release: make(chan struct{})}
	var writeOnce sync.Once
	defer writeOnce.Do(func() { close(writer.release) })
	requestDone := make(chan struct{})
	go func() {
		transport.Mux.ServeHTTP(writer, httptest.NewRequest(http.MethodPost, "/rpc/execute", nil))
		close(requestDone)
	}()
	awaitShutdown(t, started)
	returned := make(chan error, 2)
	go func() { returned <- sub.Close() }()
	awaitShutdown(t, s.done)
	go func() { returned <- sub.Close() }()
	denied := httptest.NewRecorder()
	transport.Mux.ServeHTTP(denied, httptest.NewRequest(http.MethodPost, "/rpc/execute", nil))
	if denied.Code != http.StatusServiceUnavailable {
		t.Fatalf("new request status=%d", denied.Code)
	}
	noCloseReturn(t, returned)
	releaseOnce.Do(func() { close(release) })
	awaitShutdown(t, writer.writing)
	noCloseReturn(t, returned)
	writeOnce.Do(func() { close(writer.release) })
	awaitShutdown(t, acknowledged)
	awaitShutdown(t, requestDone)
	for range 2 {
		select {
		case err := <-returned:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent Close did not join original drain")
		}
	}
	if writer.Code != http.StatusOK {
		t.Fatalf("original response status=%d", writer.Code)
	}
}

type observedDoneContext struct {
	context.Context
	once     sync.Once
	observed chan struct{}
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func TestSubscriptionCloseRejectsRequestAlreadyWaitingForConcurrency(t *testing.T) {
	transport := New(&Transport{Mux: http.NewServeMux()})
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var calls atomic.Int32
	sub, err := transport.Consume(rpc.ConsumerConfig{Endpoint: "execute", Concurrency: 1, Timeout: 5 * time.Second, Handler: func(context.Context, rpc.Delivery) error { calls.Add(1); close(started); <-release; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan struct{})
	go func() {
		transport.Mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/rpc/execute", nil))
		close(firstDone)
	}()
	awaitShutdown(t, started)
	waiting := &observedDoneContext{Context: context.Background(), observed: make(chan struct{})}
	second := httptest.NewRecorder()
	secondDone := make(chan struct{})
	go func() {
		transport.Mux.ServeHTTP(second, httptest.NewRequest(http.MethodPost, "/rpc/execute", nil).WithContext(waiting))
		close(secondDone)
	}()
	awaitShutdown(t, waiting.observed)
	returned := make(chan error, 1)
	go func() { returned <- sub.Close() }()
	awaitShutdown(t, secondDone)
	if second.Code != http.StatusServiceUnavailable || calls.Load() != 1 {
		t.Fatalf("waiting request status=%d handler calls=%d", second.Code, calls.Load())
	}
	noCloseReturn(t, returned)
	once.Do(func() { close(release) })
	awaitShutdown(t, firstDone)
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not finish")
	}
}

func TestCloseContextDeadlineDoesNotCancelOriginalHandler(t *testing.T) {
	transport := New(&Transport{Mux: http.NewServeMux()})
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	handlerErr := make(chan error, 1)
	var once sync.Once
	defer once.Do(func() { close(release) })
	_, err := transport.Consume(rpc.ConsumerConfig{Endpoint: "execute", Timeout: 5 * time.Second, Handler: func(ctx context.Context, _ rpc.Delivery) error {
		close(started)
		<-release
		handlerErr <- ctx.Err()
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		transport.Mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/rpc/execute", nil))
		close(done)
	}()
	awaitShutdown(t, started)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := transport.CloseContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseContext error=%v", err)
	}
	once.Do(func() { close(release) })
	awaitShutdown(t, done)
	if err := <-handlerErr; err != nil {
		t.Fatalf("shutdown cancelled original handler: %v", err)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
}
