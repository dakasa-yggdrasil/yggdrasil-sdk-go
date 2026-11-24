package adapter

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
	sdkhttp "github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc/http"
)

func originalDrainSignal(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("original drain handshake timed out")
	}
}

func originalDrainResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("original drain did not return")
		return nil
	}
}

func TestWaitDrainedBeforeRunAndRepeatedRunKeepOneHandle(t *testing.T) {
	a := New(Config{})
	if err := a.WaitDrained(context.Background()); !errors.Is(err, ErrRunNotStarted) {
		t.Fatal(err)
	}
	original := a.Run(context.Background())
	if original == nil {
		t.Fatal("startup with no handlers succeeded")
	}
	handle := a.drain
	if err := a.WaitDrained(context.Background()); err != original {
		t.Fatalf("original startup error was replaced: %v vs %v", err, original)
	}
	if err := a.Run(context.Background()); !errors.Is(err, ErrRunAlreadyStarted) {
		t.Fatal(err)
	}
	if a.drain != handle {
		t.Fatal("repeated Run replaced original handle")
	}
}

func TestWaitDrainedConcurrentRunAndCancelledWaiterDoNotStealStartup(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	startupErr := errors.New("owned startup failed")
	a := New(Config{}).Register("execute", func(context.Context, rpc.Delivery) ([]byte, string, error) { return nil, "", nil })
	a.beforeRun = func(context.Context) error { close(started); <-release; return startupErr }
	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()
	originalDrainSignal(t, started)
	handle := a.drain
	if err := a.Run(context.Background()); !errors.Is(err, ErrRunAlreadyStarted) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.WaitDrained(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-handle.done:
		t.Fatal("waiter cancelled original startup")
	default:
	}
	close(release)
	err := originalDrainResult(t, done)
	if !errors.Is(err, startupErr) || a.drain != handle {
		t.Fatal("original startup ownership lost")
	}
	if joined := a.WaitDrained(context.Background()); joined != err {
		t.Fatal("WaitDrained did not return the identical published final error")
	}
}

func TestStartupFailureStillJoinsItsOriginalInstalledCleanup(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	startupErr, cleanupErr := errors.New("original startup"), errors.New("original cleanup")
	a := New(Config{DefaultTimeout: 10 * time.Millisecond}).Register("execute", func(context.Context, rpc.Delivery) ([]byte, string, error) { return nil, "", nil })
	a.beforeRun = func(context.Context) error {
		a.afterRun = func(context.Context) error { close(started); <-release; return cleanupErr }
		return startupErr
	}
	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()
	originalDrainSignal(t, started)
	if err := originalDrainResult(t, done); !errors.Is(err, startupErr) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	joined := make(chan error, 3)
	for range 3 {
		go func() { joined <- a.WaitDrained(context.Background()) }()
	}
	close(release)
	final := originalDrainResult(t, joined)
	if err := final; !errors.Is(err, startupErr) || !errors.Is(err, cleanupErr) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("final owned error=%v", err)
	}
	for range 2 {
		if actual := originalDrainResult(t, joined); actual != final {
			t.Fatal("waiters did not observe identical final error")
		}
	}
}

func TestRunParentCancellationPreservesOriginalAdmittedHTTPContext(t *testing.T) {
	mux := http.NewServeMux()
	tr := sdkhttp.New(&sdkhttp.Transport{Mux: mux})
	stopped := make(chan struct{})
	observed := &observedAdmissionTransport{Transport: tr, stopped: stopped}
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	originalContext := make(chan error, 1)
	a := New(Config{DefaultTimeout: 5 * time.Second}).Transport(observed).Register("execute", func(ctx context.Context, _ rpc.Delivery) ([]byte, string, error) {
		close(started)
		<-release
		originalContext <- ctx.Err()
		return []byte("joined"), "text/plain", nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- a.Run(ctx) }()
	request := httptest.NewRequest(http.MethodPost, "/rpc/execute", nil)
	deadline := time.Now().Add(time.Second)
	for {
		_, pattern := mux.Handler(request)
		if pattern != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("HTTP endpoint not registered")
		}
		time.Sleep(time.Millisecond)
	}
	requestDone := make(chan struct{})
	go func() { defer close(requestDone); mux.ServeHTTP(httptest.NewRecorder(), request) }()
	originalDrainSignal(t, started)
	cancel()
	// The parent cancellation stops admission, but the actual admitted
	// request retains its independent five-second deadline.
	originalDrainSignal(t, stopped) // Actual built-in StopAdmission already returned.
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/rpc/execute", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatal("HTTP admission did not stop")
	}
	once.Do(func() { close(release) })
	if err := originalDrainResult(t, originalContext); err != nil {
		t.Fatalf("Run parent cancelled original admitted request: %v", err)
	}
	originalDrainSignal(t, requestDone)
	if err := originalDrainResult(t, runDone); err != nil {
		t.Fatal(err)
	}
	if err := a.WaitDrained(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type observedAdmissionTransport struct {
	rpc.Transport
	stopped chan struct{}
}
type observedAdmissionSub struct {
	rpc.Subscription
	stopped chan struct{}
	once    sync.Once
}

func (t *observedAdmissionTransport) Consume(cfg rpc.ConsumerConfig) (rpc.Subscription, error) {
	sub, err := t.Transport.Consume(cfg)
	if err != nil {
		return nil, err
	}
	return &observedAdmissionSub{Subscription: sub, stopped: t.stopped}, nil
}
func (s *observedAdmissionSub) StopAdmission() {
	s.Subscription.(interface{ StopAdmission() }).StopAdmission()
	s.once.Do(func() { close(s.stopped) })
}
func (s *observedAdmissionSub) CloseContext(ctx context.Context) error {
	return s.Subscription.(interface{ CloseContext(context.Context) error }).CloseContext(ctx)
}

type orderedStopSubscription struct {
	all  []chan struct{}
	own  chan struct{}
	once sync.Once
}

func (*orderedStopSubscription) Endpoint() string { return "stop-probe" }
func (s *orderedStopSubscription) StopAdmission() { s.once.Do(func() { close(s.own) }) }
func (s *orderedStopSubscription) Close() error {
	for _, stopped := range s.all {
		select {
		case <-stopped:
		default:
			return errors.New("join began before all admission stopped")
		}
	}
	return nil
}

type orderedStopTransport struct {
	subs  []*orderedStopSubscription
	ready chan struct{}
	next  int
}

func (t *orderedStopTransport) Consume(rpc.ConsumerConfig) (rpc.Subscription, error) {
	s := t.subs[t.next]
	t.next++
	if t.next == len(t.subs) {
		close(t.ready)
	}
	return s, nil
}
func (*orderedStopTransport) Publish(context.Context, rpc.Request) error { return nil }
func (*orderedStopTransport) Request(context.Context, rpc.Request) (rpc.Reply, error) {
	return rpc.Reply{}, nil
}
func (*orderedStopTransport) Close() error { return nil }

func TestOriginalDrainStopsAllBuiltInStyleAdmissionBeforeAnyJoin(t *testing.T) {
	stops := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	tr := &orderedStopTransport{ready: make(chan struct{})}
	a := New(Config{}).Transport(tr)
	for i, name := range []string{"one", "two", "three"} {
		tr.subs = append(tr.subs, &orderedStopSubscription{all: stops, own: stops[i]})
		a.Register(name, func(context.Context, rpc.Delivery) ([]byte, string, error) { return nil, "", nil })
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	originalDrainSignal(t, tr.ready)
	cancel()
	if err := originalDrainResult(t, done); err != nil {
		t.Fatal(err)
	}
	if err := a.WaitDrained(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestOriginalHTTPDrainOverrunJoinsAllRepliesAndAfterRun(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()
	tr := sdkhttp.New(&sdkhttp.Transport{Mux: mux})
	a := New(Config{DefaultTimeout: 20 * time.Millisecond}).Transport(tr)
	started := []chan struct{}{make(chan struct{}), make(chan struct{})}
	release := []chan struct{}{make(chan struct{}), make(chan struct{})}
	var once [2]sync.Once
	defer func() {
		for i := range release {
			once[i].Do(func() { close(release[i]) })
		}
	}()
	var calls [2]atomic.Int32
	for i, name := range []string{"one", "two"} {
		a.Register(name, func(context.Context, rpc.Delivery) ([]byte, string, error) {
			calls[i].Add(1)
			close(started[i])
			<-release[i]
			return []byte(name + "-original"), "text/plain", nil
		})
	}
	afterStarted, afterRelease := make(chan struct{}), make(chan struct{})
	var afterOnce sync.Once
	defer afterOnce.Do(func() { close(afterRelease) })
	a.afterRun = func(ctx context.Context) error {
		close(afterStarted)
		<-afterRelease
		return server.Config.Shutdown(ctx)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	runDone := make(chan error, 1)
	go func() { runDone <- a.Run(runCtx) }()
	requestResults := make(chan error, 2)
	for _, name := range []string{"one", "two"} {
		request := httptest.NewRequest(http.MethodPost, "/rpc/"+name, nil)
		deadline := time.Now().Add(3 * time.Second)
		for {
			_, pattern := mux.Handler(request)
			if pattern != "" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("HTTP endpoint not registered")
			}
			time.Sleep(time.Millisecond)
		}
		go func() {
			response, err := server.Client().Post(server.URL+"/rpc/"+name, "application/json", strings.NewReader(`{}`))
			if err == nil {
				body, readErr := io.ReadAll(response.Body)
				_ = response.Body.Close()
				err = readErr
				if response.StatusCode != http.StatusOK || !strings.Contains(string(body), name+"-original") {
					err = errors.New("original HTTP response lost")
				}
			}
			requestResults <- err
		}()
	}
	for _, s := range started {
		originalDrainSignal(t, s)
	}
	cancelRun()
	if err := originalDrainResult(t, runDone); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("original Run overrun=%v", err)
	}
	// Both original endpoints already reject admission, before either active
	// handler or the deferred server.Shutdown is allowed to finish.
	for i, name := range []string{"one", "two"} {
		response, err := server.Client().Post(server.URL+"/rpc/"+name, "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusServiceUnavailable || calls[i].Load() != 1 {
			t.Fatal("HTTP admitted work during original drain")
		}
	}
	select {
	case <-afterStarted:
		t.Fatal("afterRun started before original replies joined")
	default:
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.WaitDrained(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	joined := make(chan error, 3)
	for range 3 {
		go func() { joined <- a.WaitDrained(context.Background()) }()
	}
	for i := range release {
		once[i].Do(func() { close(release[i]) })
	}
	for range 2 {
		if err := originalDrainResult(t, requestResults); err != nil {
			t.Fatal(err)
		}
	}
	originalDrainSignal(t, afterStarted)
	select {
	case err := <-joined:
		t.Fatalf("join omitted original afterRun: %v", err)
	default:
	}
	afterOnce.Do(func() { close(afterRelease) })
	for range 3 {
		if err := originalDrainResult(t, joined); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Run(context.Background()); !errors.Is(err, ErrRunAlreadyStarted) {
		t.Fatal(err)
	}
}
