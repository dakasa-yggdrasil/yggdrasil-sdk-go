package adapter

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
)

func TestOriginalCleanupNilPanicNeverQualifiesSuccessfulReturn(t *testing.T) {
	for _, compat := range []string{"0", "1"} {
		t.Run("panicnil="+compat, func(t *testing.T) {
			// Go dynamically observes GODEBUG changes. Keep all other configured
			// debug settings and restore the environment through testing cleanup.
			settings := os.Getenv("GODEBUG")
			if settings != "" {
				settings += ","
			}
			t.Setenv("GODEBUG", settings+"panicnil="+compat)
			err := callOriginalCleanup("nil-panic-fixture", func() error { panic(nil) })
			if !errors.Is(err, errOriginalCleanupPanicked) {
				t.Fatalf("panic(nil) became success under compatibility=%s: %v", compat, err)
			}
		})
	}
}

type panicDrainSubscription struct {
	name                  string
	stopPanic, closePanic bool
	stopped, closeStarted chan struct{}
	release               <-chan struct{}
	stopOnce, closeOnce   sync.Once
	effectRelease         chan struct{}
	effectDone            <-chan struct{}
	effectOnce            *sync.Once
}

func (s *panicDrainSubscription) Endpoint() string { return s.name }
func (s *panicDrainSubscription) StopAdmission() {
	if s.stopPanic {
		panic("CI-only-unpublishable-panic-value")
	}
	s.stopOnce.Do(func() { close(s.stopped) })
}
func (s *panicDrainSubscription) Close() error {
	s.closeOnce.Do(func() { close(s.closeStarted) })
	if s.closePanic {
		panic("CI-only-unpublishable-panic-value")
	}
	if s.stopPanic {
		return errors.New("fixture original owner remains unknown after stop panic")
	}
	if s.release != nil {
		<-s.release
	}
	if s.effectRelease != nil {
		s.effectOnce.Do(func() { close(s.effectRelease) })
		<-s.effectDone
	}
	return nil
}

type panicDrainTransport struct {
	byName   map[string]*panicDrainSubscription
	consumed int
	ready    chan struct{}
}

func (t *panicDrainTransport) Consume(c rpc.ConsumerConfig) (rpc.Subscription, error) {
	s := t.byName[c.Endpoint]
	t.consumed++
	if t.consumed == len(t.byName) {
		close(t.ready)
	}
	return s, nil
}
func (*panicDrainTransport) Publish(context.Context, rpc.Request) error { return nil }
func (*panicDrainTransport) Request(context.Context, rpc.Request) (rpc.Reply, error) {
	return rpc.Reply{}, nil
}
func (*panicDrainTransport) Close() error { return nil }

func TestOriginalCallbackPanicKeepsUnknownEffectsUnqualifiedAndJoinsOtherEndpoints(t *testing.T) {
	for _, phase := range []string{"stop", "close", "after-run"} {
		t.Run(phase, func(t *testing.T) {
			activeRelease := make(chan struct{})
			var activeOnce sync.Once
			defer activeOnce.Do(func() { close(activeRelease) })
			// A real separate fixture goroutine models work which a failed foreign
			// callback did NOT join. Only the isolated CI owner may release it later.
			externalRelease, externalDone := make(chan struct{}), make(chan struct{})
			go func() { defer close(externalDone); <-externalRelease }()
			var externalOnce sync.Once
			defer func() { externalOnce.Do(func() { close(externalRelease) }); originalDrainSignal(t, externalDone) }()
			fault := &panicDrainSubscription{name: "fault", stopPanic: phase == "stop", closePanic: phase == "close", stopped: make(chan struct{}), closeStarted: make(chan struct{})}
			if phase != "after-run" {
				fault.effectRelease, fault.effectDone, fault.effectOnce = externalRelease, externalDone, &externalOnce
			}
			active := &panicDrainSubscription{name: "active", stopped: make(chan struct{}), closeStarted: make(chan struct{}), release: activeRelease}
			tr := &panicDrainTransport{byName: map[string]*panicDrainSubscription{"fault": fault, "active": active}, ready: make(chan struct{})}
			a := New(Config{DefaultTimeout: 10 * time.Millisecond}).Transport(tr)
			for _, name := range []string{"fault", "active"} {
				a.Register(name, func(context.Context, rpc.Delivery) ([]byte, string, error) { return nil, "", nil })
			}
			var afterCalls atomic.Int32
			a.afterRun = func(context.Context) error {
				afterCalls.Add(1)
				if phase == "after-run" {
					panic("CI-only-unpublishable-panic-value") // Before joining its own resource.
				}
				externalOnce.Do(func() { close(externalRelease) })
				<-externalDone
				return nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runDone := make(chan error, 1)
			go func() { runDone <- a.Run(ctx) }()
			originalDrainSignal(t, tr.ready)
			cancel()
			originalDrainSignal(t, active.stopped)
			originalDrainSignal(t, active.closeStarted)
			originalDrainSignal(t, fault.closeStarted)
			if afterCalls.Load() != 0 {
				t.Fatal("global cleanup ran while another original endpoint remained active")
			}
			if err := originalDrainResult(t, runDone); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			waitCtx, cancelWait := context.WithTimeout(context.Background(), 10*time.Millisecond)
			if err := a.WaitDrained(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("panic promoted unknown endpoint to completion: %v", err)
			}
			cancelWait()
			activeOnce.Do(func() { close(activeRelease) })
			waitCtx, cancelWait = context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelWait()
			finalErr := a.WaitDrained(waitCtx)
			if !errors.Is(finalErr, errOriginalCleanupPanicked) || strings.Contains(finalErr.Error(), "CI-only-unpublishable-panic-value") {
				t.Fatalf("panic result is not fixed/unqualified: %v", finalErr)
			}
			if phase == "after-run" {
				if afterCalls.Load() != 1 {
					t.Fatal("original afterRun was replayed or omitted")
				}
			} else if afterCalls.Load() != 0 {
				t.Fatal("transport cleanup ran under unqualified callback settlement")
			}
			select {
			case <-externalDone:
				t.Fatal("callback panic falsely proved external goroutine joined")
			default:
			}
			// finalErr must keep the application in HOLD. The deliberately unknown
			// fixture goroutine is still alive here; no no-effect or safe-exit claim.
		})
	}
}
