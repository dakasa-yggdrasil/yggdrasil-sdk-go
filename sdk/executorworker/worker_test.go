package executorworker

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/adapter"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type fakeKernel struct {
	counts map[string]int
	done   chan error
	calls  int
	bad    bool
	fail   bool
}

func (k *fakeKernel) Identity() (string, string, error) {
	if k.bad {
		return "", "", ErrHold
	}
	return "boot", "lifetime", nil
}
func (k *fakeKernel) Census() (map[string]int, error) { return k.counts, nil }
func (k *fakeKernel) Spawn(id string, c Command) (<-chan error, error) {
	k.calls++
	if k.fail {
		return nil, errors.New("uncertain")
	}
	k.done = make(chan error, 1)
	return k.done, nil
}
func setup(t *testing.T) (*Worker, *fakeKernel, Request) {
	t.Helper()
	k := &fakeKernel{counts: map[string]int{}}
	r := Resource{"123456789012", "us-east-1", "i-123", "adoption"}
	w, e := New(Config{Enabled: true, Resource: r, StateRoot: t.TempDir(), Profiles: map[string]Command{"test": {Path: "/usr/bin/true", UID: 1000, GID: 1000}}}, k)
	if e != nil {
		t.Fatal(e)
	}
	return w, k, Request{Resource: r, BootID: "boot", Epoch: 1}
}
func TestWorkerFencingDedupeAndOriginalLifetime(t *testing.T) {
	w, k, r := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	if _, e := w.Apply(ctx, "ensure_executor_admission_open", r); e != nil {
		t.Fatal(e)
	}
	r.JobID = "job1"
	r.Profile = "test"
	if _, e := w.Apply(ctx, "ensure_executor_job", r); e != nil {
		t.Fatal(e)
	}
	cancel()
	if _, e := w.Apply(context.Background(), "ensure_executor_job", r); e != nil || k.calls != 1 {
		t.Fatal(e, k.calls)
	}
	k.done <- nil
	for i := 0; i < 100; i++ {
		s, _ := w.Apply(context.Background(), "observe_executor_worker", r)
		if s.Jobs["job1"] == "succeeded" {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("original waiter did not settle")
}
func TestWorkerUnknownCensusAndUncertainSpawnHold(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		w, k, r := setup(t)
		w.Apply(context.Background(), "ensure_executor_admission_open", r)
		if unknown {
			k.counts["foreign"] = 1
		} else {
			k.fail = true
			r.JobID = "one"
			r.Profile = "test"
			w.Apply(context.Background(), "ensure_executor_job", r)
		}
		s, _ := w.Apply(context.Background(), "observe_executor_worker", r)
		if s.Complete || s.Ready {
			t.Fatal("unsafe complete", s)
		}
	}
}
func TestWorkerRestartAndCorruptionHold(t *testing.T) {
	w, k, r := setup(t)
	w.Apply(context.Background(), "ensure_executor_admission_open", r)
	w.lock.Close()
	os.Remove(filepath.Join(w.cfg.StateRoot, "lock"))
	again, e := New(w.cfg, k)
	if e != nil {
		t.Fatal(e)
	}
	s, _ := again.Apply(context.Background(), "observe_executor_worker", r)
	if s.Complete {
		t.Fatal("open restart must hold")
	}
	again.lock.Close()
	os.Remove(filepath.Join(w.cfg.StateRoot, "lock"))
	os.WriteFile(filepath.Join(w.cfg.StateRoot, "journal.json"), []byte(`{"version":1}`), 0600)
	again, e = New(w.cfg, k)
	if e != nil {
		t.Fatal(e)
	}
	s, _ = again.Apply(context.Background(), "observe_executor_worker", r)
	if s.Complete {
		t.Fatal("corruption must hold")
	}
}
func TestWorkerEpochAndClosedAdmission(t *testing.T) {
	w, k, r := setup(t)
	r.JobID = "job"
	r.Profile = "test"
	if _, e := w.Apply(context.Background(), "ensure_executor_job", r); e == nil || k.calls != 0 {
		t.Fatal("closed admitted")
	}
	r.JobID = ""
	r.Profile = ""
	w.Apply(context.Background(), "ensure_executor_admission_open", r)
	if _, e := w.Apply(context.Background(), "ensure_executor_admission_closed", r); e == nil {
		t.Fatal("same epoch changed state")
	}
	r.Epoch = 2
	if _, e := w.Apply(context.Background(), "ensure_executor_admission_closed", r); e != nil {
		t.Fatal(e)
	}
	r.Epoch = 1
	if _, e := w.Apply(context.Background(), "ensure_executor_admission_open", r); e == nil {
		t.Fatal("stale opened")
	}
}

type transport struct{ ready chan rpc.ConsumerConfig }

func (t *transport) Consume(c rpc.ConsumerConfig) (rpc.Subscription, error) {
	t.ready <- c
	return sub{}, nil
}
func (*transport) Request(context.Context, rpc.Request) (rpc.Reply, error) { return rpc.Reply{}, nil }
func (*transport) Publish(context.Context, rpc.Request) error              { return nil }
func (*transport) Close() error                                            { return nil }

type sub struct{}

func (sub) Endpoint() string { return "execute" }
func (sub) Close() error     { return nil }
func TestWorkerRealAdapterDispatch(t *testing.T) {
	w, _, r := setup(t)
	a := adapter.New(adapter.Config{})
	w.Register(a)
	tr := &transport{make(chan rpc.ConsumerConfig, 1)}
	a.Transport(tr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.Run(ctx)
	c := <-tr.ready
	in, _ := json.Marshal(r)
	b, _ := json.Marshal(executeRequest{Operation: "observe_executor_worker", Input: in})
	var reply Snapshot
	err := c.Handler(ctx, rpc.Delivery{Body: b, ReplyFn: func(_ context.Context, b []byte, _ string) error { return json.Unmarshal(b, &reply) }})
	if err != nil || !reply.Complete || !reply.Closed {
		t.Fatal(err, reply)
	}
	b = []byte(`{"operation":"observe_executor_worker","input":{"resource":{},"complete":true}}`)
	err = c.Handler(ctx, rpc.Delivery{Body: b, ReplyFn: func(context.Context, []byte, string) error { return nil }, NackFn: func(bool) error { return ErrHold }})
	if err == nil {
		t.Fatal("asserted proof accepted")
	}
}
func TestWorkerDisabled(t *testing.T) {
	if _, e := New(Config{}, nil); e == nil {
		t.Fatal("enabled by default")
	}
}

func TestWorkerCensusHoldSurvivesClosedRestart(t *testing.T) {
	w, k, r := setup(t)
	k.counts["foreign"] = 1
	s, err := w.Apply(context.Background(), "observe_executor_worker", r)
	if err != nil || s.Complete {
		t.Fatal("foreign census accepted", err, s)
	}
	delete(k.counts, "foreign")
	w.lock.Close()
	again, err := New(w.cfg, k)
	if err != nil {
		t.Fatal(err)
	}
	defer again.lock.Close()
	s, err = again.Apply(context.Background(), "observe_executor_worker", r)
	if err != nil || s.Complete || s.Ready {
		t.Fatal("restart erased sticky HOLD", err, s)
	}
}

func TestWorkerUnjoinedDescendantsRemainDurableHold(t *testing.T) {
	w, k, r := setup(t)
	if _, err := w.Apply(context.Background(), "ensure_executor_admission_open", r); err != nil {
		t.Fatal(err)
	}
	r.JobID = "parent"
	r.Profile = "test"
	if _, err := w.Apply(context.Background(), "ensure_executor_job", r); err != nil {
		t.Fatal(err)
	}
	k.done <- ErrHold
	deadline := time.Now().Add(time.Second)
	for {
		s, _ := w.Apply(context.Background(), "observe_executor_worker", r)
		if s.Jobs["parent"] == "failed" {
			if s.Complete {
				t.Fatal("unjoined descendants converted to ordinary job failure")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("original waiter not joined")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWorkerRetriesHoldPersistenceAfterStorageRecovery(t *testing.T) {
	w, k, r := setup(t)
	path := filepath.Join(w.cfg.StateRoot, "journal.json")
	backup := path + ".original"
	if err := os.Rename(path, backup); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	k.counts["foreign"] = 1
	s, _ := w.Apply(context.Background(), "observe_executor_worker", r)
	if s.Complete {
		t.Fatal("foreign process accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(backup, path); err != nil {
		t.Fatal(err)
	}
	delete(k.counts, "foreign")
	s, _ = w.Apply(context.Background(), "observe_executor_worker", r)
	if s.Complete {
		t.Fatal("in-memory HOLD lost")
	}
	w.lock.Close()
	again, err := New(w.cfg, k)
	if err != nil {
		t.Fatal(err)
	}
	defer again.lock.Close()
	s, _ = again.Apply(context.Background(), "observe_executor_worker", r)
	if s.Complete || s.Ready {
		t.Fatal("storage recovery did not persist HOLD before restart", s)
	}
}

func TestWorkerPreservesInvalidOriginalJournalBytes(t *testing.T) {
	for _, bad := range []string{`{"bad/id":{"profile":"test","state":"succeeded"}}`, `{"valid":{"profile":"test","state":"invented"}}`} {
		t.Run(bad, func(t *testing.T) {
			w, k, r := setup(t)
			w.lock.Close()
			b, err := json.Marshal(w.j)
			if err != nil {
				t.Fatal(err)
			}
			var j map[string]json.RawMessage
			if err = json.Unmarshal(b, &j); err != nil {
				t.Fatal(err)
			}
			j["jobs"] = json.RawMessage(bad)
			b, err = json.Marshal(j)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(w.cfg.StateRoot, "journal.json")
			if err = os.WriteFile(path, b, 0600); err != nil {
				t.Fatal(err)
			}
			again, err := New(w.cfg, k)
			if err != nil {
				t.Fatal(err)
			}
			defer again.lock.Close()
			s, _ := again.Apply(context.Background(), "observe_executor_worker", r)
			if s.Complete {
				t.Fatal("invalid original accepted")
			}
			after, err := os.ReadFile(path)
			if err != nil || string(after) != string(b) {
				t.Fatal("invalid original evidence overwritten", err)
			}
		})
	}
}
