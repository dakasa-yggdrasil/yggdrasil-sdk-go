//go:build linux

package executorworker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Explicitly opt in on the dedicated disposable lab. No production paths are used.
func realWorker(t *testing.T) (*Worker, *LinuxKernel, Request) {
	t.Helper()
	if os.Getenv("EXECUTOR_WORKER_LINUX_TEST") != "isolated" {
		t.Skip("dedicated Linux cgroup lab required")
	}
	if os.Geteuid() != 0 {
		t.Fatal("root-owned cgroup fixture required")
	}
	root, err := os.MkdirTemp("/var/lib/dakasa-lab/worker-tests", "worker-")
	if err != nil {
		t.Fatal(err)
	}
	cg := filepath.Join("/sys/fs/cgroup", filepath.Base(root))
	if err = os.Mkdir(cg, 0755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		entries, _ := os.ReadDir(cg)
		for _, e := range entries {
			if e.IsDir() {
				if err := os.Remove(filepath.Join(cg, e.Name())); err != nil {
					t.Error(err)
				}
			}
		}
		if err := os.Remove(cg); err != nil {
			t.Error(err)
		}
		os.RemoveAll(root)
	})
	k, err := NewLinuxKernel(cg)
	if err != nil {
		t.Fatal(err)
	}
	r := Resource{"153828470928", "us-east-1", "i-0cb486ae31a45d15e", "isolated-kernel-fixture"}
	w, err := New(Config{Enabled: true, Resource: r, StateRoot: filepath.Join(root, "state"), Profiles: map[string]Command{"sleep": {Path: "/usr/bin/sleep", Args: []string{"0.25"}, UID: 65534, GID: 65534}}}, k)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if w.lock != nil {
			w.lock.Close()
		}
	})
	boot, _, err := k.Identity()
	if err != nil {
		t.Fatal(err)
	}
	return w, k, Request{Resource: r, BootID: boot, Epoch: 1}
}

func TestLinuxWorkerOriginalProcessAndRestart(t *testing.T) {
	w, k, r := realWorker(t)
	if _, err := w.Apply(context.Background(), "ensure_executor_admission_open", r); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.JobID = "original"
	r.Profile = "sleep"
	if _, err := w.Apply(ctx, "ensure_executor_job", r); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	if _, err := w.Apply(context.Background(), "ensure_executor_job", r); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		s, err := w.Apply(context.Background(), "observe_executor_worker", r)
		if err != nil {
			t.Fatal(err)
		}
		if s.Jobs[r.JobID] == "succeeded" {
			if !s.Complete {
				t.Fatal("lost original process custody", s)
			}
			break
		}
		if s.Jobs[r.JobID] == "failed" || time.Now().After(deadline) {
			t.Fatal("process not joined successfully", s)
		}
		time.Sleep(10 * time.Millisecond)
	}
	r.JobID = ""
	r.Profile = ""
	r.Epoch = 2
	if _, err := w.Apply(context.Background(), "ensure_executor_admission_closed", r); err != nil {
		t.Fatal(err)
	}
	w.lock.Close()
	again, err := New(w.cfg, k)
	if err != nil {
		t.Fatal(err)
	}
	defer again.lock.Close()
	s, err := again.Apply(context.Background(), "observe_executor_worker", r)
	if err != nil || !s.Complete || !s.Closed || s.Jobs["original"] != "succeeded" {
		t.Fatal("closed restart lost custody", err, s)
	}
}

func TestLinuxWorkerUnknownCgroupHolds(t *testing.T) {
	w, k, r := realWorker(t)
	if err := os.Mkdir(filepath.Join(k.Root, "foreign"), 0755); err != nil {
		t.Fatal(err)
	}
	s, err := w.Apply(context.Background(), "observe_executor_worker", r)
	if err != nil || s.Complete || s.Ready {
		t.Fatal("foreign cgroup accepted", err, s)
	}
	if err := os.Remove(filepath.Join(k.Root, "foreign")); err != nil {
		t.Fatal(err)
	}
	s, err = w.Apply(context.Background(), "ensure_executor_admission_open", r)
	if !errors.Is(err, ErrHold) || s.Complete {
		t.Fatal("HOLD cleared after foreign evidence vanished", err, s)
	}
}

func TestLinuxKernelSpawnOriginal(t *testing.T) {
	_, k, _ := realWorker(t)
	done, err := k.Spawn("direct", Command{Path: "/usr/bin/sleep", Args: []string{"0.01"}, UID: 65534, GID: 65534})
	if err != nil {
		t.Fatal("native Spawn:", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("native Wait timeout")
	}
}
