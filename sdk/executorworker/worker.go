package executorworker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

var ErrHold = errors.New("executor worker HOLD")
var name = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

type job struct {
	Profile string `json:"profile"`
	State   string `json:"state"`
}
type journal struct {
	Version  int            `json:"version"`
	Hold     bool           `json:"hold,omitempty"`
	Resource Resource       `json:"resource"`
	Boot     string         `json:"boot"`
	Lifetime string         `json:"lifetime"`
	Epoch    uint64         `json:"epoch"`
	Closed   bool           `json:"closed"`
	Jobs     map[string]job `json:"jobs"`
}
type Worker struct {
	mu               sync.Mutex
	cfg              Config
	kernel           Kernel
	j                journal
	joined           map[string]bool
	hold             bool
	holdPersisted    bool
	preserveOriginal bool
	lock             *os.File
}

// New is inert when disabled. Enabled construction never opens admission automatically.
func New(cfg Config, kernel Kernel) (*Worker, error) {
	if !cfg.Enabled {
		return nil, errors.New("executor worker disabled")
	}
	if kernel == nil || !filepath.IsAbs(cfg.StateRoot) || cfg.Resource.AccountID == "" || cfg.Resource.Region == "" || cfg.Resource.InstanceID == "" || cfg.Resource.OwnershipBindingID == "" {
		return nil, ErrHold
	}
	if cfg.MaxJobs == 0 {
		cfg.MaxJobs = 128
	}
	if cfg.MaxJobs < 1 || cfg.MaxJobs > 1024 {
		return nil, ErrHold
	}
	profiles := make(map[string]Command, len(cfg.Profiles))
	for k, c := range cfg.Profiles {
		if !name.MatchString(k) || !filepath.IsAbs(c.Path) || c.UID == 0 || c.GID == 0 {
			return nil, ErrHold
		}
		c.Args = append([]string(nil), c.Args...)
		profiles[k] = c
	}
	cfg.Profiles = profiles
	boot, life, err := kernel.Identity()
	if err != nil || boot == "" || life == "" {
		return nil, ErrHold
	}
	if err = os.MkdirAll(cfg.StateRoot, 0700); err != nil {
		return nil, err
	}
	if _, real := kernel.(*LinuxKernel); real {
		if err := validateStateRoot(cfg.StateRoot); err != nil {
			return nil, err
		}
	}
	_, lockStatErr := os.Lstat(filepath.Join(cfg.StateRoot, "lock"))
	hadLock := lockStatErr == nil
	lock, err := lockRoot(cfg.StateRoot)
	if err != nil {
		return nil, err
	}
	w := &Worker{cfg: cfg, kernel: kernel, lock: lock, joined: map[string]bool{}}
	w.j = journal{Version: 1, Resource: cfg.Resource, Boot: boot, Lifetime: life, Closed: true, Jobs: map[string]job{}}
	b, err := os.ReadFile(filepath.Join(cfg.StateRoot, "journal.json"))
	if err == nil {
		var old journal
		if len(b) > 1<<20 || decode(b, &old) != nil || old.Version != 1 || old.Resource != cfg.Resource || old.Boot != boot || old.Lifetime != life || len(old.Jobs) > cfg.MaxJobs || old.Jobs == nil {
			w.hold = true
			w.preserveOriginal = true // Preserve the invalid original journal on disk.
		} else {
			w.j = old
			w.hold = old.Hold
			w.holdPersisted = old.Hold
			for id, j := range old.Jobs {
				if !name.MatchString(id) || j.State != "succeeded" && j.State != "failed" {
					w.hold = true
					w.preserveOriginal = true
				}
			}
			if !old.Closed {
				w.hold = true
			}
		}
	} else if hadLock || !os.IsNotExist(err) {
		w.hold = true
		w.preserveOriginal = true // Missing original evidence remains missing; never initialize over it.
	}
	if !w.hold {
		if err = w.persist(); err != nil {
			w.hold = true
		}
	}
	return w, nil
}
func decode(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return ErrHold
	}
	return nil
}
func (w *Worker) persist() error {
	b, e := json.Marshal(w.j)
	if e != nil || len(b) > 1<<20 {
		return ErrHold
	}
	f, e := os.CreateTemp(w.cfg.StateRoot, "journal-*")
	if e != nil {
		return e
	}
	p := f.Name()
	defer os.Remove(p)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e == nil {
		e = os.Rename(p, filepath.Join(w.cfg.StateRoot, "journal.json"))
	}
	if e != nil {
		return e
	}
	d, e := os.Open(w.cfg.StateRoot)
	if e != nil {
		return e
	}
	defer d.Close()
	e = d.Sync()
	if e == nil && w.j.Hold {
		w.holdPersisted = true
	}
	return e
}
func (w *Worker) snapshot() Snapshot {
	s := Snapshot{Resource: w.cfg.Resource, BootID: w.j.Boot, LifetimeID: w.j.Lifetime, ObservedAt: time.Now().UTC(), Closed: w.j.Closed, Epoch: w.j.Epoch, Jobs: map[string]string{}}
	boot, life, e := w.kernel.Identity()
	if e != nil || boot != w.j.Boot || life != w.j.Lifetime {
		w.hold = true
	}
	counts, e := w.kernel.Census()
	if e != nil {
		w.hold = true
	}
	for id, n := range counts {
		j, ok := w.j.Jobs[id]
		if !ok || n < 0 || n > 0 && (!w.joined[id] || j.State != "running") {
			w.hold = true
		}
	}
	for id, j := range w.j.Jobs {
		s.Jobs[id] = j.State
		if j.State == "possible_send" || j.State == "running" && !w.joined[id] {
			w.hold = true
		}
	}
	if w.hold && !w.holdPersisted && !w.preserveOriginal {
		w.j.Hold = true
		// Persist sticky uncertainty before a closed restart can accept new work.
		// A write failure keeps the current process in HOLD.
		_ = w.persist()
	}
	s.Complete = !w.hold
	s.Ready = s.Complete
	return s
}

// Apply serializes fencing, durable pre-send intent and process creation. RPC cancellation
// never kills an admitted process; the original Wait goroutine owns its settlement.
func (w *Worker) Apply(ctx context.Context, op string, r Request) (Snapshot, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if ctx.Err() != nil {
		return Snapshot{}, ctx.Err()
	}
	if r.Resource != w.cfg.Resource {
		return Snapshot{}, ErrHold
	}
	s := w.snapshot()
	if op == "observe_executor_worker" {
		return s, nil
	}
	if !s.Complete || r.BootID != w.j.Boot || r.Epoch < w.j.Epoch {
		return s, ErrHold
	}
	switch op {
	case "ensure_executor_admission_closed", "ensure_executor_admission_open":
		desired := op == "ensure_executor_admission_closed"
		if r.JobID != "" || r.Profile != "" || r.Epoch == 0 || r.Epoch == w.j.Epoch && desired != w.j.Closed {
			return s, ErrHold
		}
		w.j.Epoch = r.Epoch
		w.j.Closed = desired
	case "ensure_executor_job":
		if w.j.Closed || r.Epoch != w.j.Epoch || !name.MatchString(r.JobID) {
			return s, ErrHold
		}
		c, ok := w.cfg.Profiles[r.Profile]
		if !ok {
			return s, ErrHold
		}
		if old, ok := w.j.Jobs[r.JobID]; ok {
			if old.Profile != r.Profile {
				return s, ErrHold
			}
			return w.snapshot(), nil
		}
		if len(w.j.Jobs) >= w.cfg.MaxJobs {
			return s, ErrHold
		}
		w.j.Jobs[r.JobID] = job{r.Profile, "possible_send"}
		if e := w.persist(); e != nil {
			w.hold = true
			return w.snapshot(), e
		}
		done, e := w.kernel.Spawn(r.JobID, c)
		if e != nil || done == nil {
			w.hold = true
			return w.snapshot(), ErrHold
		}
		w.joined[r.JobID] = true
		w.j.Jobs[r.JobID] = job{r.Profile, "running"}
		go func() {
			err, ok := <-done
			w.mu.Lock()
			defer w.mu.Unlock()
			if !ok {
				w.hold = true
				return
			}
			state := "succeeded"
			if errors.Is(err, ErrHold) {
				w.hold = true
				w.j.Hold = true
			}
			if err != nil {
				state = "failed"
			}
			w.j.Jobs[r.JobID] = job{r.Profile, state}
			delete(w.joined, r.JobID)
			if w.persist() != nil {
				w.hold = true
			}
		}()
	default:
		return s, fmt.Errorf("unknown worker operation")
	}
	if e := w.persist(); e != nil {
		w.hold = true
		return w.snapshot(), e
	}
	return w.snapshot(), nil
}
