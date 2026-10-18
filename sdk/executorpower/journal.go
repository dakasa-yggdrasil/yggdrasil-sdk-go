package executorpower

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

type FileJournal struct{ root string }

func NewFileJournal(root string) (*FileJournal, error) {
	if !filepath.IsAbs(root) {
		return nil, ErrDenied
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, ErrDenied
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return nil, ErrDenied
	}
	return &FileJournal{root: root}, nil
}
func (j *FileJournal) With(ctx context.Context, id string, fn func(*Entry, func(Entry) error) error) error {
	if j == nil || !uuidString(id) || fn == nil || ctx.Err() != nil {
		return ErrDenied
	}
	// No symlink-following lock or state file: these contain the ephemeral
	// original settlement nonce, never a reusable authentication credential.
	lockPath := filepath.Join(j.root, id+".lock")
	_, priorErr := os.Lstat(lockPath)
	if priorErr != nil && !errors.Is(priorErr, os.ErrNotExist) {
		return ErrUnknown
	}
	hadLock := priorErr == nil
	fd, err := syscall.Open(lockPath, syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	lock := os.NewFile(uintptr(fd), "power journal lock")
	defer lock.Close()
	if err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return ErrUnknown
	}
	defer syscall.Flock(fd, syscall.LOCK_UN)
	var old *Entry
	path := filepath.Join(j.root, id+".json")
	raw, err := readJournal(path)
	if err == nil {
		var e Entry
		d := json.NewDecoder(bytes.NewReader(raw))
		d.DisallowUnknownFields()
		if d.Decode(&e) != nil || d.Decode(new(any)) != io.EOF || e.Grant.ID != id || validateEntry(e) != nil {
			return ErrUnknown
		}
		old = &e
	} else if hadLock || !errors.Is(err, os.ErrNotExist) {
		return ErrUnknown
	}
	save := func(e Entry) error {
		if e.Grant.ID != id || validateEntry(e) != nil {
			return ErrDenied
		}
		b, er := json.Marshal(e)
		if er != nil || len(b) > 65536 {
			return ErrDenied
		}
		tmp, er := os.CreateTemp(j.root, ".original-")
		if er != nil {
			return er
		}
		name := tmp.Name()
		defer os.Remove(name)
		if _, er = tmp.Write(b); er == nil {
			er = tmp.Sync()
		}
		closeErr := tmp.Close()
		if er != nil {
			return er
		}
		if closeErr != nil {
			return closeErr
		}
		if er = os.Rename(name, path); er != nil {
			return er
		}
		dir, er := os.Open(j.root)
		if er != nil {
			return er
		}
		defer dir.Close()
		return dir.Sync()
	}
	return fn(old, save)
}
func readJournal(path string) ([]byte, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), "original power result")
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return nil, ErrUnknown
	}
	b, err := io.ReadAll(io.LimitReader(f, 65537))
	if err != nil || len(b) > 65536 {
		return nil, ErrUnknown
	}
	return b, nil
}

// A syntactically valid JSON document is not necessarily the original receipt.
// Validate every persisted phase before Runtime can take its settled fast path.
func validateEntry(e Entry) error {
	if ValidateGrant(e.Grant) != nil || e.Grant.State != "issued" || !uuidString(e.AttemptID) || len(e.Token) != 64 {
		return ErrUnknown
	}
	if _, err := hex.DecodeString(e.Token); err != nil {
		return ErrUnknown
	}
	switch e.Phase {
	case "claim_possible", "send_possible":
		if e.Result != nil {
			return ErrUnknown
		}
	case "original_returned", "settled":
		r := e.Result
		if r == nil || r.GrantID != e.Grant.ID || r.AttemptID != e.AttemptID || r.RequestSHA != e.Grant.RequestSHA {
			return ErrUnknown
		}
		switch r.Outcome {
		case "accepted":
			if !r.OriginalJoined || len(r.ServiceRequestID) == 0 || len(r.ServiceRequestID) > 256 {
				return ErrUnknown
			}
		case "uncertain":
			if r.ServiceRequestID != "" {
				return ErrUnknown
			}
		default:
			return ErrUnknown
		}
	default:
		return ErrUnknown
	}
	return nil
}
