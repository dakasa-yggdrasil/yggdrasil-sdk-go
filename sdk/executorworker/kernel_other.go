//go:build !linux

package executorworker

import (
	"errors"
	"os"
)

type LinuxKernel struct{}

func NewLinuxKernel(string) (*LinuxKernel, error) {
	return nil, errors.New("executor worker requires Linux cgroup v2")
}
func (*LinuxKernel) Identity() (string, string, error)           { return "", "", ErrHold }
func (*LinuxKernel) Census() (map[string]int, error)             { return nil, ErrHold }
func (*LinuxKernel) Spawn(string, Command) (<-chan error, error) { return nil, ErrHold }

// Tests may supply a kernel on other platforms; production LinuxKernel fails closed.
func lockRoot(root string) (*os.File, error) {
	return os.OpenFile(root+"/lock", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
}

func validateStateRoot(string) error { return ErrHold }
