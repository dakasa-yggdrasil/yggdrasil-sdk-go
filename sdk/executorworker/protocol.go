// Package executorworker owns bounded Linux process jobs independently of RPC leases.
package executorworker

import "time"

type Resource struct {
	AccountID          string `json:"account_id"`
	Region             string `json:"region"`
	InstanceID         string `json:"instance_id"`
	OwnershipBindingID string `json:"ownership_binding_id"`
}
type Request struct {
	Resource Resource `json:"resource"`
	BootID   string   `json:"boot_id"`
	Epoch    uint64   `json:"epoch"`
	JobID    string   `json:"job_id,omitempty"`
	Profile  string   `json:"profile,omitempty"`
}
type Snapshot struct {
	Resource   Resource          `json:"resource"`
	BootID     string            `json:"boot_id"`
	LifetimeID string            `json:"lifetime_id"`
	ObservedAt time.Time         `json:"observed_at"`
	Complete   bool              `json:"complete"`
	Ready      bool              `json:"ready"`
	Closed     bool              `json:"closed"`
	Epoch      uint64            `json:"epoch"`
	Jobs       map[string]string `json:"jobs"`
}

// Command is operator configuration only. Jobs cannot supply argv, environment or paths.
type Command struct {
	Path     string
	Args     []string
	UID, GID uint32
}

// Kernel implementations must census every descendant and spawn inside the owned cgroup.
type Kernel interface {
	Identity() (bootID, lifetimeID string, err error)
	Census() (map[string]int, error)
	Spawn(job string, command Command) (<-chan error, error)
}
type Config struct {
	Enabled   bool
	Resource  Resource
	StateRoot string
	Profiles  map[string]Command
	MaxJobs   int
}
