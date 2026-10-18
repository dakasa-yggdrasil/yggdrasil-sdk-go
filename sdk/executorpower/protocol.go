// Package executorpower implements the opt-in operator-adopted EC2 power variant
// of Core's mutation ledger. It cannot authorize creation or destruction.
package executorpower

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
)

const Kind = "ec2_power_v1"
const AdoptionKind = "ec2_operator_adoption_v1"
const Running = "ensure_ec2_instance_running"
const Stopped = "ensure_ec2_instance_stopped"

var ErrDenied = errors.New("executor power authority denied")
var ErrUnknown = errors.New("original executor power outcome remains unknown")

type Resource struct {
	AccountID  string   `json:"account_id"`
	Region     string   `json:"region"`
	InstanceID string   `json:"instance_id"`
	Volumes    []string `json:"ebs_volume_ids"`
}
type Workflow struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}
type Binding struct {
	Mode                string   `json:"mode"`
	Enabled             bool     `json:"enabled"`
	Resource            Resource `json:"resource"`
	IntegrationID       string   `json:"integration_instance_id"`
	IntegrationChecksum string   `json:"integration_checksum"`
	TypeID              string   `json:"integration_type_id"`
	TypeChecksum        string   `json:"integration_type_checksum"`
	AdapterPrincipal    string   `json:"adapter_principal_id"`
	ControllerPrincipal string   `json:"controller_principal_id"`
	Operators           []string `json:"operator_collaborator_ids"`
	AdoptionWorkflow    Workflow `json:"adoption_workflow"`
	GrantSeconds        int      `json:"grant_seconds"`
}
type Scope struct {
	Namespace      string `json:"namespace"`
	Environment    string `json:"environment"`
	Domain         string `json:"domain"`
	Dimension      string `json:"dimension"`
	PolicyID       string `json:"policy_id"`
	PolicyChecksum string `json:"policy_checksum"`
	WorkflowID     string `json:"workflow_id"`
}
type Observation struct {
	Resource    Resource  `json:"resource"`
	State       string    `json:"state"`
	ObservedAt  time.Time `json:"observed_at"`
	WorkerReady bool      `json:"worker_ready"`
	BootID      string    `json:"boot_id"`
}
type Adoption struct {
	Kind       string    `json:"authority_kind"`
	ID         string    `json:"adoption_id"`
	Resource   Resource  `json:"resource"`
	Operator   string    `json:"operator_id"`
	ObservedAt time.Time `json:"observed_at"`
	AdoptedAt  time.Time `json:"adopted_at"`
	BindingSHA string    `json:"binding_sha256"`
}
type Grant struct {
	SettledAt        *time.Time `json:"settled_at,omitempty"`
	Kind             string     `json:"authority_kind"`
	ID               string     `json:"grant_id"`
	AdoptionID       string     `json:"adoption_id"`
	Action           string     `json:"action"`
	CommandID        string     `json:"command_id"`
	Epoch            uint64     `json:"epoch"`
	State            string     `json:"state"`
	RequestSHA       string     `json:"request_sha256"`
	IssuedAt         time.Time  `json:"issued_at"`
	ExpiresAt        time.Time  `json:"expires_at"`
	Binding          Binding    `json:"binding"`
	Scope            Scope      `json:"scope"`
	AttemptID        string     `json:"attempt_id,omitempty"`
	SettlementSHA    string     `json:"settlement_sha256,omitempty"`
	Outcome          string     `json:"outcome,omitempty"`
	ServiceRequestID string     `json:"service_request_id,omitempty"`
	OriginalJoined   bool       `json:"original_joined"`
	ConfirmedAt      *time.Time `json:"confirmed_at,omitempty"`
}
type Claim struct {
	GrantID       string `json:"grant_id"`
	Action        string `json:"action"`
	CommandID     string `json:"command_id"`
	RequestSHA    string `json:"request_sha256"`
	AttemptID     string `json:"attempt_id"`
	SettlementSHA string `json:"settlement_sha256"`
}
type ClaimReply struct {
	Mode     string    `json:"mode"`
	GrantID  string    `json:"grant_id"`
	Deadline time.Time `json:"deadline"`
}
type Result struct {
	GrantID          string `json:"grant_id"`
	AttemptID        string `json:"attempt_id"`
	RequestSHA       string `json:"request_sha256"`
	Outcome          string `json:"outcome"`
	ServiceRequestID string `json:"service_request_id"`
	OriginalJoined   bool   `json:"original_joined"`
}

func Capability(action string) string {
	switch action {
	case "start":
		return Running
	case "stop":
		return Stopped
	}
	return ""
}
func Digest(v any) string   { raw, _ := json.Marshal(v); return fmt.Sprintf("%x", sha256.Sum256(raw)) }
func Key(r Resource) string { return r.AccountID + "/" + r.Region + "/" + r.InstanceID }
func SameResource(a, b Resource) bool {
	return a.AccountID == b.AccountID && a.Region == b.Region && a.InstanceID == b.InstanceID && slices.Equal(a.Volumes, b.Volumes)
}
func ValidateResource(r Resource) error {
	if !regexp.MustCompile(`^[0-9]{12}$`).MatchString(r.AccountID) || !regexp.MustCompile(`^[a-z]{2}-[a-z]+-[0-9]+$`).MatchString(r.Region) || !regexp.MustCompile(`^i-[a-f0-9]{17}$`).MatchString(r.InstanceID) || len(r.Volumes) < 1 || len(r.Volumes) > 16 {
		return ErrDenied
	}
	for i, v := range r.Volumes {
		if !regexp.MustCompile(`^vol-[a-f0-9]{17}$`).MatchString(v) || (i > 0 && r.Volumes[i-1] >= v) {
			return ErrDenied
		}
	}
	return nil
}
func ValidateBinding(b Binding) error {
	for _, v := range []string{b.AdapterPrincipal, b.ControllerPrincipal, b.AdoptionWorkflow.Namespace, b.AdoptionWorkflow.Name} {
		if len(v) < 1 || len(v) > 128 || strings.TrimSpace(v) != v || strings.ContainsAny(v, "\r\n\t") {
			return ErrDenied
		}
	}
	if b.Mode != Kind || ValidateResource(b.Resource) != nil || b.GrantSeconds < 5 || b.GrantSeconds > 60 || len(b.Operators) < 1 || len(b.Operators) > 32 || b.AdoptionWorkflow.Namespace == "" || b.AdoptionWorkflow.Name == "" || b.AdapterPrincipal == "" || b.ControllerPrincipal == "" || b.AdapterPrincipal == b.ControllerPrincipal {
		return ErrDenied
	}
	for _, v := range []string{b.IntegrationID, b.TypeID} {
		if !uuidString(v) {
			return ErrDenied
		}
	}
	for _, v := range []string{b.IntegrationChecksum, b.TypeChecksum} {
		if !hash(v) {
			return ErrDenied
		}
	}
	seen := map[string]bool{}
	for _, id := range b.Operators {
		if !uuidString(id) || seen[id] {
			return ErrDenied
		}
		seen[id] = true
	}
	return nil
}
func CheckObservation(b Binding, o Observation, now time.Time) error {
	if !SameResource(b.Resource, o.Resource) || o.ObservedAt.IsZero() || o.ObservedAt.After(now) || now.Sub(o.ObservedAt) > 30*time.Second || (o.State != "running" && o.State != "stopped") {
		return ErrDenied
	}
	return nil
}
func GrantSHA(g Grant) string {
	return Digest([]any{Kind, g.ID, g.AdoptionID, g.Action, g.CommandID, g.Epoch, g.Binding, g.Scope, g.IssuedAt, g.ExpiresAt})
}
func uuidString(v string) bool {
	return regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`).MatchString(v) && v != "00000000-0000-0000-0000-000000000000"
}
func hash(v string) bool { return regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(v) }

func ValidateGrant(g Grant) error {
	switch g.State {
	case "issued":
		if g.AttemptID != "" || g.SettlementSHA != "" || g.Outcome != "" || g.SettledAt != nil || g.ConfirmedAt != nil {
			return ErrDenied
		}
	case "redeemed":
		if !uuidString(g.AttemptID) || !hash(g.SettlementSHA) || g.Outcome != "" || g.SettledAt != nil || g.ConfirmedAt != nil {
			return ErrDenied
		}
	case "settled", "confirmed":
		if !uuidString(g.AttemptID) || !hash(g.SettlementSHA) || g.SettledAt == nil || g.SettledAt.Before(g.IssuedAt) || (g.Outcome != "accepted" && g.Outcome != "uncertain") {
			return ErrDenied
		}
		if g.Outcome == "accepted" && (!g.OriginalJoined || g.ServiceRequestID == "" || len(g.ServiceRequestID) > 256) {
			return ErrDenied
		}
		if g.State == "confirmed" && (g.ConfirmedAt == nil || g.ConfirmedAt.Before(*g.SettledAt) || g.Outcome != "accepted") {
			return ErrDenied
		}
	case "rejected", "expired":
		if g.Outcome != "rejected_before_send" || g.AttemptID != "" || g.SettlementSHA != "" || g.SettledAt != nil || g.ConfirmedAt != nil {
			return ErrDenied
		}
	default:
		return ErrDenied
	}

	if !uuidString(g.Scope.PolicyID) || !uuidString(g.Scope.WorkflowID) || g.Scope.PolicyChecksum == "" || g.Scope.Namespace == "" || g.Scope.Environment == "" || g.Scope.Domain == "" || g.Scope.Dimension == "" {
		return ErrDenied
	}
	if g.Kind != Kind || ValidateBinding(g.Binding) != nil || !g.Binding.Enabled || !uuidString(g.ID) || !uuidString(g.AdoptionID) || Capability(g.Action) == "" || g.CommandID == "" || len(g.CommandID) > 128 || g.RequestSHA != GrantSHA(g) || g.IssuedAt.IsZero() || !g.ExpiresAt.Equal(g.IssuedAt.Add(time.Duration(g.Binding.GrantSeconds)*time.Second)) {
		return ErrDenied
	}
	return nil
}
