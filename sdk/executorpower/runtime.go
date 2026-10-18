package executorpower

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/google/uuid"
	"time"
)

// Authority is the existing Core mutation inventory, not a second credential
// service. Its implementation must never retry redemption after a lost reply.
type Authority interface {
	Redeem(context.Context, string, Claim) (ClaimReply, error)
	Settle(context.Context, string, string, string, Result) error
}

// Provider is implemented by the source-owned AWS SDK shim. It owns exact
// independent identity reads and single-attempt calls, never Core policy decisions.
type Provider interface {
	Observe(context.Context, Resource) (Observation, error)
	StartInstances(context.Context, Resource) (string, error)
	StopInstances(context.Context, Resource) (string, error)
}
type Entry struct {
	Grant     Grant   `json:"grant"`
	AttemptID string  `json:"attempt_id"`
	Token     string  `json:"settlement_token"`
	Phase     string  `json:"phase"`
	Result    *Result `json:"result,omitempty"`
}

// Journal serializes different adapter processes and persists before each phase.
// Missing/corrupt/replaced state must never grant replay permission.
type Journal interface {
	With(context.Context, string, func(*Entry, func(Entry) error) error) error
}

type Runtime struct {
	Enabled   bool
	Binding   Binding
	Authority Authority
	Provider  Provider
	Journal   Journal
	Now       func() time.Time
}

func (r Runtime) Execute(ctx context.Context, g Grant) error {
	if !r.Enabled || !r.Binding.Enabled || r.Authority == nil || r.Provider == nil || r.Journal == nil || r.Now == nil || ValidateGrant(g) != nil || g.State != "issued" || Digest(r.Binding) != Digest(g.Binding) || ctx.Err() != nil {
		return ErrDenied
	}
	return r.Journal.With(ctx, g.ID, func(old *Entry, save func(Entry) error) error {
		if old != nil {
			if old.Grant.RequestSHA != g.RequestSHA || old.Grant.ID != g.ID {
				return ErrDenied
			}
			// Only replay the exact original result to Core. Never call EC2 or redeem.
			if old.Result == nil {
				return ErrUnknown
			}
			if old.Phase == "settled" {
				if old.Result.Outcome != "accepted" {
					return ErrUnknown
				}
				return nil
			}
			return r.settleOriginal(ctx, *old, save)
		}
		if !r.Now().Before(g.ExpiresAt) {
			return ErrDenied
		}
		// Independent STS/Describe occurs before acquiring send authority; observation
		// is not acknowledgement and never clears a previously unknown invocation.
		o, err := r.Provider.Observe(ctx, g.Binding.Resource)
		if err != nil || CheckObservation(g.Binding, o, r.Now()) != nil {
			return ErrDenied
		}
		nonce := make([]byte, 32)
		if _, err = rand.Read(nonce); err != nil {
			return err
		}
		entry := Entry{Grant: g, AttemptID: uuid.NewString(), Token: hex.EncodeToString(nonce), Phase: "claim_possible"}
		if err = save(entry); err != nil {
			return ErrUnknown
		}
		c := Claim{GrantID: g.ID, Action: g.Action, CommandID: g.CommandID, RequestSHA: g.RequestSHA, AttemptID: entry.AttemptID, SettlementSHA: Digest(entry.Token)}
		reply, err := r.Authority.Redeem(ctx, g.Binding.IntegrationID, c)
		if err != nil || reply.Mode != "execute_once" || reply.GrantID != g.ID || !reply.Deadline.Equal(g.ExpiresAt) {
			return ErrUnknown
		}
		entry.Phase = "send_possible"
		if err = save(entry); err != nil {
			return ErrUnknown
		}
		result := Result{GrantID: g.ID, AttemptID: entry.AttemptID, RequestSHA: g.RequestSHA, Outcome: "uncertain"}
		callCtx, cancel := context.WithDeadline(ctx, g.ExpiresAt)
		if callCtx.Err() == nil && r.Now().Before(g.ExpiresAt) {
			var serviceID string
			switch g.Action {
			case "start":
				serviceID, err = r.Provider.StartInstances(callCtx, g.Binding.Resource)
			case "stop":
				serviceID, err = r.Provider.StopInstances(callCtx, g.Binding.Resource)
			default:
				cancel()
				return ErrDenied
			}
			result.OriginalJoined = true
			if err == nil && serviceID != "" && len(serviceID) <= 256 {
				result.Outcome = "accepted"
				result.ServiceRequestID = serviceID
			}
		}
		cancel()
		entry.Phase = "original_returned"
		entry.Result = &result
		if err = save(entry); err != nil {
			return ErrUnknown
		}
		if err = r.settleOriginal(ctx, entry, save); err != nil {
			return err
		}
		if result.Outcome != "accepted" {
			return ErrUnknown
		}
		return nil
	})
}
func (r Runtime) settleOriginal(ctx context.Context, entry Entry, save func(Entry) error) error {
	if entry.Result == nil || entry.Result.GrantID != entry.Grant.ID || entry.Result.AttemptID != entry.AttemptID || entry.Result.RequestSHA != entry.Grant.RequestSHA {
		return ErrDenied
	}
	// The original SDK call has returned. Cancellation of the original handler
	// cannot discard that receipt: only a separately bounded settlement is retried.
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := r.Authority.Settle(settleCtx, entry.Grant.Binding.IntegrationID, entry.Grant.Action, entry.Token, *entry.Result); err != nil {
		return ErrUnknown
	}
	entry.Phase = "settled"
	if err := save(entry); err != nil {
		return fmt.Errorf("%w: original settlement persistence", ErrUnknown)
	}
	if entry.Result.Outcome != "accepted" {
		return ErrUnknown
	}
	return nil
}
