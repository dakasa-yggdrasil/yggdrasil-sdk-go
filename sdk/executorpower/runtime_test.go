package executorpower

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func powerFixture(t *testing.T) (Runtime, Grant, *testAuthority, *testProvider) {
	t.Helper()
	now := time.Now().UTC()
	b := Binding{Mode: Kind, Enabled: true, Resource: Resource{"153828470928", "us-east-1", "i-0cb486ae31a45d15e", []string{"vol-0f1d6076c1ce64255"}}, IntegrationID: "11111111-1111-4111-8111-111111111111", IntegrationChecksum: strings.Repeat("a", 64), TypeID: "22222222-2222-4222-8222-222222222222", TypeChecksum: strings.Repeat("b", 64), AdapterPrincipal: "adapter", ControllerPrincipal: "controller", Operators: []string{"33333333-3333-4333-8333-333333333333"}, AdoptionWorkflow: Workflow{"lab", "adopt"}, GrantSeconds: 30}
	g := Grant{Kind: Kind, ID: "44444444-4444-4444-8444-444444444444", AdoptionID: "55555555-5555-4555-8555-555555555555", Action: "start", CommandID: "original", Epoch: 1, State: "issued", IssuedAt: now, ExpiresAt: now.Add(30 * time.Second), Binding: b, Scope: Scope{Namespace: "lab", Environment: "lab", Domain: "ai", Dimension: "executor", PolicyID: b.IntegrationID, PolicyChecksum: strings.Repeat("c", 64), WorkflowID: b.TypeID}}
	g.RequestSHA = GrantSHA(g)
	if err := ValidateGrant(g); err != nil {
		t.Fatal(err)
	}
	j, err := NewFileJournal(filepath.Join(t.TempDir(), "journal"))
	if err != nil {
		t.Fatal(err)
	}
	a := &testAuthority{g: g}
	p := &testProvider{now: now}
	return Runtime{true, b, a, p, j, func() time.Time { return now }}, g, a, p
}

type testAuthority struct {
	g                     Grant
	redeems, settles      int
	lostClaim, failSettle bool
	result                Result
}

func (a *testAuthority) Redeem(_ context.Context, _ string, c Claim) (ClaimReply, error) {
	a.redeems++
	if a.lostClaim {
		return ClaimReply{}, errors.New("lost original redemption")
	}
	return ClaimReply{Mode: "execute_once", GrantID: c.GrantID, Deadline: a.g.ExpiresAt}, nil
}
func (a *testAuthority) Settle(ctx context.Context, _, _, _ string, r Result) error {
	a.settles++
	a.result = r
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if a.failSettle {
		return errors.New("lost settlement")
	}
	return nil
}

type testProvider struct {
	now             time.Time
	observes, calls int
	fail            bool
	cancel          context.CancelFunc
}

func (p *testProvider) Observe(_ context.Context, r Resource) (Observation, error) {
	p.observes++
	return Observation{Resource: r, State: "stopped", ObservedAt: p.now}, nil
}
func (p *testProvider) StartInstances(context.Context, Resource) (string, error) {
	p.calls++
	if p.cancel != nil {
		p.cancel()
	}
	if p.fail {
		return "", errors.New("lost EC2 return")
	}
	return "original-request", nil
}
func (p *testProvider) StopInstances(ctx context.Context, r Resource) (string, error) {
	return p.StartInstances(ctx, r)
}

func TestRuntimeOriginalSettlementRetryNeverResends(t *testing.T) {
	r, g, a, p := powerFixture(t)
	a.failSettle = true
	if err := r.Execute(context.Background(), g); !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
	original := a.result
	a.failSettle = false
	if err := r.Execute(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	if a.result != original || p.calls != 1 || a.redeems != 1 || a.settles != 2 || p.observes != 1 {
		t.Fatal("original custody lost", a, p)
	}
	if err := r.Execute(context.Background(), g); err != nil || p.calls != 1 || a.settles != 2 {
		t.Fatal(err, a, p)
	}
}
func TestRuntimeLostRedeemHoldsWithoutProviderCall(t *testing.T) {
	r, g, a, p := powerFixture(t)
	a.lostClaim = true
	for i := 0; i < 2; i++ {
		if err := r.Execute(context.Background(), g); !errors.Is(err, ErrUnknown) {
			t.Fatal(err)
		}
	}
	if a.redeems != 1 || p.calls != 0 || a.settles != 0 {
		t.Fatal(a, p)
	}
}
func TestRuntimeUncertainStaysHoldAfterSettledRetry(t *testing.T) {
	r, g, a, p := powerFixture(t)
	p.fail = true
	for i := 0; i < 3; i++ {
		if err := r.Execute(context.Background(), g); !errors.Is(err, ErrUnknown) {
			t.Fatal(err)
		}
	}
	if a.redeems != 1 || p.calls != 1 || a.settles != 1 || a.result.Outcome != "uncertain" || !a.result.OriginalJoined {
		t.Fatal(a, p)
	}
}
func TestRuntimeCancellationRetainsOriginalSettlement(t *testing.T) {
	r, g, a, p := powerFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.cancel = cancel
	if err := r.Execute(ctx, g); err != nil {
		t.Fatal(err)
	}
	if a.result.ServiceRequestID != "original-request" || a.settles != 1 || !a.result.OriginalJoined {
		t.Fatal(a, p)
	}
}
func TestJournalMissingOriginalCannotReauthorize(t *testing.T) {
	r, g, a, p := powerFixture(t)
	if err := r.Execute(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	j := r.Journal.(*FileJournal)
	if err := os.Remove(filepath.Join(j.root, g.ID+".json")); err != nil {
		t.Fatal(err)
	}
	if err := r.Execute(context.Background(), g); !errors.Is(err, ErrUnknown) {
		t.Fatal("missing original authorized replay", err)
	}
	if a.redeems != 1 || p.calls != 1 {
		t.Fatal("missing journal resent", a, p)
	}
}

func TestJournalRejectsInconsistentOriginalReceipts(t *testing.T) {
	cases := map[string]func(*Entry){
		"phase":                   func(e *Entry) { e.Phase = "invented" },
		"missing_result":          func(e *Entry) { e.Result = nil },
		"grant_identity":          func(e *Entry) { e.Result.GrantID = "55555555-5555-4555-8555-555555555555" },
		"attempt_identity":        func(e *Entry) { e.Result.AttemptID = "55555555-5555-4555-8555-555555555555" },
		"request_identity":        func(e *Entry) { e.Result.RequestSHA = strings.Repeat("d", 64) },
		"unjoined":                func(e *Entry) { e.Result.OriginalJoined = false },
		"missing_service_receipt": func(e *Entry) { e.Result.ServiceRequestID = "" },
		"uncertain_with_receipt":  func(e *Entry) { e.Result.Outcome = "uncertain" },
		"nonce":                   func(e *Entry) { e.Token = strings.Repeat("z", 64) },
		"pre_send_result":         func(e *Entry) { e.Phase = "send_possible" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r, g, a, p := powerFixture(t)
			if err := r.Execute(context.Background(), g); err != nil {
				t.Fatal(err)
			}
			j := r.Journal.(*FileJournal)
			path := filepath.Join(j.root, g.ID+".json")
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var e Entry
			if err = json.Unmarshal(b, &e); err != nil {
				t.Fatal(err)
			}
			mutate(&e)
			b, err = json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, b, 0600); err != nil {
				t.Fatal(err)
			}
			if err = r.Execute(context.Background(), g); !errors.Is(err, ErrUnknown) {
				t.Fatal("corrupt original accepted", err)
			}
			if p.calls != 1 || a.redeems != 1 || a.settles != 1 {
				t.Fatal("corruption reauthorized original", a, p)
			}
		})
	}
}
