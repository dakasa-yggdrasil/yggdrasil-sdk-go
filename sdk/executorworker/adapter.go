package executorworker

import (
	"context"
	"encoding/json"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/adapter"
	"github.com/dakasa-yggdrasil/yggdrasil-sdk-go/rpc"
)

type executeRequest struct {
	Operation   string          `json:"operation,omitempty"`
	Capability  string          `json:"capability,omitempty"`
	Idempotency string          `json:"idempotency,omitempty"`
	InstanceID  string          `json:"instance_id,omitempty"`
	Input       json.RawMessage `json:"input"`
}

// Handler implements the existing execute endpoint without starting any listener.
func (w *Worker) Handler(ctx context.Context, d rpc.Delivery) ([]byte, string, error) {
	var e executeRequest
	if len(d.Body) > 16384 || decode(d.Body, &e) != nil {
		return nil, "", ErrHold
	}
	op := e.Operation
	if op == "" {
		op = e.Capability
	}
	if e.Operation != "" && e.Capability != "" && e.Operation != e.Capability {
		return nil, "", ErrHold
	}
	var r Request
	if decode(e.Input, &r) != nil {
		return nil, "", ErrHold
	}
	s, err := w.Apply(ctx, op, r)
	if err != nil {
		return nil, "", err
	}
	b, err := json.Marshal(s)
	return b, "application/json", err
}
func (w *Worker) Register(a *adapter.Adapter) { a.Register("execute", w.Handler) }
