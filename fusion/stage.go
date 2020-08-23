package fusion

import (
	"context"
)

type ProcStage struct {
	// Proc should process a message and return the processed message.
	Proc func(msg Msg) *Msg

	inCh  <-chan Msg
	outCh chan<- Msg
}

func (ps *ProcStage) Run(ctx context.Context, inCh <-chan Msg, outCh chan<- Msg) error {
	ps.inCh = inCh
	ps.outCh = outCh

	return nil
}
