// SPDX-License-Identifier: AGPL-3.0-only

package pool

import "go.uber.org/atomic"

type TrackedPool struct {
	Parent  Interface
	Balance atomic.Int64
	Gets    atomic.Int64
}

func (p *TrackedPool) Get() any {
	p.Balance.Inc()
	p.Gets.Inc()
	return p.Parent.Get()
}

func (p *TrackedPool) Put(x any) {
	p.Balance.Dec()
	p.Parent.Put(x)
}

func (p *TrackedPool) Reset() {
	p.Balance.Store(0)
	p.Gets.Store(0)
}
