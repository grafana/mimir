package limiter

import (
	"sync"

	"github.com/platinummonkey/go-concurrency-limits/core"
)

// DelegateListener wraps the wrapped Limiter's Listener to simply delegate as a wrapper.
type DelegateListener struct {
	delegateListener core.Listener
	c                *sync.Cond
}

// NewDelegateListener creates a new wrapped listener.
func NewDelegateListener(delegateListener core.Listener) *DelegateListener {
	mu := sync.Mutex{}
	return &DelegateListener{
		delegateListener: delegateListener,
		c:                sync.NewCond(&mu),
	}
}

func (l *DelegateListener) unblock() {
	l.c.Broadcast()
}

// OnDropped is called to indicate the request failed and was dropped due to being rejected by an external limit or
// hitting a timeout.  Loss based Limit implementations will likely do an aggressive reducing in limit when this
// happens.
func (l *DelegateListener) OnDropped() {
	l.delegateListener.OnDropped()
	l.unblock()
}

// OnIgnore is called to indicate the operation failed before any meaningful RTT measurement could be made and
// should be ignored to not introduce an artificially low RTT.
func (l *DelegateListener) OnIgnore() {
	l.delegateListener.OnIgnore()
	l.unblock()
}

// OnSuccess is called as a notification that the operation succeeded and internally measured latency should be
// used as an RTT sample.
func (l *DelegateListener) OnSuccess() {
	l.delegateListener.OnSuccess()
	l.unblock()
}
