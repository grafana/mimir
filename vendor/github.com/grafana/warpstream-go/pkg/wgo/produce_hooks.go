package wgo

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

// produceHooks holds the produce-record lifecycle hooks driven on this client's
// own produce path. franz-go fires these from its producer state machine, which
// this client bypasses (it produces via Broker.Request), so they would never
// fire for a wgo produce unless invoked here.
//
// The slices are set once by newProduceHooks and never mutated afterwards, so
// firing from the caller and background flush goroutines needs no locking.
type produceHooks struct {
	buffered   []kgo.HookProduceRecordBuffered
	unbuffered []kgo.HookProduceRecordUnbuffered
}

// newProduceHooks selects the produce-record hooks from the caller-supplied
// hooks. A single hook may implement either or both interfaces.
func newProduceHooks(hooks []kgo.Hook) produceHooks {
	var ph produceHooks
	for _, h := range hooks {
		if hb, ok := h.(kgo.HookProduceRecordBuffered); ok {
			ph.buffered = append(ph.buffered, hb)
		}
		if hu, ok := h.(kgo.HookProduceRecordUnbuffered); ok {
			ph.unbuffered = append(ph.unbuffered, hu)
		}
	}
	return ph
}

// enabled reports whether any produce-record hook was supplied.
func (p produceHooks) enabled() bool {
	return len(p.buffered) > 0 || len(p.unbuffered) > 0
}

func (p produceHooks) fireBuffered(record *kgo.Record) {
	for _, h := range p.buffered {
		h.OnProduceRecordBuffered(record)
	}
}

func (p produceHooks) fireUnbuffered(record *kgo.Record, err error) {
	for _, h := range p.unbuffered {
		h.OnProduceRecordUnbuffered(record, err)
	}
}
