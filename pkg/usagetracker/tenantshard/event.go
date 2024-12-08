// SPDX-License-Identifier: AGPL-3.0-only

package tenantshard

import (
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
)

func Track(refs []uint64, value clock.Minutes, series *atomic.Uint64, limit uint64, resp chan TrackResponse) Event {
	return Event{
		typ:           track,
		series:        series,
		limit:         limit,
		refs:          refs,
		timestamp:     value,
		trackResponse: resp,
	}
}

func Create(refs []uint64, ts clock.Minutes, series *atomic.Uint64, done chan struct{}) Event {
	return Event{
		typ:       create,
		series:    series,
		refs:      refs,
		timestamp: ts,
		done:      done,
	}
}

func Load(refs []RefTimestamp, series *atomic.Uint64, done chan struct{}) Event {
	return Event{
		typ:      load,
		series:   series,
		loadRefs: refs,
		done:     done,
	}
}

func Clone(cloner chan<- func(LengthCallback, IteratorCallback)) Event {
	return Event{
		typ:    clone,
		cloner: cloner,
	}
}

func Cleanup(watermark clock.Minutes, series *atomic.Uint64, done chan struct{}) Event {
	return Event{
		typ:       cleanup,
		series:    series,
		watermark: watermark,
		done:      done,
	}
}

func Shutdown() Event {
	return Event{
		typ: shutdown,
	}
}

type RefTimestamp struct {
	Ref       uint64
	Timestamp clock.Minutes
}

type eventType uint8

const (
	track eventType = iota
	create
	load
	clone
	cleanup
	shutdown
)

type Event struct {
	typ eventType

	// Series is sent Track, Load and Cleanup.
	series *atomic.Uint64

	// Limit is sent in Track event, it could be math.MaxUint64.
	limit uint64

	// Refs is sent in Track and Create events.
	refs []uint64

	// Timestamp is sent in Track and Create events.
	timestamp clock.Minutes

	// LoadRefs is sent in Load event.
	loadRefs []RefTimestamp

	// Watermark is sent in Cleanup event.
	watermark clock.Minutes

	// TrackResponse is sent in Track event.
	trackResponse chan TrackResponse

	// Cloner is sent in Clone event.
	// The received function will call LengthCallback first, and then IteratorCallback for each one of the series.
	cloner chan<- func(LengthCallback, IteratorCallback)

	// Done is sent in Cleanup event.
	done chan struct{}
}

type TrackResponse struct {
	Created  []uint64
	Rejected []uint64
}

// Events returns the event channel for the map where the events can be sent.
func (m *Map) Events() chan<- Event { return m.events }

func (m *Map) worker() {
	for ev := range m.events {
		switch ev.typ {
		case track:
			var resp TrackResponse
			for _, ref := range ev.refs {
				if created, rejected := m.put(ref, ev.timestamp, ev.series, ev.limit, true); created {
					resp.Created = append(resp.Created, ref)
				} else if rejected {
					resp.Rejected = append(resp.Rejected, ref)
				}
			}
			ev.trackResponse <- resp
			// TODO: we could use this idle time to resize even larger.

		case create:
			for _, ref := range ev.refs {
				m.put(ref, ev.timestamp, ev.series, 0, false)
			}
			ev.done <- struct{}{}

		case load:
			for _, entry := range ev.loadRefs {
				m.put(entry.Ref, entry.Timestamp, ev.series, 0, false)
			}
			ev.done <- struct{}{}

		case clone:
			ev.cloner <- m.cloner()

		case cleanup:
			m.cleanup(ev.watermark, ev.series)
			if m.dead > m.limit/2 {
				m.rehash(m.nextSize())
			}
			ev.done <- struct{}{}

		case shutdown:
			return
		}
	}
}
