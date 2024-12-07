// SPDX-License-Identifier: AGPL-3.0-only

package tenantshard

import (
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
)

type EventType uint8

const (
	TrackSeries EventType = iota
	Load
	Clone
	Cleanup
	Shutdown
)

type Event struct {
	Type EventType

	// Series is sent in all events.
	Series *atomic.Uint64

	// Limit is sent in TrackSeries event, it could be math.MaxUint64.
	Limit uint64

	// Refs is sent in TrackSeries event.
	Refs []uint64

	// Value is sent in TrackSeries event.
	Value clock.Minutes

	// LoadRefs is sent in Load event.
	// TODO: we could optimize allocations making this a slice of ref/ts pairs.
	LoadRefs map[uint64]clock.Minutes

	// Watermark is sent in Cleanup event.
	Watermark clock.Minutes

	// Created is sent in TrackSeries event.
	Created chan []uint64
	// Rejected is sent in TrackSeries event.
	Rejected chan []uint64

	// Cloner is sent in Clone event.
	// The received function will call LengthCallback first, and then IteratorCallback for each one of the series.
	Cloner chan<- func(LengthCallback, IteratorCallback)

	// Done is sent in Cleanup event.
	Done chan struct{}
}

// Events returns the event channel for the map where the events can be sent.
func (m *Map) Events() chan<- Event { return m.events }

func (m *Map) worker() {
	for ev := range m.events {
		switch ev.Type {
		case TrackSeries:
			var createdRefs []uint64
			var rejectedRefs []uint64
			for _, ref := range ev.Refs {
				if created, rejected := m.put(ref, ev.Value, ev.Series, ev.Limit, true); created {
					createdRefs = append(createdRefs, ref)
				} else if rejected {
					rejectedRefs = append(rejectedRefs, ref)
				}
			}
			ev.Created <- createdRefs
			ev.Rejected <- rejectedRefs
			// TODO: we could use this idle time to resize even larger.

		case Load:
			for ref, v := range ev.LoadRefs {
				m.put(ref, v, ev.Series, 0, false)
			}
			ev.Done <- struct{}{}

		case Clone:
			ev.Cloner <- m.cloner()

		case Cleanup:
			m.cleanup(ev.Watermark, ev.Series)
			if m.dead > m.limit/2 {
				m.rehash(m.nextSize())
			}
			ev.Done <- struct{}{}

		case Shutdown:
			return
		}
	}
}
