/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package arrow

// Events are represented as Arrow records.
//
// An event accumulator is used to collect the events across all spans, and
// once the entire trace is processed, the events are being globally sorted and
// written to the Arrow record batch. This process improves the compression
// ratio of the Arrow record batch.

import (
	"errors"
	"math"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	acommon "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

var (
	// EventSchema is the Arrow schema representing events.
	// Related record.
	EventSchema = arrow.NewSchema([]arrow.Field{
		{Name: constants.ID, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.DeltaEncoding), Nullable: true},
		{Name: constants.ParentID, Type: arrow.PrimitiveTypes.Uint16},
		{Name: constants.TimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true},
		{Name: constants.Name, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.DroppedAttributesCount, Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
	}, nil)
)

type (
	// EventBuilder is an Arrow builder for events.
	EventBuilder struct {
		released bool

		builder *builder.RecordBuilderExt

		ib   *builder.Uint32DeltaBuilder // `id` builder
		pib  *builder.Uint16Builder      // `parent_id` builder
		tunb *builder.TimestampBuilder   // `time_unix_nano` builder
		nb   *builder.StringBuilder      // `name` builder
		dacb *builder.Uint32Builder      // `dropped_attributes_count` builder

		accumulator *EventAccumulator
		attrsAccu   *acommon.Attributes32Accumulator

		config *EventConfig
	}

	// Event is an internal representation of an event used by the
	// EventAccumulator.
	Event struct {
		ParentID               uint16
		TimeUnixNano           pcommon.Timestamp
		Name                   string
		Attributes             pcommon.Map
		DroppedAttributesCount uint32
	}

	// EventAccumulator is an accumulator for events that is used to sort events
	// globally in order to improve compression.
	EventAccumulator struct {
		groupCount uint16
		events     []*Event
		sorter     EventSorter
	}

	EventSorter interface {
		Sort(events []*Event) []string
		Encode(parentID uint16, event *Event) uint16
		Reset()
	}

	EventsByNothing          struct{}
	EventsByNameTimeUnixNano struct {
		prevParentID uint16
	}
	EventsByNameParentId struct {
		prevParentID uint16
		prevEvent    *Event
	}
)

func NewEventBuilder(rBuilder *builder.RecordBuilderExt, conf *EventConfig) *EventBuilder {
	b := &EventBuilder{
		released:    false,
		builder:     rBuilder,
		accumulator: NewEventAccumulator(conf.Sorter),
		config:      conf,
	}

	b.init()
	return b
}

func (b *EventBuilder) init() {
	b.ib = b.builder.Uint32DeltaBuilder(constants.ID)
	// As the events are sorted before insertion, the delta between two
	// consecutive ID should always be <=1.
	b.ib.SetMaxDelta(1)
	b.pib = b.builder.Uint16Builder(constants.ParentID)

	b.tunb = b.builder.TimestampBuilder(constants.TimeUnixNano)
	b.nb = b.builder.StringBuilder(constants.Name)
	b.dacb = b.builder.Uint32Builder(constants.DroppedAttributesCount)
}

func (b *EventBuilder) SetAttributesAccumulator(accu *acommon.Attributes32Accumulator) {
	b.attrsAccu = accu
}

func (b *EventBuilder) SchemaID() string {
	return b.builder.SchemaID()
}

func (b *EventBuilder) Schema() *arrow.Schema {
	return b.builder.Schema()
}

func (b *EventBuilder) IsEmpty() bool {
	return b.accumulator.IsEmpty()
}

func (b *EventBuilder) Accumulator() *EventAccumulator {
	return b.accumulator
}

func (b *EventBuilder) Build() (record arrow.Record, err error) {
	schemaNotUpToDateCount := 0

	// Loop until the record is built successfully.
	// Intermediaries steps may be required to update the schema.
	for {
		b.attrsAccu.Reset()
		record, err = b.TryBuild(b.attrsAccu)
		if err != nil {
			if record != nil {
				record.Release()
			}

			switch {
			case errors.Is(err, schema.ErrSchemaNotUpToDate):
				schemaNotUpToDateCount++
				if schemaNotUpToDateCount > 5 {
					panic("Too many consecutive schema updates. This shouldn't happen.")
				}
			default:
				return nil, werror.Wrap(err)
			}
		} else {
			break
		}
	}

	return record, werror.Wrap(err)
}

func (b *EventBuilder) TryBuild(attrsAccu *acommon.Attributes32Accumulator) (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	b.accumulator.sorter.Reset()
	sortingColumns := b.accumulator.sorter.Sort(b.accumulator.events)
	b.builder.AddMetadata(constants.SortingColumns, strings.Join(sortingColumns, ","))

	eventID := uint32(0)

	b.builder.Reserve(len(b.accumulator.events))

	for _, event := range b.accumulator.events {
		if event.Attributes.Len() == 0 {
			b.ib.AppendNull()
		} else {
			b.ib.Append(eventID)

			// Attributes
			err = attrsAccu.Append(eventID, event.Attributes)
			if err != nil {
				return
			}

			eventID++
		}

		b.pib.Append(b.accumulator.sorter.Encode(event.ParentID, event))
		b.tunb.Append(arrow.Timestamp(event.TimeUnixNano.AsTime().UnixNano()))
		b.nb.AppendNonEmpty(event.Name)

		b.dacb.AppendNonZero(event.DroppedAttributesCount)
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		b.init()
	}
	return
}

func (b *EventBuilder) Reset() {
	b.accumulator.Reset()
}

func (b *EventBuilder) PayloadType() *acommon.PayloadType {
	return acommon.PayloadTypes.Event
}

// Release releases the memory allocated by the builder.
func (b *EventBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}

// NewEventAccumulator creates a new EventAccumulator.
func NewEventAccumulator(sorter EventSorter) *EventAccumulator {
	return &EventAccumulator{
		groupCount: 0,
		events:     make([]*Event, 0),
		sorter:     sorter,
	}
}

func (a *EventAccumulator) IsEmpty() bool {
	return len(a.events) == 0
}

// Append appends a slice of events to the accumulator.
func (a *EventAccumulator) Append(spanID uint16, events ptrace.SpanEventSlice) error {
	if a.groupCount == math.MaxUint16 {
		panic("The maximum number of group of events has been reached (max is uint16).")
	}

	if events.Len() == 0 {
		return nil
	}

	for i := 0; i < events.Len(); i++ {
		evt := events.At(i)
		a.events = append(a.events, &Event{
			ParentID:               spanID,
			TimeUnixNano:           evt.Timestamp(),
			Name:                   evt.Name(),
			Attributes:             evt.Attributes(),
			DroppedAttributesCount: evt.DroppedAttributesCount(),
		})
	}

	a.groupCount++

	return nil
}

func (a *EventAccumulator) Reset() {
	a.groupCount = 0
	a.events = a.events[:0]
}

// No sorting
// ==========

func UnsortedEvents() *EventsByNothing {
	return &EventsByNothing{}
}

func (s *EventsByNothing) Sort(_ []*Event) []string {
	return []string{}
}

func (s *EventsByNothing) Encode(parentID uint16, _ *Event) uint16 {
	return parentID
}

func (s *EventsByNothing) Reset() {}

// Sorts events by name and time.
// ==============================

func SortEventsByNameTimeUnixNano() *EventsByNameTimeUnixNano {
	return &EventsByNameTimeUnixNano{}
}

func (s *EventsByNameTimeUnixNano) Sort(events []*Event) []string {
	sort.Slice(events, func(i, j int) bool {
		if events[i].Name == events[j].Name {
			return events[i].TimeUnixNano < events[j].TimeUnixNano
		} else {
			return events[i].Name < events[j].Name
		}
	})
	return []string{constants.Name, constants.TimeUnixNano}
}

func (s *EventsByNameTimeUnixNano) Encode(parentID uint16, _ *Event) uint16 {
	delta := parentID - s.prevParentID
	s.prevParentID = parentID
	return delta
}

func (s *EventsByNameTimeUnixNano) Reset() {
	s.prevParentID = 0
}

// Sorts events by name and parentID.
// ==================================

func SortEventsByNameParentId() *EventsByNameParentId {
	return &EventsByNameParentId{}
}

func (s *EventsByNameParentId) Sort(events []*Event) []string {
	sort.Slice(events, func(i, j int) bool {
		if events[i].Name == events[j].Name {
			return events[i].ParentID < events[j].ParentID
		} else {
			return events[i].Name < events[j].Name
		}
	})
	return []string{constants.Name, constants.ParentID}
}

func (s *EventsByNameParentId) Encode(parentID uint16, event *Event) uint16 {
	if s.prevEvent == nil {
		s.prevEvent = event
		s.prevParentID = parentID
		return parentID
	}

	if s.IsSameGroup(event) {
		delta := parentID - s.prevParentID
		s.prevParentID = parentID
		return delta
	} else {
		s.prevEvent = event
		s.prevParentID = parentID
		return parentID
	}
}

func (s *EventsByNameParentId) Reset() {
	s.prevParentID = 0
	s.prevEvent = nil
}

func (s *EventsByNameParentId) IsSameGroup(event *Event) bool {
	if s.prevEvent == nil {
		return false
	}

	return s.prevEvent.Name == event.Name
}
