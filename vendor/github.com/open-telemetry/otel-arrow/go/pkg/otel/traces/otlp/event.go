// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otlp

import (
	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	arrowutils "github.com/open-telemetry/otel-arrow/go/pkg/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/otlp"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	tarrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/traces/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

type (
	// SpanEventIDs is a struct containing the Arrow field IDs for the Event
	// struct.
	SpanEventIDs struct {
		ParentID               int // Span ID
		TimeUnixNano           int
		Name                   int
		ID                     int // Event ID (used by attributes of the event)
		DroppedAttributesCount int
	}

	// SpanEventsStore contains a set of events indexed by span ID.
	// This store is initialized from an arrow.Record representing all the
	// events for a batch of spans.
	SpanEventsStore struct {
		nextID     uint16
		eventsByID map[uint16][]*ptrace.SpanEvent
		config     *tarrow.EventConfig
	}

	EventParentIdDecoder struct {
		prevParentID uint16
		prevName     string
	}
)

// NewSpanEventsStore creates a new SpanEventsStore.
func NewSpanEventsStore(config *tarrow.EventConfig) *SpanEventsStore {
	return &SpanEventsStore{
		eventsByID: make(map[uint16][]*ptrace.SpanEvent),
		config:     config,
	}
}

// EventsByID returns the events for the given span ID.
func (s *SpanEventsStore) EventsByID(ID uint16) []*ptrace.SpanEvent {
	if events, ok := s.eventsByID[ID]; ok {
		return events
	}
	return nil
}

// SpanEventsStoreFrom creates an SpanEventsStore from an arrow.Record.
//
// Important Note: This function doesn't take ownership of the record. The
// caller is responsible for releasing it.
func SpanEventsStoreFrom(
	record arrow.Record,
	attrsStore *otlp.AttributesStore[uint32],
	conf *tarrow.EventConfig,
) (*SpanEventsStore, error) {
	store := &SpanEventsStore{
		eventsByID: make(map[uint16][]*ptrace.SpanEvent),
	}
	// ToDo Make this decoding dependent on the encoding type column metadata.
	parentIdDecoder := NewEventParentIdDecoder()

	spanEventIDs, err := SchemaToSpanEventIDs(record.Schema())
	if err != nil {
		return nil, werror.Wrap(err)
	}

	eventsCount := int(record.NumRows())

	// Read all event fields from the record and reconstruct the event lists
	// by ID.
	for row := 0; row < eventsCount; row++ {
		ID, err := arrowutils.NullableU32FromRecord(record, spanEventIDs.ID, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		name, err := arrowutils.StringFromRecord(record, spanEventIDs.Name, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		parentID, err := arrowutils.U16FromRecord(record, spanEventIDs.ParentID, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		parentID = parentIdDecoder.Decode(parentID, name)

		timeUnixNano, err := arrowutils.TimestampFromRecord(record, spanEventIDs.TimeUnixNano, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		dac, err := arrowutils.U32FromRecord(record, spanEventIDs.DroppedAttributesCount, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		event := ptrace.NewSpanEvent()
		event.SetTimestamp(pcommon.Timestamp(timeUnixNano))
		event.SetName(name)

		if ID != nil {
			attrs := attrsStore.AttributesByDeltaID(*ID)
			if attrs != nil {
				attrs.CopyTo(event.Attributes())
			}
		}

		event.SetDroppedAttributesCount(dac)
		store.eventsByID[parentID] = append(store.eventsByID[parentID], &event)
	}

	return store, nil
}

// SchemaToSpanEventIDs pre-computes the field IDs for the events record.
func SchemaToSpanEventIDs(schema *arrow.Schema) (*SpanEventIDs, error) {
	ID, err := arrowutils.FieldIDFromSchema(schema, constants.ID)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	ParentID, err := arrowutils.FieldIDFromSchema(schema, constants.ParentID)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	timeUnixNano, err := arrowutils.FieldIDFromSchema(schema, constants.TimeUnixNano)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	name, err := arrowutils.FieldIDFromSchema(schema, constants.Name)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	dac, err := arrowutils.FieldIDFromSchema(schema, constants.DroppedAttributesCount)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	return &SpanEventIDs{
		ID:                     ID,
		ParentID:               ParentID,
		TimeUnixNano:           timeUnixNano,
		Name:                   name,
		DroppedAttributesCount: dac,
	}, nil
}

func NewEventParentIdDecoder() *EventParentIdDecoder {
	return &EventParentIdDecoder{
		prevParentID: 0,
		prevName:     "",
	}
}

func (d *EventParentIdDecoder) Decode(value uint16, name string) uint16 {
	if d.prevName == name {
		parentID := d.prevParentID + value
		d.prevParentID = parentID
		return parentID
	} else {
		d.prevName = name
		d.prevParentID = value
		return value
	}
}
