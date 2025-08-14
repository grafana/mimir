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
	"bytes"

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
	// SpanLinkIDs is a struct containing the Arrow field IDs for the Link
	// struct.
	SpanLinkIDs struct {
		ID                     int
		ParentID               int
		TraceID                int
		SpanID                 int
		TraceState             int
		DroppedAttributesCount int
	}

	// SpanLinksStore contains a set of links indexed by span ID.
	// This store is initialized from an arrow.Record representing all the
	// links for a batch of spans.
	SpanLinksStore struct {
		nextID    uint16
		linksByID map[uint16][]*ptrace.SpanLink
	}

	LinkParentIdDecoder struct {
		prevParentID uint16
		prevTraceID  []byte
	}
)

// NewSpanLinksStore creates a new SpanLinksStore.
func NewSpanLinksStore() *SpanLinksStore {
	return &SpanLinksStore{
		linksByID: make(map[uint16][]*ptrace.SpanLink),
	}
}

// LinksByID returns the links for the given ID.
func (s *SpanLinksStore) LinksByID(ID uint16) []*ptrace.SpanLink {
	if links, ok := s.linksByID[ID]; ok {
		return links
	}
	return nil
}

// SpanLinksStoreFrom creates an SpanLinksStore from an arrow.Record.
//
// Important Note: This function doesn't take ownership of the record. The
// caller is responsible for releasing it.
func SpanLinksStoreFrom(
	record arrow.Record,
	attrsStore *otlp.AttributesStore[uint32],
	conf *tarrow.LinkConfig,
) (*SpanLinksStore, error) {
	store := &SpanLinksStore{
		linksByID: make(map[uint16][]*ptrace.SpanLink),
	}

	spanLinkIDs, err := SchemaToSpanLinkIDs(record.Schema())
	if err != nil {
		return nil, werror.Wrap(err)
	}

	linksCount := int(record.NumRows())
	// ToDo Make this decoding dependent on the encoding type column metadata.
	parentIdDecoder := NewLinkParentIdDecoder()

	// Read all link fields from the record and reconstruct the link lists
	// by ID.
	for row := 0; row < linksCount; row++ {
		ID, err := arrowutils.NullableU32FromRecord(record, spanLinkIDs.ID, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		traceID, err := arrowutils.FixedSizeBinaryFieldByIDFromRecord(record, spanLinkIDs.TraceID, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		parentID, err := arrowutils.U16FromRecord(record, spanLinkIDs.ParentID, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		parentID = parentIdDecoder.Decode(parentID, traceID)

		spanID, err := arrowutils.FixedSizeBinaryFieldByIDFromRecord(record, spanLinkIDs.SpanID, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		traceState, err := arrowutils.StringFromRecord(record, spanLinkIDs.TraceState, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		dac, err := arrowutils.U32FromRecord(record, spanLinkIDs.DroppedAttributesCount, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		link := ptrace.NewSpanLink()

		var tid pcommon.TraceID
		var sid pcommon.SpanID

		copy(tid[:], traceID)
		copy(sid[:], spanID)

		link.SetTraceID(tid)
		link.SetSpanID(sid)
		link.TraceState().FromRaw(traceState)

		if ID != nil {
			attrs := attrsStore.AttributesByDeltaID(*ID)
			if attrs != nil {
				attrs.CopyTo(link.Attributes())
			}
		}

		link.SetDroppedAttributesCount(dac)
		store.linksByID[parentID] = append(store.linksByID[parentID], &link)
	}

	return store, nil
}

// SchemaToSpanLinkIDs pre-computes the field IDs for the links record.
func SchemaToSpanLinkIDs(schema *arrow.Schema) (*SpanLinkIDs, error) {
	ID, err := arrowutils.FieldIDFromSchema(schema, constants.ID)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	ParentID, err := arrowutils.FieldIDFromSchema(schema, constants.ParentID)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	traceID, err := arrowutils.FieldIDFromSchema(schema, constants.TraceId)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	spanID, err := arrowutils.FieldIDFromSchema(schema, constants.SpanId)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	traceState, err := arrowutils.FieldIDFromSchema(schema, constants.TraceState)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	dac, err := arrowutils.FieldIDFromSchema(schema, constants.DroppedAttributesCount)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	return &SpanLinkIDs{
		ParentID:               ParentID,
		TraceID:                traceID,
		SpanID:                 spanID,
		TraceState:             traceState,
		ID:                     ID,
		DroppedAttributesCount: dac,
	}, nil
}

func NewLinkParentIdDecoder() *LinkParentIdDecoder {
	return &LinkParentIdDecoder{
		prevParentID: 0,
	}
}

func (d *LinkParentIdDecoder) Decode(value uint16, traceID []byte) uint16 {
	if bytes.Equal(d.prevTraceID, traceID) {
		parentID := d.prevParentID + value
		d.prevParentID = parentID
		return parentID
	} else {
		d.prevTraceID = traceID
		d.prevParentID = value
		return value
	}
}
