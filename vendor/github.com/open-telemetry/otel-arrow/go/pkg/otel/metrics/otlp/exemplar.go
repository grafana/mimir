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
	"go.opentelemetry.io/collector/pdata/pmetric"

	arrowutils "github.com/open-telemetry/otel-arrow/go/pkg/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/otlp"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

const (
	UndefinedTypeValue = 0
	IntValue           = 1
	DoubleValue        = 2
)

type (
	// ExemplarIDs contains the field IDs for the exemplar struct.
	ExemplarIDs struct {
		ID           int
		ParentID     int
		TimeUnixNano int
		SpanID       int
		TraceID      int
		IntValue     int
		DoubleValue  int
	}

	ExemplarsStore struct {
		nextID         uint32
		exemplarsByIDs map[uint32]pmetric.ExemplarSlice
	}

	ExemplarParentIdDecoder struct {
		prevParentID    uint32
		prevType        int
		prevIntValue    *int64
		prevDoubleValue *float64
	}
)

func NewExemplarsStore() *ExemplarsStore {
	return &ExemplarsStore{
		exemplarsByIDs: make(map[uint32]pmetric.ExemplarSlice),
	}
}

func SchemaToExemplarIDs(schema *arrow.Schema) (*ExemplarIDs, error) {
	ID, err := arrowutils.FieldIDFromSchema(schema, constants.ID)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	ParentID, err := arrowutils.FieldIDFromSchema(schema, constants.ParentID)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	timeUnixNanoId, _ := arrowutils.FieldIDFromSchema(schema, constants.TimeUnixNano)
	spanIdId, _ := arrowutils.FieldIDFromSchema(schema, constants.SpanId)
	traceIdId, _ := arrowutils.FieldIDFromSchema(schema, constants.TraceId)
	intValueId, _ := arrowutils.FieldIDFromSchema(schema, constants.IntValue)
	doubleValueId, _ := arrowutils.FieldIDFromSchema(schema, constants.DoubleValue)

	return &ExemplarIDs{
		ID:           ID,
		ParentID:     ParentID,
		TimeUnixNano: timeUnixNanoId,
		SpanID:       spanIdId,
		TraceID:      traceIdId,
		IntValue:     intValueId,
		DoubleValue:  doubleValueId,
	}, nil
}

func (s *ExemplarsStore) ExemplarsByID(ID uint32) pmetric.ExemplarSlice {
	exemplars, found := s.exemplarsByIDs[ID]
	if !found {
		return pmetric.NewExemplarSlice()
	}
	return exemplars
}

// ExemplarsStoreFrom creates an ExemplarsStore from an arrow.Record.
//
// Important Note: This function doesn't take ownership of the record. The
// caller is responsible for releasing it.
func ExemplarsStoreFrom(
	record arrow.Record,
	attrsStore *otlp.AttributesStore[uint32],
) (*ExemplarsStore, error) {
	store := &ExemplarsStore{
		exemplarsByIDs: make(map[uint32]pmetric.ExemplarSlice),
	}
	// ToDo Make this decoding dependent on the encoding type column metadata.
	parentIdDecoder := NewExemplarParentIdDecoder()

	exemplarIDs, err := SchemaToExemplarIDs(record.Schema())
	if err != nil {
		return nil, werror.Wrap(err)
	}

	rows := int(record.NumRows())

	// Read all exemplar fields from the record and reconstruct the exemplar
	// slices by ID.
	for row := 0; row < rows; row++ {
		ID, err := arrowutils.NullableU32FromRecord(record, exemplarIDs.ID, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		intValue, err := arrowutils.I64OrNilFromRecord(record, exemplarIDs.IntValue, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		doubleValue, err := arrowutils.F64OrNilFromRecord(record, exemplarIDs.DoubleValue, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		parentID, err := arrowutils.U32FromRecord(record, exemplarIDs.ParentID, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		parentID = parentIdDecoder.Decode(parentID, intValue, doubleValue)
		exemplars, found := store.exemplarsByIDs[parentID]
		if !found {
			exemplars = pmetric.NewExemplarSlice()
			store.exemplarsByIDs[parentID] = exemplars
		}
		exemplar := exemplars.AppendEmpty()

		timeUnixNano, err := arrowutils.TimestampFromRecord(record, exemplarIDs.TimeUnixNano, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		exemplar.SetTimestamp(pcommon.Timestamp(timeUnixNano))

		spanId, err := arrowutils.FixedSizeBinaryFromRecord(record, exemplarIDs.SpanID, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		if len(spanId) == 8 {
			var sid pcommon.SpanID

			copy(sid[:], spanId)
			exemplar.SetSpanID(sid)
		} else {
			return nil, werror.WrapWithContext(common.ErrInvalidSpanIDLength, map[string]interface{}{"spanID": spanId})
		}

		traceID, err := arrowutils.FixedSizeBinaryFromRecord(record, exemplarIDs.TraceID, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		if len(traceID) == 16 {
			var tid pcommon.TraceID

			copy(tid[:], traceID)
			exemplar.SetTraceID(tid)
		} else {
			return nil, werror.WrapWithContext(common.ErrInvalidTraceIDLength, map[string]interface{}{"traceID": traceID})
		}

		if intValue != nil {
			exemplar.SetIntValue(*intValue)
		}
		if doubleValue != nil {
			exemplar.SetDoubleValue(*doubleValue)
		}

		if ID != nil {
			attrs := attrsStore.AttributesByDeltaID(*ID)
			if attrs != nil {
				attrs.CopyTo(exemplar.FilteredAttributes())
			}
		}
	}

	return store, nil
}

func NewExemplarParentIdDecoder() *ExemplarParentIdDecoder {
	return &ExemplarParentIdDecoder{
		prevParentID:    0,
		prevType:        UndefinedTypeValue,
		prevIntValue:    nil,
		prevDoubleValue: nil,
	}
}

func (d *ExemplarParentIdDecoder) Decode(value uint32, intValue *int64, doubleValue *float64) uint32 {
	if intValue != nil {
		if d.prevType == IntValue && d.prevIntValue != nil && *d.prevIntValue == *intValue {
			parentID := d.prevParentID + value
			d.prevParentID = parentID
			return parentID
		} else {
			d.prevType = IntValue
			d.prevIntValue = intValue
			d.prevDoubleValue = nil
			d.prevParentID = value
			return value
		}
	}

	if doubleValue != nil {
		if d.prevType == DoubleValue && d.prevDoubleValue != nil && *d.prevDoubleValue == *doubleValue {
			parentID := d.prevParentID + value
			d.prevParentID = parentID
			return parentID
		} else {
			d.prevType = DoubleValue
			d.prevIntValue = nil
			d.prevDoubleValue = doubleValue
			d.prevParentID = value
			return value
		}
	}

	parentID := d.prevParentID + value
	d.prevParentID = parentID
	return parentID
}
