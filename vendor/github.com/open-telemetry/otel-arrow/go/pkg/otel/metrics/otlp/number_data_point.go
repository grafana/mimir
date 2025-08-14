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
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/otlp"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

type (
	NumberDataPointIDs struct {
		ID                int
		ParentID          int
		StartTimeUnixNano int
		TimeUnixNano      int
		IntValue          int
		DoubleValue       int
		Flags             int
	}

	NumberDataPointsStore struct {
		nextID         uint16
		dataPointsByID map[uint16]pmetric.NumberDataPointSlice
	}
)

func NewNumberDataPointsStore() *NumberDataPointsStore {
	return &NumberDataPointsStore{
		dataPointsByID: make(map[uint16]pmetric.NumberDataPointSlice),
	}
}

func (s *NumberDataPointsStore) NumberDataPointsByID(ID uint16) pmetric.NumberDataPointSlice {
	nbps, ok := s.dataPointsByID[ID]
	if !ok {
		return pmetric.NewNumberDataPointSlice()
	}
	return nbps
}

func SchemaToNDPIDs(schema *arrow.Schema) (*NumberDataPointIDs, error) {
	ID, err := arrowutils.FieldIDFromSchema(schema, constants.ID)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	parentID, err := arrowutils.FieldIDFromSchema(schema, constants.ParentID)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	startTimeUnixNano, err := arrowutils.FieldIDFromSchema(schema, constants.StartTimeUnixNano)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	timeUnixNano, err := arrowutils.FieldIDFromSchema(schema, constants.TimeUnixNano)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	intValue, err := arrowutils.FieldIDFromSchema(schema, constants.IntValue)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	doubleValue, err := arrowutils.FieldIDFromSchema(schema, constants.DoubleValue)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	flags, err := arrowutils.FieldIDFromSchema(schema, constants.Flags)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	return &NumberDataPointIDs{
		ID:                ID,
		ParentID:          parentID,
		StartTimeUnixNano: startTimeUnixNano,
		TimeUnixNano:      timeUnixNano,
		IntValue:          intValue,
		DoubleValue:       doubleValue,
		Flags:             flags,
	}, nil
}

// NumberDataPointsStoreFrom converts an Arrow record to a NumberDataPointsStore.
//
// Important Note: This function doesn't take ownership of the record. The
// caller is responsible for releasing it.
func NumberDataPointsStoreFrom(record arrow.Record, exemplarsStore *ExemplarsStore, attrsStore *otlp.AttributesStore[uint32]) (*NumberDataPointsStore, error) {
	store := &NumberDataPointsStore{
		dataPointsByID: make(map[uint16]pmetric.NumberDataPointSlice),
	}

	fieldIDs, err := SchemaToNDPIDs(record.Schema())
	if err != nil {
		return nil, werror.Wrap(err)
	}

	count := int(record.NumRows())
	prevParentID := uint16(0)
	lastID := uint32(0)

	for row := 0; row < count; row++ {
		// Number Data Point ID
		ID, err := arrowutils.NullableU32FromRecord(record, fieldIDs.ID, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		// ParentID = Scope ID
		delta, err := arrowutils.U16FromRecord(record, fieldIDs.ParentID, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		parentID := prevParentID + delta
		prevParentID = parentID

		nbdps, found := store.dataPointsByID[parentID]
		if !found {
			nbdps = pmetric.NewNumberDataPointSlice()
			store.dataPointsByID[parentID] = nbdps
		}

		ndp := nbdps.AppendEmpty()

		startTimeUnixNano, err := arrowutils.TimestampFromRecord(record, fieldIDs.StartTimeUnixNano, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		ndp.SetStartTimestamp(pcommon.Timestamp(startTimeUnixNano))

		timeUnixNano, err := arrowutils.TimestampFromRecord(record, fieldIDs.TimeUnixNano, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		ndp.SetTimestamp(pcommon.Timestamp(timeUnixNano))

		intValue, err := arrowutils.I64OrNilFromRecord(record, fieldIDs.IntValue, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		if intValue != nil {
			ndp.SetIntValue(*intValue)
		} else {
			doubleValue, err := arrowutils.F64OrNilFromRecord(record, fieldIDs.DoubleValue, row)
			if err != nil {
				return nil, werror.Wrap(err)
			}
			if doubleValue != nil {
				ndp.SetDoubleValue(*doubleValue)
			}
		}

		flags, err := arrowutils.U32FromRecord(record, fieldIDs.Flags, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		ndp.SetFlags(pmetric.DataPointFlags(flags))

		if ID != nil {
			lastID += *ID

			exemplars := exemplarsStore.ExemplarsByID(lastID)
			exemplars.MoveAndAppendTo(ndp.Exemplars())

			attrs := attrsStore.AttributesByID(lastID)
			if attrs != nil {
				attrs.CopyTo(ndp.Attributes())
			}
		}
	}

	return store, nil
}
