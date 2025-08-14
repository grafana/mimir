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
	SummaryDataPointIDs struct {
		ID                int
		ParentID          int
		StartTimeUnixNano int
		TimeUnixNano      int
		Count             int
		Sum               int
		QuantileValues    *QuantileValueIds
		Flags             int
	}

	SummaryDataPointsStore struct {
		nextID         uint16
		dataPointsByID map[uint16]pmetric.SummaryDataPointSlice
	}
)

func NewSummaryDataPointsStore() *SummaryDataPointsStore {
	return &SummaryDataPointsStore{
		dataPointsByID: make(map[uint16]pmetric.SummaryDataPointSlice),
	}
}

func (s *SummaryDataPointsStore) SummaryMetricsByID(ID uint16) pmetric.SummaryDataPointSlice {
	nbdps, ok := s.dataPointsByID[ID]
	if !ok {
		return pmetric.NewSummaryDataPointSlice()
	}
	return nbdps
}

func SchemaToSummaryIDs(schema *arrow.Schema) (*SummaryDataPointIDs, error) {
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

	count, err := arrowutils.FieldIDFromSchema(schema, constants.SummaryCount)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	sum, err := arrowutils.FieldIDFromSchema(schema, constants.SummarySum)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	quantileValues, err := NewQuantileValueIds(schema)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	flags, err := arrowutils.FieldIDFromSchema(schema, constants.Flags)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	return &SummaryDataPointIDs{
		ID:                ID,
		ParentID:          parentID,
		StartTimeUnixNano: startTimeUnixNano,
		TimeUnixNano:      timeUnixNano,
		Count:             count,
		Sum:               sum,
		QuantileValues:    quantileValues,
		Flags:             flags,
	}, nil
}

// SummaryDataPointsStoreFrom converts an Arrow record into a SummaryDataPointsStore.
//
// Important Note: This function doesn't take ownership of the record. The
// caller is responsible for releasing it.
func SummaryDataPointsStoreFrom(record arrow.Record, attrsStore *otlp.AttributesStore[uint32]) (*SummaryDataPointsStore, error) {
	store := &SummaryDataPointsStore{
		dataPointsByID: make(map[uint16]pmetric.SummaryDataPointSlice),
	}

	fieldIDs, err := SchemaToSummaryIDs(record.Schema())
	if err != nil {
		return nil, werror.Wrap(err)
	}

	count := int(record.NumRows())
	prevParentID := uint16(0)

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
			nbdps = pmetric.NewSummaryDataPointSlice()
			store.dataPointsByID[parentID] = nbdps
		}

		sdp := nbdps.AppendEmpty()

		startTimeUnixNano, err := arrowutils.TimestampFromRecord(record, fieldIDs.StartTimeUnixNano, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		sdp.SetStartTimestamp(pcommon.Timestamp(startTimeUnixNano))

		timeUnixNano, err := arrowutils.TimestampFromRecord(record, fieldIDs.TimeUnixNano, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		sdp.SetTimestamp(pcommon.Timestamp(timeUnixNano))

		summaryCount, err := arrowutils.U64FromRecord(record, fieldIDs.Count, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		sdp.SetCount(summaryCount)

		sum, err := arrowutils.F64FromRecord(record, fieldIDs.Sum, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		sdp.SetSum(sum)

		err = AppendQuantileValuesInto(sdp.QuantileValues(), record, row, fieldIDs.QuantileValues)
		if err != nil {
			return nil, werror.Wrap(err)
		}

		flags, err := arrowutils.U32FromRecord(record, fieldIDs.Flags, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		sdp.SetFlags(pmetric.DataPointFlags(flags))

		if ID != nil {
			attrs := attrsStore.AttributesByDeltaID(*ID)
			if attrs != nil {
				attrs.CopyTo(sdp.Attributes())
			}
		}
	}

	return store, nil
}
