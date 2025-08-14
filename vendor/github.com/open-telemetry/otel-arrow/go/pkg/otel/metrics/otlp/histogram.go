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
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	arrowutils "github.com/open-telemetry/otel-arrow/go/pkg/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/otlp"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

type (
	HistogramDataPointIDs struct {
		ID                int
		ParentID          int
		StartTimeUnixNano int
		TimeUnixNano      int
		Count             int
		Sum               int
		BucketCounts      int // List of uint64
		ExplicitBounds    int // List of float64
		Flags             int
		Min               int
		Max               int
	}

	HistogramDataPointsStore struct {
		nextID         uint16
		dataPointsByID map[uint16]pmetric.HistogramDataPointSlice
	}
)

func NewHistogramDataPointsStore() *HistogramDataPointsStore {
	return &HistogramDataPointsStore{
		dataPointsByID: make(map[uint16]pmetric.HistogramDataPointSlice),
	}
}

func (s *HistogramDataPointsStore) HistogramMetricsByID(ID uint16) pmetric.HistogramDataPointSlice {
	dps, ok := s.dataPointsByID[ID]
	if !ok {
		return pmetric.NewHistogramDataPointSlice()
	}
	return dps
}

func SchemaToHistogramIDs(schema *arrow.Schema) (*HistogramDataPointIDs, error) {
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

	count, err := arrowutils.FieldIDFromSchema(schema, constants.HistogramCount)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	sum, err := arrowutils.FieldIDFromSchema(schema, constants.HistogramSum)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	bucketCounts, err := arrowutils.FieldIDFromSchema(schema, constants.HistogramBucketCounts)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	explicitBounds, err := arrowutils.FieldIDFromSchema(schema, constants.HistogramExplicitBounds)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	flags, err := arrowutils.FieldIDFromSchema(schema, constants.Flags)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	min, err := arrowutils.FieldIDFromSchema(schema, constants.HistogramMin)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	max, err := arrowutils.FieldIDFromSchema(schema, constants.HistogramMax)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	return &HistogramDataPointIDs{
		ID:                ID,
		ParentID:          parentID,
		StartTimeUnixNano: startTimeUnixNano,
		TimeUnixNano:      timeUnixNano,
		Count:             count,
		Sum:               sum,
		BucketCounts:      bucketCounts,
		ExplicitBounds:    explicitBounds,
		Flags:             flags,
		Min:               min,
		Max:               max,
	}, nil
}

// HistogramDataPointsStoreFrom converts an arrow Record to a HistogramDataPointsStore.
//
// Important Note: This function doesn't take ownership of the record. The
// caller is responsible for releasing it.
func HistogramDataPointsStoreFrom(record arrow.Record, exemplarsStore *ExemplarsStore, attrsStore *otlp.AttributesStore[uint32]) (*HistogramDataPointsStore, error) {
	store := &HistogramDataPointsStore{
		dataPointsByID: make(map[uint16]pmetric.HistogramDataPointSlice),
	}

	fieldIDs, err := SchemaToHistogramIDs(record.Schema())
	if err != nil {
		return nil, werror.Wrap(err)
	}

	count := int(record.NumRows())
	prevParentID := uint16(0)
	lastID := uint32(0)

	for row := 0; row < count; row++ {
		// Data Point ID
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

		hdps, found := store.dataPointsByID[parentID]
		if !found {
			hdps = pmetric.NewHistogramDataPointSlice()
			store.dataPointsByID[parentID] = hdps
		}

		hdp := hdps.AppendEmpty()

		startTimeUnixNano, err := arrowutils.TimestampFromRecord(record, fieldIDs.StartTimeUnixNano, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		hdp.SetStartTimestamp(pcommon.Timestamp(startTimeUnixNano))

		timeUnixNano, err := arrowutils.TimestampFromRecord(record, fieldIDs.TimeUnixNano, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		hdp.SetTimestamp(pcommon.Timestamp(timeUnixNano))

		histogramCount, err := arrowutils.U64FromRecord(record, fieldIDs.Count, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		hdp.SetCount(histogramCount)

		sum, err := arrowutils.F64OrNilFromRecord(record, fieldIDs.Sum, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		if sum != nil {
			hdp.SetSum(*sum)
		}

		bucketCounts, start, end, err := arrowutils.ListValuesByIDFromRecord(record, fieldIDs.BucketCounts, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		if values, ok := bucketCounts.(*array.Uint64); ok {
			bucketCountsSlice := hdp.BucketCounts()
			bucketCountsSlice.EnsureCapacity(end - start)
			for i := start; i < end; i++ {
				bucketCountsSlice.Append(values.Value(i))
			}
		} else if bucketCounts != nil {
			return nil, werror.Wrap(ErrNotArrayUint64)
		}

		explicitBounds, start, end, err := arrowutils.ListValuesByIDFromRecord(record, fieldIDs.ExplicitBounds, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		if values, ok := explicitBounds.(*array.Float64); ok {
			explicitBoundsSlice := hdp.ExplicitBounds()
			explicitBoundsSlice.EnsureCapacity(end - start)
			for i := start; i < end; i++ {
				explicitBoundsSlice.Append(values.Value(i))
			}
		} else if explicitBounds != nil {
			return nil, werror.Wrap(ErrNotArrayFloat64)
		}

		flags, err := arrowutils.U32FromRecord(record, fieldIDs.Flags, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		hdp.SetFlags(pmetric.DataPointFlags(flags))

		min, err := arrowutils.F64OrNilFromRecord(record, fieldIDs.Min, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		if min != nil {
			hdp.SetMin(*min)
		}

		max, err := arrowutils.F64OrNilFromRecord(record, fieldIDs.Max, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		if max != nil {
			hdp.SetMax(*max)
		}

		if ID != nil {
			lastID += *ID

			exemplars := exemplarsStore.ExemplarsByID(lastID)
			exemplars.MoveAndAppendTo(hdp.Exemplars())

			attrs := attrsStore.AttributesByID(lastID)
			if attrs != nil {
				attrs.CopyTo(hdp.Attributes())
			}
		}
	}

	return store, nil
}
