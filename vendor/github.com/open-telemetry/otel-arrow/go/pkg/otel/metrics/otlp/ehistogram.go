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
	EHistogramDataPointIDs struct {
		ID                int
		ParentID          int
		StartTimeUnixNano int
		TimeUnixNano      int
		Count             int
		Sum               int
		Scale             int
		ZeroCount         int
		Positive          *EHistogramDataPointBucketsIds
		Negative          *EHistogramDataPointBucketsIds
		Flags             int
		Min               int
		Max               int
	}

	EHistogramDataPointsStore struct {
		nextID         uint16
		dataPointsByID map[uint16]pmetric.ExponentialHistogramDataPointSlice
	}
)

func NewEHistogramDataPointsStore() *EHistogramDataPointsStore {
	return &EHistogramDataPointsStore{
		dataPointsByID: make(map[uint16]pmetric.ExponentialHistogramDataPointSlice),
	}
}

func (s *EHistogramDataPointsStore) EHistogramMetricsByID(ID uint16) pmetric.ExponentialHistogramDataPointSlice {
	dps, ok := s.dataPointsByID[ID]
	if !ok {
		return pmetric.NewExponentialHistogramDataPointSlice()
	}
	return dps
}

func SchemaToEHistogramIDs(schema *arrow.Schema) (*EHistogramDataPointIDs, error) {
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

	scale, err := arrowutils.FieldIDFromSchema(schema, constants.ExpHistogramScale)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	zeroCount, err := arrowutils.FieldIDFromSchema(schema, constants.ExpHistogramZeroCount)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	positive, err := NewEHistogramDataPointBucketsIds(schema, constants.ExpHistogramPositive)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	negative, err := NewEHistogramDataPointBucketsIds(schema, constants.ExpHistogramNegative)
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

	return &EHistogramDataPointIDs{
		ID:                ID,
		ParentID:          parentID,
		StartTimeUnixNano: startTimeUnixNano,
		TimeUnixNano:      timeUnixNano,
		Count:             count,
		Sum:               sum,
		Scale:             scale,
		ZeroCount:         zeroCount,
		Positive:          positive,
		Negative:          negative,
		Flags:             flags,
		Min:               min,
		Max:               max,
	}, nil
}

// EHistogramDataPointsStoreFrom converts an arrow Record into an EHistogramDataPointsStore.
//
// Important Note: This function doesn't take ownership of the record. The
// caller is responsible for releasing it.
func EHistogramDataPointsStoreFrom(record arrow.Record, exemplarsStore *ExemplarsStore, attrsStore *otlp.AttributesStore[uint32]) (*EHistogramDataPointsStore, error) {
	store := &EHistogramDataPointsStore{
		dataPointsByID: make(map[uint16]pmetric.ExponentialHistogramDataPointSlice),
	}

	fieldIDs, err := SchemaToEHistogramIDs(record.Schema())
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

		ehdps, found := store.dataPointsByID[parentID]
		if !found {
			ehdps = pmetric.NewExponentialHistogramDataPointSlice()
			store.dataPointsByID[parentID] = ehdps
		}

		hdp := ehdps.AppendEmpty()

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

		scale, err := arrowutils.I32FromRecord(record, fieldIDs.Scale, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		hdp.SetScale(scale)

		zeroCount, err := arrowutils.U64FromRecord(record, fieldIDs.ZeroCount, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		hdp.SetZeroCount(zeroCount)

		positive, err := arrowutils.StructFromRecord(record, fieldIDs.Positive.ID, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		if positive != nil {
			if err := AppendUnivariateEHistogramDataPointBucketsInto(hdp.Positive(), positive, fieldIDs.Positive, row); err != nil {
				return nil, werror.Wrap(err)
			}
		}

		negative, err := arrowutils.StructFromRecord(record, fieldIDs.Negative.ID, row)
		if err != nil {
			return nil, werror.Wrap(err)
		}
		if negative != nil {
			if err := AppendUnivariateEHistogramDataPointBucketsInto(hdp.Negative(), negative, fieldIDs.Negative, row); err != nil {
				return nil, werror.Wrap(err)
			}
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
