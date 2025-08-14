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

package arrow

// DataPointBuilder is used to build Sum and Gauge data points.

import (
	"errors"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/collector/pdata/pmetric"

	carrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

var (
	// DataPointSchema is the Arrow schema representing data points.
	// Related record.
	DataPointSchema = arrow.NewSchema([]arrow.Field{
		// Unique identifier of the DP. This ID is used to identify the
		// relationship between the DP, its attributes and exemplars.
		{Name: constants.ID, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.DeltaEncoding)},
		// The ID of the parent scope metric.
		{Name: constants.ParentID, Type: arrow.PrimitiveTypes.Uint16},
		{Name: constants.StartTimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true},
		{Name: constants.TimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns},
		{Name: constants.IntValue, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: constants.DoubleValue, Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: constants.Flags, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional)},
	}, nil)
)

type (
	// DataPointBuilder is a builder for int data points.
	DataPointBuilder struct {
		released bool

		builder *builder.RecordBuilderExt

		ib  *builder.Uint32DeltaBuilder // id builder
		pib *builder.Uint16Builder      // parent_id builder

		stunb *builder.TimestampBuilder // start_time_unix_nano builder
		tunb  *builder.TimestampBuilder // time_unix_nano builder
		ivb   *builder.Int64Builder     // int_value builder
		dvb   *builder.Float64Builder   // double_value builder
		fb    *builder.Uint32Builder    // flags builder

		dataPointAccumulator *DPAccumulator
		attrsAccu            *carrow.Attributes32Accumulator
		exemplarAccumulator  *ExemplarAccumulator

		config      *NumberDataPointConfig
		payloadType *carrow.PayloadType
	}

	// DP is an internal representation of a data point used by the
	// DPAccumulator.
	DP struct {
		ParentID uint16
		Orig     *pmetric.NumberDataPoint
	}

	// DPAccumulator is an accumulator for data points.
	DPAccumulator struct {
		dps    []DP
		sorter NumberDataPointSorter
	}

	NumberDataPointSorter interface {
		Sort(dps []DP)
		Encode(parentID uint16, dp *pmetric.NumberDataPoint) uint16
		Reset()
	}

	NumberDataPointsByNothing  struct{}
	NumberDataPointsByParentID struct {
		prevParentID uint16
	}
	NumberDataPointsByTimestampParentID struct {
		prevParentID uint16
		prevNbDP     *pmetric.NumberDataPoint
	}
	NumberDataPointsByTimestampParentIDTypeValue struct {
		prevParentID uint16
		prevNbDP     *pmetric.NumberDataPoint
	}
	NumberDataPointsByTimestampTypeValueParentID struct {
		prevParentID uint16
		prevNbDP     *pmetric.NumberDataPoint
	}
	NumberDataPointsByTypeValueTimestampParentID struct {
		prevParentID uint16
		prevNbDP     *pmetric.NumberDataPoint
	}
)

// NewDataPointBuilder creates a new DataPointBuilder.
func NewDataPointBuilder(rBuilder *builder.RecordBuilderExt, payloadType *carrow.PayloadType, conf *NumberDataPointConfig) *DataPointBuilder {
	b := &DataPointBuilder{
		released:             false,
		builder:              rBuilder,
		dataPointAccumulator: NewDPAccumulator(conf.Sorter),
		config:               conf,
		payloadType:          payloadType,
	}

	b.init()
	return b
}

func (b *DataPointBuilder) init() {
	b.ib = b.builder.Uint32DeltaBuilder(constants.ID)
	// As the attributes are sorted before insertion, the delta between two
	// consecutive attributes ID should always be <=1.
	b.ib.SetMaxDelta(1)
	b.pib = b.builder.Uint16Builder(constants.ParentID)

	b.stunb = b.builder.TimestampBuilder(constants.StartTimeUnixNano)
	b.tunb = b.builder.TimestampBuilder(constants.TimeUnixNano)
	b.ivb = b.builder.Int64Builder(constants.IntValue)
	b.dvb = b.builder.Float64Builder(constants.DoubleValue)
	b.fb = b.builder.Uint32Builder(constants.Flags)
}

func (b *DataPointBuilder) SetAttributesAccumulator(accu *carrow.Attributes32Accumulator) {
	b.attrsAccu = accu
}

func (b *DataPointBuilder) SetExemplarAccumulator(accu *ExemplarAccumulator) {
	b.exemplarAccumulator = accu
}

func (b *DataPointBuilder) SchemaID() string {
	return b.builder.SchemaID()
}

func (b *DataPointBuilder) Schema() *arrow.Schema {
	return b.builder.Schema()
}

func (b *DataPointBuilder) IsEmpty() bool {
	return b.dataPointAccumulator.IsEmpty()
}

func (b *DataPointBuilder) Accumulator() *DPAccumulator {
	return b.dataPointAccumulator
}

func (b *DataPointBuilder) Build() (record arrow.Record, err error) {
	schemaNotUpToDateCount := 0

	// Loop until the record is built successfully.
	// Intermediaries steps may be required to update the schema.
	for {
		b.attrsAccu.Reset()
		b.exemplarAccumulator.Reset()
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

func (b *DataPointBuilder) TryBuild(attrsAccu *carrow.Attributes32Accumulator) (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(carrow.ErrBuilderAlreadyReleased)
	}

	b.dataPointAccumulator.sorter.Reset()
	b.dataPointAccumulator.sorter.Sort(b.dataPointAccumulator.dps)

	ID := uint32(0)

	b.builder.Reserve(len(b.dataPointAccumulator.dps))

	for _, ndp := range b.dataPointAccumulator.dps {
		b.ib.Append(ID)
		b.pib.Append(b.dataPointAccumulator.sorter.Encode(ndp.ParentID, ndp.Orig))

		// Attributes
		err = attrsAccu.Append(ID, ndp.Orig.Attributes())
		if err != nil {
			return nil, werror.Wrap(err)
		}

		startTime := ndp.Orig.StartTimestamp()
		if startTime == 0 {
			b.stunb.AppendNull()
		} else {
			b.stunb.Append(arrow.Timestamp(startTime))
		}
		b.tunb.Append(arrow.Timestamp(ndp.Orig.Timestamp()))
		switch ndp.Orig.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			b.ivb.Append(ndp.Orig.IntValue())
			b.dvb.AppendNull()
		case pmetric.NumberDataPointValueTypeDouble:
			b.dvb.Append(ndp.Orig.DoubleValue())
			b.ivb.AppendNull()
		case pmetric.NumberDataPointValueTypeEmpty:
			b.ivb.AppendNull()
			b.dvb.AppendNull()
		}
		b.fb.Append(uint32(ndp.Orig.Flags()))

		exemplars := ndp.Orig.Exemplars()
		if exemplars.Len() > 0 {
			err = b.exemplarAccumulator.Append(ID, exemplars)
			if err != nil {
				return nil, werror.Wrap(err)
			}
		}

		ID++
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		b.init()
	}
	return
}

func (b *DataPointBuilder) Reset() {
	b.dataPointAccumulator.Reset()
}

func (b *DataPointBuilder) PayloadType() *carrow.PayloadType {
	return b.payloadType
}

// Release releases the underlying memory.
func (b *DataPointBuilder) Release() {
	if b.released {
		return
	}
	b.builder.Release()
	b.released = true
}

// NewDPAccumulator creates a new DPAccumulator.
func NewDPAccumulator(sorter NumberDataPointSorter) *DPAccumulator {
	return &DPAccumulator{
		dps:    make([]DP, 0),
		sorter: sorter,
	}
}

func (a *DPAccumulator) IsEmpty() bool {
	return len(a.dps) == 0
}

// Append appends a slice of number data points to the accumulator.
func (a *DPAccumulator) Append(
	parentId uint16,
	dp *pmetric.NumberDataPoint,
) {
	a.dps = append(a.dps, DP{
		ParentID: parentId,
		Orig:     dp,
	})
}

func (a *DPAccumulator) Reset() {
	a.dps = a.dps[:0]
}

// No sorting
// ==========

func UnsortedNumberDataPoints() *NumberDataPointsByNothing {
	return &NumberDataPointsByNothing{}
}

func (a *NumberDataPointsByNothing) Sort(_ []DP) {
	// Do nothing
}

func (a *NumberDataPointsByNothing) Encode(parentID uint16, _ *pmetric.NumberDataPoint) uint16 {
	return parentID
}

func (a *NumberDataPointsByNothing) Reset() {}

// Sort by parentID
// ================

func SortNumberDataPointsByParentID() *NumberDataPointsByParentID {
	return &NumberDataPointsByParentID{}
}

func (a *NumberDataPointsByParentID) Sort(dps []DP) {
	sort.Slice(dps, func(i, j int) bool {
		dpsI := dps[i]
		dpsJ := dps[j]
		return dpsI.ParentID < dpsJ.ParentID
	})
}

func (a *NumberDataPointsByParentID) Encode(parentID uint16, _ *pmetric.NumberDataPoint) uint16 {
	delta := parentID - a.prevParentID
	a.prevParentID = parentID
	return delta
}

func (a *NumberDataPointsByParentID) Reset() {
	a.prevParentID = 0
}

// Sort by timestamp and parentID
// ==============================

func SortNumberDataPointsByTimeParentID() *NumberDataPointsByTimestampParentID {
	return &NumberDataPointsByTimestampParentID{}
}

func (a *NumberDataPointsByTimestampParentID) Sort(dps []DP) {
	sort.Slice(dps, func(i, j int) bool {
		dpsI := dps[i]
		dpsJ := dps[j]
		if dpsI.Orig.Timestamp() == dpsJ.Orig.Timestamp() {
			return dpsI.ParentID < dpsJ.ParentID
		} else {
			return dpsI.Orig.Timestamp() < dpsJ.Orig.Timestamp()
		}
	})
}

func (a *NumberDataPointsByTimestampParentID) Encode(parentID uint16, dp *pmetric.NumberDataPoint) uint16 {
	if a.prevNbDP == nil {
		a.prevNbDP = dp
		a.prevParentID = parentID
		return parentID
	}

	if a.prevNbDP.Timestamp() == dp.Timestamp() {
		delta := parentID - a.prevParentID
		a.prevParentID = parentID
		return delta
	} else {
		a.prevNbDP = dp
		a.prevParentID = parentID
		return parentID
	}
}

func (a *NumberDataPointsByTimestampParentID) Reset() {
	a.prevParentID = 0
	a.prevNbDP = nil
}

// Sort by timestamp, parentID, value type, value
// ==============================================

func SortNumberDataPointsByTimeParentIDTypeValue() *NumberDataPointsByTimestampParentIDTypeValue {
	return &NumberDataPointsByTimestampParentIDTypeValue{}
}

func (a *NumberDataPointsByTimestampParentIDTypeValue) Sort(dps []DP) {
	sort.Slice(dps, func(i, j int) bool {
		dpsI := dps[i]
		dpsJ := dps[j]
		if dpsI.Orig.Timestamp() == dpsJ.Orig.Timestamp() {
			if dpsI.ParentID == dpsJ.ParentID {
				if dpsI.Orig.ValueType() == dpsJ.Orig.ValueType() {
					switch dpsI.Orig.ValueType() {
					case pmetric.NumberDataPointValueTypeInt:
						return dpsI.Orig.IntValue() < dpsJ.Orig.IntValue()
					case pmetric.NumberDataPointValueTypeDouble:
						return dpsI.Orig.DoubleValue() < dpsJ.Orig.DoubleValue()
					default:
						return false
					}
				} else {
					return dpsI.Orig.ValueType() < dpsJ.Orig.ValueType()
				}
			} else {
				return dpsI.ParentID < dpsJ.ParentID
			}
		} else {
			return dpsI.Orig.Timestamp() < dpsJ.Orig.Timestamp()
		}
	})
}

func (a *NumberDataPointsByTimestampParentIDTypeValue) Encode(parentID uint16, dp *pmetric.NumberDataPoint) uint16 {
	if a.prevNbDP == nil {
		a.prevNbDP = dp
		a.prevParentID = parentID
		return parentID
	}

	if a.prevNbDP.Timestamp() == dp.Timestamp() {
		delta := parentID - a.prevParentID
		a.prevParentID = parentID
		return delta
	} else {
		a.prevNbDP = dp
		a.prevParentID = parentID
		return parentID
	}
}

func (a *NumberDataPointsByTimestampParentIDTypeValue) Reset() {
	a.prevParentID = 0
	a.prevNbDP = nil
}

// Sort by type, value, parentID
// =============================

func SortNumberDataPointsByTypeValueTimestampParentID() *NumberDataPointsByTypeValueTimestampParentID {
	return &NumberDataPointsByTypeValueTimestampParentID{}
}

func (a *NumberDataPointsByTypeValueTimestampParentID) Sort(dps []DP) {
	sort.Slice(dps, func(i, j int) bool {
		dpsI := dps[i]
		dpsJ := dps[j]
		if dpsI.Orig.ValueType() == dpsJ.Orig.ValueType() {
			switch dpsI.Orig.ValueType() {
			case pmetric.NumberDataPointValueTypeInt:
				if dpsI.Orig.IntValue() == dpsJ.Orig.IntValue() {
					if dpsI.Orig.Timestamp() == dpsJ.Orig.Timestamp() {
						return dpsI.ParentID < dpsJ.ParentID
					} else {
						return dpsI.Orig.Timestamp() < dpsJ.Orig.Timestamp()
					}
				} else {
					return dpsI.Orig.IntValue() < dpsJ.Orig.IntValue()
				}
			case pmetric.NumberDataPointValueTypeDouble:
				if dpsI.Orig.DoubleValue() == dpsJ.Orig.DoubleValue() {
					if dpsI.Orig.Timestamp() == dpsJ.Orig.Timestamp() {
						return dpsI.ParentID < dpsJ.ParentID
					} else {
						return dpsI.Orig.Timestamp() < dpsJ.Orig.Timestamp()
					}
				} else {
					return dpsI.Orig.DoubleValue() < dpsJ.Orig.DoubleValue()
				}
			default:
				if dpsI.Orig.Timestamp() == dpsJ.Orig.Timestamp() {
					return dpsI.ParentID < dpsJ.ParentID
				} else {
					return dpsI.Orig.Timestamp() < dpsJ.Orig.Timestamp()
				}
			}
		} else {
			return dpsI.Orig.ValueType() < dpsJ.Orig.ValueType()
		}
	})
}

func (a *NumberDataPointsByTypeValueTimestampParentID) Encode(parentID uint16, dp *pmetric.NumberDataPoint) uint16 {
	if a.prevNbDP == nil {
		a.prevNbDP = dp
		a.prevParentID = parentID
		return parentID
	}

	if a.Equal(a.prevNbDP, dp) {
		delta := parentID - a.prevParentID
		a.prevParentID = parentID
		return delta
	} else {
		a.prevNbDP = dp
		a.prevParentID = parentID
		return parentID
	}
}

func (a *NumberDataPointsByTypeValueTimestampParentID) Reset() {
	a.prevParentID = 0
	a.prevNbDP = nil
}

func (a *NumberDataPointsByTypeValueTimestampParentID) Equal(nbDP1, nbDP2 *pmetric.NumberDataPoint) bool {
	if nbDP1 == nil || nbDP2 == nil {
		return false
	}

	if nbDP1.ValueType() == nbDP2.ValueType() {
		switch nbDP1.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			if nbDP1.IntValue() == nbDP2.IntValue() {
				return nbDP1.Timestamp() == nbDP2.Timestamp()
			} else {
				return false
			}
		case pmetric.NumberDataPointValueTypeDouble:
			if nbDP1.DoubleValue() == nbDP2.DoubleValue() {
				return nbDP1.Timestamp() == nbDP2.Timestamp()
			} else {
				return false
			}
		default:
			return true
		}
	} else {
		return false
	}
}

// Sort by timestamp, value type, value, parentID
// ==============================================

func SortNumberDataPointsByTimestampTypeValueParentID() *NumberDataPointsByTimestampTypeValueParentID {
	return &NumberDataPointsByTimestampTypeValueParentID{}
}

func (a *NumberDataPointsByTimestampTypeValueParentID) Sort(dps []DP) {
	sort.Slice(dps, func(i, j int) bool {
		dpsI := dps[i]
		dpsJ := dps[j]
		if dpsI.Orig.Timestamp() == dpsJ.Orig.Timestamp() {
			if dpsI.Orig.ValueType() == dpsJ.Orig.ValueType() {
				switch dpsI.Orig.ValueType() {
				case pmetric.NumberDataPointValueTypeInt:
					if dpsI.Orig.IntValue() == dpsJ.Orig.IntValue() {
						return dpsI.ParentID < dpsJ.ParentID
					} else {
						return dpsI.Orig.IntValue() < dpsJ.Orig.IntValue()
					}
				case pmetric.NumberDataPointValueTypeDouble:
					if dpsI.Orig.DoubleValue() == dpsJ.Orig.DoubleValue() {
						return dpsI.ParentID < dpsJ.ParentID
					} else {
						return dpsI.Orig.DoubleValue() < dpsJ.Orig.DoubleValue()
					}
				default:
					return dpsI.ParentID < dpsJ.ParentID
				}
			} else {
				return dpsI.Orig.ValueType() < dpsJ.Orig.ValueType()
			}
		} else {
			return dpsI.Orig.Timestamp() < dpsJ.Orig.Timestamp()
		}
	})
}

func (a *NumberDataPointsByTimestampTypeValueParentID) Encode(parentID uint16, dp *pmetric.NumberDataPoint) uint16 {
	if a.prevNbDP == nil {
		a.prevNbDP = dp
		a.prevParentID = parentID
		return parentID
	}

	if a.Equal(a.prevNbDP, dp) {
		delta := parentID - a.prevParentID
		a.prevParentID = parentID
		return delta
	} else {
		a.prevNbDP = dp
		a.prevParentID = parentID
		return parentID
	}
}

func (a *NumberDataPointsByTimestampTypeValueParentID) Reset() {
	a.prevParentID = 0
	a.prevNbDP = nil
}

func (a *NumberDataPointsByTimestampTypeValueParentID) Equal(nbDP1, nbDP2 *pmetric.NumberDataPoint) bool {
	if nbDP1 == nil || nbDP2 == nil {
		return false
	}

	if nbDP1.Timestamp() == nbDP2.Timestamp() {
		if nbDP1.ValueType() == nbDP2.ValueType() {
			switch nbDP1.ValueType() {
			case pmetric.NumberDataPointValueTypeInt:
				return nbDP1.IntValue() == nbDP2.IntValue()
			case pmetric.NumberDataPointValueTypeDouble:
				return nbDP1.DoubleValue() == nbDP2.DoubleValue()
			default:
				return true
			}
		} else {
			return false
		}
	} else {
		return false
	}
}
