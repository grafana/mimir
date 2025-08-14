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

// This file contains the Arrow schema and encoding logic for histogram metrics
// and associated data points. Our Arrow schema employs a flattened structure,
// optimally utilizing Arrow's columnar format. This denormalization obviates
// the need for an auxiliary pair of ID and ParentID, which would otherwise be
// necessary to denote the relationship between a metric and its data points.
// Consequently, the metric fields `Name`, `Description`, `Unit`,
// `AggregationTemporality`, and `IsMonotonic` will appear duplicated across
// each data point. However, due to the high compressibility of these fields,
// this redundancy isn't a concern. The corresponding Arrow records are sorted
// by the metric name, further enhancing the schema's efficiency and accessibility.

import (
	"sort"

	"github.com/apache/arrow-go/v18/arrow"

	"errors"
	"math"

	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"

	"go.opentelemetry.io/collector/pdata/pmetric"

	carrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

var (
	// HistogramDataPointSchema is the Arrow Schema describing a histogram
	// data point.
	// Related record.
	HistogramDataPointSchema = arrow.NewSchema([]arrow.Field{
		// Unique identifier of the NDP. This ID is used to identify the
		// relationship between the NDP, its attributes and exemplars.
		{Name: constants.ID, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional, schema.DeltaEncoding)},
		// The ID of the parent metric.
		{Name: constants.ParentID, Type: arrow.PrimitiveTypes.Uint16},
		{Name: constants.StartTimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.TimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.HistogramCount, Type: arrow.PrimitiveTypes.Uint64, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.HistogramSum, Type: arrow.PrimitiveTypes.Float64, Metadata: schema.Metadata(schema.Optional), Nullable: true},
		{Name: constants.HistogramBucketCounts, Type: arrow.ListOf(arrow.PrimitiveTypes.Uint64), Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.HistogramExplicitBounds, Type: arrow.ListOf(arrow.PrimitiveTypes.Float64), Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.Flags, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.HistogramMin, Type: arrow.PrimitiveTypes.Float64, Metadata: schema.Metadata(schema.Optional), Nullable: true},
		{Name: constants.HistogramMax, Type: arrow.PrimitiveTypes.Float64, Metadata: schema.Metadata(schema.Optional), Nullable: true},
	}, nil)
)

type (
	// HistogramDataPointBuilder is a builder for histogram data points.
	HistogramDataPointBuilder struct {
		released bool

		builder *builder.RecordBuilderExt

		ib  *builder.Uint32DeltaBuilder // id builder
		pib *builder.Uint16Builder      // parent_id builder

		stunb *builder.TimestampBuilder // start_time_unix_nano builder
		tunb  *builder.TimestampBuilder // time_unix_nano builder
		hcb   *builder.Uint64Builder    // histogram_count builder
		hsb   *builder.Float64Builder   // histogram_sum builder
		hbclb *builder.ListBuilder      // histogram_bucket_counts list builder
		hbcb  *builder.Uint64Builder    // histogram_bucket_counts builder
		heblb *builder.ListBuilder      // histogram_explicit_bounds list builder
		hebb  *builder.Float64Builder   // histogram_explicit_bounds builder
		fb    *builder.Uint32Builder    // flags builder
		hmib  *builder.Float64Builder   // histogram_min builder
		hmab  *builder.Float64Builder   // histogram_max builder

		dataPointAccumulator *HDPAccumulator
		attrsAccu            *carrow.Attributes32Accumulator
		exemplarAccumulator  *ExemplarAccumulator
		config               *HistogramConfig
	}

	HDP struct {
		ParentID uint16
		Orig     *pmetric.HistogramDataPoint
	}

	HDPAccumulator struct {
		groupCount uint32
		hdps       []HDP
		sorter     HistogramSorter
	}

	HistogramSorter interface {
		Sort(histograms []HDP)
		Encode(parentID uint16, dp *pmetric.HistogramDataPoint) uint16
		Reset()
	}

	HistogramsByNothing  struct{}
	HistogramsByParentID struct {
		prevParentID uint16
	}
	// ToDo explore other sorting options
)

// NewHistogramDataPointBuilder creates a new HistogramDataPointBuilder.
func NewHistogramDataPointBuilder(rBuilder *builder.RecordBuilderExt, conf *HistogramConfig) *HistogramDataPointBuilder {
	b := &HistogramDataPointBuilder{
		released:             false,
		builder:              rBuilder,
		dataPointAccumulator: NewHDPAccumulator(conf.Sorter),
	}

	b.init()
	return b
}

func (b *HistogramDataPointBuilder) init() {
	b.ib = b.builder.Uint32DeltaBuilder(constants.ID)
	b.ib.SetMaxDelta(1)
	b.pib = b.builder.Uint16Builder(constants.ParentID)

	b.stunb = b.builder.TimestampBuilder(constants.StartTimeUnixNano)
	b.tunb = b.builder.TimestampBuilder(constants.TimeUnixNano)
	b.hcb = b.builder.Uint64Builder(constants.HistogramCount)
	b.hsb = b.builder.Float64Builder(constants.HistogramSum)
	b.hbclb = b.builder.ListBuilder(constants.HistogramBucketCounts)
	b.heblb = b.builder.ListBuilder(constants.HistogramExplicitBounds)
	b.hbcb = b.hbclb.Uint64Builder()
	b.hebb = b.heblb.Float64Builder()
	b.fb = b.builder.Uint32Builder(constants.Flags)
	b.hmib = b.builder.Float64Builder(constants.HistogramMin)
	b.hmab = b.builder.Float64Builder(constants.HistogramMax)
}

func (b *HistogramDataPointBuilder) SetAttributesAccumulator(accu *carrow.Attributes32Accumulator) {
	b.attrsAccu = accu
}

func (b *HistogramDataPointBuilder) SetExemplarAccumulator(accu *ExemplarAccumulator) {
	b.exemplarAccumulator = accu
}

func (b *HistogramDataPointBuilder) SchemaID() string {
	return b.builder.SchemaID()
}

func (b *HistogramDataPointBuilder) Schema() *arrow.Schema {
	return b.builder.Schema()
}

func (b *HistogramDataPointBuilder) IsEmpty() bool {
	return b.dataPointAccumulator.IsEmpty()
}

func (b *HistogramDataPointBuilder) Accumulator() *HDPAccumulator {
	return b.dataPointAccumulator
}

// Build builds the underlying array.
//
// Once the array is no longer needed, Release() should be called to free the memory.
func (b *HistogramDataPointBuilder) Build() (record arrow.Record, err error) {
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

func (b *HistogramDataPointBuilder) Reset() {
	b.dataPointAccumulator.Reset()
}

func (b *HistogramDataPointBuilder) PayloadType() *carrow.PayloadType {
	return carrow.PayloadTypes.Histogram
}

// Release releases the underlying memory.
func (b *HistogramDataPointBuilder) Release() {
	if b.released {
		return
	}

	b.released = true
	b.builder.Release()
}

func (b *HistogramDataPointBuilder) TryBuild(attrsAccu *carrow.Attributes32Accumulator) (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(carrow.ErrBuilderAlreadyReleased)
	}

	b.dataPointAccumulator.sorter.Reset()
	b.dataPointAccumulator.sorter.Sort(b.dataPointAccumulator.hdps)

	b.builder.Reserve(len(b.dataPointAccumulator.hdps))

	for ID, hdpRec := range b.dataPointAccumulator.hdps {
		hdp := hdpRec.Orig
		b.ib.Append(uint32(ID))
		b.pib.Append(b.dataPointAccumulator.sorter.Encode(hdpRec.ParentID, hdp))

		// Attributes
		err = attrsAccu.Append(uint32(ID), hdp.Attributes())
		if err != nil {
			return nil, werror.Wrap(err)
		}

		b.stunb.Append(arrow.Timestamp(hdp.StartTimestamp()))
		b.tunb.Append(arrow.Timestamp(hdp.Timestamp()))

		b.hcb.Append(hdp.Count())
		if hdp.HasSum() {
			b.hsb.AppendNonZero(hdp.Sum())
		} else {
			b.hsb.AppendNull()
		}

		hbc := hdp.BucketCounts()
		hbcc := hbc.Len()
		if err := b.hbclb.Append(hbcc, func() error {
			for i := 0; i < hbcc; i++ {
				b.hbcb.Append(hbc.At(i))
			}
			return nil
		}); err != nil {
			return nil, werror.Wrap(err)
		}

		heb := hdp.ExplicitBounds()
		hebc := heb.Len()
		if err := b.heblb.Append(hebc, func() error {
			for i := 0; i < hebc; i++ {
				b.hebb.AppendNonZero(heb.At(i))
			}
			return nil
		}); err != nil {
			return nil, werror.Wrap(err)
		}

		exemplars := hdp.Exemplars()
		if exemplars.Len() > 0 {
			err = b.exemplarAccumulator.Append(uint32(ID), exemplars)
			if err != nil {
				return nil, werror.Wrap(err)
			}
		}

		b.fb.Append(uint32(hdp.Flags()))

		if hdp.HasMin() {
			b.hmib.AppendNonZero(hdp.Min())
		} else {
			b.hmib.AppendNull()
		}
		if hdp.HasMax() {
			b.hmab.AppendNonZero(hdp.Max())
		} else {
			b.hmab.AppendNull()
		}
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		b.init()
	}
	return
}

func NewHDPAccumulator(sorter HistogramSorter) *HDPAccumulator {
	return &HDPAccumulator{
		groupCount: 0,
		hdps:       make([]HDP, 0),
		sorter:     sorter,
	}
}

func (a *HDPAccumulator) IsEmpty() bool {
	return len(a.hdps) == 0
}

func (a *HDPAccumulator) Append(
	parentID uint16,
	hdps pmetric.HistogramDataPointSlice,
) {
	if a.groupCount == math.MaxUint32 {
		panic("The maximum number of group of histogram data points has been reached (max is uint32).")
	}

	if hdps.Len() == 0 {
		return
	}

	for i := 0; i < hdps.Len(); i++ {
		hdp := hdps.At(i)

		a.hdps = append(a.hdps, HDP{
			ParentID: parentID,
			Orig:     &hdp,
		})
	}

	a.groupCount++
}

func (a *HDPAccumulator) Reset() {
	a.groupCount = 0
	a.hdps = a.hdps[:0]
}

// No sorting
// ==========

func UnsortedHistograms() *HistogramsByNothing {
	return &HistogramsByNothing{}
}

func (a *HistogramsByNothing) Sort(_ []HDP) {
	// Do nothing
}

func (a *HistogramsByNothing) Encode(parentID uint16, _ *pmetric.HistogramDataPoint) uint16 {
	return parentID
}

func (a *HistogramsByNothing) Reset() {}

// Sort by parentID
// ================

func SortHistogramsByParentID() *HistogramsByParentID {
	return &HistogramsByParentID{}
}

func (a *HistogramsByParentID) Sort(histograms []HDP) {
	sort.Slice(histograms, func(i, j int) bool {
		dpsI := histograms[i]
		dpsJ := histograms[j]
		return dpsI.ParentID < dpsJ.ParentID
	})
}

func (a *HistogramsByParentID) Encode(parentID uint16, _ *pmetric.HistogramDataPoint) uint16 {
	delta := parentID - a.prevParentID
	a.prevParentID = parentID
	return delta
}

func (a *HistogramsByParentID) Reset() {
	a.prevParentID = 0
}
