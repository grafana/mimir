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

// Exemplars are represented as Arrow records.

import (
	"errors"
	"math"
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
	// ExemplarSchema is the Arrow schema representing an OTLP metric exemplar.
	ExemplarSchema = arrow.NewSchema([]arrow.Field{
		{Name: constants.ID, Type: arrow.PrimitiveTypes.Uint32, Metadata: schema.Metadata(schema.Optional, schema.DeltaEncoding)},
		{Name: constants.ParentID, Type: arrow.PrimitiveTypes.Uint32},
		{Name: constants.TimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.IntValue, Type: arrow.PrimitiveTypes.Int64, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.DoubleValue, Type: arrow.PrimitiveTypes.Float64, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.SpanId, Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.TraceId, Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
	}, nil)
)

type (
	// ExemplarBuilder is a helper to build an Arrow array containing a collection of OTLP metric exemplar.
	ExemplarBuilder struct {
		released bool

		builder *builder.RecordBuilderExt

		ib   *builder.Uint32DeltaBuilder     // `id` builder
		pib  *builder.Uint32Builder          // `parent_id` builder
		tunb *builder.TimestampBuilder       // `time_unix_nano` builder
		ivb  *builder.Int64Builder           // `int_value` metric builder
		dvb  *builder.Float64Builder         // `double_value` metric builder
		sib  *builder.FixedSizeBinaryBuilder // span id builder
		tib  *builder.FixedSizeBinaryBuilder // trace id builder

		accumulator *ExemplarAccumulator
		attrsAccu   *carrow.Attributes32Accumulator

		config      *ExemplarConfig
		payloadType *carrow.PayloadType
	}

	Exemplar struct {
		ParentID uint32
		Orig     *pmetric.Exemplar
	}

	ExemplarAccumulator struct {
		groupCount uint32
		exemplars  []Exemplar
		sorter     ExemplarSorter
	}

	ExemplarParentIdEncoder struct {
		prevParentID uint32
		encoderType  int
	}

	ExemplarSorter interface {
		Sort(exemplars []Exemplar)
		Encode(parentID uint32, dp *pmetric.Exemplar) uint32
		Reset()
	}

	ExemplarsByNothing           struct{}
	ExemplarsByTypeValueParentId struct {
		prevParentID uint32
		prevExemplar *pmetric.Exemplar
	}
)

// NewExemplarBuilder creates a new ExemplarBuilder.
func NewExemplarBuilder(rBuilder *builder.RecordBuilderExt, payloadType *carrow.PayloadType, conf *ExemplarConfig) *ExemplarBuilder {
	b := &ExemplarBuilder{
		released:    false,
		builder:     rBuilder,
		accumulator: NewExemplarAccumulator(conf.Sorter),
		config:      conf,
		payloadType: payloadType,
	}

	b.init()
	return b
}

func (b *ExemplarBuilder) init() {
	b.ib = b.builder.Uint32DeltaBuilder(constants.ID)
	// As the events are sorted before insertion, the delta between two
	// consecutive ID should always be <=1.
	b.ib.SetMaxDelta(1)
	b.pib = b.builder.Uint32Builder(constants.ParentID)

	b.tunb = b.builder.TimestampBuilder(constants.TimeUnixNano)
	b.ivb = b.builder.Int64Builder(constants.IntValue)
	b.dvb = b.builder.Float64Builder(constants.DoubleValue)
	b.sib = b.builder.FixedSizeBinaryBuilder(constants.SpanId)
	b.tib = b.builder.FixedSizeBinaryBuilder(constants.TraceId)
}

func (b *ExemplarBuilder) SetAttributesAccumulator(accu *carrow.Attributes32Accumulator) {
	b.attrsAccu = accu
}

func (b *ExemplarBuilder) SchemaID() string {
	return b.builder.SchemaID()
}

func (b *ExemplarBuilder) Schema() *arrow.Schema {
	return b.builder.Schema()
}

func (b *ExemplarBuilder) IsEmpty() bool {
	return b.accumulator.IsEmpty()
}

func (b *ExemplarBuilder) Accumulator() *ExemplarAccumulator {
	return b.accumulator
}

func (b *ExemplarBuilder) Build() (record arrow.Record, err error) {
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

func (b *ExemplarBuilder) TryBuild(attrsAccu *carrow.Attributes32Accumulator) (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(carrow.ErrBuilderAlreadyReleased)
	}

	b.accumulator.sorter.Reset()
	b.accumulator.sorter.Sort(b.accumulator.exemplars)

	exemplarID := uint32(0)

	b.builder.Reserve(len(b.accumulator.exemplars))

	for _, exemplar := range b.accumulator.exemplars {
		ex := exemplar.Orig
		attrs := ex.FilteredAttributes()
		if attrs.Len() == 0 {
			b.ib.AppendNull()
		} else {
			b.ib.Append(exemplarID)

			// Attributes
			err = attrsAccu.Append(exemplarID, attrs)
			if err != nil {
				return
			}

			exemplarID++
		}

		b.pib.Append(b.accumulator.sorter.Encode(exemplar.ParentID, exemplar.Orig))
		b.tunb.Append(arrow.Timestamp(ex.Timestamp().AsTime().UnixNano()))

		switch ex.ValueType() {
		case pmetric.ExemplarValueTypeInt:
			b.ivb.Append(ex.IntValue())
			b.dvb.AppendNull()
		case pmetric.ExemplarValueTypeDouble:
			b.ivb.AppendNull()
			b.dvb.Append(ex.DoubleValue())
		default:
			b.ivb.AppendNull()
			b.dvb.AppendNull()
		}

		sid := ex.SpanID()
		b.sib.Append(sid[:])

		tid := ex.TraceID()
		b.tib.Append(tid[:])
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		b.init()
	}
	return
}

func (b *ExemplarBuilder) Reset() {
	b.accumulator.Reset()
}

func (b *ExemplarBuilder) PayloadType() *carrow.PayloadType {
	return b.payloadType
}

// Release releases the memory allocated by the builder.
func (b *ExemplarBuilder) Release() {
	if !b.released {
		b.builder.Release()

		b.released = true
	}
}

func NewExemplarAccumulator(sorter ExemplarSorter) *ExemplarAccumulator {
	return &ExemplarAccumulator{
		groupCount: 0,
		exemplars:  make([]Exemplar, 0),
		sorter:     sorter,
	}
}

func (a *ExemplarAccumulator) IsEmpty() bool {
	return len(a.exemplars) == 0
}

// Append appends a slice of exemplars to the accumulator.
func (a *ExemplarAccumulator) Append(dpID uint32, exemplars pmetric.ExemplarSlice) error {
	if a.groupCount == math.MaxUint32 {
		panic("The maximum number of group of exemplars has been reached (max is uint32).")
	}

	if exemplars.Len() == 0 {
		return nil
	}

	for i := 0; i < exemplars.Len(); i++ {
		evt := exemplars.At(i)
		a.exemplars = append(a.exemplars, Exemplar{
			ParentID: dpID,
			Orig:     &evt,
		})
	}

	a.groupCount++

	return nil
}

func (a *ExemplarAccumulator) Reset() {
	a.groupCount = 0
	a.exemplars = a.exemplars[:0]
}

func NewExemplarParentIdEncoder(encoderType int) *ExemplarParentIdEncoder {
	return &ExemplarParentIdEncoder{
		prevParentID: 0,
		encoderType:  encoderType,
	}
}

func (e *ExemplarParentIdEncoder) Encode(parentID uint32) uint32 {
	delta := parentID - e.prevParentID
	e.prevParentID = parentID
	return delta
}

// No sorting
// ==========

func UnsortedExemplars() *ExemplarsByNothing {
	return &ExemplarsByNothing{}
}

func (s *ExemplarsByNothing) Sort(_ []Exemplar) {
}

func (s *ExemplarsByNothing) Encode(parentID uint32, _ *pmetric.Exemplar) uint32 {
	return parentID
}

func (s *ExemplarsByNothing) Reset() {}

// Sorts exemplars by type, value, and parentID.
// =============================================

func SortExemplarsByTypeValueParentId() *ExemplarsByTypeValueParentId {
	return &ExemplarsByTypeValueParentId{}
}

func (s *ExemplarsByTypeValueParentId) Sort(exemplars []Exemplar) {
	sort.Slice(exemplars, func(i, j int) bool {
		exI := exemplars[i].Orig
		exJ := exemplars[j].Orig

		if exI.ValueType() == exJ.ValueType() {
			switch exI.ValueType() {
			case pmetric.ExemplarValueTypeInt:
				if exI.IntValue() == exJ.IntValue() {
					return exemplars[i].ParentID < exemplars[j].ParentID
				} else {
					return exI.IntValue() < exJ.IntValue()
				}
			case pmetric.ExemplarValueTypeDouble:
				if exI.DoubleValue() == exJ.DoubleValue() {
					return exemplars[i].ParentID < exemplars[j].ParentID
				} else {
					return exI.DoubleValue() < exJ.DoubleValue()
				}
			default:
				return false
			}
		} else {
			return exI.ValueType() < exJ.ValueType()
		}
	})
}

func (s *ExemplarsByTypeValueParentId) Encode(parentID uint32, exemplar *pmetric.Exemplar) uint32 {
	if s.prevExemplar == nil {
		s.prevExemplar = exemplar
		s.prevParentID = parentID
		return parentID
	}

	if s.Equal(s.prevExemplar, exemplar) {
		delta := parentID - s.prevParentID
		s.prevParentID = parentID
		return delta
	} else {
		s.prevExemplar = exemplar
		s.prevParentID = parentID
		return parentID
	}
}

func (s *ExemplarsByTypeValueParentId) Reset() {
	s.prevParentID = 0
	s.prevExemplar = nil
}

func (s *ExemplarsByTypeValueParentId) Equal(ex1, ex2 *pmetric.Exemplar) bool {
	if ex1 == nil || ex2 == nil {
		return false
	}

	if ex1.ValueType() == ex2.ValueType() {
		switch ex1.ValueType() {
		case pmetric.ExemplarValueTypeInt:
			return ex1.IntValue() == ex2.IntValue()
		case pmetric.ExemplarValueTypeDouble:
			return ex1.DoubleValue() == ex2.DoubleValue()
		default:
			return true
		}
	} else {
		return false
	}
}
