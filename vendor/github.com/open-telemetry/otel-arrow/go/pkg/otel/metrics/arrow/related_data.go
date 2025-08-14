/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package arrow

// The definition of all the related data for metrics,

import (
	"math"

	carrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/observer"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/stats"
	"github.com/open-telemetry/otel-arrow/go/pkg/record_message"
)

// Infrastructure to manage metrics related records.

type (
	// RelatedData is a collection of related/dependent data to metrics entities.
	RelatedData struct {
		nextMetricScopeID uint64

		relatedRecordsManager *carrow.RelatedRecordsManager

		attrsBuilders       *AttrsBuilders
		numberDPBuilder     *DataPointBuilder
		summaryDPBuilder    *SummaryDataPointBuilder
		histogramDPBuilder  *HistogramDataPointBuilder
		ehistogramDPBuilder *EHistogramDataPointBuilder

		numberDPExemplarBuilder   *ExemplarBuilder
		histogramExemplarBuilder  *ExemplarBuilder
		ehistogramExemplarBuilder *ExemplarBuilder
	}

	// AttrsBuilders groups together AttrsBuilder instances used to build related
	// data attributes (i.e. resource attributes, scope attributes, metrics
	// attributes.
	AttrsBuilders struct {
		resource *carrow.Attrs16Builder
		scope    *carrow.Attrs16Builder

		// metrics attributes
		number_dp  *carrow.Attrs32Builder
		summary    *carrow.Attrs32Builder
		histogram  *carrow.Attrs32Builder
		eHistogram *carrow.Attrs32Builder

		// exemplar attributes
		numberDPExemplar   *carrow.Attrs32Builder
		histogramExemplar  *carrow.Attrs32Builder
		eHistogramExemplar *carrow.Attrs32Builder
	}
)

func NewRelatedData(cfg *Config, stats *stats.ProducerStats, observer observer.ProducerObserver) (*RelatedData, error) {
	rrManager := carrow.NewRelatedRecordsManager(cfg.Global, stats)

	resourceAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.ResourceAttrs, carrow.PayloadTypes.Metrics, carrow.AttrsSchema16, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return carrow.NewAttrs16BuilderWithEncoding(b, carrow.PayloadTypes.ResourceAttrs, cfg.Attrs.Resource)
	}, observer)

	scopeAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.ScopeAttrs, carrow.PayloadTypes.Metrics, carrow.AttrsSchema16, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return carrow.NewAttrs16BuilderWithEncoding(b, carrow.PayloadTypes.ScopeAttrs, cfg.Attrs.Scope)
	}, observer)

	numberDPBuilder := rrManager.Declare(carrow.PayloadTypes.NumberDataPoints, carrow.PayloadTypes.Metrics, DataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewDataPointBuilder(b, carrow.PayloadTypes.NumberDataPoints, cfg.NumberDP)
	}, observer)

	numberDPAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.NumberDataPointAttrs, carrow.PayloadTypes.NumberDataPoints, carrow.DeltaEncodedAttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		nab := carrow.NewAttrs32BuilderWithEncoding(b, carrow.PayloadTypes.NumberDataPointAttrs, cfg.Attrs.NumberDataPoint)
		numberDPBuilder.(*DataPointBuilder).SetAttributesAccumulator(nab.Accumulator())
		return nab
	}, observer)

	numberDPExemplarBuilder := rrManager.Declare(carrow.PayloadTypes.NumberDataPointExemplars, carrow.PayloadTypes.NumberDataPoints, ExemplarSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		eb := NewExemplarBuilder(b, carrow.PayloadTypes.NumberDataPointExemplars, cfg.NumberDataPointExemplar)
		numberDPBuilder.(*DataPointBuilder).SetExemplarAccumulator(eb.Accumulator())
		return eb
	}, observer)

	numberDPExemplarAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.NumberDataPointExemplarAttrs, carrow.PayloadTypes.NumberDataPointExemplars, carrow.DeltaEncodedAttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		eb := carrow.NewAttrs32BuilderWithEncoding(b, carrow.PayloadTypes.NumberDataPointExemplarAttrs, cfg.Attrs.NumberDataPointExemplar)
		numberDPExemplarBuilder.(*ExemplarBuilder).SetAttributesAccumulator(eb.Accumulator())
		return eb
	}, observer)

	summaryDPBuilder := rrManager.Declare(carrow.PayloadTypes.Summary, carrow.PayloadTypes.Metrics, SummaryDataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewSummaryDataPointBuilder(b, cfg.Summary)
	}, observer)

	summaryAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.SummaryAttrs, carrow.PayloadTypes.Summary, carrow.DeltaEncodedAttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		sab := carrow.NewAttrs32BuilderWithEncoding(b, carrow.PayloadTypes.SummaryAttrs, cfg.Attrs.Summary)
		summaryDPBuilder.(*SummaryDataPointBuilder).SetAttributesAccumulator(sab.Accumulator())
		return sab
	}, observer)

	histogramDPBuilder := rrManager.Declare(carrow.PayloadTypes.Histogram, carrow.PayloadTypes.Metrics, HistogramDataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewHistogramDataPointBuilder(b, cfg.Histogram)
	}, observer)

	histogramAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.HistogramAttrs, carrow.PayloadTypes.Histogram, carrow.DeltaEncodedAttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		hab := carrow.NewAttrs32BuilderWithEncoding(b, carrow.PayloadTypes.HistogramAttrs, cfg.Attrs.Histogram)
		histogramDPBuilder.(*HistogramDataPointBuilder).SetAttributesAccumulator(hab.Accumulator())
		return hab
	}, observer)

	histogramExemplarBuilder := rrManager.Declare(carrow.PayloadTypes.HistogramExemplars, carrow.PayloadTypes.Histogram, ExemplarSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		eb := NewExemplarBuilder(b, carrow.PayloadTypes.HistogramExemplars, cfg.HistogramExemplar)
		histogramDPBuilder.(*HistogramDataPointBuilder).SetExemplarAccumulator(eb.Accumulator())
		return eb
	}, observer)

	histogramExemplarAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.HistogramExemplarAttrs, carrow.PayloadTypes.HistogramExemplars, carrow.DeltaEncodedAttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		eb := carrow.NewAttrs32BuilderWithEncoding(b, carrow.PayloadTypes.HistogramExemplarAttrs, cfg.Attrs.HistogramExemplar)
		histogramExemplarBuilder.(*ExemplarBuilder).SetAttributesAccumulator(eb.Accumulator())
		return eb
	}, observer)

	ehistogramDPBuilder := rrManager.Declare(carrow.PayloadTypes.ExpHistogram, carrow.PayloadTypes.Metrics, EHistogramDataPointSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewEHistogramDataPointBuilder(b, cfg.ExpHistogram)
	}, observer)

	ehistogramAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.ExpHistogramAttrs, carrow.PayloadTypes.ExpHistogram, carrow.DeltaEncodedAttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		hab := carrow.NewAttrs32BuilderWithEncoding(b, carrow.PayloadTypes.ExpHistogramAttrs, cfg.Attrs.ExpHistogram)
		ehistogramDPBuilder.(*EHistogramDataPointBuilder).SetAttributesAccumulator(hab.Accumulator())
		return hab
	}, observer)

	ehistogramExemplarBuilder := rrManager.Declare(carrow.PayloadTypes.ExpHistogramExemplars, carrow.PayloadTypes.ExpHistogram, ExemplarSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		eb := NewExemplarBuilder(b, carrow.PayloadTypes.ExpHistogramExemplars, cfg.HistogramExemplar)
		ehistogramDPBuilder.(*EHistogramDataPointBuilder).SetExemplarAccumulator(eb.Accumulator())
		return eb
	}, observer)

	ehistogramExemplarAttrsBuilder := rrManager.Declare(carrow.PayloadTypes.ExpHistogramExemplarAttrs, carrow.PayloadTypes.ExpHistogramExemplars, carrow.DeltaEncodedAttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		eb := carrow.NewAttrs32BuilderWithEncoding(b, carrow.PayloadTypes.ExpHistogramExemplarAttrs, cfg.Attrs.HistogramExemplar)
		ehistogramExemplarBuilder.(*ExemplarBuilder).SetAttributesAccumulator(eb.Accumulator())
		return eb
	}, observer)

	return &RelatedData{
		relatedRecordsManager: rrManager,
		attrsBuilders: &AttrsBuilders{
			resource:           resourceAttrsBuilder.(*carrow.Attrs16Builder),
			scope:              scopeAttrsBuilder.(*carrow.Attrs16Builder),
			summary:            summaryAttrsBuilder.(*carrow.Attrs32Builder),
			number_dp:          numberDPAttrsBuilder.(*carrow.Attrs32Builder),
			histogram:          histogramAttrsBuilder.(*carrow.Attrs32Builder),
			eHistogram:         ehistogramAttrsBuilder.(*carrow.Attrs32Builder),
			numberDPExemplar:   numberDPExemplarAttrsBuilder.(*carrow.Attrs32Builder),
			histogramExemplar:  histogramExemplarAttrsBuilder.(*carrow.Attrs32Builder),
			eHistogramExemplar: ehistogramExemplarAttrsBuilder.(*carrow.Attrs32Builder),
		},
		numberDPBuilder:           numberDPBuilder.(*DataPointBuilder),
		summaryDPBuilder:          summaryDPBuilder.(*SummaryDataPointBuilder),
		histogramDPBuilder:        histogramDPBuilder.(*HistogramDataPointBuilder),
		ehistogramDPBuilder:       ehistogramDPBuilder.(*EHistogramDataPointBuilder),
		numberDPExemplarBuilder:   numberDPExemplarBuilder.(*ExemplarBuilder),
		histogramExemplarBuilder:  histogramExemplarBuilder.(*ExemplarBuilder),
		ehistogramExemplarBuilder: ehistogramExemplarBuilder.(*ExemplarBuilder),
	}, nil
}

func (r *RelatedData) Schemas() []carrow.SchemaWithPayload {
	return r.relatedRecordsManager.Schemas()
}

func (r *RelatedData) Release() {
	r.relatedRecordsManager.Release()
}

func (r *RelatedData) RecordBuilderExt(payloadType *carrow.PayloadType) *builder.RecordBuilderExt {
	return r.relatedRecordsManager.RecordBuilderExt(payloadType)
}

func (r *RelatedData) AttrsBuilders() *AttrsBuilders {
	return r.attrsBuilders
}

func (r *RelatedData) NumberDPBuilder() *DataPointBuilder {
	return r.numberDPBuilder
}

func (r *RelatedData) SummaryDPBuilder() *SummaryDataPointBuilder {
	return r.summaryDPBuilder
}

func (r *RelatedData) HistogramDPBuilder() *HistogramDataPointBuilder {
	return r.histogramDPBuilder
}

func (r *RelatedData) EHistogramDPBuilder() *EHistogramDataPointBuilder {
	return r.ehistogramDPBuilder
}

func (r *RelatedData) NumberDPExemplarBuilder() *ExemplarBuilder {
	return r.numberDPExemplarBuilder
}

func (r *RelatedData) HistogramExemplarBuilder() *ExemplarBuilder {
	return r.histogramExemplarBuilder
}

func (r *RelatedData) EHistogramExemplarBuilder() *ExemplarBuilder {
	return r.ehistogramExemplarBuilder
}

func (r *RelatedData) Reset() {
	r.nextMetricScopeID = 0
	r.relatedRecordsManager.Reset()
}

func (r *RelatedData) NextMetricScopeID() uint16 {
	c := r.nextMetricScopeID

	if c == math.MaxUint16 {
		panic("maximum number of scope metrics reached per batch, please reduce the batch size to a maximum of 65535 metrics")
	}

	r.nextMetricScopeID++
	return uint16(c)
}

func (r *RelatedData) BuildRecordMessages() ([]*record_message.RecordMessage, error) {
	return r.relatedRecordsManager.BuildRecordMessages()
}

func (ab *AttrsBuilders) Release() {
	ab.resource.Release()
	ab.scope.Release()

	ab.number_dp.Release()
	ab.summary.Release()
	ab.histogram.Release()
	ab.eHistogram.Release()
	ab.numberDPExemplar.Release()
	ab.histogramExemplar.Release()
	ab.eHistogramExemplar.Release()
}

func (ab *AttrsBuilders) Resource() *carrow.Attrs16Builder {
	return ab.resource
}

func (ab *AttrsBuilders) Scope() *carrow.Attrs16Builder {
	return ab.scope
}

func (ab *AttrsBuilders) Reset() {
	ab.resource.Accumulator().Reset()
	ab.scope.Accumulator().Reset()
	ab.number_dp.Reset()
	ab.summary.Reset()
	ab.histogram.Reset()
	ab.eHistogram.Reset()
	ab.numberDPExemplar.Reset()
	ab.histogramExemplar.Reset()
	ab.eHistogramExemplar.Reset()
}
