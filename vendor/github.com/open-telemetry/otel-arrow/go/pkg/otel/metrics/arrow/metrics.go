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

import (
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/collector/pdata/pmetric"

	carrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/observer"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/stats"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

// MetricsSchema is the Arrow schema for the OTLP Arrow Metrics record.
var (
	MetricsSchema = arrow.NewSchema([]arrow.Field{
		{Name: constants.ID, Type: arrow.PrimitiveTypes.Uint16, Metadata: schema.Metadata(schema.DeltaEncoding)},
		{Name: constants.Resource, Type: carrow.ResourceDT, Metadata: schema.Metadata(schema.Optional)},
		{Name: constants.Scope, Type: carrow.ScopeDT, Metadata: schema.Metadata(schema.Optional)},
		// This schema URL applies to the span and span events (the schema URL
		// for the resource is in the resource struct).
		{Name: constants.SchemaUrl, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.MetricType, Type: arrow.PrimitiveTypes.Uint8},
		{Name: constants.Name, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.Description, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.Unit, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8)},
		{Name: constants.AggregationTemporality, Type: arrow.PrimitiveTypes.Int32, Metadata: schema.Metadata(schema.Optional, schema.Dictionary8), Nullable: true},
		{Name: constants.IsMonotonic, Type: arrow.FixedWidthTypes.Boolean, Metadata: schema.Metadata(schema.Optional), Nullable: true},
	}, nil)
)

// MetricsBuilder is a helper to build a list of resource metrics.
type MetricsBuilder struct {
	released bool

	builder *builder.RecordBuilderExt   // Record builder
	rb      *carrow.ResourceBuilder     // `resource` builder
	scb     *carrow.ScopeBuilder        // `scope` builder
	sschb   *builder.StringBuilder      // scope `schema_url` builder
	ib      *builder.Uint16DeltaBuilder //  id builder
	mtb     *builder.Uint8Builder       // metric type builder
	nb      *builder.StringBuilder      // metric name builder
	db      *builder.StringBuilder      // metric description builder
	ub      *builder.StringBuilder      // metric unit builder
	atb     *builder.Int32Builder       // aggregation temporality builder
	imb     *builder.BooleanBuilder     // is monotonic builder

	optimizer *MetricsOptimizer
	analyzer  *MetricsAnalyzer

	relatedData *RelatedData
}

// NewMetricsBuilder creates a new MetricsBuilder.
//
// Important Note: The rBuilder parameter will not be released by this
// MetricsBuilder as it's shared with other instances of metrics builders.
func NewMetricsBuilder(
	rBuilder *builder.RecordBuilderExt,
	cfg *Config,
	stats *stats.ProducerStats,
	observer observer.ProducerObserver,
) (*MetricsBuilder, error) {
	var optimizer *MetricsOptimizer
	var analyzer *MetricsAnalyzer

	relatedData, err := NewRelatedData(cfg, stats, observer)
	if err != nil {
		panic(err)
	}

	if stats.SchemaStats {
		optimizer = NewMetricsOptimizer(cfg.Metric.Sorter)
		analyzer = NewMetricsAnalyzer()
	} else {
		optimizer = NewMetricsOptimizer(cfg.Metric.Sorter)
	}

	b := &MetricsBuilder{
		released:    false,
		builder:     rBuilder,
		optimizer:   optimizer,
		analyzer:    analyzer,
		relatedData: relatedData,
	}

	if err := b.init(); err != nil {
		return nil, werror.Wrap(err)
	}

	return b, nil
}

func (b *MetricsBuilder) init() error {
	b.ib = b.builder.Uint16DeltaBuilder(constants.ID)
	// As metrics are sorted before insertion, the delta between two
	// consecutive attributes ID should always be <=1.
	b.ib.SetMaxDelta(1)

	b.rb = carrow.ResourceBuilderFrom(b.builder.StructBuilder(constants.Resource))
	b.scb = carrow.ScopeBuilderFrom(b.builder.StructBuilder(constants.Scope))
	b.sschb = b.builder.StringBuilder(constants.SchemaUrl)

	b.mtb = b.builder.Uint8Builder(constants.MetricType)
	b.nb = b.builder.StringBuilder(constants.Name)
	b.db = b.builder.StringBuilder(constants.Description)
	b.ub = b.builder.StringBuilder(constants.Unit)
	b.atb = b.builder.Int32Builder(constants.AggregationTemporality)
	b.imb = b.builder.BooleanBuilder(constants.IsMonotonic)

	return nil
}

func (b *MetricsBuilder) RecordBuilderExt() *builder.RecordBuilderExt {
	return b.builder
}

func (b *MetricsBuilder) RelatedData() *RelatedData {
	return b.relatedData
}

// Build builds an Arrow Record from the builder.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the record.
func (b *MetricsBuilder) Build() (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(carrow.ErrBuilderAlreadyReleased)
	}

	record, err = b.builder.NewRecord()
	if err != nil {
		initErr := b.init()
		if initErr != nil {
			err = werror.Wrap(initErr)
		}
	}

	return
}

// Append appends a new set of resource metrics to the builder.
func (b *MetricsBuilder) Append(metrics pmetric.Metrics) error {
	if b.released {
		return werror.Wrap(carrow.ErrBuilderAlreadyReleased)
	}

	optimizedMetrics := b.optimizer.Optimize(metrics)
	if b.analyzer != nil {
		b.analyzer.Analyze(optimizedMetrics)
		b.analyzer.ShowStats("")
	}

	metricID := uint16(0)
	resID := int64(-1)
	scopeID := int64(-1)
	var resMetricsID, scopeMetricsID string
	var err error

	b.builder.Reserve(len(optimizedMetrics.Metrics))

	for _, metric := range optimizedMetrics.Metrics {
		ID := metricID

		b.ib.Append(ID)
		metricID++

		// === Process resource and schema URL ===
		resAttrs := metric.Resource.Attributes()
		if resMetricsID != metric.ResourceMetricsID {
			// New resource ID detected =>
			// - Increment the resource ID
			// - Append the resource attributes to the resource attributes accumulator
			resMetricsID = metric.ResourceMetricsID
			resID++
			err = b.relatedData.AttrsBuilders().Resource().Accumulator().
				AppendWithID(uint16(resID), resAttrs)
			if err != nil {
				return werror.Wrap(err)
			}
		}
		// Check resID validity
		if resID == -1 || resID > math.MaxUint16 {
			return werror.WrapWithContext(carrow.ErrInvalidResourceID, map[string]interface{}{
				"resource_id": resID,
			})
		}
		// Append the resource schema URL if exists
		if err = b.rb.Append(resID, metric.Resource, metric.ResourceSchemaUrl); err != nil {
			return werror.Wrap(err)
		}

		// === Process scope and schema URL ===
		scopeAttrs := metric.Scope.Attributes()
		if scopeMetricsID != metric.ScopeMetricsID {
			// New scope ID detected =>
			// - Increment the scope ID
			// - Append the scope attributes to the scope attributes accumulator
			scopeMetricsID = metric.ScopeMetricsID
			scopeID++
			err = b.relatedData.AttrsBuilders().scope.Accumulator().
				AppendWithID(uint16(scopeID), scopeAttrs)
			if err != nil {
				return werror.Wrap(err)
			}
		}
		// Check scopeID validity
		if scopeID == -1 || scopeID > math.MaxUint16 {
			return werror.WrapWithContext(carrow.ErrInvalidScopeID, map[string]interface{}{
				"scope_id": scopeID,
			})
		}
		// Append the scope name, version, and schema URL (if exists)
		if err = b.scb.Append(scopeID, metric.Scope); err != nil {
			return werror.Wrap(err)
		}
		b.sschb.AppendNonEmpty(metric.ScopeSchemaUrl)

		// === Process metric ===
		// Metric type is an int32 in the proto spec, but we don't expect more
		// than 256 types, so we use an uint8 instead.
		b.mtb.Append(uint8(metric.Metric.Type()))
		b.nb.AppendNonEmpty(metric.Metric.Name())
		b.db.Append(metric.Metric.Description())
		b.ub.AppendNonEmpty(metric.Metric.Unit())

		switch metric.Metric.Type() {
		case pmetric.MetricTypeGauge:
			b.atb.AppendNull()
			b.imb.AppendNull()
			dps := metric.Metric.Gauge().DataPoints()
			for i := 0; i < dps.Len(); i++ {
				dp := dps.At(i)
				b.relatedData.NumberDPBuilder().Accumulator().Append(ID, &dp)
			}
		case pmetric.MetricTypeSum:
			sum := metric.Metric.Sum()
			b.atb.Append(int32(sum.AggregationTemporality()))
			b.imb.Append(sum.IsMonotonic())
			dps := sum.DataPoints()
			for i := 0; i < dps.Len(); i++ {
				dp := dps.At(i)
				b.relatedData.NumberDPBuilder().Accumulator().Append(ID, &dp)
			}
		case pmetric.MetricTypeSummary:
			b.atb.AppendNull()
			b.imb.AppendNull()
			dps := metric.Metric.Summary().DataPoints()
			b.relatedData.SummaryDPBuilder().Accumulator().Append(ID, dps)
		case pmetric.MetricTypeHistogram:
			histogram := metric.Metric.Histogram()
			b.atb.Append(int32(histogram.AggregationTemporality()))
			b.imb.AppendNull()
			dps := histogram.DataPoints()
			b.relatedData.HistogramDPBuilder().Accumulator().Append(ID, dps)
		case pmetric.MetricTypeExponentialHistogram:
			exponentialHistogram := metric.Metric.ExponentialHistogram()
			b.atb.Append(int32(exponentialHistogram.AggregationTemporality()))
			b.imb.AppendNull()
			dps := exponentialHistogram.DataPoints()
			b.relatedData.EHistogramDPBuilder().Accumulator().Append(ID, dps)
		case pmetric.MetricTypeEmpty:
			b.atb.AppendNull()
			b.imb.AppendNull()
		default:
			// ToDo should log and ignore unknown metric types.
		}
	}
	return nil
}

// Release releases the memory allocated by the builder.
func (b *MetricsBuilder) Release() {
	if !b.released {
		// b.builder is a shared resource, so we don't release it here.

		b.relatedData.Release()
		b.released = true
	}
}

func (b *MetricsBuilder) ShowSchema() {
	b.builder.ShowSchema()
}
