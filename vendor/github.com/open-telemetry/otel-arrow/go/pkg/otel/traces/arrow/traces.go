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

import (
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/collector/pdata/ptrace"

	acommon "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/observer"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/stats"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

var (
	// TracesSchema is the Arrow schema for the OTLP Arrow Traces record.
	TracesSchema = arrow.NewSchema([]arrow.Field{
		{Name: constants.ID, Type: arrow.PrimitiveTypes.Uint16, Metadata: schema.Metadata(schema.DeltaEncoding), Nullable: true},
		{Name: constants.Resource, Type: acommon.ResourceDT, Nullable: true},
		{Name: constants.Scope, Type: acommon.ScopeDT, Nullable: true},
		// This schema URL applies to the span and span events (the schema URL
		// for the resource is in the resource struct).
		{Name: constants.SchemaUrl, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8), Nullable: true},
		{Name: constants.StartTimeUnixNano, Type: arrow.FixedWidthTypes.Timestamp_ns},
		{Name: constants.DurationTimeUnixNano, Type: arrow.FixedWidthTypes.Duration_ns, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.TraceId, Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}},
		{Name: constants.SpanId, Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}},
		{Name: constants.TraceState, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8), Nullable: true},
		{Name: constants.ParentSpanId, Type: &arrow.FixedSizeBinaryType{ByteWidth: 8}, Nullable: true},
		{Name: constants.Name, Type: arrow.BinaryTypes.String, Metadata: schema.Metadata(schema.Dictionary8)},
		{Name: constants.KIND, Type: arrow.PrimitiveTypes.Int32, Metadata: schema.Metadata(schema.Dictionary8), Nullable: true},
		{Name: constants.DroppedAttributesCount, Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: constants.DroppedEventsCount, Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: constants.DroppedLinksCount, Type: arrow.PrimitiveTypes.Uint32, Nullable: true},
		{Name: constants.Status, Type: StatusDT, Nullable: true},
	}, nil)
)

// TracesBuilder is a helper to build a list of resource spans.
type TracesBuilder struct {
	released bool

	builder *builder.RecordBuilderExt // Record builder

	rb    *acommon.ResourceBuilder        // `resource` builder
	scb   *acommon.ScopeBuilder           // `scope` builder
	sschb *builder.StringBuilder          // scope `schema_url` builder
	ib    *builder.Uint16DeltaBuilder     //  id builder
	stunb *builder.TimestampBuilder       // start time unix nano builder
	dtunb *builder.DurationBuilder        // duration time unix nano builder
	tib   *builder.FixedSizeBinaryBuilder // trace id builder
	sib   *builder.FixedSizeBinaryBuilder // span id builder
	tsb   *builder.StringBuilder          // trace state builder
	psib  *builder.FixedSizeBinaryBuilder // parent span id builder
	nb    *builder.StringBuilder          // name builder
	kb    *builder.Int32Builder           // kind builder
	dacb  *builder.Uint32Builder          // dropped attributes count builder
	decb  *builder.Uint32Builder          // dropped events count builder
	dlcb  *builder.Uint32Builder          // dropped links count builder
	sb    *StatusBuilder                  // status builder

	optimizer *TracesOptimizer
	analyzer  *TracesAnalyzer

	relatedData *RelatedData
}

// NewTracesBuilder creates a new TracesBuilder.
//
// Important Note: This function doesn't take ownership of the rBuilder parameter.
// The caller is responsible for releasing it
func NewTracesBuilder(
	rBuilder *builder.RecordBuilderExt,
	cfg *Config,
	stats *stats.ProducerStats,
	observer observer.ProducerObserver,
) (*TracesBuilder, error) {
	var optimizer *TracesOptimizer
	var analyzer *TracesAnalyzer

	relatedData, err := NewRelatedData(cfg, stats, observer)
	if err != nil {
		panic(err)
	}

	if stats.SchemaStats {
		optimizer = NewTracesOptimizer(cfg.Span.Sorter)
		analyzer = NewTraceAnalyzer()
	} else {
		optimizer = NewTracesOptimizer(cfg.Span.Sorter)
	}

	b := &TracesBuilder{
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

func (b *TracesBuilder) init() error {
	ib := b.builder.Uint16DeltaBuilder(constants.ID)
	// As traces are sorted before insertion, the delta between two
	// consecutive attributes ID should always be <=1.
	ib.SetMaxDelta(1)

	b.ib = ib
	b.rb = acommon.ResourceBuilderFrom(b.builder.StructBuilder(constants.Resource))
	b.scb = acommon.ScopeBuilderFrom(b.builder.StructBuilder(constants.Scope))
	b.sschb = b.builder.StringBuilder(constants.SchemaUrl)

	b.stunb = b.builder.TimestampBuilder(constants.StartTimeUnixNano)
	b.dtunb = b.builder.DurationBuilder(constants.DurationTimeUnixNano)
	b.tib = b.builder.FixedSizeBinaryBuilder(constants.TraceId)
	b.sib = b.builder.FixedSizeBinaryBuilder(constants.SpanId)
	b.tsb = b.builder.StringBuilder(constants.TraceState)
	b.psib = b.builder.FixedSizeBinaryBuilder(constants.ParentSpanId)
	b.nb = b.builder.StringBuilder(constants.Name)
	b.kb = b.builder.Int32Builder(constants.KIND)
	b.dacb = b.builder.Uint32Builder(constants.DroppedAttributesCount)
	b.decb = b.builder.Uint32Builder(constants.DroppedEventsCount)
	b.dlcb = b.builder.Uint32Builder(constants.DroppedLinksCount)
	b.sb = StatusBuilderFrom(b.builder.StructBuilder(constants.Status))

	return nil
}

func (b *TracesBuilder) RelatedData() *RelatedData {
	return b.relatedData
}

// Build builds an Arrow Record from the builder.
//
// Once the array is no longer needed, Release() must be called to free the
// memory allocated by the record.
//
// This method returns a DictionaryOverflowError if the cardinality of a dictionary
// (or several) exceeds the maximum allowed value.
func (b *TracesBuilder) Build() (record arrow.Record, err error) {
	if b.released {
		return nil, werror.Wrap(acommon.ErrBuilderAlreadyReleased)
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

// Append appends a new set of resource spans to the builder.
func (b *TracesBuilder) Append(traces ptrace.Traces) error {
	if b.released {
		return werror.Wrap(acommon.ErrBuilderAlreadyReleased)
	}

	optimTraces := b.optimizer.Optimize(traces)
	if b.analyzer != nil {
		b.analyzer.Analyze(optimTraces)
		b.analyzer.ShowStats("")
	}

	spanID := uint16(0)
	resID := int64(-1)
	scopeID := int64(-1)
	var resSpanID, scopeSpanID string
	var err error

	attrsAccu := b.relatedData.AttrsBuilders().Span().Accumulator()
	eventsAccu := b.relatedData.EventBuilder().Accumulator()
	linksAccu := b.relatedData.LinkBuilder().Accumulator()

	b.builder.Reserve(len(optimTraces.Spans))

	for _, span := range optimTraces.Spans {
		spanAttrs := span.Span.Attributes()
		spanEvents := span.Span.Events()
		spanLinks := span.Span.Links()

		ID := spanID
		if spanAttrs.Len() == 0 && spanEvents.Len() == 0 && spanLinks.Len() == 0 {
			// No related data found
			b.ib.AppendNull()
		} else {
			b.ib.Append(ID)
			spanID++
		}

		// === Process resource and schema URL ===
		resAttrs := span.Resource.Attributes()
		if resSpanID != span.ResourceSpanID {
			// New resource ID detected =>
			// - Increment the resource ID
			// - Append the resource attributes to the resource attributes accumulator
			resSpanID = span.ResourceSpanID
			resID++
			err = b.relatedData.AttrsBuilders().Resource().Accumulator().
				AppendWithID(uint16(resID), resAttrs)
			if err != nil {
				return werror.Wrap(err)
			}
		}
		// Check resID validity
		if resID == -1 || resID > math.MaxUint16 {
			return werror.WrapWithContext(acommon.ErrInvalidResourceID, map[string]interface{}{
				"resource_id": resID,
			})
		}
		// Append the resource schema URL if exists
		if err = b.rb.Append(resID, span.Resource, span.ResourceSchemaUrl); err != nil {
			return werror.Wrap(err)
		}

		// === Process scope and schema URL ===
		scopeAttrs := span.Scope.Attributes()
		if scopeSpanID != span.ScopeSpanID {
			// New scope ID detected =>
			// - Increment the scope ID
			// - Append the scope attributes to the scope attributes accumulator
			scopeSpanID = span.ScopeSpanID
			scopeID++
			err = b.relatedData.AttrsBuilders().scope.Accumulator().
				AppendWithID(uint16(scopeID), scopeAttrs)
			if err != nil {
				return werror.Wrap(err)
			}
		}
		// Check scopeID validity
		if scopeID == -1 || scopeID > math.MaxUint16 {
			return werror.WrapWithContext(acommon.ErrInvalidScopeID, map[string]interface{}{
				"scope_id": scopeID,
			})
		}
		// Append the scope name, version, and schema URL (if exists)
		if err = b.scb.Append(scopeID, span.Scope); err != nil {
			return werror.Wrap(err)
		}
		b.sschb.AppendNonEmpty(span.ScopeSchemaUrl)

		// === Process span ===
		b.stunb.Append(arrow.Timestamp(span.Span.StartTimestamp()))
		duration := span.Span.EndTimestamp().AsTime().Sub(span.Span.StartTimestamp().AsTime()).Nanoseconds()
		b.dtunb.Append(arrow.Duration(duration))
		tib := span.Span.TraceID()
		b.tib.Append(tib[:])
		sib := span.Span.SpanID()
		b.sib.Append(sib[:])
		b.tsb.AppendNonEmpty(span.Span.TraceState().AsRaw())
		psib := span.Span.ParentSpanID()
		if psib.IsEmpty() {
			b.psib.AppendNull()
		} else {
			b.psib.Append(psib[:])
		}
		b.nb.AppendNonEmpty(span.Span.Name())
		b.kb.AppendNonZero(int32(span.Span.Kind()))

		// Span Attributes
		if spanAttrs.Len() > 0 {
			err = attrsAccu.AppendWithID(ID, spanAttrs)
			if err != nil {
				return werror.Wrap(err)
			}
		}
		b.dacb.AppendNonZero(span.Span.DroppedAttributesCount())

		// Events
		if spanEvents.Len() > 0 {
			err = eventsAccu.Append(ID, spanEvents)
			if err != nil {
				return werror.Wrap(err)
			}
		}
		b.decb.AppendNonZero(span.Span.DroppedEventsCount())

		// Links
		if spanLinks.Len() > 0 {
			err = linksAccu.Append(ID, spanLinks)
			if err != nil {
				return werror.Wrap(err)
			}
		}
		b.dlcb.AppendNonZero(span.Span.DroppedLinksCount())

		if err = b.sb.Append(span.Span.Status()); err != nil {
			return werror.Wrap(err)
		}
	}
	return nil
}

// Release releases the memory allocated by the builder.
func (b *TracesBuilder) Release() {
	if !b.released {
		// b.builder is a shared resource, we don't own it so we don't release it.

		b.relatedData.Release()
		b.released = true
	}
}

func (b *TracesBuilder) ShowSchema() {
	b.builder.ShowSchema()
}
