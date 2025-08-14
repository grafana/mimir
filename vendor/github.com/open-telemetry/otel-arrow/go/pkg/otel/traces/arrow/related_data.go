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

// Infrastructure to manage span related records.

import (
	"math"

	carrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/observer"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/stats"
	"github.com/open-telemetry/otel-arrow/go/pkg/record_message"
)

type (
	// RelatedData is a collection of related/dependent data to span entities.
	RelatedData struct {
		spanCount uint64

		relatedRecordsManager *carrow.RelatedRecordsManager

		attrsBuilders *AttrsBuilders
		eventBuilder  *EventBuilder
		linkBuilder   *LinkBuilder
	}

	// AttrsBuilders groups together AttrsBuilder instances used to build related
	// data attributes (i.e. resource attributes, scope attributes, span attributes,
	// event attributes, and link attributes).
	AttrsBuilders struct {
		resource *carrow.Attrs16Builder
		scope    *carrow.Attrs16Builder
		span     *carrow.Attrs16Builder
		event    *carrow.Attrs32Builder
		link     *carrow.Attrs32Builder
	}
)

func NewRelatedData(cfg *Config, stats *stats.ProducerStats, observer observer.ProducerObserver) (*RelatedData, error) {
	rrManager := carrow.NewRelatedRecordsManager(cfg.Global, stats)

	attrsResourceBuilder := rrManager.Declare(carrow.PayloadTypes.ResourceAttrs, carrow.PayloadTypes.Spans, carrow.AttrsSchema16, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return carrow.NewAttrs16BuilderWithEncoding(b, carrow.PayloadTypes.ResourceAttrs, cfg.Attrs.Resource)
	}, observer)

	attrsScopeBuilder := rrManager.Declare(carrow.PayloadTypes.ScopeAttrs, carrow.PayloadTypes.Spans, carrow.AttrsSchema16, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return carrow.NewAttrs16BuilderWithEncoding(b, carrow.PayloadTypes.ScopeAttrs, cfg.Attrs.Scope)
	}, observer)

	attrsSpanBuilder := rrManager.Declare(carrow.PayloadTypes.SpanAttrs, carrow.PayloadTypes.Spans, carrow.AttrsSchema16, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return carrow.NewAttrs16BuilderWithEncoding(b, carrow.PayloadTypes.SpanAttrs, cfg.Attrs.Span)
	}, observer)

	eventBuilder := rrManager.Declare(carrow.PayloadTypes.Event, carrow.PayloadTypes.Spans, EventSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewEventBuilder(b, cfg.Event)
	}, observer)

	linkBuilder := rrManager.Declare(carrow.PayloadTypes.Link, carrow.PayloadTypes.Spans, LinkSchema, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return NewLinkBuilder(b, cfg.Link)
	}, observer)

	attrsEventBuilder := rrManager.Declare(carrow.PayloadTypes.EventAttrs, carrow.PayloadTypes.Event, carrow.AttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		ab := carrow.NewAttrs32BuilderWithEncoding(b, carrow.PayloadTypes.EventAttrs, cfg.Attrs.Event)
		eventBuilder.(*EventBuilder).SetAttributesAccumulator(ab.Accumulator())
		return ab
	}, observer)

	attrsLinkBuilder := rrManager.Declare(carrow.PayloadTypes.LinkAttrs, carrow.PayloadTypes.Link, carrow.AttrsSchema32, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		ab := carrow.NewAttrs32BuilderWithEncoding(b, carrow.PayloadTypes.LinkAttrs, cfg.Attrs.Link)
		linkBuilder.(*LinkBuilder).SetAttributesAccumulator(ab.Accumulator())
		return ab
	}, observer)

	return &RelatedData{
		relatedRecordsManager: rrManager,
		attrsBuilders: &AttrsBuilders{
			resource: attrsResourceBuilder.(*carrow.Attrs16Builder),
			scope:    attrsScopeBuilder.(*carrow.Attrs16Builder),
			span:     attrsSpanBuilder.(*carrow.Attrs16Builder),
			event:    attrsEventBuilder.(*carrow.Attrs32Builder),
			link:     attrsLinkBuilder.(*carrow.Attrs32Builder),
		},
		eventBuilder: eventBuilder.(*EventBuilder),
		linkBuilder:  linkBuilder.(*LinkBuilder),
	}, nil
}

func (r *RelatedData) Schemas() []carrow.SchemaWithPayload {
	return r.relatedRecordsManager.Schemas()
}

func (r *RelatedData) Release() {
	r.relatedRecordsManager.Release()
}

func (r *RelatedData) AttrsBuilders() *AttrsBuilders {
	return r.attrsBuilders
}

func (r *RelatedData) EventBuilder() *EventBuilder {
	return r.eventBuilder
}

func (r *RelatedData) LinkBuilder() *LinkBuilder {
	return r.linkBuilder
}

func (r *RelatedData) RecordBuilderExt(payloadType *carrow.PayloadType) *builder.RecordBuilderExt {
	return r.relatedRecordsManager.RecordBuilderExt(payloadType)
}

func (r *RelatedData) Reset() {
	r.spanCount = 0
	r.relatedRecordsManager.Reset()
}

func (r *RelatedData) SpanCount() uint16 {
	return uint16(r.spanCount)
}

func (r *RelatedData) NextSpanID() uint16 {
	sc := r.spanCount

	if sc == math.MaxUint16 {
		panic("maximum number of spans reached per batch, please reduce the batch size to a maximum of 65535 spans")
	}

	r.spanCount++
	return uint16(sc)
}

func (r *RelatedData) BuildRecordMessages() ([]*record_message.RecordMessage, error) {
	return r.relatedRecordsManager.BuildRecordMessages()
}

func (ab *AttrsBuilders) Resource() *carrow.Attrs16Builder {
	return ab.resource
}

func (ab *AttrsBuilders) Scope() *carrow.Attrs16Builder {
	return ab.scope
}

func (ab *AttrsBuilders) Span() *carrow.Attrs16Builder {
	return ab.span
}

func (ab *AttrsBuilders) Event() *carrow.Attrs32Builder {
	return ab.event
}

func (ab *AttrsBuilders) Link() *carrow.Attrs32Builder {
	return ab.link
}
