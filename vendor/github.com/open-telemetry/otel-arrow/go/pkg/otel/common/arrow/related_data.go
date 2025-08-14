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

// A `related data` is an entity that is attached or related to a another entity.
// For example, `attributes` are related to `resource`, `span`, ...

import (
	"github.com/apache/arrow-go/v18/arrow"

	colarspb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	cfg "github.com/open-telemetry/otel-arrow/go/pkg/config"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	config "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/config"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/observer"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/stats"
	"github.com/open-telemetry/otel-arrow/go/pkg/record_message"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

type (
	// RelatedRecordBuilder is the common interface for all related record
	// builders.
	RelatedRecordBuilder interface {
		IsEmpty() bool
		Build() (arrow.Record, error)
		SchemaID() string
		Schema() *arrow.Schema
		PayloadType() *PayloadType
		Reset()
		Release()
	}

	// RelatedRecordsManager manages all related record builders for a given
	// main OTel entity.
	RelatedRecordsManager struct {
		cfg   *cfg.Config
		stats *stats.ProducerStats

		builders    []RelatedRecordBuilder
		builderExts []*builder.RecordBuilderExt

		schemas []SchemaWithPayload
	}

	// PayloadType wraps the protobuf payload type generated from the protobuf
	// definition and adds a prefix to it.
	PayloadType struct {
		prefix      string
		payloadType record_message.PayloadType
	}

	// All the payload types currently supported by the adapter.
	payloadTypes struct {
		Metrics *PayloadType
		Logs    *PayloadType
		Spans   *PayloadType

		ResourceAttrs                *PayloadType
		ScopeAttrs                   *PayloadType
		Metric                       *PayloadType
		NumberDataPoints             *PayloadType
		NumberDataPointAttrs         *PayloadType
		NumberDataPointExemplars     *PayloadType
		NumberDataPointExemplarAttrs *PayloadType
		Summary                      *PayloadType
		SummaryAttrs                 *PayloadType
		Histogram                    *PayloadType
		HistogramAttrs               *PayloadType
		HistogramExemplars           *PayloadType
		HistogramExemplarAttrs       *PayloadType
		ExpHistogram                 *PayloadType
		ExpHistogramAttrs            *PayloadType
		ExpHistogramExemplars        *PayloadType
		ExpHistogramExemplarAttrs    *PayloadType
		LogRecordAttrs               *PayloadType
		SpanAttrs                    *PayloadType
		Event                        *PayloadType
		EventAttrs                   *PayloadType
		Link                         *PayloadType
		LinkAttrs                    *PayloadType
	}

	SchemaWithPayload struct {
		Schema            *arrow.Schema
		PayloadType       *PayloadType
		ParentPayloadType *PayloadType
	}
)

// All the payload types currently supported by the adapter and their specific
// prefix and protobuf payload type.

var (
	PayloadTypes = payloadTypes{
		Metrics: &PayloadType{
			prefix:      "metrics",
			payloadType: colarspb.ArrowPayloadType_UNIVARIATE_METRICS,
		},
		Logs: &PayloadType{
			prefix:      "logs",
			payloadType: colarspb.ArrowPayloadType_LOGS,
		},
		Spans: &PayloadType{
			prefix:      "spans",
			payloadType: colarspb.ArrowPayloadType_SPANS,
		},
		ResourceAttrs: &PayloadType{
			prefix:      "resource-attrs",
			payloadType: colarspb.ArrowPayloadType_RESOURCE_ATTRS,
		},
		ScopeAttrs: &PayloadType{
			prefix:      "scope-attrs",
			payloadType: colarspb.ArrowPayloadType_SCOPE_ATTRS,
		},
		NumberDataPoints: &PayloadType{
			prefix:      "number-dps",
			payloadType: colarspb.ArrowPayloadType_NUMBER_DATA_POINTS,
		},
		NumberDataPointAttrs: &PayloadType{
			prefix:      "number-dp-attrs",
			payloadType: colarspb.ArrowPayloadType_NUMBER_DP_ATTRS,
		},
		NumberDataPointExemplars: &PayloadType{
			prefix:      "number-dp-exemplars",
			payloadType: colarspb.ArrowPayloadType_NUMBER_DP_EXEMPLARS,
		},
		NumberDataPointExemplarAttrs: &PayloadType{
			prefix:      "number-dp-exemplar-attrs",
			payloadType: colarspb.ArrowPayloadType_NUMBER_DP_EXEMPLAR_ATTRS,
		},
		Summary: &PayloadType{
			prefix:      "summary-dps",
			payloadType: colarspb.ArrowPayloadType_SUMMARY_DATA_POINTS,
		},
		SummaryAttrs: &PayloadType{
			prefix:      "summary-dp-attrs",
			payloadType: colarspb.ArrowPayloadType_SUMMARY_DP_ATTRS,
		},
		Histogram: &PayloadType{
			prefix:      "histogram-dps",
			payloadType: colarspb.ArrowPayloadType_HISTOGRAM_DATA_POINTS,
		},
		HistogramAttrs: &PayloadType{
			prefix:      "histogram-dp-attrs",
			payloadType: colarspb.ArrowPayloadType_HISTOGRAM_DP_ATTRS,
		},
		HistogramExemplars: &PayloadType{
			prefix:      "histogram-dp-exemplars",
			payloadType: colarspb.ArrowPayloadType_HISTOGRAM_DP_EXEMPLARS,
		},
		HistogramExemplarAttrs: &PayloadType{
			prefix:      "histogram-dp-exemplar-attrs",
			payloadType: colarspb.ArrowPayloadType_HISTOGRAM_DP_EXEMPLAR_ATTRS,
		},
		ExpHistogram: &PayloadType{
			prefix:      "exp-histogram-dps",
			payloadType: colarspb.ArrowPayloadType_EXP_HISTOGRAM_DATA_POINTS,
		},
		ExpHistogramAttrs: &PayloadType{
			prefix:      "exp-histogram-dp-attrs",
			payloadType: colarspb.ArrowPayloadType_EXP_HISTOGRAM_DP_ATTRS,
		},
		ExpHistogramExemplars: &PayloadType{
			prefix:      "exp-histogram-dp-exemplars",
			payloadType: colarspb.ArrowPayloadType_EXP_HISTOGRAM_DP_EXEMPLARS,
		},
		ExpHistogramExemplarAttrs: &PayloadType{
			prefix:      "exp-histogram-dp-exemplar-attrs",
			payloadType: colarspb.ArrowPayloadType_EXP_HISTOGRAM_DP_EXEMPLAR_ATTRS,
		},
		LogRecordAttrs: &PayloadType{
			prefix:      "logs-attrs",
			payloadType: colarspb.ArrowPayloadType_LOG_ATTRS,
		},
		SpanAttrs: &PayloadType{
			prefix:      "span-attrs",
			payloadType: colarspb.ArrowPayloadType_SPAN_ATTRS,
		},
		Event: &PayloadType{
			prefix:      "span-event",
			payloadType: colarspb.ArrowPayloadType_SPAN_EVENTS,
		},
		EventAttrs: &PayloadType{
			prefix:      "span-event-attrs",
			payloadType: colarspb.ArrowPayloadType_SPAN_EVENT_ATTRS,
		},
		Link: &PayloadType{
			prefix:      "span-link",
			payloadType: colarspb.ArrowPayloadType_SPAN_LINKS,
		},
		LinkAttrs: &PayloadType{
			prefix:      "span-link-attrs",
			payloadType: colarspb.ArrowPayloadType_SPAN_LINK_ATTRS,
		},
	}
)

func NewRelatedRecordsManager(cfg *cfg.Config, stats *stats.ProducerStats) *RelatedRecordsManager {
	return &RelatedRecordsManager{
		cfg:         cfg,
		stats:       stats,
		builders:    make([]RelatedRecordBuilder, 0),
		builderExts: make([]*builder.RecordBuilderExt, 0),
	}
}

func (m *RelatedRecordsManager) Declare(
	payloadType *PayloadType,
	parentPayloadType *PayloadType,
	schema *arrow.Schema,
	rrBuilder func(b *builder.RecordBuilderExt) RelatedRecordBuilder,
	observer observer.ProducerObserver,
) RelatedRecordBuilder {
	builderExt := builder.NewRecordBuilderExt(
		m.cfg.Pool,
		schema,
		config.NewDictionary(m.cfg.LimitIndexSize, m.cfg.DictResetThreshold),
		m.stats,
		observer,
	)
	builderExt.SetLabel(payloadType.SchemaPrefix())
	rBuilder := rrBuilder(builderExt)
	m.builders = append(m.builders, rBuilder)
	m.builderExts = append(m.builderExts, builderExt)
	m.schemas = append(m.schemas, SchemaWithPayload{
		Schema:            schema,
		PayloadType:       payloadType,
		ParentPayloadType: parentPayloadType,
	})
	return rBuilder
}

func (m *RelatedRecordsManager) BuildRecordMessages() ([]*record_message.RecordMessage, error) {
	recordMessages := make([]*record_message.RecordMessage, 0, len(m.builders))
	for _, b := range m.builders {
		if b.IsEmpty() {
			continue
		}
		record, err := b.Build()
		if err != nil {
			return nil, werror.WrapWithContext(
				err,
				map[string]interface{}{"schema_prefix": b.PayloadType().SchemaPrefix()},
			)
		}
		schemaID := b.PayloadType().SchemaPrefix() + ":" + b.SchemaID()
		relatedDataMessage := record_message.NewRelatedDataMessage(schemaID, record, b.PayloadType().PayloadType())
		recordMessages = append(recordMessages, relatedDataMessage)
	}
	return recordMessages, nil
}

func (m *RelatedRecordsManager) Schemas() []SchemaWithPayload {
	return m.schemas
}

func (m *RelatedRecordsManager) Reset() {
	for _, b := range m.builders {
		b.Reset()
	}
}

func (m *RelatedRecordsManager) Release() {
	for _, b := range m.builders {
		b.Release()
	}
	// m.builderExts are not released because the releasable resources are
	// already released by the builders.
}

func (m *RelatedRecordsManager) RecordBuilderExt(payloadType *PayloadType) *builder.RecordBuilderExt {
	for i, b := range m.builders {
		if b.PayloadType() == payloadType {
			return m.builderExts[i]
		}
	}
	return nil
}

func (p *PayloadType) SchemaPrefix() string {
	return p.prefix
}

func (p *PayloadType) PayloadType() record_message.PayloadType {
	return p.payloadType
}
