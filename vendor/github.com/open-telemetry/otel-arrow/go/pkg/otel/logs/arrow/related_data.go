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

// Infrastructure to manage related records.

import (
	carrow "github.com/open-telemetry/otel-arrow/go/pkg/otel/common/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/schema/builder"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/observer"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/stats"
	"github.com/open-telemetry/otel-arrow/go/pkg/record_message"
)

type (
	// RelatedData is a collection of related/dependent data to log record
	// entities.
	RelatedData struct {
		relatedRecordsManager *carrow.RelatedRecordsManager

		attrsBuilders *AttrsBuilders
	}

	// AttrsBuilders groups together AttrsBuilder instances used to build related
	// data attributes (i.e. resource attributes, scope attributes, and log record
	// attributes.
	AttrsBuilders struct {
		resource  *carrow.Attrs16Builder
		scope     *carrow.Attrs16Builder
		logRecord *carrow.Attrs16Builder
	}
)

func NewRelatedData(cfg *Config, stats *stats.ProducerStats, observer observer.ProducerObserver) (*RelatedData, error) {
	rrManager := carrow.NewRelatedRecordsManager(cfg.Global, stats)

	attrsResourceBuilder := rrManager.Declare(carrow.PayloadTypes.ResourceAttrs, carrow.PayloadTypes.Logs, carrow.AttrsSchema16, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return carrow.NewAttrs16BuilderWithEncoding(b, carrow.PayloadTypes.ResourceAttrs, cfg.Attrs.Resource)
	}, observer)

	attrsScopeBuilder := rrManager.Declare(carrow.PayloadTypes.ScopeAttrs, carrow.PayloadTypes.Logs, carrow.AttrsSchema16, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return carrow.NewAttrs16BuilderWithEncoding(b, carrow.PayloadTypes.ScopeAttrs, cfg.Attrs.Scope)
	}, observer)

	attrsLogRecordBuilder := rrManager.Declare(carrow.PayloadTypes.LogRecordAttrs, carrow.PayloadTypes.Logs, carrow.AttrsSchema16, func(b *builder.RecordBuilderExt) carrow.RelatedRecordBuilder {
		return carrow.NewAttrs16BuilderWithEncoding(b, carrow.PayloadTypes.LogRecordAttrs, cfg.Attrs.Log)
	}, observer)

	return &RelatedData{
		relatedRecordsManager: rrManager,
		attrsBuilders: &AttrsBuilders{
			resource:  attrsResourceBuilder.(*carrow.Attrs16Builder),
			scope:     attrsScopeBuilder.(*carrow.Attrs16Builder),
			logRecord: attrsLogRecordBuilder.(*carrow.Attrs16Builder),
		},
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

func (r *RelatedData) RecordBuilderExt(payloadType *carrow.PayloadType) *builder.RecordBuilderExt {
	return r.relatedRecordsManager.RecordBuilderExt(payloadType)
}

func (r *RelatedData) Reset() {
	r.relatedRecordsManager.Reset()
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

func (ab *AttrsBuilders) LogRecord() *carrow.Attrs16Builder {
	return ab.logRecord
}
