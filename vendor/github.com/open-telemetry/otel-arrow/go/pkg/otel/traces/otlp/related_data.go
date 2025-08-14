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

package otlp

// Infrastructure used to process related records.

import (
	colarspb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/otlp"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/traces/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/record_message"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

type (
	RelatedData struct {
		SpanID                uint16
		ResAttrMapStore       *otlp.AttributesStore[uint16]
		ScopeAttrMapStore     *otlp.AttributesStore[uint16]
		SpanAttrMapStore      *otlp.AttributesStore[uint16]
		SpanEventAttrMapStore *otlp.AttributesStore[uint32]
		SpanLinkAttrMapStore  *otlp.AttributesStore[uint32]
		SpanEventsStore       *SpanEventsStore
		SpanLinksStore        *SpanLinksStore
	}
)

func NewRelatedData(conf *arrow.Config) *RelatedData {
	return &RelatedData{
		ResAttrMapStore:       otlp.NewAttributesStore[uint16](),
		ScopeAttrMapStore:     otlp.NewAttributesStore[uint16](),
		SpanAttrMapStore:      otlp.NewAttributesStore[uint16](),
		SpanEventAttrMapStore: otlp.NewAttributesStore[uint32](),
		SpanLinkAttrMapStore:  otlp.NewAttributesStore[uint32](),
		SpanEventsStore:       NewSpanEventsStore(conf.Event),
		SpanLinksStore:        NewSpanLinksStore(),
	}
}

func (r *RelatedData) SpanIDFromDelta(delta uint16) uint16 {
	r.SpanID += delta
	return r.SpanID
}

func RelatedDataFrom(records []*record_message.RecordMessage, conf *arrow.Config) (relatedData *RelatedData, tracesRecord *record_message.RecordMessage, err error) {
	defer func() {
		for _, record := range records {
			record.Record().Release()
		}
	}()

	var spanEventRecord *record_message.RecordMessage
	var spanLinkRecord *record_message.RecordMessage

	relatedData = NewRelatedData(conf)

	// Scan the records to find the traces record and the span event record.
	// Create the attribute map stores for all the attribute records.
	for _, record := range records {
		switch record.PayloadType() {
		case colarspb.ArrowPayloadType_RESOURCE_ATTRS:
			err = otlp.AttributesStoreFrom[uint16](
				record.Record(),
				relatedData.ResAttrMapStore,
			)
			if err != nil {
				return nil, nil, werror.Wrap(err)
			}
		case colarspb.ArrowPayloadType_SCOPE_ATTRS:
			err = otlp.AttributesStoreFrom[uint16](record.Record(), relatedData.ScopeAttrMapStore)
			if err != nil {
				return nil, nil, werror.Wrap(err)
			}
		case colarspb.ArrowPayloadType_SPAN_ATTRS:
			err = otlp.AttributesStoreFrom[uint16](record.Record(), relatedData.SpanAttrMapStore)
			if err != nil {
				return nil, nil, werror.Wrap(err)
			}
		case colarspb.ArrowPayloadType_SPAN_EVENTS:
			if spanEventRecord != nil {
				return nil, nil, werror.Wrap(otel.ErrMultipleSpanEventsRecords)
			}
			spanEventRecord = record
		case colarspb.ArrowPayloadType_SPAN_EVENT_ATTRS:
			err = otlp.AttributesStoreFrom[uint32](record.Record(), relatedData.SpanEventAttrMapStore)
			if err != nil {
				return nil, nil, werror.Wrap(err)
			}
		case colarspb.ArrowPayloadType_SPAN_LINKS:
			if spanLinkRecord != nil {
				return nil, nil, werror.Wrap(otel.ErrMultipleSpanEventsRecords)
			}
			spanLinkRecord = record
		case colarspb.ArrowPayloadType_SPAN_LINK_ATTRS:
			err = otlp.AttributesStoreFrom[uint32](record.Record(), relatedData.SpanLinkAttrMapStore)
			if err != nil {
				return nil, nil, werror.Wrap(err)
			}
		case colarspb.ArrowPayloadType_SPANS:
			if tracesRecord != nil {
				return nil, nil, werror.Wrap(otel.ErrMultipleTracesRecords)
			}
			tracesRecord = record
		default:
			return nil, nil, werror.Wrap(otel.UnknownPayloadType)
		}
	}

	if spanEventRecord != nil {
		relatedData.SpanEventsStore, err = SpanEventsStoreFrom(
			spanEventRecord.Record(),
			relatedData.SpanEventAttrMapStore,
			conf.Event,
		)
		if err != nil {
			return nil, nil, werror.Wrap(err)
		}
	}

	if spanLinkRecord != nil {
		relatedData.SpanLinksStore, err = SpanLinksStoreFrom(
			spanLinkRecord.Record(),
			relatedData.SpanLinkAttrMapStore,
			conf.Link,
		)
		if err != nil {
			return nil, nil, werror.Wrap(err)
		}
	}

	return
}
