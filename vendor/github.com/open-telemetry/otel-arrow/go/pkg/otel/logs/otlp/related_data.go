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

import (
	colarspb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/otlp"
	"github.com/open-telemetry/otel-arrow/go/pkg/record_message"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

// Infrastructure used to process related records.

type (
	RelatedData struct {
		LogRecordID           uint16
		ResAttrMapStore       *otlp.AttributesStore[uint16]
		ScopeAttrMapStore     *otlp.AttributesStore[uint16]
		LogRecordAttrMapStore *otlp.AttributesStore[uint16]
	}
)

func NewRelatedData() *RelatedData {
	return &RelatedData{
		ResAttrMapStore:       otlp.NewAttributesStore[uint16](),
		ScopeAttrMapStore:     otlp.NewAttributesStore[uint16](),
		LogRecordAttrMapStore: otlp.NewAttributesStore[uint16](),
	}
}

func (r *RelatedData) LogRecordIDFromDelta(delta uint16) uint16 {
	r.LogRecordID += delta
	return r.LogRecordID
}

func RelatedDataFrom(records []*record_message.RecordMessage) (relatedData *RelatedData, logsRecord *record_message.RecordMessage, err error) {
	defer func() {
		for _, record := range records {
			record.Record().Release()
		}
	}()

	relatedData = NewRelatedData()

	// Create the attribute map stores for all the attribute records.
	for _, record := range records {
		switch record.PayloadType() {
		case colarspb.ArrowPayloadType_RESOURCE_ATTRS:
			err = otlp.AttributesStoreFrom[uint16](record.Record(), relatedData.ResAttrMapStore)
			if err != nil {
				return nil, nil, werror.Wrap(err)
			}
		case colarspb.ArrowPayloadType_SCOPE_ATTRS:
			err = otlp.AttributesStoreFrom[uint16](record.Record(), relatedData.ScopeAttrMapStore)
			if err != nil {
				return nil, nil, werror.Wrap(err)
			}
		case colarspb.ArrowPayloadType_LOG_ATTRS:
			err = otlp.AttributesStoreFrom[uint16](record.Record(), relatedData.LogRecordAttrMapStore)
			if err != nil {
				return nil, nil, werror.Wrap(err)
			}
		case colarspb.ArrowPayloadType_LOGS:
			if logsRecord != nil {
				return nil, nil, werror.Wrap(otel.ErrMultipleTracesRecords)
			}
			logsRecord = record
		default:
			return nil, nil, werror.Wrap(otel.UnknownPayloadType)
		}
	}

	return
}
