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

type (
	RelatedData struct {
		MetricID uint16

		// Attributes stores
		ResAttrMapStore                *otlp.AttributesStore[uint16]
		ScopeAttrMapStore              *otlp.AttributesStore[uint16]
		NumberDPAttrsStore             *otlp.AttributesStore[uint32]
		SummaryAttrsStore              *otlp.AttributesStore[uint32]
		HistogramAttrsStore            *otlp.AttributesStore[uint32]
		ExpHistogramAttrsStore         *otlp.AttributesStore[uint32]
		NumberDPExemplarAttrsStore     *otlp.AttributesStore[uint32]
		HistogramExemplarAttrsStore    *otlp.AttributesStore[uint32]
		ExpHistogramExemplarAttrsStore *otlp.AttributesStore[uint32]

		// Metric stores
		NumberDataPointsStore     *NumberDataPointsStore
		SummaryDataPointsStore    *SummaryDataPointsStore
		HistogramDataPointsStore  *HistogramDataPointsStore
		EHistogramDataPointsStore *EHistogramDataPointsStore

		// Exemplar stores
		NumberDataPointExemplarsStore     *ExemplarsStore
		HistogramDataPointExemplarsStore  *ExemplarsStore
		EHistogramDataPointExemplarsStore *ExemplarsStore
	}
)

func NewRelatedData() *RelatedData {
	return &RelatedData{
		ResAttrMapStore:                otlp.NewAttributesStore[uint16](),
		ScopeAttrMapStore:              otlp.NewAttributesStore[uint16](),
		NumberDPAttrsStore:             otlp.NewAttributesStore[uint32](),
		SummaryAttrsStore:              otlp.NewAttributesStore[uint32](),
		HistogramAttrsStore:            otlp.NewAttributesStore[uint32](),
		ExpHistogramAttrsStore:         otlp.NewAttributesStore[uint32](),
		NumberDPExemplarAttrsStore:     otlp.NewAttributesStore[uint32](),
		HistogramExemplarAttrsStore:    otlp.NewAttributesStore[uint32](),
		ExpHistogramExemplarAttrsStore: otlp.NewAttributesStore[uint32](),

		NumberDataPointsStore:     NewNumberDataPointsStore(),
		SummaryDataPointsStore:    NewSummaryDataPointsStore(),
		HistogramDataPointsStore:  NewHistogramDataPointsStore(),
		EHistogramDataPointsStore: NewEHistogramDataPointsStore(),

		NumberDataPointExemplarsStore:     NewExemplarsStore(),
		HistogramDataPointExemplarsStore:  NewExemplarsStore(),
		EHistogramDataPointExemplarsStore: NewExemplarsStore(),
	}
}

func (r *RelatedData) MetricIDFromDelta(delta uint16) uint16 {
	r.MetricID += delta
	return r.MetricID
}

func RelatedDataFrom(records []*record_message.RecordMessage) (relatedData *RelatedData, metricsRecord *record_message.RecordMessage, err error) {
	defer func() {
		for _, record := range records {
			record.Record().Release()
		}
	}()

	var numberDPRec *record_message.RecordMessage
	var summaryDPRec *record_message.RecordMessage
	var histogramDPRec *record_message.RecordMessage
	var expHistogramDPRec *record_message.RecordMessage
	var numberDBExRec *record_message.RecordMessage
	var histogramDBExRec *record_message.RecordMessage
	var expHistogramDBExRec *record_message.RecordMessage

	relatedData = NewRelatedData()

	for _, record := range records {
		switch record.PayloadType() {
		case colarspb.ArrowPayloadType_RESOURCE_ATTRS:
			err = otlp.AttributesStoreFrom(record.Record(), relatedData.ResAttrMapStore)
			if err != nil {
				return nil, nil, werror.Wrap(err)
			}
		case colarspb.ArrowPayloadType_SCOPE_ATTRS:
			err = otlp.AttributesStoreFrom(record.Record(), relatedData.ScopeAttrMapStore)
			if err != nil {
				return nil, nil, werror.Wrap(err)
			}
		case colarspb.ArrowPayloadType_NUMBER_DP_ATTRS:
			err = otlp.AttributesStoreFrom(record.Record(), relatedData.NumberDPAttrsStore)
			if err != nil {
				return nil, nil, werror.Wrap(err)
			}
		case colarspb.ArrowPayloadType_SUMMARY_DP_ATTRS:
			err = otlp.AttributesStoreFrom(record.Record(), relatedData.SummaryAttrsStore)
			if err != nil {
				return nil, nil, werror.Wrap(err)
			}
		case colarspb.ArrowPayloadType_HISTOGRAM_DP_ATTRS:
			err = otlp.AttributesStoreFrom(record.Record(), relatedData.HistogramAttrsStore)
			if err != nil {
				return nil, nil, werror.Wrap(err)
			}
		case colarspb.ArrowPayloadType_EXP_HISTOGRAM_DP_ATTRS:
			err = otlp.AttributesStoreFrom(record.Record(), relatedData.ExpHistogramAttrsStore)
			if err != nil {
				return nil, nil, werror.Wrap(err)
			}
		case colarspb.ArrowPayloadType_NUMBER_DATA_POINTS:
			if numberDPRec != nil {
				return nil, nil, werror.Wrap(otel.ErrDuplicatePayloadType)
			}
			numberDPRec = record
		case colarspb.ArrowPayloadType_SUMMARY_DATA_POINTS:
			if summaryDPRec != nil {
				return nil, nil, werror.Wrap(otel.ErrDuplicatePayloadType)
			}
			summaryDPRec = record
		case colarspb.ArrowPayloadType_HISTOGRAM_DATA_POINTS:
			if histogramDPRec != nil {
				return nil, nil, werror.Wrap(otel.ErrDuplicatePayloadType)
			}
			histogramDPRec = record
		case colarspb.ArrowPayloadType_EXP_HISTOGRAM_DATA_POINTS:
			if expHistogramDPRec != nil {
				return nil, nil, werror.Wrap(otel.ErrDuplicatePayloadType)
			}
			expHistogramDPRec = record
		case colarspb.ArrowPayloadType_UNIVARIATE_METRICS:
			if metricsRecord != nil {
				return nil, nil, werror.Wrap(otel.ErrDuplicatePayloadType)
			}
			metricsRecord = record
		case colarspb.ArrowPayloadType_NUMBER_DP_EXEMPLARS:
			if numberDBExRec != nil {
				return nil, nil, werror.Wrap(otel.ErrDuplicatePayloadType)
			}
			numberDBExRec = record
		case colarspb.ArrowPayloadType_HISTOGRAM_DP_EXEMPLARS:
			if histogramDBExRec != nil {
				return nil, nil, werror.Wrap(otel.ErrDuplicatePayloadType)
			}
			histogramDBExRec = record
		case colarspb.ArrowPayloadType_EXP_HISTOGRAM_DP_EXEMPLARS:
			if expHistogramDBExRec != nil {
				return nil, nil, werror.Wrap(otel.ErrDuplicatePayloadType)
			}
			expHistogramDBExRec = record
		case colarspb.ArrowPayloadType_NUMBER_DP_EXEMPLAR_ATTRS:
			err = otlp.AttributesStoreFrom[uint32](record.Record(), relatedData.NumberDPExemplarAttrsStore)
			if err != nil {
				return nil, nil, werror.Wrap(err)
			}
		case colarspb.ArrowPayloadType_HISTOGRAM_DP_EXEMPLAR_ATTRS:
			err = otlp.AttributesStoreFrom[uint32](record.Record(), relatedData.HistogramExemplarAttrsStore)
			if err != nil {
				return nil, nil, werror.Wrap(err)
			}
		case colarspb.ArrowPayloadType_EXP_HISTOGRAM_DP_EXEMPLAR_ATTRS:
			err = otlp.AttributesStoreFrom[uint32](record.Record(), relatedData.ExpHistogramExemplarAttrsStore)
			if err != nil {
				return nil, nil, werror.Wrap(err)
			}
		default:
			return nil, nil, werror.Wrap(otel.UnknownPayloadType)
		}
	}

	// Process exemplar records
	if numberDBExRec != nil {
		relatedData.NumberDataPointExemplarsStore, err = ExemplarsStoreFrom(
			numberDBExRec.Record(),
			relatedData.NumberDPExemplarAttrsStore,
		)
		if err != nil {
			return nil, nil, werror.Wrap(err)
		}
	}

	if histogramDBExRec != nil {
		relatedData.HistogramDataPointExemplarsStore, err = ExemplarsStoreFrom(
			histogramDBExRec.Record(),
			relatedData.HistogramExemplarAttrsStore,
		)
		if err != nil {
			return nil, nil, werror.Wrap(err)
		}
	}

	if expHistogramDBExRec != nil {
		relatedData.EHistogramDataPointExemplarsStore, err = ExemplarsStoreFrom(
			expHistogramDBExRec.Record(),
			relatedData.ExpHistogramExemplarAttrsStore,
		)
		if err != nil {
			return nil, nil, werror.Wrap(err)
		}
	}

	// Process data point records
	if numberDPRec != nil {
		relatedData.NumberDataPointsStore, err = NumberDataPointsStoreFrom(
			numberDPRec.Record(),
			relatedData.NumberDataPointExemplarsStore,
			relatedData.NumberDPAttrsStore,
		)
		if err != nil {
			return nil, nil, werror.Wrap(err)
		}
	}

	if summaryDPRec != nil {
		relatedData.SummaryDataPointsStore, err = SummaryDataPointsStoreFrom(
			summaryDPRec.Record(),
			relatedData.SummaryAttrsStore,
		)
		if err != nil {
			return nil, nil, werror.Wrap(err)
		}
	}

	if histogramDPRec != nil {
		relatedData.HistogramDataPointsStore, err = HistogramDataPointsStoreFrom(
			histogramDPRec.Record(),
			relatedData.HistogramDataPointExemplarsStore,
			relatedData.HistogramAttrsStore,
		)
		if err != nil {
			return nil, nil, werror.Wrap(err)
		}
	}

	if expHistogramDPRec != nil {
		relatedData.EHistogramDataPointsStore, err = EHistogramDataPointsStoreFrom(
			expHistogramDPRec.Record(),
			relatedData.EHistogramDataPointExemplarsStore,
			relatedData.ExpHistogramAttrsStore,
		)
		if err != nil {
			return nil, nil, werror.Wrap(err)
		}
	}

	return
}
