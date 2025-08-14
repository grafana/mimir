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

package otlp

import (
	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/collector/pdata/pmetric"

	arrowutils "github.com/open-telemetry/otel-arrow/go/pkg/arrow"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/common/otlp"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

const None = -1

type (
	// MetricsIds contains the IDs of the fields in the Arrow Record.
	MetricsIds struct {
		ID                     int // Numerical ID of the current span
		Resource               *otlp.ResourceIds
		Scope                  *otlp.ScopeIds
		SchemaUrl              int
		MetricType             int
		Name                   int
		Description            int
		Unit                   int
		AggregationTemporality int
		IsMonotonic            int
	}
)

// MetricsFrom creates a [pmetric.Metrics] from the given Arrow Record.
//
// Important Note: This function doesn't take ownership of the record, so the
// record must be released by the caller.
func MetricsFrom(record arrow.Record, relatedData *RelatedData) (pmetric.Metrics, error) {
	metrics := pmetric.NewMetrics()

	if relatedData == nil {
		return metrics, werror.Wrap(otlp.ErrMissingRelatedData)
	}

	metricsIDs, err := SchemaToIds(record.Schema())
	if err != nil {
		return metrics, werror.Wrap(err)
	}

	var resMetrics pmetric.ResourceMetrics
	var scopeMetricsSlice pmetric.ScopeMetricsSlice
	var metricSlice pmetric.MetricSlice

	resMetricsSlice := metrics.ResourceMetrics()
	rows := int(record.NumRows())

	prevResID := None
	prevScopeID := None

	var resID uint16
	var scopeID uint16

	for row := 0; row < rows; row++ {
		// Process resource spans, resource, schema url (resource)
		resDeltaID, err := otlp.ResourceIDFromRecord(record, row, metricsIDs.Resource)
		resID += resDeltaID
		if err != nil {
			return metrics, werror.Wrap(err)
		}
		if prevResID != int(resID) {
			prevResID = int(resID)
			resMetrics = resMetricsSlice.AppendEmpty()
			scopeMetricsSlice = resMetrics.ScopeMetrics()
			prevScopeID = None
			schemaUrl, err := otlp.UpdateResourceFromRecord(resMetrics.Resource(), record, row, metricsIDs.Resource, relatedData.ResAttrMapStore)
			if err != nil {
				return metrics, werror.Wrap(err)
			}
			resMetrics.SetSchemaUrl(schemaUrl)
		}

		// Process scope spans, scope, schema url (scope)
		scopeDeltaID, err := otlp.ScopeIDFromRecord(record, row, metricsIDs.Scope)
		scopeID += scopeDeltaID
		if err != nil {
			return metrics, werror.Wrap(err)
		}
		if prevScopeID != int(scopeID) {
			prevScopeID = int(scopeID)
			scopeMetrics := scopeMetricsSlice.AppendEmpty()
			metricSlice = scopeMetrics.Metrics()
			if err = otlp.UpdateScopeFromRecord(scopeMetrics.Scope(), record, row, metricsIDs.Scope, relatedData.ScopeAttrMapStore); err != nil {
				return metrics, werror.Wrap(err)
			}

			schemaUrl, err := arrowutils.StringFromRecord(record, metricsIDs.SchemaUrl, row)
			if err != nil {
				return metrics, werror.Wrap(err)
			}
			scopeMetrics.SetSchemaUrl(schemaUrl)
		}

		// Process metric fields
		metric := metricSlice.AppendEmpty()
		deltaID, err := arrowutils.U16FromRecord(record, metricsIDs.ID, row)
		if err != nil {
			return metrics, werror.Wrap(err)
		}
		ID := relatedData.MetricIDFromDelta(deltaID)

		metricType, err := arrowutils.U8FromRecord(record, metricsIDs.MetricType, row)
		if err != nil {
			return metrics, werror.Wrap(err)
		}

		name, err := arrowutils.StringFromRecord(record, metricsIDs.Name, row)
		if err != nil {
			return metrics, werror.Wrap(err)
		}
		metric.SetName(name)

		description, err := arrowutils.StringFromRecord(record, metricsIDs.Description, row)
		if err != nil {
			return metrics, werror.Wrap(err)
		}
		metric.SetDescription(description)

		unit, err := arrowutils.StringFromRecord(record, metricsIDs.Unit, row)
		if err != nil {
			return metrics, werror.Wrap(err)
		}
		metric.SetUnit(unit)

		aggregationTemporality, err := arrowutils.I32FromRecord(record, metricsIDs.AggregationTemporality, row)
		if err != nil {
			return metrics, werror.Wrap(err)
		}

		isMonotonic, err := arrowutils.BoolFromRecord(record, metricsIDs.IsMonotonic, row)
		if err != nil {
			return metrics, werror.Wrap(err)
		}

		switch pmetric.MetricType(metricType) {
		case pmetric.MetricTypeGauge:
			dps := relatedData.NumberDataPointsStore.NumberDataPointsByID(ID)
			gauge := metric.SetEmptyGauge()
			dps.MoveAndAppendTo(gauge.DataPoints())
		case pmetric.MetricTypeSum:
			dps := relatedData.NumberDataPointsStore.NumberDataPointsByID(ID)
			sum := metric.SetEmptySum()
			sum.SetAggregationTemporality(pmetric.AggregationTemporality(aggregationTemporality))
			sum.SetIsMonotonic(isMonotonic)
			dps.MoveAndAppendTo(sum.DataPoints())
		case pmetric.MetricTypeSummary:
			dps := relatedData.SummaryDataPointsStore.SummaryMetricsByID(ID)
			summary := metric.SetEmptySummary()
			dps.MoveAndAppendTo(summary.DataPoints())
		case pmetric.MetricTypeHistogram:
			dps := relatedData.HistogramDataPointsStore.HistogramMetricsByID(ID)
			histogram := metric.SetEmptyHistogram()
			histogram.SetAggregationTemporality(pmetric.AggregationTemporality(aggregationTemporality))
			dps.MoveAndAppendTo(histogram.DataPoints())
		case pmetric.MetricTypeExponentialHistogram:
			dps := relatedData.EHistogramDataPointsStore.EHistogramMetricsByID(ID)
			expHistogram := metric.SetEmptyExponentialHistogram()
			expHistogram.SetAggregationTemporality(pmetric.AggregationTemporality(aggregationTemporality))
			dps.MoveAndAppendTo(expHistogram.DataPoints())
		default:
			// Todo log unknown metric type
		}

	}

	return metrics, err
}

func SchemaToIds(schema *arrow.Schema) (*MetricsIds, error) {
	ID, _ := arrowutils.FieldIDFromSchema(schema, constants.ID)
	resourceIDs, err := otlp.NewResourceIdsFromSchema(schema)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	scopeIDs, err := otlp.NewScopeIdsFromSchema(schema)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	schemaUrlID, err := arrowutils.FieldIDFromSchema(schema, constants.SchemaUrl)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	metricTypeID, err := arrowutils.FieldIDFromSchema(schema, constants.MetricType)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	nameID, err := arrowutils.FieldIDFromSchema(schema, constants.Name)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	descID, err := arrowutils.FieldIDFromSchema(schema, constants.Description)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	unitID, err := arrowutils.FieldIDFromSchema(schema, constants.Unit)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	aggrTempID, err := arrowutils.FieldIDFromSchema(schema, constants.AggregationTemporality)
	if err != nil {
		return nil, werror.Wrap(err)
	}
	isMonotonicID, err := arrowutils.FieldIDFromSchema(schema, constants.IsMonotonic)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	return &MetricsIds{
		ID:                     ID,
		Resource:               resourceIDs,
		Scope:                  scopeIDs,
		SchemaUrl:              schemaUrlID,
		MetricType:             metricTypeID,
		Name:                   nameID,
		Description:            descID,
		Unit:                   unitID,
		AggregationTemporality: aggrTempID,
		IsMonotonic:            isMonotonicID,
	}, nil
}
