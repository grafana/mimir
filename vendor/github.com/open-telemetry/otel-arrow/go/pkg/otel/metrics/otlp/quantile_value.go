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
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/constants"
	"github.com/open-telemetry/otel-arrow/go/pkg/werror"
)

type QuantileValueIds struct {
	Id       int
	Quantile int
	Value    int
}

func NewQuantileValueIds(schema *arrow.Schema) (*QuantileValueIds, error) {
	id, quantileValueDT, err := arrowutils.ListOfStructsFieldIDFromSchema(schema, constants.SummaryQuantileValues)
	if err != nil {
		return nil, werror.Wrap(err)
	}

	quantile, _ := arrowutils.FieldIDFromStruct(quantileValueDT, constants.SummaryQuantile)
	value, _ := arrowutils.FieldIDFromStruct(quantileValueDT, constants.SummaryValue)

	return &QuantileValueIds{
		Id:       id,
		Quantile: quantile,
		Value:    value,
	}, nil
}

func AppendQuantileValuesInto(quantileSlice pmetric.SummaryDataPointValueAtQuantileSlice, record arrow.Record, ndpIdx int, ids *QuantileValueIds) error {
	quantileValues, err := arrowutils.ListOfStructsFromRecord(record, ids.Id, ndpIdx)
	if err != nil {
		return werror.Wrap(err)
	}
	if quantileValues == nil {
		return nil
	}

	for quantileIdx := quantileValues.Start(); quantileIdx < quantileValues.End(); quantileIdx++ {
		quantileValue := quantileSlice.AppendEmpty()

		if quantileValues.IsNull(quantileIdx) {
			continue
		}

		quantile, err := quantileValues.F64FieldByID(ids.Quantile, quantileIdx)
		if err != nil {
			return werror.Wrap(err)
		}
		quantileValue.SetQuantile(quantile)

		value, err := quantileValues.F64FieldByID(ids.Value, quantileIdx)
		if err != nil {
			return werror.Wrap(err)
		}
		quantileValue.SetValue(value)
	}
	return nil
}
