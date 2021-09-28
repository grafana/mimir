// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util/math"
)

type CardinalityReporter struct {
}

//labelNamesCardinality streams in batches the labels and values of these labels that match the matchers.
// The count of label values per batch is defined in `batchSize` param. One label can be split into a few batches if the count of values is greater than `batchSize`.
// For example: if `batchSize` is 100K and `label-a` has 250K values, these values will be sent in three batches:
// batch-1. {LabelName:'label-a', LabelValues:[first 100K values]}
// batch-2. {LabelName:'label-a', LabelValues:[second 100K values]}
// batch-3. {LabelName:'label-a', LabelValues:[rest 50K values]} + any other labels with values but the total count of values in this batch must not be greater then 100K
func (c CardinalityReporter) labelNamesCardinality(index tsdb.IndexReader, matchers []*labels.Matcher,
	batchSize int, server *client.Ingester_LabelNamesCardinalityServer) error {
	labelNames, err := index.LabelNames(matchers...)
	if err != nil {
		return err
	}
	valuesInBatch := 0
	response := client.LabelNamesCardinalityResponse{}
	for _, labelName := range labelNames {
		if labelName == labels.MetricName {
			continue
		}
		values, err := index.LabelValues(labelName, matchers...)
		if err != nil {
			return err
		}
		valuesToSendCount := valuesInBatch + len(values)
		if valuesToSendCount < batchSize {
			response.Items = append(response.Items, &client.LabelValues{LabelName: labelName, Values: values})
			valuesInBatch += len(values)
			continue
		}

		countOfBatches := valuesToSendCount / batchSize
		if valuesToSendCount%batchSize > 0 {
			countOfBatches++
		}
		for i := 0; i < countOfBatches; i++ {
			var shift int
			if valuesInBatch > 0 {
				shift = batchSize - valuesInBatch
			} else {
				shift = math.Min(batchSize, len(values))
			}
			cardinality := client.LabelValues{
				LabelName: labelName,
				Values:    values[:shift],
			}
			response.Items = append(response.Items, &cardinality)
			valuesInBatch += len(cardinality.Values)
			values = values[shift:]

			if valuesInBatch == batchSize {
				err = client.SendLabelNamesCardinalityResponse(server, &response)
				if err != nil {
					return err
				}
				response.Items = response.Items[:0]
				valuesInBatch = 0
			}
		}
	}
	if valuesInBatch > 0 {
		return client.SendLabelNamesCardinalityResponse(server, &response)
	}
	return nil
}
