package ingester

import (
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util/math"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
)

type CardinalityReporter struct {
}

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
			response.Items = append(response.Items, &client.LabelNamesCardinality{LabelName: labelName, LabelValues: values})
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
			cardinality := client.LabelNamesCardinality{
				LabelName:   labelName,
				LabelValues: values[:shift],
			}
			response.Items = append(response.Items, &cardinality)
			valuesInBatch += len(cardinality.LabelValues)
			values = values[shift:]

			if valuesInBatch == batchSize {
				err = client.SendLabelNamesCardinalityResponse(server, &response)
				if err != nil {
					return err
				}
				response = client.LabelNamesCardinalityResponse{}
				valuesInBatch = 0
			}
		}
	}
	if valuesInBatch > 0 {
		return client.SendLabelNamesCardinalityResponse(server, &response)
	}
	return nil
}
