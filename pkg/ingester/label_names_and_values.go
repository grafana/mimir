// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/ingester/client"
)

// labelNamesAndValues streams the messages with the labels and values of the labels matching the `matchers` param.
// Messages are immediately sent as soon they reach message size threshold defined in `messageSizeThreshold` param.
func labelNamesAndValues(
	index tsdb.IndexReader,
	matchers []*labels.Matcher,
	messageSizeThreshold int,
	server client.Ingester_LabelNamesAndValuesServer,
) error {
	labelNames, err := index.LabelNames(matchers...)
	if err != nil {
		return err
	}

	response := client.LabelNamesAndValuesResponse{}
	responseSizeBytes := 0
	for _, labelName := range labelNames {
		labelItem := &client.LabelValues{LabelName: labelName}
		responseSizeBytes += len(labelName)
		// send message if (response size + size of label name of current label) is greater or equals to threshold
		if responseSizeBytes >= messageSizeThreshold {
			err = client.SendLabelNamesAndValuesResponse(server, &response)
			if err != nil {
				return err
			}
			response.Items = response.Items[:0]
			responseSizeBytes = len(labelName)
		}
		values, err := index.LabelValues(labelName, matchers...)
		if err != nil {
			return err
		}

		lastAddedValueIndex := -1
		for i, val := range values {
			// sum up label values length until response size reached the threshold and after that add all values to the response
			// starting from last sent value or from the first element and up to the current element (including).
			responseSizeBytes += len(val)
			if responseSizeBytes >= messageSizeThreshold {
				labelItem.Values = values[lastAddedValueIndex+1 : i+1]
				lastAddedValueIndex = i
				response.Items = append(response.Items, labelItem)
				err = client.SendLabelNamesAndValuesResponse(server, &response)
				if err != nil {
					return err
				}
				// reset label values to reuse labelItem for the next values of current label.
				labelItem.Values = labelItem.Values[:0]
				response.Items = response.Items[:0]
				if i+1 == len(values) {
					// if it's the last value for this label then response size must be set to `0`
					responseSizeBytes = 0
				} else {
					// if it is not the last value for this label then response size must be set to length of current label name.
					responseSizeBytes = len(labelName)
				}
			} else if i+1 == len(values) {
				// if response size does not reach the threshold, but it's the last label value then it must be added to labelItem
				// and label item must be added to response.
				labelItem.Values = values[lastAddedValueIndex+1 : i+1]
				response.Items = append(response.Items, labelItem)
			}
		}
	}
	// send the last message if there is some data that was not sent.
	if response.Size() > 0 {
		return client.SendLabelNamesAndValuesResponse(server, &response)
	}
	return nil
}

type labelValuesCardinalityIndexReader struct {
	tsdb.IndexReader
	PostingsForMatchers func(tsdb.IndexPostingsReader, ...*labels.Matcher) (index.Postings, error)
}

// labelValuesCardinality returns all values and series total count for label_names labels that match the matchers.
// Messages are immediately sent as soon they reach message size threshold defined in `messageSizeThreshold` param.
func labelValuesCardinality(
	idxReader labelValuesCardinalityIndexReader,
	lbNames []string,
	matchers []*labels.Matcher,
	messageSizeThreshold int,
	server client.Ingester_LabelValuesCardinalityServer,
) error {
	resp := client.LabelValuesCardinalityResponse{}
	baseSize := resp.Size()
	respSize := baseSize

	lblValMatchers := make([]*labels.Matcher, 0, len(matchers)+1)
	for _, lbName := range lbNames {
		// Obtain all values for current label name.
		lbValues, err := idxReader.LabelValues(lbName, matchers...)
		if err != nil {
			return err
		}
		// For each value count total number of series storing the result into cardinality response.
		for _, lbValue := range lbValues {
			lblValMatchers = append(lblValMatchers, labels.MustNewMatcher(labels.MatchEqual, lbName, lbValue))
			lblValMatchers = append(lblValMatchers, matchers...)

			// Get total series count applying label matchers.
			seriesCount, err := countLabelValueSeries(idxReader, lblValMatchers)
			if err != nil {
				return err
			}
			item := &client.LabelValueSeriesCount{
				LabelName:   lbName,
				LabelValue:  lbValue,
				SeriesCount: seriesCount,
			}
			resp.Items = append(resp.Items, item)

			// Flush the response when reached message threshold.
			respSize += item.Size()
			if respSize >= messageSizeThreshold {
				if err := client.SendLabelValuesCardinalityResponse(server, &resp); err != nil {
					return err
				}
				resp.Items = resp.Items[:0]
				respSize = baseSize
			}
			lblValMatchers = lblValMatchers[:0]
		}
	}
	// Send response in case nothing has been previously sent or there are pending items.
	if len(resp.Items) > 0 {
		return client.SendLabelValuesCardinalityResponse(server, &resp)
	}
	return nil
}

func countLabelValueSeries(idxReader labelValuesCardinalityIndexReader, lblValMatchers []*labels.Matcher) (uint64, error) {
	var count uint64

	p, err := idxReader.PostingsForMatchers(idxReader.IndexReader, lblValMatchers...)
	if err != nil {
		return 0, err
	}
	for p.Next() {
		count++
	}
	if p.Err() != nil {
		return 0, p.Err()
	}
	return count, nil
}
