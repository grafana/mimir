// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/ingester/client"
)

const (
	checkContextErrorSeriesCount = 1000 // series count interval in which context cancellation must be checked.
)

type labelValueCountResult struct {
	val   string
	count uint64
	err   error
}

// labelNamesAndValues streams the messages with the labels and values of the labels matching the `matchers` param.
// Messages are immediately sent as soon they reach message size threshold defined in `messageSizeThreshold` param.
func labelNamesAndValues(
	index tsdb.IndexReader,
	matchers []*labels.Matcher,
	messageSizeThreshold int,
	stream client.Ingester_LabelNamesAndValuesServer,
	filter func(name, value string) (bool, error),
) error {
	ctx := stream.Context()

	labelNames, err := index.LabelNames(ctx, matchers...)
	if err != nil {
		return err
	}

	response := client.LabelNamesAndValuesResponse{}
	responseSizeBytes := 0
	for _, labelName := range labelNames {
		if err := ctx.Err(); err != nil {
			return err
		}
		labelItem := &client.LabelValues{LabelName: labelName}
		responseSizeBytes += len(labelName)
		// send message if (response size + size of label name of current label) is greater or equals to threshold
		if responseSizeBytes >= messageSizeThreshold {
			err = client.SendLabelNamesAndValuesResponse(stream, &response)
			if err != nil {
				return err
			}
			response.Items = response.Items[:0]
			responseSizeBytes = len(labelName)
		}
		values, err := index.LabelValues(ctx, labelName, matchers...)
		if err != nil {
			return err
		}

		filteredValues := values[:0]
		for _, val := range values {
			if ok, err := filter(labelName, val); err != nil {
				return err
			} else if ok {
				// This append is safe because filteredValues is strictly smaller than values.
				filteredValues = append(filteredValues, val)
			}
		}
		values = filteredValues

		lastAddedValueIndex := -1
		for i, val := range values {
			// sum up label values length until response size reached the threshold and after that add all values to the response
			// starting from last sent value or from the first element and up to the current element (including).
			responseSizeBytes += len(val)
			if responseSizeBytes >= messageSizeThreshold {
				labelItem.Values = values[lastAddedValueIndex+1 : i+1]
				lastAddedValueIndex = i
				response.Items = append(response.Items, labelItem)
				err = client.SendLabelNamesAndValuesResponse(stream, &response)
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
		return client.SendLabelNamesAndValuesResponse(stream, &response)
	}
	return nil
}

// labelValuesCardinality returns all values and series total count for label_names labels that match the matchers.
// Messages are immediately sent as soon they reach message size threshold.
func labelValuesCardinality(
	lbNames []string,
	matchers []*labels.Matcher,
	idxReader tsdb.IndexReader,
	postingsForMatchersFn func(context.Context, tsdb.IndexPostingsReader, ...*labels.Matcher) (index.Postings, error),
	msgSizeThreshold int,
	srv client.Ingester_LabelValuesCardinalityServer,
) error {
	ctx := srv.Context()

	resp := client.LabelValuesCardinalityResponse{}
	respSize := 0

	for _, lblName := range lbNames {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Obtain all values for current label name.
		lblValues, err := idxReader.LabelValues(ctx, lblName, matchers...)
		if err != nil {
			return err
		}
		// For each value count total number of series storing the result into cardinality response item.
		var respItem *client.LabelValueSeriesCount

		resultCh := computeLabelValuesSeriesCount(ctx, lblName, lblValues, matchers, idxReader, postingsForMatchersFn)

		for countRes := range resultCh {
			if countRes.err != nil {
				return countRes.err
			}
			if countRes.count == 0 {
				continue
			}
			if respItem == nil {
				respItem = &client.LabelValueSeriesCount{
					LabelName:        lblName,
					LabelValueSeries: make(map[string]uint64),
				}
				resp.Items = append(resp.Items, respItem)
			}

			respItem.LabelValueSeries[countRes.val] = countRes.count

			respSize += len(countRes.val)
			if respSize < msgSizeThreshold {
				continue
			}
			// Flush the response when reached message threshold.
			if err := client.SendLabelValuesCardinalityResponse(srv, &resp); err != nil {
				return err
			}
			resp.Items = resp.Items[:0]
			respSize = 0
			respItem = nil
		}
	}
	// Send response in case there are any pending items.
	if len(resp.Items) > 0 {
		return client.SendLabelValuesCardinalityResponse(srv, &resp)
	}
	return nil
}

func computeLabelValuesSeriesCount(
	ctx context.Context,
	lblName string,
	lblValues []string,
	matchers []*labels.Matcher,
	idxReader tsdb.IndexReader,
	postingsForMatchersFn func(context.Context, tsdb.IndexPostingsReader, ...*labels.Matcher) (index.Postings, error),
) <-chan labelValueCountResult {
	maxConcurrency := 16
	if len(lblValues) < maxConcurrency {
		maxConcurrency = len(lblValues)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(lblValues))

	countCh := make(chan labelValueCountResult, len(lblValues))

	indexes := atomic.NewInt64(-1)
	for ix := 0; ix < maxConcurrency; ix++ {
		go func() {
			for {
				idx := int(indexes.Inc())
				if idx >= len(lblValues) {
					return
				}
				seriesCount, err := countLabelValueSeries(ctx, lblName, lblValues[idx], matchers, idxReader, postingsForMatchersFn)
				countCh <- labelValueCountResult{
					val:   lblValues[idx],
					count: seriesCount,
					err:   err,
				}
				wg.Done()
			}
		}()
	}
	go func() {
		wg.Wait()
		close(countCh)
	}()

	return countCh
}

func countLabelValueSeries(
	ctx context.Context,
	lblName string,
	lblValue string,
	matchers []*labels.Matcher,
	idxReader tsdb.IndexReader,
	postingsForMatchersFn func(context.Context, tsdb.IndexPostingsReader, ...*labels.Matcher) (index.Postings, error),
) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var count uint64

	// We will use original matchers + one extra matcher for label value.
	lblValMatchers := make([]*labels.Matcher, len(matchers)+1)
	copy(lblValMatchers, matchers)

	lblValMatchers[len(lblValMatchers)-1] = labels.MustNewMatcher(labels.MatchEqual, lblName, lblValue)

	p, err := postingsForMatchersFn(ctx, idxReader, lblValMatchers...)
	if err != nil {
		return 0, err
	}
	for p.Next() {
		count++
		if count%checkContextErrorSeriesCount == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
	}
	if p.Err() != nil {
		return 0, p.Err()
	}
	return count, nil
}
