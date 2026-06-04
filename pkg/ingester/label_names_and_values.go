// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"flag"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util/workerpool"
)

const (
	checkContextErrorSeriesCount         = 1000 // series count interval in which context cancellation must be checked.
	labelValuesSeriesCountMaxConcurrency = 16
)

// LabelValuesCountConfig configures how label-values-cardinality responses are
// computed on the ingester. The work itself is dispatched to the ingester's
// shared tenant-fair query worker pool (see Config.QueryWorkers); this config
// only carries the label-values-specific knobs.
type LabelValuesCountConfig struct {
	ChunkSize int `yaml:"chunk_size" category:"experimental"`
}

// RegisterFlags registers config flags.
func (cfg *LabelValuesCountConfig) RegisterFlags(f *flag.FlagSet) {
	// 32 is a gut-feel default (not benchmarked): big enough to amortize
	// per-task dispatch overhead, small enough to stay fair across tenants.
	f.IntVar(&cfg.ChunkSize, "ingester.label-values-count-chunk-size", 32, "Number of label values processed per work unit submitted to the ingester query worker pool.")
}

// Validate returns an error if the config is invalid.
func (cfg *LabelValuesCountConfig) Validate() error {
	if cfg.ChunkSize < 1 {
		return errors.New("-ingester.label-values-count-chunk-size must be >= 1")
	}
	return nil
}

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
		values, err := index.LabelValues(ctx, labelName, nil, matchers...)
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
	pool *workerpool.Pool,
	tenantID string,
	chunkSize int,
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
		lblValues, err := idxReader.LabelValues(ctx, lblName, nil, matchers...)
		if err != nil {
			return err
		}
		// For each value count total number of series storing the result into cardinality response item.
		var respItem *client.LabelValueSeriesCount

		resultCh := computeLabelValuesSeriesCount(ctx, pool, tenantID, chunkSize, lblName, lblValues, matchers, idxReader, postingsForMatchersFn)

		for countRes := range resultCh {
			if countRes.err != nil {
				// Drain to wait for in-flight chunks to release idxReader before
				// the caller's defer idx.Close() fires. The producer goroutine
				// closes resultCh after wg.Wait, so this returns once every
				// chunk is done.
				for range resultCh { //nolint:revive
				}
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
	pool *workerpool.Pool,
	tenantID string,
	chunkSize int,
	lblName string,
	lblValues []string,
	matchers []*labels.Matcher,
	idxReader tsdb.IndexReader,
	postingsForMatchersFn func(context.Context, tsdb.IndexPostingsReader, ...*labels.Matcher) (index.Postings, error),
) <-chan labelValueCountResult {
	countCh := make(chan labelValueCountResult, len(lblValues))

	if len(lblValues) == 0 {
		close(countCh)
		return countCh
	}

	processChunk := func(start, end int) {
		for i := start; i < end; i++ {
			seriesCount, err := countLabelValueSeries(ctx, lblName, lblValues[i], matchers, idxReader, postingsForMatchersFn)
			countCh <- labelValueCountResult{
				val:   lblValues[i],
				count: seriesCount,
				err:   err,
			}
		}
	}

	inFlightChunks := make(chan struct{}, labelValuesSeriesCountMaxConcurrency)
	var wg sync.WaitGroup
	go func() {
		defer func() {
			wg.Wait()
			close(countCh)
		}()
		for start := 0; start < len(lblValues); start += chunkSize {
			if err := ctx.Err(); err != nil {
				countCh <- labelValueCountResult{err: err}
				return
			}
			select {
			case inFlightChunks <- struct{}{}:
			case <-ctx.Done():
				countCh <- labelValueCountResult{err: ctx.Err()}
				return
			}
			if err := ctx.Err(); err != nil {
				<-inFlightChunks
				countCh <- labelValueCountResult{err: err}
				return
			}
			end := min(start+chunkSize, len(lblValues))
			s, e := start, end
			wg.Add(1)
			err := pool.Submit(tenantID, func() {
				defer func() {
					<-inFlightChunks
					wg.Done()
				}()
				processChunk(s, e)
			})
			if err != nil {
				<-inFlightChunks
				wg.Done()
				countCh <- labelValueCountResult{err: err}
				return
			}
		}
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
