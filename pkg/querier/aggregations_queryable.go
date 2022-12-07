package querier

import (
	"context"
	"math"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/multierror"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/storage/series"
)

func newAggregationsQueryable(normalIngesters, aggregatedIngesters QueryableWithFilter, provideRawSamplesFor time.Duration, logger log.Logger) QueryableWithFilter {
	return &aggregationsQueryable{
		normalIngesters:      normalIngesters,
		aggregatedIngesters:  aggregatedIngesters,
		provideRawSamplesFor: provideRawSamplesFor,
		logger:               logger,
	}
}

type aggregationsQueryable struct {
	normalIngesters      QueryableWithFilter
	aggregatedIngesters  QueryableWithFilter
	logger               log.Logger
	provideRawSamplesFor time.Duration
}

func (a *aggregationsQueryable) UseQueryable(now time.Time, queryMinT, queryMaxT int64) bool {
	return a.normalIngesters.UseQueryable(now, queryMinT, queryMaxT)
}

func (a *aggregationsQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	queryTime := time.Now()
	// We don't return raw series before cutoff time.
	rawSeriesCutoffTime := queryTime.Add(-a.provideRawSamplesFor)

	normalQuerier, err := a.normalIngesters.Querier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}
	aggQuerier, err := a.aggregatedIngesters.Querier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}

	return &aggregationsQuerier{
		normalQuerier:       normalQuerier,
		aggQuerier:          aggQuerier,
		ctx:                 ctx,
		mint:                mint,
		maxt:                maxt,
		rawSeriesCutoffTime: model.TimeFromUnixNano(rawSeriesCutoffTime.UnixNano()),
	}, nil
}

type aggregationsQuerier struct {
	queryTime     time.Time
	normalQuerier storage.Querier
	aggQuerier    storage.Querier
	ctx           context.Context
	mint, maxt    int64

	rawSeriesCutoffTime model.Time
}

func (a *aggregationsQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	// TODO: merge both
	return a.normalQuerier.LabelValues(name, matchers...)
}

func (a *aggregationsQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	// TODO: merge both
	return a.normalQuerier.LabelNames(matchers...)
}

const droppedLabelsLabelName = "dropped_labels"

func (a *aggregationsQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	// we fetch series from both queriers
	// we iterate through all series to find raw and aggregated series
	// if there are both:
	// - we use aggregated series up to now - provideRawSamplesFor.
	// - we only use raw samples for "provideRawSamplesFor"
	// what if there aren't both?
	// - that can happen when someone uses "dropped_labels" label for selection. we return what we have and send a warning.

	// TODO: run these two Selects concurrently.
	normal := a.normalQuerier.Select(sortSeries, hints, matchers...)
	agg := a.aggQuerier.Select(sortSeries, hints, matchers...)

	// All metrics (metric names) returned by agg are aggregated, although agg can provide both aggregated and raw series. we filter out aggregated series,
	// as they are also written to "normal" ingesters.
	rawMetricNames := map[string]bool{}

	// Keep a copy of series from aggregation ingesters, but only raw series (no dropped_labels label).
	var rawSeries []storage.Series
	for agg.Next() {
		s := agg.At()

		lbls := s.Labels()
		droppedLabels := lbls.Get(droppedLabelsLabelName)
		if droppedLabels != "" {
			// We ignore all aggregated series returned by aggregation ingesters.
			continue
		}

		fs, err := filterSamplesFromSeries(s, func(ts model.Time) bool {
			return ts > a.rawSeriesCutoffTime
		}, false)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		if fs != nil {
			rawSeries = append(rawSeries, fs)

			metricName := lbls.Get(labels.MetricName)
			rawMetricNames[metricName] = true
		}
	}
	if err := agg.Err(); err != nil {
		return storage.ErrSeriesSet(err)
	}

	// Iterate through series from normal ingesters. If we have raw series for them, we only include aggregated samples from before cutoff time.
	var normalSeries []storage.Series
	for normal.Next() {
		s := normal.At()

		lbls := s.Labels()
		droppedLabels := lbls.Get(droppedLabelsLabelName)
		// Not aggregated or doesn't have raw series => always include in the result.
		if droppedLabels == "" || !rawMetricNames[lbls.Get(labels.MetricName)] {
			normalSeries = append(normalSeries, s)
			continue
		}

		fs, err := filterSamplesFromSeries(s, func(ts model.Time) bool {
			return ts <= a.rawSeriesCutoffTime
		}, true)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		if fs != nil {
			normalSeries = append(normalSeries, fs)
		}
	}
	if err := normal.Err(); err != nil {
		return storage.ErrSeriesSet(err)
	}

	return storage.NewMergeSeriesSet([]storage.SeriesSet{newSliceSeriesSet(rawSeries), newSliceSeriesSet(normalSeries)}, storage.ChainedSeriesMerge)
}

func (a *aggregationsQuerier) Close() error {
	return multierror.New(a.normalQuerier.Close(), a.aggQuerier.Close()).Err()
}

func filterSamplesFromSeries(s storage.Series, include func(ts model.Time) bool, appendStaleMarker bool) (storage.Series, error) {
	var includedSamples []model.SamplePair
	it := s.Iterator()
	for it.Next() {
		t, v := it.At()
		if include(model.Time(t)) {
			includedSamples = append(includedSamples, model.SamplePair{Timestamp: model.Time(t), Value: model.SampleValue(v)})
		}
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	if len(includedSamples) == 0 {
		return nil, nil
	}

	if appendStaleMarker {
		lastSample := includedSamples[len(includedSamples)-1]
		lastSample.Timestamp++
		lastSample.Value = model.SampleValue(math.Float64frombits(value.StaleNaN))
		includedSamples = append(includedSamples, lastSample)
	}
	return series.NewConcreteSeries(s.Labels(), includedSamples), nil
}
