// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/value.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"math"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/series"
)

// FromResult transforms a promql query result into a samplestream
func FromResult(res *promql.Result) ([]SampleStream, error) {
	if res.Err != nil {
		// The error could be wrapped by the PromQL engine. We get the error's cause in order to
		// correctly parse the error in parent callers (eg. gRPC response status code extraction).
		return nil, errors.Cause(res.Err)
	}
	switch v := res.Value.(type) {
	case promql.Scalar:
		return []SampleStream{
			{
				Samples: []mimirpb.Sample{
					{
						Value:       v.V,
						TimestampMs: v.T,
					},
				},
			},
		}, nil

	case promql.Vector:
		res := make([]SampleStream, 0, len(v))
		for _, sample := range v {
			res = append(res, SampleStream{
				Labels: mimirpb.FromLabelsToLabelAdapters(sample.Metric),
				Samples: []mimirpb.Sample{{
					TimestampMs: sample.Point.T,
					Value:       sample.Point.V,
				}},
			})
		}
		return res, nil

	case promql.Matrix:
		res := make([]SampleStream, 0, len(v))
		for _, series := range v {
			res = append(res, SampleStream{
				Labels:  mimirpb.FromLabelsToLabelAdapters(series.Metric),
				Samples: mimirpb.FromPointsToSamples(series.Points),
			})
		}
		return res, nil

	}

	return nil, errors.Errorf("Unexpected value type: [%s]", res.Value.Type())
}

// ResponseToSamples is needed to map back from api response to the underlying series data
func ResponseToSamples(resp Response) ([]SampleStream, error) {
	promRes, ok := resp.(*PrometheusResponse)
	if !ok {
		return nil, errors.Errorf("error invalid response type: %T, expected: %T", resp, &PrometheusResponse{})
	}
	if promRes.Error != "" {
		return nil, errors.New(promRes.Error)
	}
	switch promRes.Data.ResultType {
	case string(parser.ValueTypeVector), string(parser.ValueTypeMatrix):
		return promRes.Data.Result, nil
	}

	return nil, errors.Errorf(
		"Invalid promql.Value type: [%s]. Only %s and %s supported",
		promRes.Data.ResultType,
		parser.ValueTypeVector,
		parser.ValueTypeMatrix,
	)
}

// newSeriesSetFromEmbeddedQueriesResults returns an in memory storage.SeriesSet from embedded queries results.
// The passed hints (if any) is used to inject stale markers at the beginning of each gap in the embedded query
// results.
//
// The returned storage.SeriesSet series is sorted.
func newSeriesSetFromEmbeddedQueriesResults(results []SampleStream, hints *storage.SelectHints) storage.SeriesSet {
	var (
		set  = make([]storage.Series, 0, len(results))
		step int64
	)

	// Get the query step from hints (if they've been passed).
	if hints != nil {
		step = hints.Step
	}

	for _, stream := range results {
		// We add an extra 10 items to account for some stale markers that could be injected.
		// We're trading a lower chance of reallocation in case stale markers are added for a
		// slightly higher memory utilisation.
		samples := make([]model.SamplePair, 0, len(stream.Samples)+10)

		for idx, sample := range stream.Samples {
			// When an embedded query is executed by PromQL engine, any stale marker in the time-series
			// data is used the engine to stop applying the lookback delta but the stale marker is removed
			// from the query results. The result of embedded queries, which we are processing in this function,
			// is then used as input to run an outer query in the PromQL engine. This data will not contain
			// the stale marker (because has been removed when running the embedded query) but we still need
			// the PromQL engine to not apply the lookback delta when there are gaps in the embedded queries
			// results. For this reason, here we do inject a stale marker at the beginning of each gap in the
			// embedded queries results.
			if step > 0 && idx > 0 && sample.TimestampMs > stream.Samples[idx-1].TimestampMs+step {
				samples = append(samples, model.SamplePair{
					Timestamp: model.Time(stream.Samples[idx-1].TimestampMs + step),
					Value:     model.SampleValue(math.Float64frombits(value.StaleNaN)),
				})
			}

			samples = append(samples, model.SamplePair{
				Timestamp: model.Time(sample.TimestampMs),
				Value:     model.SampleValue(sample.Value),
			})
		}

		// In case the embedded query processed series which all ended before the end of the query time range,
		// we don't want the outer query to apply the lookback at the end of the embedded query results. To keep it
		// simple, it's safe always to add an extra stale marker at the end of the query results.
		//
		// This could result into an extra sample (stale marker) after the end of the query time range, but that's
		// not a problem when running the outer query because it will just be discarded.
		if len(samples) > 0 && step > 0 {
			samples = append(samples, model.SamplePair{
				Timestamp: samples[len(samples)-1].Timestamp + model.Time(step),
				Value:     model.SampleValue(math.Float64frombits(value.StaleNaN)),
			})
		}

		set = append(set, series.NewConcreteSeries(mimirpb.FromLabelAdaptersToLabels(stream.Labels), samples))
	}
	return series.NewConcreteSeriesSet(set)
}
