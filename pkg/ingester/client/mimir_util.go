// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/cortex_util.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"cmp"
	context "context"
	"slices"
)

// SendQueryStream wraps the stream's Send() checking if the context is done
// before calling Send().
func SendQueryStream(s Ingester_QueryStreamServer, m *QueryStreamResponse) error {
	return sendWithContextErrChecking(s.Context(), func() error {
		return s.Send(m)
	})
}

// SendLabelNamesAndValuesResponse wraps the stream's Send() checking if the context is done
// before calling Send().
func SendLabelNamesAndValuesResponse(s Ingester_LabelNamesAndValuesServer, response *LabelNamesAndValuesResponse) error {
	return sendWithContextErrChecking(s.Context(), func() error {
		return s.Send(response)
	})
}

// SendLabelValuesCardinalityResponse wraps the stream's Send() checking if the context is done
// before calling Send().
func SendLabelValuesCardinalityResponse(s Ingester_LabelValuesCardinalityServer, response *LabelValuesCardinalityResponse) error {
	return sendWithContextErrChecking(s.Context(), func() error {
		return s.Send(response)
	})
}

func SendActiveSeriesResponse(s Ingester_ActiveSeriesServer, response *ActiveSeriesResponse) error {
	return sendWithContextErrChecking(s.Context(), func() error {
		return s.Send(response)
	})
}

func sendWithContextErrChecking(ctx context.Context, send func() error) error {
	// If the context has been canceled or its deadline exceeded, we should return it
	// instead of the cryptic error the Send() will return.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	if err := send(); err != nil {
		// Experimentally, we've seen the context switching to done after the Send()
		// has been  called, so here we do recheck the context in case of error.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		return err
	}

	return nil
}

// AccumulateChunks builds a slice of chunks, eliminating duplicates.
// This is O(N^2) but most of the time N is small.
// AccumulateChunks may return or modify either of the passed in slices.
func AccumulateChunks(a, b []Chunk) []Chunk {
	// If a is empty, we can just return b.
	// The loop below effectively does the same thing for the opposite scenario (if b is empty, we'll just return a unmodified).
	if len(a) == 0 {
		return b
	}

	ret := a
	for j := range b {
		if !containsChunk(a, b[j]) {
			ret = append(ret, b[j])
		}
	}
	return ret
}

// TopLabelChurnMetricNames returns the limit metric names with the most sampled series, in descending order.
func TopLabelChurnMetricNames(sampledSeriesPerMetricName map[string]uint64, limit int) []LabelChurnMetricName {
	metricNames := make([]LabelChurnMetricName, 0, len(sampledSeriesPerMetricName))
	for metricName, sampledSeries := range sampledSeriesPerMetricName {
		metricNames = append(metricNames, LabelChurnMetricName{MetricName: metricName, SampledSeries: sampledSeries})
	}
	slices.SortFunc(metricNames, func(a, b LabelChurnMetricName) int {
		return cmp.Or(cmp.Compare(b.SampledSeries, a.SampledSeries), cmp.Compare(a.MetricName, b.MetricName))
	})
	return metricNames[:min(limit, len(metricNames))]
}

func containsChunk(a []Chunk, b Chunk) bool {
	for i := range a {
		if a[i].Equal(b) {
			return true
		}
	}
	return false
}
