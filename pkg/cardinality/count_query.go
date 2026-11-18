// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"errors"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
)

// CounterForQuery validates q and the max_groups of the request that carries it. It returns the options q counts
// with, the group-by label filled in, a counter for them and q's matchers.
func CounterForQuery(q *cardinalitypb.CountQuery, maxGroups int64) (CounterOptions, *SeriesCounter, []*labels.Matcher, error) {
	opts := CounterOptions{
		MinT:    q.StartMs,
		MaxT:    q.EndMs,
		GroupBy: q.GroupBy,
		Hashes:  q.Hashes,
	}
	counter, err := NewSeriesCounter(opts)
	if err != nil {
		return opts, nil, nil, err
	}
	opts.GroupBy = counter.GroupBy()
	if maxGroups < 0 {
		return opts, nil, nil, errors.New("max_groups can't be negative")
	}
	matchers, err := cardinalitypb.FromLabelMatchers(q.Matchers)
	if err != nil {
		return opts, nil, nil, err
	}
	return opts, counter, matchers, nil
}
