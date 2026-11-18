// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
)

func TestCounterForQuery(t *testing.T) {
	want := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "job", "a"),
		labels.MustNewMatcher(labels.MatchNotRegexp, "pod", "p.+"),
	}
	q := &cardinalitypb.CountQuery{Matchers: cardinalitypb.ToLabelMatchers(want), StartMs: 10, EndMs: 70}
	opts, counter, matchers, err := CounterForQuery(q, 5)
	require.NoError(t, err)
	assert.Equal(t, CounterOptions{MinT: 10, MaxT: 70, GroupBy: model.MetricNameLabel}, opts)
	assert.Equal(t, model.MetricNameLabel, counter.GroupBy())
	require.Len(t, matchers, len(want))
	for i := range want {
		assert.Equal(t, want[i].String(), matchers[i].String())
	}

	for name, tc := range map[string]struct {
		q         cardinalitypb.CountQuery
		maxGroups int64
		want      string
	}{
		"an empty window":            {q: cardinalitypb.CountQuery{StartMs: 10, EndMs: 10}, want: "must be after its start"},
		"negative max_index_bytes":   {q: cardinalitypb.CountQuery{EndMs: 10, MaxIndexBytes: -1}, want: "max_index_bytes can't be negative"},
		"negative max_hashes":        {q: cardinalitypb.CountQuery{EndMs: 10, MaxHashes: -1}, want: "max_hashes can't be negative"},
		"negative max_groups":        {q: cardinalitypb.CountQuery{EndMs: 10}, maxGroups: -1, want: "max_groups can't be negative"},
		"an unknown matcher type":    {q: cardinalitypb.CountQuery{EndMs: 10, Matchers: []cardinalitypb.LabelMatcher{{Type: 7, Name: "job"}}}, want: "invalid matcher type 7"},
		"a regex that doesn't parse": {q: cardinalitypb.CountQuery{EndMs: 10, Matchers: []cardinalitypb.LabelMatcher{{Type: labels.MatchRegexp, Name: "job", Value: "("}}}, want: "missing closing"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, _, err := CounterForQuery(&tc.q, tc.maxGroups)
			assert.ErrorContains(t, err, tc.want)
		})
	}
}
