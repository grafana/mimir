// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"math"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func newTestActiveSeriesResponse(t *testing.T, series ...labels.Labels) *activeSeriesResponse {
	t.Helper()
	res := newActiveSeriesResponse(prometheus.NewCounter(prometheus.CounterOpts{}), math.MaxInt, true)
	metrics := make([]*mimirpb.Metric, 0, len(series))
	for _, s := range series {
		metrics = append(metrics, &mimirpb.Metric{Labels: mimirpb.FromLabelsToLabelAdapters(s)})
	}
	require.NoError(t, res.add(metrics, nil))
	return res
}

func TestActiveSeriesResponse_labelPresenceResult(t *testing.T) {
	res := newTestActiveSeriesResponse(t,
		labels.FromStrings("__name__", "m", "cluster", "a", "namespace", "x"),
		labels.FromStrings("__name__", "m", "cluster", "b"),
	)

	resp, fetched := res.labelPresenceResult([]string{"cluster", "namespace"}, 20)

	assert.Equal(t, uint64(2), fetched)
	assert.Equal(t, uint64(2), resp.TotalSeries)
	assert.Equal(t, uint64(1), resp.CompliantSeries)
	assert.ElementsMatch(t, []cardinality.LabelPresenceItem{
		{LabelName: "cluster", MissingCount: 0},
		{LabelName: "namespace", MissingCount: 1},
	}, resp.Labels)
	require.Len(t, resp.Examples, 1)
	assert.Equal(t, "b", resp.Examples[0].Get("cluster"))
}

func TestActiveSeriesResponse_labelPresenceResult_limitsExamples(t *testing.T) {
	res := newTestActiveSeriesResponse(t,
		labels.FromStrings("__name__", "m", "id", "1"),
		labels.FromStrings("__name__", "m", "id", "2"),
		labels.FromStrings("__name__", "m", "id", "3"),
	)

	resp, _ := res.labelPresenceResult([]string{"cluster"}, 2)

	assert.Equal(t, uint64(3), resp.TotalSeries)
	assert.Equal(t, uint64(0), resp.CompliantSeries)
	assert.Equal(t, uint64(3), resp.Labels[0].MissingCount)
	assert.Len(t, resp.Examples, 2)
}

func TestActiveSeriesResponse_labelPresenceResult_emptyValueIsMissing(t *testing.T) {
	res := newTestActiveSeriesResponse(t,
		labels.FromStrings("__name__", "m", "cluster", ""),
	)

	resp, _ := res.labelPresenceResult([]string{"cluster"}, 20)

	assert.Equal(t, uint64(1), resp.TotalSeries)
	assert.Equal(t, uint64(0), resp.CompliantSeries)
	assert.Equal(t, uint64(1), resp.Labels[0].MissingCount)
}
