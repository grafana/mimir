// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This is not great, but we deal with unsorted labels in prePushRelabelMiddleware.
func TestShardByAllLabelAdaptersReturnsWrongResultsForUnsortedLabels(t *testing.T) {
	val1 := ShardByAllLabelAdapters("test", []LabelAdapter{
		{Name: "__name__", Value: "foo"},
		{Name: "bar", Value: "baz"},
		{Name: "sample", Value: "1"},
	})

	val2 := ShardByAllLabelAdapters("test", []LabelAdapter{
		{Name: "__name__", Value: "foo"},
		{Name: "sample", Value: "1"},
		{Name: "bar", Value: "baz"},
	})

	assert.NotEqual(t, val1, val2)
}

// TestShardWithSeedEquivalence ensures the *WithSeed variants produce bit-identical tokens to the
// userID-based functions, i.e. hoisting ShardByUser out of a loop never changes the result.
func TestShardWithSeedEquivalence(t *testing.T) {
	users := []string{"", "u", "tenant-1234567890", "user/with/slashes"}
	labelSets := [][]LabelAdapter{
		nil,
		{{Name: "__name__", Value: "up"}},
		{{Name: "__name__", Value: "http_requests_total"}, {Name: "method", Value: "GET"}, {Name: "code", Value: "200"}},
	}
	metricNames := []string{"", "up", "http_requests_total"}

	for _, user := range users {
		seed := ShardByUser(user)

		for _, ls := range labelSets {
			require.Equal(t, ShardByAllLabelAdapters(user, ls), ShardByAllLabelAdaptersWithSeed(seed, ls),
				"ShardByAllLabelAdapters mismatch for user %q", user)

			promLabels := labelAdaptersToLabels(ls)
			require.Equal(t, ShardByAllLabels(user, promLabels), ShardByAllLabelsWithSeed(seed, promLabels),
				"ShardByAllLabels mismatch for user %q", user)
		}

		for _, name := range metricNames {
			require.Equal(t, ShardByMetricName(user, name), ShardByMetricNameWithSeed(seed, name),
				"ShardByMetricName mismatch for user %q metric %q", user, name)
		}
	}
}

func labelAdaptersToLabels(ls []LabelAdapter) labels.Labels {
	b := labels.NewScratchBuilder(len(ls))
	for _, l := range ls {
		b.Add(l.Name, l.Value)
	}
	b.Sort()
	return b.Labels()
}

func benchmarkLabelAdapters(numLabels int) []LabelAdapter {
	ls := make([]LabelAdapter, 0, numLabels+1)
	ls = append(ls, LabelAdapter{Name: "__name__", Value: "http_requests_total"})
	for i := 0; i < numLabels; i++ {
		ls = append(ls, LabelAdapter{Name: fmt.Sprintf("label_name_%d", i), Value: fmt.Sprintf("label_value_%d", i)})
	}
	return ls
}

// BenchmarkShardByAllLabelAdaptersBatch exercises the distributor hot path: computing one token per
// series for a whole request, hoisting the per-user seed out of the loop as getTokensForSeries does.
func BenchmarkShardByAllLabelAdaptersBatch(b *testing.B) {
	const userID = "tenant-1234567890"

	for _, numSeries := range []int{1, 100, 1000} {
		for _, numLabels := range []int{5, 15, 30} {
			series := make([][]LabelAdapter, numSeries)
			for i := range series {
				series[i] = benchmarkLabelAdapters(numLabels)
			}

			b.Run(fmt.Sprintf("series=%d/labels=%d", numSeries, numLabels), func(b *testing.B) {
				out := make([]uint32, numSeries)
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					seed := ShardByUser(userID)
					for i, ls := range series {
						out[i] = ShardByAllLabelAdaptersWithSeed(seed, ls)
					}
				}
				runtimeKeepUint32(out)
			})
		}
	}
}

var sinkUint32 uint32

func runtimeKeepUint32(s []uint32) {
	var x uint32
	for _, v := range s {
		x ^= v
	}
	sinkUint32 = x
}
