// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
)

// The merge of the ingesters' answers to a lookup: what each answers about a series merges by change points, its
// flags add up, the lookup's coverage starts when every ingester's does, and the limit applies to the merged series.
func TestNativeLookupMerge(t *testing.T) {
	queryStats, ctx := stats.ContextWithEmptyStats(t.Context())
	pod := func(name string) []mimirpb.LabelAdapter {
		return []mimirpb.LabelAdapter{{Name: "__name__", Value: "up"}, {Name: "pod", Value: name}}
	}
	version := func(minTime, maxTime int64, help string) ingester_client.NativeMetricMetadataVersion {
		return ingester_client.NativeMetricMetadataVersion{MinTimeMs: minTime, MaxTimeMs: maxTime, Type: mimirpb.GAUGE, Help: help}
	}
	merge := newNativeLookupMerge[ingester_client.NativeMetricMetadataVersion]()

	// Two ingesters of one partition answer about pod-b, the one behind without its latest change; another
	// partition's ingester answers about pod-a, and has truncated its versions. The ingester without series of the
	// tenant doesn't hold the coverage back.
	merge.addResponse(ctx, 100, false, 1_000, 2)
	merge.addSeries(pod("pod-b"), []ingester_client.NativeMetricMetadataVersion{version(1_000, 1_999, "Up."), version(2_000, 3_000, "Up now.")}, false, false)
	merge.addResponse(ctx, 50, false, 1_000, 1)
	merge.addSeries(pod("pod-b"), []ingester_client.NativeMetricMetadataVersion{version(1_000, 1_500, "Up.")}, false, false)
	merge.addResponse(ctx, 80, false, 1_200, 1)
	merge.addSeries(pod("pod-a"), []ingester_client.NativeMetricMetadataVersion{version(1_200, 2_500, "Up.")}, true, false)
	merge.addResponse(ctx, 10, false, math.MinInt64, 0)

	merged, limited, coverageStart := merge.result(0, ingester_client.MergeNativeMetricMetadataVersions)
	require.False(t, limited)
	require.Len(t, merged, 2)
	require.Equal(t, pod("pod-a"), merged[0].labels)
	require.True(t, merged[0].truncated)
	require.Equal(t, pod("pod-b"), merged[1].labels)
	require.Equal(t, []ingester_client.NativeMetricMetadataVersion{version(1_000, 1_999, "Up."), version(2_000, 3_000, "Up now.")}, merged[1].versions)
	require.Equal(t, int64(1_200), coverageStart)
	require.Equal(t, uint64(240), queryStats.LoadNativeLookupResponseBytes())
	require.Equal(t, uint64(4), queryStats.LoadNativeLookupSeriesVisited())

	// Each ingester's first series by labels may be past the limit overall, so it applies to the merged series.
	merged, limited, _ = merge.result(1, ingester_client.MergeNativeMetricMetadataVersions)
	require.True(t, limited)
	require.Len(t, merged, 1)
	require.Equal(t, pod("pod-a"), merged[0].labels)

	// An ingester that let fewer series through than matched makes the merged lookup limited too.
	merge.addResponse(ctx, 0, true, 1_000, 0)
	_, limited, _ = merge.result(0, ingester_client.MergeNativeMetricMetadataVersions)
	require.True(t, limited)
}

// The requests a lookup's quorum cancelled may still answer after it returns, since DoUntilQuorum doesn't wait for
// them. Once closed, the merge takes none of their answers, so that its result and the query's stats stay those of the
// answers before the close; under -race, the result taken while answers arrive also shows it's read under the lock.
func TestNativeLookupMerge_TakesNoAnswersOnceClosed(t *testing.T) {
	queryStats, ctx := stats.ContextWithEmptyStats(t.Context())
	pod := func(name string) []mimirpb.LabelAdapter {
		return []mimirpb.LabelAdapter{{Name: "__name__", Value: "up"}, {Name: "pod", Value: name}}
	}
	versions := []ingester_client.NativeResourceVersion{{MinTimeMs: 1_000, MaxTimeMs: 2_000}}
	merge := newNativeLookupMerge[ingester_client.NativeResourceVersion]()
	merge.addResponse(ctx, 100, false, 1_000, 1)
	merge.addSeries(pod("pod-a"), versions, false, false)

	// Stragglers keep answering, about other series and with a later coverage start, until the test ends.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for n := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				merge.addResponse(ctx, 10, true, 5_000, 1)
				merge.addSeries(pod(fmt.Sprintf("straggler-%d-%d", n, i)), versions, false, false)
			}
		}()
	}

	merge.close()
	bytes, visited := queryStats.LoadNativeLookupResponseBytes(), queryStats.LoadNativeLookupSeriesVisited()
	merged, limited, coverageStart := merge.result(0, ingester_client.MergeNativeResourceVersions)
	close(stop)
	wg.Wait()

	// Whatever the stragglers added before the close is in, and nothing after it.
	again, limitedAgain, coverageStartAgain := merge.result(0, ingester_client.MergeNativeResourceVersions)
	require.Len(t, again, len(merged))
	require.Equal(t, limited, limitedAgain)
	require.Equal(t, coverageStart, coverageStartAgain)
	require.Equal(t, bytes, queryStats.LoadNativeLookupResponseBytes())
	require.Equal(t, visited, queryStats.LoadNativeLookupSeriesVisited())
}
