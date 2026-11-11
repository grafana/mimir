// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
)

func TestLabelPostingsRequestsMatchTSDB(t *testing.T) {
	heads := []headUnderTest{labelRequestHead(t, "prometheus", 400), labelRequestHead(t, "seriesstore", 400)}
	for _, name := range []string{"__name__", "pod", "service_name", "deployment_cluster", "missing"} {
		for _, pattern := range []string{".*", ".+", "(?i).*(?:checkout).*", "(?i).*(?:pod-1).*", "pod-0-[0-9]+", "^$"} {
			for _, window := range [][2]int64{{0, 6 * 3600000}, {10 * 60000, 20 * 60000}, {5 * 3600000, 6 * 3600000}, {6*3600000 + 1, 7 * 3600000}} {
				for _, limit := range []int{0, 3} {
					t.Run(fmt.Sprintf("%s/%s/%v/%d", name, pattern, window, limit), func(t *testing.T) {
						matcher := promlabels.MustNewMatcher(promlabels.MatchRegexp, name, pattern)
						var results [2][]string
						for i, head := range heads {
							q, err := head.Querier(window[0], window[1])
							require.NoError(t, err)
							results[i], _, err = q.LabelValues(t.Context(), name, &storage.LabelHints{Limit: limit, LimitSmallest: true}, matcher)
							require.NoError(t, err)
							require.NoError(t, q.Close())
						}
						require.True(t, slices.Equal(results[0], results[1]), "TSDB %v, engine %v", results[0], results[1])
					})
				}
			}
		}
	}
}

func TestLabelPostingsRequestsRespectLimit(t *testing.T) {
	head := labelRequestHead(t, "seriesstore", 400)
	for _, name := range []string{"pod", "service_name"} {
		q, err := head.Querier(0, 6*3600000)
		require.NoError(t, err)
		matcher := promlabels.MustNewMatcher(promlabels.MatchRegexp, name, ".+")
		all, _, err := q.LabelValues(t.Context(), name, nil, matcher)
		require.NoError(t, err)
		require.Greater(t, len(all), 5)
		for _, limit := range []int{1, 3, 5} {
			got, _, err := q.LabelValues(t.Context(), name, &storage.LabelHints{Limit: limit}, matcher)
			require.NoError(t, err)
			require.Len(t, got, limit)
			require.Subset(t, all, got)
		}
		require.NoError(t, q.Close())
	}
}

func TestSelectiveColdLabelRequestDoesNotBuildValueIndex(t *testing.T) {
	var rows []frozenSeries
	for i := range 2000 {
		rows = append(rows, frozen("tenant", [][2]string{{"__name__", "metric"}, {"key", fmt.Sprint(i)}, {"result", fmt.Sprint(i % 20)}}, [2]int64{1, 2}))
	}
	block, err := buildColdBlock(t.TempDir(), 1, rows)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, block.close()) })
	table := block.tenant("tenant")
	var hashes []uint64
	for i := range table.seriesCount {
		hashes = append(hashes, block.labelsHash(table, i))
	}
	slices.Sort(hashes)
	window := labelWindow{blocks: []emulatedBlock{{minTime: 0, maxTime: 3, series: hashes}}}
	matchers, err := compileMatchers([]LabelMatcher{{Type: 0, Name: "key", Value: "123"}})
	require.NoError(t, err)
	values := map[string]struct{}{}
	window.coldLabelValues(coldView{block}, "tenant", "result", matchers, values, 0)
	require.Equal(t, map[string]struct{}{"3": {}}, values)
	require.Empty(t, table.valueIndexes)
}

func BenchmarkLabelPostingsRequests(b *testing.B) {
	old := countersEnabled
	countersEnabled = false
	defer func() { countersEnabled = old }()
	for _, cold := range []bool{false, true} {
		name := "head"
		if cold {
			name = "cold"
		}
		b.Run(name, func(b *testing.B) {
			e := labelDebugEngine(b, cold)
			for _, tc := range []struct{ name, label, pattern string }{
				{"names", "", ""}, {"metric_names", "__name__", ""},
				{"namespace_regex", "namespace", "(?i).*(?:tenet).*"},
				{"pod_regex", "pod", "(?i).*(?:tenet-check).*"},
				{"pod_selective", "pod", "(?i).*(?:pod-1234).*"},
				{"cluster_selective", "cluster", ""},
				{"cluster_selective_fresh", "cluster", ""},
			} {
				b.Run(tc.name, func(b *testing.B) {
					var matchers []*promlabels.Matcher
					if tc.pattern != "" {
						matchers = append(matchers, promlabels.MustNewMatcher(promlabels.MatchRegexp, tc.label, tc.pattern))
					}
					if strings.HasPrefix(tc.name, "cluster_selective") {
						matchers = []*promlabels.Matcher{
							promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "kube_pod_info"),
							promlabels.MustNewMatcher(promlabels.MatchEqual, "namespace", "unified-storage-dev-0"),
							promlabels.MustNewMatcher(promlabels.MatchRegexp, "pod", "foundationdb-.*"),
						}
					}
					run := func() {
						if cold && strings.HasSuffix(tc.name, "_fresh") {
							b.StopTimer()
							e.store.perShardWithCold("tenant", func(_ *tenant, _ *chunks.DiskMapper, cold *coldState) {
								for _, block := range cold.blocks {
									if table := block.tenant("tenant"); table != nil {
										table.valueIndexesLock.Lock()
										table.valueIndexes = nil
										table.valueIndexesLock.Unlock()
									}
								}
							})
							b.StartTimer()
						}
						q, err := e.Querier(0, 12*3600000)
						if err != nil {
							b.Fatal(err)
						}
						if tc.label == "" {
							_, _, err = q.LabelNames(b.Context(), nil)
						} else {
							_, _, err = q.LabelValues(b.Context(), tc.label, &storage.LabelHints{Limit: 10000}, matchers...)
						}
						if err != nil {
							b.Fatal(err)
						}
						if err = q.Close(); err != nil {
							b.Fatal(err)
						}
					}
					run()
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						run()
					}
				})
			}
		})
	}
}
