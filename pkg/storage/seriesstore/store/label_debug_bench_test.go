// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"sync"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func labelDebugEngine(b *testing.B, cold bool) *Engine {
	e, err := OpenEngine(b.TempDir(), "tenant", EngineOptions{Shards: 4, SecondaryHashFunction: secondaryHash})
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, e.Close()) })
	total := realisticEnvInt("MIMIR_LABEL_DEBUG_SERIES", 200_000)
	refs := make([]storage.SeriesRef, total)
	app := e.Appender(b.Context())
	for n := range total {
		metric := fmt.Sprintf("metric_%d", n%4000)
		if n%5 == 1 {
			metric = fmt.Sprintf("http_request_duration_%d", n%4000)
		}
		namespace, pod := fmt.Sprintf("namespace-%d", n%20), fmt.Sprintf("pod-%d", n)
		if n%50 == 0 {
			metric = "kube_pod_info"
			if n%5000 < 500 {
				namespace = fmt.Sprintf("unified-storage-dev-%d", (n/50)%16)
				pod = fmt.Sprintf("foundationdb-%d", n)
			}
		}
		pairs := []string{"__name__", metric, "cluster", fmt.Sprintf("cluster-%d", (n/50)%8), "namespace", namespace, "pod", pod, "instance", fmt.Sprintf("instance-%d", n%10000), "job", fmt.Sprintf("job-%d", n%40)}
		if n%10 == 0 {
			pairs = append(pairs, "asserts_assertion_name", fmt.Sprintf("assertion-%d", (n/10)%64))
		}
		ref, err := app.Append(0, promlabels.FromStrings(pairs...), 0, 1000, 1, nil, nil, storage.AppendV2Options{})
		require.NoError(b, err)
		refs[n] = ref
		if n%2000 == 1999 {
			require.NoError(b, app.Commit())
			app = e.Appender(b.Context())
		}
	}
	require.NoError(b, app.Commit())
	if cold {
		require.NoError(b, e.CompactSelectedSeries(refs))
		app = e.Appender(b.Context())
		_, err := app.Append(0, promlabels.FromStrings("__name__", "current"), 0, 12*3_600_000, 1, nil, nil, storage.AppendV2Options{})
		require.NoError(b, err)
		require.NoError(b, app.Commit())
	}
	return e
}

func BenchmarkLabelDebug(b *testing.B) {
	for _, cold := range []bool{false, true} {
		b.Run(map[bool]string{false: "head", true: "blocks"}[cold], func(b *testing.B) {
			e := labelDebugEngine(b, cold)
			cases := []struct {
				name, label string
				limit       int
				matchers    []*promlabels.Matcher
			}{
				{name: "namespace", label: "namespace", limit: 10000},
				{name: "asserts", label: "asserts_assertion_name", limit: 10000},
				{name: "metric-names", label: "__name__", limit: 10000},
				{name: "metric-regex", label: "__name__", limit: 201, matchers: []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", "(?s:.*)(?-s:(?i)(http|request|duration))(?s:.*)")}},
				{name: "cluster-regex", label: "cluster", limit: 40000, matchers: []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "kube_pod_info"), promlabels.MustNewMatcher(promlabels.MatchRegexp, "namespace", "unified-storage-dev-.*"), promlabels.MustNewMatcher(promlabels.MatchRegexp, "pod", "foundationdb-.*")}},
			}
			for _, tc := range cases {
				for _, fresh := range []bool{false, true} {
					b.Run(tc.name+"/"+map[bool]string{false: "warm", true: "fresh-index"}[fresh], func(b *testing.B) {
						run := func() {
							q, err := e.Querier(0, 12*3_600_000)
							if err != nil {
								b.Fatal(err)
							}
							got, _, err := q.LabelValues(b.Context(), tc.label, &storage.LabelHints{Limit: tc.limit}, tc.matchers...)
							if err != nil {
								b.Fatal(err)
							}
							if len(got) == 0 {
								b.Fatal("empty result")
							}
							if tc.name == "cluster-regex" && len(got) != 8 {
								b.Fatalf("got %d clusters", len(got))
							}
							if err = q.Close(); err != nil {
								b.Fatal(err)
							}
						}
						run()
						b.ReportAllocs()
						for b.Loop() {
							if fresh {
								b.StopTimer()
								for _, shard := range e.store.shards {
									shard.Lock()
									if t := shard.tenant.Load(); t != nil {
										t.series.removals++
									}
									for _, block := range shard.cold.blocks {
										if table := block.tenant(e.tenantID); table != nil {
											table.valueIndexes = nil
											table.metricNames = nil
											table.metricNamesOnce = sync.Once{}
											table.nameMatches = nil
										}
									}
									shard.Unlock()
								}
								b.StartTimer()
							}
							run()
						}
					})
				}
			}
		})
	}
}
