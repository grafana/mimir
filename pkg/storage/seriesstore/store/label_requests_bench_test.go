// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"runtime"
	"slices"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
)

// The request shapes come from slow query-tee requests: unfiltered recent names,
// historical values and six-hour regexes. Generations leave cold series behind.
func BenchmarkEngineLabelRequests(b *testing.B) {
	for _, backend := range benchEngines {
		b.Run(backend, func(b *testing.B) {
			head := labelRequestHead(b, backend, 50_000)
			for _, tc := range labelRequestCases() {
				b.Run(tc.name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						q, err := head.Querier(tc.start, tc.end)
						if err != nil {
							b.Fatal(err)
						}
						if tc.label == "" {
							_, _, err = q.LabelNames(b.Context(), nil, tc.matchers...)
						} else {
							_, _, err = q.LabelValues(b.Context(), tc.label, &storage.LabelHints{Limit: 10000}, tc.matchers...)
						}
						if err != nil {
							b.Fatal(err)
						}
						if err = q.Close(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func labelRequestHead(t testing.TB, backend string, seriesCount int) headUnderTest {
	const hour = int64(3_600_000)
	var head headUnderTest
	if backend == "prometheus" {
		head = openPrometheus(t, t.TempDir(), differentialOptions{timely: true}, nil)
	} else {
		e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, TimelyCompaction: true, IdleEvictionMs: 30 * 60_000, SecondaryHashFunction: secondaryHash})
		require.NoError(t, err)
		head = e
	}
	t.Cleanup(func() { require.NoError(t, head.Close()) })
	for generation := range 4 {
		for sample := range 6 {
			app := head.Appender(t.Context())
			for n := range seriesCount {
				service := fmt.Sprintf("service-%d", n%100)
				if n%10 == 0 {
					service = fmt.Sprintf("checkout-%d", n%100)
				}
				metric := fmt.Sprintf("metric_%d", n%1000)
				if n%5 == 0 {
					metric = fmt.Sprintf("azure_metric_%d", n%1000)
				}
				lset := promlabels.FromStrings("__name__", metric, "service_name", service, "deployment_cluster", fmt.Sprintf("cluster-%d", n%20), "peer_service", fmt.Sprintf("peer-%d", n%50), "pod", fmt.Sprintf("pod-%d-%d", generation, n))
				_, err := app.Append(0, lset, 0, int64(generation)*90*60_000+int64(sample)*15*60_000, 1, nil, nil, storage.AppendV2Options{})
				require.NoError(t, err)
			}
			require.NoError(t, app.Commit())
			require.NoError(t, head.Compact(t.Context()))
		}
	}
	app := head.Appender(t.Context())
	_, err := app.Append(0, promlabels.FromStrings("__name__", "fresh"), 0, 6*hour, 1, nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.NoError(t, head.Compact(t.Context()))

	return head
}

type labelRequestCase struct {
	name, label string
	start, end  int64
	matchers    []*promlabels.Matcher
}

func labelRequestCases() []labelRequestCase {
	const hour = int64(3_600_000)
	return []labelRequestCase{
		{"names_recent", "", 6*hour - 30*60_000, 6 * hour, nil},
		{"names_history", "", 10 * 60_000, 20 * 60_000, nil},
		{"values_history", "deployment_cluster", 10 * 60_000, 20 * 60_000, nil},
		{"values_absent_history", "client_cloud_region", 10 * 60_000, 20 * 60_000, nil},
		{"service_regex", "service_name", 0, 6 * hour, []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "service_name", "(?i).*(?:checkout).*")}},
		{"metric_regex", "__name__", 0, 6 * hour, []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", "^azure_.*|.*(?:azure.*virtualmachine).*|.*(?:azure_microsoft_compute).*")}},
		{"narrow_matcher", "pod", 0, 6 * hour, []*promlabels.Matcher{promlabels.MustNewMatcher(promlabels.MatchEqual, "deployment_cluster", "cluster-1")}},
	}
}

func TestEngineLabelRequestsMatchPrometheus(t *testing.T) {
	heads := []headUnderTest{labelRequestHead(t, "prometheus", 300), labelRequestHead(t, "seriesstore", 300)}
	cases := labelRequestCases()
	for _, r := range [][2]int64{{0, 0}, {0, 1}, {3_600_000 - 1, 3_600_000}, {2*3_600_000 - 1, 2 * 3_600_000}, {6*3_600_000 + 1, 7 * 3_600_000}} {
		cases = append(cases, labelRequestCase{name: fmt.Sprint(r), start: r[0], end: r[1]})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got [2][]string
			for i, h := range heads {
				q, err := h.Querier(tc.start, tc.end)
				require.NoError(t, err)
				if tc.label == "" {
					got[i], _, err = q.LabelNames(t.Context(), nil, tc.matchers...)
				} else {
					got[i], _, err = q.LabelValues(t.Context(), tc.label, nil, tc.matchers...)
				}
				require.NoError(t, err)
				require.NoError(t, q.Close())
			}
			require.True(t, slices.Equal(got[0], got[1]), "TSDB: %v; engine: %v", got[0], got[1])
			if tc.name == "names_recent" || tc.name == "names_history" || tc.name == "values_history" || tc.name == "service_regex" || tc.name == "metric_regex" {
				require.NotEmpty(t, got[0])
			}
		})
	}
}

// New blocks have no value indexes: repeated warm lookups hide their construction cost.
func BenchmarkEngineLabelRequestsCold(b *testing.B) {
	head := labelRequestHead(b, "seriesstore", 50_000)
	e := head.(*Engine)
	for _, tc := range labelRequestCases() {
		if tc.label == "" {
			continue
		}
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
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
				q, err := head.Querier(tc.start, tc.end)
				if err != nil {
					b.Fatal(err)
				}
				_, _, err = q.LabelValues(b.Context(), tc.label, &storage.LabelHints{Limit: 10000}, tc.matchers...)
				if err != nil {
					b.Fatal(err)
				}
				if err = q.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestColdLabelNamesRespectTenantTimeAndBlockMembership(t *testing.T) {
	series := []frozenSeries{
		frozen("a", [][2]string{{"__name__", "up"}, {"only_a", "yes"}, {"pod", "alpha"}}, [2]int64{1, 2}),
		frozen("a", [][2]string{{"__name__", "up"}, {"only_b", "yes"}, {"pod", "beta"}}, [2]int64{50, 60}),
		frozen("b", [][2]string{{"__name__", "other"}, {"other_tenant", "yes"}}, [2]int64{1, 2}),
	}
	// Enough rejected series to exercise the bounded-probe fallback.
	for n := range 300 {
		series = append(series, frozen("a", [][2]string{{"__name__", "up"}, {"pod", fmt.Sprint(n)}}, [2]int64{1, 2}))
	}
	block, err := buildColdBlock(t.TempDir(), 1, series)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, block.close()) })
	var alphaHash uint64
	table := block.tenant("a")
	for i := range table.seriesCount {
		s := block.seriesIn(table, i)
		if value, _ := s.lookup("pod"); value == "alpha" {
			alphaHash = block.labelsHash(table, i)
		}
	}
	require.NotZero(t, alphaHash)
	for _, tc := range []struct {
		name   string
		window labelWindow
		want   []string
	}{
		{"all", labelWindow{hasBlocks: true, lower: 0, upper: 100}, []string{"__name__", "only_a", "only_b", "pod"}},
		{"old", labelWindow{hasBlocks: true, lower: 0, upper: 2}, []string{"__name__", "only_a", "pod"}},
		{"gap", labelWindow{hasBlocks: true, lower: 30, upper: 40}, nil},
		{"selected", labelWindow{blocks: []emulatedBlock{{minTime: 0, maxTime: 100, series: []uint64{alphaHash}}}}, []string{"__name__", "only_a", "pod"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := map[string]struct{}{}
			tc.window.coldLabelNames(coldView{block}, "a", got)
			require.ElementsMatch(t, tc.want, sortedKeys(got))
		})
	}
}

func BenchmarkEngineLabelRequestsHeap(b *testing.B) {
	for range b.N {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		head := labelRequestHead(b, "seriesstore", 50_000)
		for _, tc := range labelRequestCases() {
			q, err := head.Querier(tc.start, tc.end)
			require.NoError(b, err)
			if tc.label == "" {
				_, _, err = q.LabelNames(b.Context(), nil, tc.matchers...)
			} else {
				_, _, err = q.LabelValues(b.Context(), tc.label, nil, tc.matchers...)
			}
			require.NoError(b, err)
			require.NoError(b, q.Close())
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc), "retained-B")
		runtime.KeepAlive(head)
	}
}

func TestEngineConcurrentLabelLookups(t *testing.T) {
	e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, IdleEvictionMs: 30_000})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })
	stop, done, started := make(chan struct{}), make(chan struct{}), make(chan struct{})
	errors := make(chan error, 1)
	go func() {
		defer close(done)
		first := true
		matcher := promlabels.MustNewMatcher(promlabels.MatchRegexp, "group", "g.*")
		for {
			select {
			case <-stop:
				return
			default:
			}
			q, err := e.Querier(0, 6*3_600_000)
			if err == nil {
				_, _, err = q.LabelNames(t.Context(), nil)
				if err == nil {
					_, _, err = q.LabelValues(t.Context(), "group", nil, matcher)
				}
				closeErr := q.Close()
				if err == nil {
					err = closeErr
				}
			}
			if first {
				close(started)
				first = false
			}
			if err != nil {
				errors <- err
				return
			}
		}
	}()
	<-started
	defer func() {
		close(stop)
		<-done
		select {
		case err := <-errors:
			require.NoError(t, err)
		default:
		}
	}()
	for round := range 30 {
		app := e.Appender(t.Context())
		for n := range 300 {
			_, err := app.Append(0, promlabels.FromStrings("__name__", "up", "group", fmt.Sprintf("g%d", n%3), "pod", fmt.Sprintf("p%d-%d", round, n)), 0, int64(round)*60_000, 1, nil, nil, storage.AppendV2Options{})
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
		require.NoError(t, e.Compact(t.Context()))
	}
}
