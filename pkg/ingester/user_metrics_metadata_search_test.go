// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"unicode"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util/validation"
)

func newSearchTestMetadataMap() *userMetricsMetadata {
	ring := &ringCountMock{instancesCount: 1, zonesCount: 1}
	limits := validation.NewOverrides(validation.Limits{}, nil)
	strategy := newIngesterRingLimiterStrategy(ring, 1, false, "", limits.IngestionTenantShardSize)
	metrics := newIngesterMetrics(prometheus.NewPedanticRegistry(), true, func() *InstanceLimits { return nil }, nil, nil, nil)
	return newMetadataMap(NewLimiter(limits, strategy), metrics, newIngesterErrSamplers(0), "test")
}

func addSearchTestMetadata(t testing.TB, mm *userMetricsMetadata, name string, helps ...string) {
	for _, help := range helps {
		require.NoError(t, mm.add(name, &mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: name, Help: help}))
	}
}

func substringSearchFilter(t testing.TB, expression string) storage.Filter {
	params, err := streaminglabelvalues.NewExpressionParams(expression, false, streaminglabelvalues.FuzzAlgSubstring, 0)
	require.NoError(t, err)
	filter, err := streaminglabelvalues.BuildFilter(params)
	require.NoError(t, err)
	return filter
}

func searchResultValues(results []storage.SearchResult) []string {
	values := make([]string, 0, len(results))
	for _, r := range results {
		values = append(values, r.Value)
	}
	return values
}

func TestUserMetricsMetadataSearch(t *testing.T) {
	mm := newSearchTestMetadataMap()
	addSearchTestMetadata(t, mm, "compression_ratio", "Compression ratio of requests.")
	addSearchTestMetadata(t, mm, "discarded_series_ratio", "Ratio of dropped series in the ingester.", "Deprecated: ratio of dropped series.")
	addSearchTestMetadata(t, mm, "sync_latency_seconds", "Latency of the sync calls.")
	addSearchTestMetadata(t, mm, "zz_ratio_total", "Total ratio events.")
	addSearchTestMetadata(t, mm, "summer_days", "ÉTÉ CHAUD.")

	tests := map[string]struct {
		expression  string
		order       storage.Ordering
		resumeAfter string
		limit       int
		want        []string
	}{
		"ascending": {
			expression: "ratio",
			want:       []string{"compression_ratio", "discarded_series_ratio", "zz_ratio_total"},
		},
		"descending": {
			expression: "ratio",
			order:      storage.OrderByValueDesc,
			want:       []string{"zz_ratio_total", "discarded_series_ratio", "compression_ratio"},
		},
		"ascending with limit": {
			expression: "ratio",
			limit:      2,
			want:       []string{"compression_ratio", "discarded_series_ratio"},
		},
		"descending with limit": {
			expression: "ratio",
			order:      storage.OrderByValueDesc,
			limit:      2,
			want:       []string{"zz_ratio_total", "discarded_series_ratio"},
		},
		"limit above the number of matches": {
			expression: "ratio",
			limit:      10,
			want:       []string{"compression_ratio", "discarded_series_ratio", "zz_ratio_total"},
		},
		"ascending cursor": {
			expression:  "ratio",
			resumeAfter: "compression_ratio",
			want:        []string{"discarded_series_ratio", "zz_ratio_total"},
		},
		"descending cursor": {
			expression:  "ratio",
			order:       storage.OrderByValueDesc,
			resumeAfter: "zz_ratio_total",
			want:        []string{"discarded_series_ratio", "compression_ratio"},
		},
		"cursor that is not a metric name": {
			expression:  "ratio",
			resumeAfter: "d",
			limit:       1,
			want:        []string{"discarded_series_ratio"},
		},
		"term inside a word matches": {
			expression: "ompress",
			want:       []string{"compression_ratio"},
		},
		"no match": {
			expression: "absent",
			want:       nil,
		},
		"NOT rejects a metric when any of its HELP texts holds the term": {
			expression: "ratio AND NOT deprecated",
			want:       []string{"compression_ratio", "zz_ratio_total"},
		},
		"terms can match in different HELP texts of one metric": {
			expression: "ingester AND deprecated",
			want:       []string{"discarded_series_ratio"},
		},
		"terms can match in the name and in a HELP text": {
			expression: "compression AND requests",
			want:       []string{"compression_ratio"},
		},
		"non-ASCII upper case matches a lower-case term": {
			expression: "été AND chaud",
			want:       []string{"summer_days"},
		},
		"NOT rejects a metric when its name holds the term": {
			expression: "ratio AND NOT discarded",
			want:       []string{"compression_ratio", "zz_ratio_total"},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			results, err := mm.search(t.Context(), substringSearchFilter(t, tc.expression), false, tc.order, tc.resumeAfter, tc.limit)
			require.NoError(t, err)
			if tc.want == nil {
				assert.Empty(t, results)
				return
			}
			assert.Equal(t, tc.want, searchResultValues(results))
			for _, r := range results {
				assert.Equal(t, 1.0, r.Score)
			}
		})
	}
}

// keepFilter accepts a text in which a HELP text is "keep".
type keepFilter struct{}

func (keepFilter) Accept(text string) (bool, float64) {
	return strings.Contains(text+"\n", "\nkeep\n"), 1.0
}

// TestUserMetricsMetadataSearchMatchesSortedReference checks the bounded heap
// against a full sort of all matches.
func TestUserMetricsMetadataSearchMatchesSortedReference(t *testing.T) {
	const numMetrics = 1000
	mm := newSearchTestMetadataMap()
	var matching []string
	for i := range numMetrics {
		name := fmt.Sprintf("metric_%04d", i)
		if i%3 == 0 {
			addSearchTestMetadata(t, mm, name, "drop", "keep")
			matching = append(matching, name)
		} else {
			addSearchTestMetadata(t, mm, name, "drop")
		}
	}
	slices.Sort(matching)

	for _, order := range []storage.Ordering{storage.OrderByValueAsc, storage.OrderByValueDesc} {
		want := slices.Clone(matching)
		if order == storage.OrderByValueDesc {
			slices.Reverse(want)
		}
		for _, resumeAfter := range []string{"", "metric_0500", "metric_0501"} {
			afterCursor := want
			if resumeAfter != "" {
				afterCursor = slices.DeleteFunc(slices.Clone(want), func(name string) bool {
					if order == storage.OrderByValueDesc {
						return name >= resumeAfter
					}
					return name <= resumeAfter
				})
			}
			for _, limit := range []int{0, 1, 2, 7, 100, len(afterCursor), len(afterCursor) + 1, numMetrics} {
				t.Run(fmt.Sprintf("order=%v/resume_after=%q/limit=%d", order, resumeAfter, limit), func(t *testing.T) {
					expected := afterCursor
					if limit > 0 && limit < len(expected) {
						expected = expected[:limit]
					}
					results, err := mm.search(t.Context(), keepFilter{}, true, order, resumeAfter, limit)
					require.NoError(t, err)
					assert.Equal(t, expected, searchResultValues(results))
				})
			}
		}
	}
}

// recordingSearchFilter accepts every text and keeps a copy of it.
type recordingSearchFilter struct {
	seen []string
}

func (f *recordingSearchFilter) Accept(text string) (bool, float64) {
	// search passes a view of a reused buffer, so the text must be copied.
	f.seen = append(f.seen, strings.Clone(text))
	return true, 1.0
}

func TestUserMetricsMetadataSearchText(t *testing.T) {
	mm := newSearchTestMetadataMap()
	addSearchTestMetadata(t, mm, "metric_a", "first help", "second help")
	addSearchTestMetadata(t, mm, "metric_b", "")

	addSearchTestMetadata(t, mm, "Metric_C", "ÉTÉ \xff Help")

	for caseSensitive, want := range map[bool][]string{
		true:  {"Metric_C\nÉTÉ \xff Help", "metric_b\n"},
		false: {"metric_b\n", "metric_c\nété � help"},
	} {
		filter := &recordingSearchFilter{}
		_, err := mm.search(t.Context(), filter, caseSensitive, storage.OrderByValueAsc, "", 0)
		require.NoError(t, err)

		require.Len(t, filter.seen, 3)
		i := slices.IndexFunc(filter.seen, func(s string) bool { return strings.HasPrefix(s, "metric_a\n") })
		require.GreaterOrEqual(t, i, 0)
		// The HELP texts of a metric are in map order.
		assert.Contains(t, []string{"metric_a\nfirst help\nsecond help", "metric_a\nsecond help\nfirst help"}, filter.seen[i])
		others := slices.Delete(filter.seen, i, i+1)
		slices.Sort(others)
		assert.Equal(t, want, others, "case sensitive %t", caseSensitive)
	}
}

func TestAppendTextMatchesStringsToLower(t *testing.T) {
	for _, s := range []string{
		"",
		"already lower",
		"Mixed CASE ascii_123",
		"ÉTÉ chaud",
		"ΣΊΣΥΦΟΣ",
		"İstanbul",
		"invalid \xff byte",
		"truncated \xe2\x82 sequence",
		"\xe2\x82\xac euro",
	} {
		assert.Equal(t, strings.ToLower(s), string(appendText(nil, s, false)), "%q", s)
		assert.Equal(t, s, string(appendText(nil, s, true)), "%q", s)
	}
}

// TestUnicodeToLowerIsIdempotent checks the assumption that lets search pass
// lower-case text through the case-folding root of a filter: lowering twice
// gives the same text as lowering once.
func TestUnicodeToLowerIsIdempotent(t *testing.T) {
	for r := rune(0); r <= unicode.MaxRune; r++ {
		lower := unicode.ToLower(r)
		if unicode.ToLower(lower) != lower {
			t.Fatalf("unicode.ToLower is not idempotent for %U", r)
		}
	}
}

func TestUserMetricsMetadataSearchCaseInsensitiveAllocationsDoNotGrowWithMetrics(t *testing.T) {
	filter := substringSearchFilter(t, "absent")
	allocs := func(numMetrics int) float64 {
		mm := newSearchTestMetadataMap()
		for i := range numMetrics {
			addSearchTestMetadata(t, mm, fmt.Sprintf("metric_%05d", i), "Total Time Spent in Été.")
		}
		return testing.AllocsPerRun(10, func() {
			_, err := mm.search(t.Context(), filter, false, storage.OrderByValueAsc, "", 10)
			require.NoError(t, err)
		})
	}
	assert.Equal(t, allocs(1000), allocs(2000))
}

func TestUserMetricsMetadataSearchSkipsFilterForNamesBeforeCursor(t *testing.T) {
	mm := newSearchTestMetadataMap()
	for _, name := range []string{"metric_a", "metric_b", "metric_c", "metric_d"} {
		addSearchTestMetadata(t, mm, name, "help")
	}

	for order, want := range map[storage.Ordering][]string{
		storage.OrderByValueAsc:  {"metric_c\nhelp", "metric_d\nhelp"},
		storage.OrderByValueDesc: {"metric_a\nhelp"},
	} {
		filter := &recordingSearchFilter{}
		_, err := mm.search(t.Context(), filter, true, order, "metric_b", 0)
		require.NoError(t, err)
		slices.Sort(filter.seen)
		assert.Equal(t, want, filter.seen, "order=%v", order)
	}
}

func TestUserMetricsMetadataSearchHonoursCtxCancellation(t *testing.T) {
	mm := newSearchTestMetadataMap()
	for i := range searchBatchSize {
		addSearchTestMetadata(t, mm, fmt.Sprintf("metric_%d", i), "help")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	results, err := mm.search(ctx, keepFilter{}, true, storage.OrderByValueAsc, "", 0)
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, results)
}

func TestUserMetricsMetadataSearchWithoutMetadata(t *testing.T) {
	mm := newSearchTestMetadataMap()
	results, err := mm.search(t.Context(), keepFilter{}, true, storage.OrderByValueAsc, "", 10)
	require.NoError(t, err)
	assert.Empty(t, results)
}

func BenchmarkUserMetricsMetadataSearch(b *testing.B) {
	const numMetrics = 100_000
	for _, helpsPerMetric := range []int{1, 10} {
		mm := newSearchTestMetadataMap()
		for i := range numMetrics {
			name := fmt.Sprintf("component_%d_requests_seconds_total", i)
			for h := range helpsPerMetric {
				addSearchTestMetadata(b, mm, name, fmt.Sprintf("Total time spent by Component%d on requests of kind %d, in seconds.", i%100, h))
			}
		}
		for _, limit := range []int{100, 10_000} {
			for _, expression := range []string{
				// Matches every metric.
				"requests",
				// Matches one metric in 100, in both cases.
				"Component42",
				"requests AND NOT Component42",
			} {
				for _, caseSensitive := range []bool{true, false} {
					params, err := streaminglabelvalues.NewExpressionParams(expression, caseSensitive, streaminglabelvalues.FuzzAlgSubstring, 0)
					require.NoError(b, err)
					filter, err := streaminglabelvalues.BuildFilter(params)
					require.NoError(b, err)
					b.Run(fmt.Sprintf("helps=%d/limit=%d/expression=%s/case_sensitive=%t", helpsPerMetric, limit, expression, caseSensitive), func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							if _, err := mm.search(b.Context(), filter, caseSensitive, storage.OrderByValueAsc, "", limit); err != nil {
								b.Fatal(err)
							}
						}
					})
				}
			}
		}
	}
}
