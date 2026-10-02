// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// substringFilter is a minimal storage.Filter for tests: it accepts any
// value containing term, with a fixed score.
type substringFilter struct {
	term string
}

func (f substringFilter) Accept(value string) (bool, float64) {
	if strings.Contains(value, f.term) {
		return true, 1.0
	}
	return false, 0
}

// newSearchHelpTestMM builds a userMetricsMetadata with no limits, so every
// add() call below is expected to succeed.
func newSearchHelpTestMM(t *testing.T) *userMetricsMetadata {
	ring := &ringCountMock{instancesCount: 1, zonesCount: 1}
	limits := validation.NewOverrides(validation.Limits{}, nil)
	strategy := newIngesterRingLimiterStrategy(ring, 1, false, "", limits.IngestionTenantShardSize)
	limiter := NewLimiter(limits, strategy)
	metrics := newIngesterMetrics(
		prometheus.NewPedanticRegistry(),
		true,
		func() *InstanceLimits { return nil },
		nil, nil, nil,
	)
	return newMetadataMap(limiter, metrics, newIngesterErrSamplers(0), "test")
}

func mustAdd(t *testing.T, mm *userMetricsMetadata, name, help string) {
	err := mm.add(name, &mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: name, Help: help})
	require.NoError(t, err)
}

// collectSearchResults drains a storage.SearchResultSet into a slice of
// values, in iteration order, and fails the test on any iteration error.
func collectSearchResults(t *testing.T, rs storage.SearchResultSet) []string {
	t.Helper()
	defer func() { require.NoError(t, rs.Close()) }()
	var got []string
	for rs.Next() {
		got = append(got, rs.At().Value)
	}
	require.NoError(t, rs.Err())
	return got
}

func TestUserMetricsMetadata_searchHelp(t *testing.T) {
	// Fixture shared by the sub-tests below:
	//   metric_a: one record, HELP "alpha text"
	//   metric_b: one record, HELP "beta text"
	//   metric_c: two records; only the second contains "beta"
	newFixture := func(t *testing.T) *userMetricsMetadata {
		mm := newSearchHelpTestMM(t)
		mustAdd(t, mm, "metric_a", "alpha text")
		mustAdd(t, mm, "metric_b", "beta text")
		mustAdd(t, mm, "metric_c", "unrelated text")
		mustAdd(t, mm, "metric_c", "also contains beta")
		return mm
	}

	t.Run("nil filter returns every metric name, sorted ascending", func(t *testing.T) {
		mm := newFixture(t)
		rs := mm.searchHelp(nil, storage.OrderByValueAsc, "", 0)
		require.Equal(t, []string{"metric_a", "metric_b", "metric_c"}, collectSearchResults(t, rs))
	})

	t.Run("filter restricts to names whose HELP matches", func(t *testing.T) {
		mm := newFixture(t)
		rs := mm.searchHelp(substringFilter{term: "beta"}, storage.OrderByValueAsc, "", 0)
		// metric_b matches directly; metric_c matches via its second record only.
		require.Equal(t, []string{"metric_b", "metric_c"}, collectSearchResults(t, rs))
	})

	t.Run("a metric matches if ANY of its metadata records match", func(t *testing.T) {
		mm := newSearchHelpTestMM(t)
		mustAdd(t, mm, "metric_c", "unrelated text")
		mustAdd(t, mm, "metric_c", "also contains beta")

		rs := mm.searchHelp(substringFilter{term: "beta"}, storage.OrderByValueAsc, "", 0)
		require.Equal(t, []string{"metric_c"}, collectSearchResults(t, rs))
	})

	t.Run("a metric is excluded if NO metadata record matches", func(t *testing.T) {
		mm := newSearchHelpTestMM(t)
		mustAdd(t, mm, "metric_c", "unrelated text")
		mustAdd(t, mm, "metric_c", "also unrelated")

		rs := mm.searchHelp(substringFilter{term: "beta"}, storage.OrderByValueAsc, "", 0)
		require.Empty(t, collectSearchResults(t, rs))
	})

	t.Run("descending order reverses the result", func(t *testing.T) {
		mm := newFixture(t)
		rs := mm.searchHelp(nil, storage.OrderByValueDesc, "", 0)
		require.Equal(t, []string{"metric_c", "metric_b", "metric_a"}, collectSearchResults(t, rs))
	})

	t.Run("resumeAfter skips already-seen names, ascending", func(t *testing.T) {
		mm := newFixture(t)
		rs := mm.searchHelp(nil, storage.OrderByValueAsc, "metric_a", 0)
		require.Equal(t, []string{"metric_b", "metric_c"}, collectSearchResults(t, rs))
	})

	t.Run("resumeAfter skips already-seen names, descending", func(t *testing.T) {
		mm := newFixture(t)
		rs := mm.searchHelp(nil, storage.OrderByValueDesc, "metric_c", 0)
		require.Equal(t, []string{"metric_b", "metric_a"}, collectSearchResults(t, rs))
	})

	t.Run("limit truncates the result", func(t *testing.T) {
		mm := newFixture(t)
		rs := mm.searchHelp(nil, storage.OrderByValueAsc, "", 1)
		require.Equal(t, []string{"metric_a"}, collectSearchResults(t, rs))
	})
}

// TestUserMetricsMetadata_searchHelp_ConcurrentAdd exercises searchHelp
// against concurrent add() calls under -race. It only verifies the absence
// of a data race (and that nothing panics); it makes no assertion about the
// exact result content, since that's inherently racing with the writer.
// This would catch a copy-out step that reads mm.metricToMetadata without
// holding mtx.RLock(), or one that escapes the lock before finishing the
// copy.
func TestUserMetricsMetadata_searchHelp_ConcurrentAdd(t *testing.T) {
	mm := newSearchHelpTestMM(t)
	mustAdd(t, mm, "metric_a", "seed text")

	const iterations = 500

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			err := mm.add("metric_b", &mimirpb.MetricMetadata{
				Type:             mimirpb.COUNTER,
				MetricFamilyName: "metric_b",
				Help:             "concurrently added text",
			})
			require.NoError(t, err)
		}
	}()

	for i := 0; i < iterations; i++ {
		rs := mm.searchHelp(nil, storage.OrderByValueAsc, "", 0)
		collectSearchResults(t, rs)
	}

	wg.Wait()
}
