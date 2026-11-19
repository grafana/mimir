// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"os"
	"slices"
	"strings"
	"testing"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

func init() {
	// Read as each test runs, not here: go test records the environment a run reads to cache its results by, from when
	// the tests start, so a read in init would let a run on one engine reuse a result of the other.
	testEngine = func() string { return os.Getenv("MIMIR_TEST_TSDB_ENGINE") }
	testNoNativeActive = func() bool { return os.Getenv("MIMIR_TEST_NO_NATIVE_ACTIVE") != "" }
	testNoIdleEviction = true
}

// skipIfSeriesstore skips tests of Prometheus TSDB internals the seriesstore engine doesn't have.
func skipIfSeriesstore(t testing.TB, reason string) {
	t.Helper()
	if testEngine() == mimir_tsdb.EngineSeriesstore {
		t.Skip("not applicable to the seriesstore engine: " + reason)
	}
}

// withoutPrometheusHeadMetrics drops the TSDB head's internal metrics, like exemplar storage and
// out-of-order deltas, which the seriesstore engine doesn't have.
func withoutPrometheusHeadMetrics(names []string) []string {
	if testEngine() != mimir_tsdb.EngineSeriesstore {
		return names
	}
	return slices.DeleteFunc(slices.Clone(names), func(name string) bool {
		return strings.HasPrefix(name, "cortex_ingester_tsdb_")
	})
}
