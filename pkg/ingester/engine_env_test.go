// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"os"
	"slices"
	"strings"
	"testing"
)

func init() {
	testEngine = os.Getenv("MIMIR_TEST_TSDB_ENGINE")
}

// skipIfSeriesstore skips tests of Prometheus TSDB internals the seriesstore engine doesn't have.
func skipIfSeriesstore(t testing.TB, reason string) {
	t.Helper()
	if testEngine == "seriesstore" {
		t.Skip("not applicable to the seriesstore engine: " + reason)
	}
}

// withoutPrometheusHeadMetrics drops the TSDB head's internal metrics, like exemplar storage and
// out-of-order deltas, which the seriesstore engine doesn't have.
func withoutPrometheusHeadMetrics(names []string) []string {
	if testEngine != "seriesstore" {
		return names
	}
	return slices.DeleteFunc(slices.Clone(names), func(name string) bool {
		return strings.HasPrefix(name, "cortex_ingester_tsdb_")
	})
}
