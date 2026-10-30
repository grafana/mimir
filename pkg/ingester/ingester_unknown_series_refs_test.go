// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"github.com/prometheus/prometheus/util/compression"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/validation"
)

// writeUnknownSeriesRefWAL crafts, by hand, the exact WAL record sequence that the
// truncateSeries gcSeries-before-tombstone-write race produces: a series declaration, a
// sample for it, a second declaration for the exact same labels (which on replay gets
// aliased onto the first one via multiRef), a sample for the alias that lands before the
// first series' tombstone (and so still resolves), the first series' full-range tombstone,
// and finally a sample for the alias that lands after the tombstone and is therefore
// reported as an unknown series reference.
//
// Writing these records directly (rather than reproducing the live race through real
// Push calls) makes the test deterministic: the by-ref worker that applies WAL records
// processes them in the order they appear in the WAL, so the ordering below reliably
// reproduces the "sample after tombstone" outcome without depending on goroutine timing.
func writeUnknownSeriesRefWAL(t *testing.T, walDir string, lbls labels.Labels) {
	t.Helper()

	enc := record.Encoder{}
	records := [][]byte{
		enc.Series([]record.RefSeries{{Ref: 1, Labels: lbls}}, nil),
		enc.Samples([]record.RefSample{{Ref: 1, T: 100, V: 1}}, nil),
		enc.Series([]record.RefSeries{{Ref: 2, Labels: lbls}}, nil),
		enc.Samples([]record.RefSample{{Ref: 2, T: 200, V: 2}}, nil),
		enc.Tombstones([]tombstones.Stone{{Ref: 1, Intervals: tombstones.Intervals{{Mint: math.MinInt64, Maxt: math.MaxInt64}}}}, nil),
		enc.Samples([]record.RefSample{{Ref: 2, T: 300, V: 3}}, nil),
	}

	wl, err := wlog.New(nil, nil, walDir, compression.None)
	require.NoError(t, err)
	require.NoError(t, wl.Log(records...))
	require.NoError(t, wl.Close())
}

func TestIngester_UnknownSeriesRefsDiagnosticsCapture(t *testing.T) {
	const userID = "user0"

	tempDir := t.TempDir()
	lbls := labels.FromStrings("name", "recreated")
	writeUnknownSeriesRefWAL(t, filepath.Join(tempDir, userID, "wal"), lbls)

	limits := defaultLimitsTestConfig()
	overrides := validation.NewOverrides(limits, nil)

	ingesterCfg := defaultIngesterTestConfig(t)
	ingesterCfg.BlocksStorageConfig.TSDB.Dir = tempDir
	ingesterCfg.BlocksStorageConfig.TSDB.UnknownRefsDiagnosticsEnabled = true
	ingesterCfg.BlocksStorageConfig.Bucket.Backend = "s3"
	ingesterCfg.BlocksStorageConfig.Bucket.S3.Endpoint = "localhost"

	diagDir := ingesterCfg.BlocksStorageConfig.TSDB.UnknownRefsDiagnosticsDir(userID)

	registry := prometheus.NewRegistry()
	ing, err := New(ingesterCfg, overrides, createAndStartRing(t, ingesterCfg.IngesterRing.ToRingConfig()), nil, nil, nil, registry, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), ing)
		ing.closeAllTSDB()
		ing.subservicesWatcher.Close()
	})

	// The TSDB must have loaded successfully despite the unknown series reference.
	require.NotNil(t, ing.getTSDB(userID))

	// The unknown series reference must have actually been reported, through Mimir's own
	// aggregated per-tenant metric.
	require.Equal(t, float64(1), unknownRefsMetricValue(t, registry, userID, "samples"),
		"precondition: this ingester startup must actually report an unknown sample reference")

	// Exactly one timestamped capture directory must have been created under the sibling
	// diagnostics dir (never nested under the tenant's own TSDB dir).
	captureDirs := listCaptureDirs(t, diagDir)
	require.Len(t, captureDirs, 1, "exactly one timestamped capture directory must have been created")
	captureDir := filepath.Join(diagDir, captureDirs[0])

	diagContent, err := os.ReadFile(filepath.Join(captureDir, "diagnostics.txt"))
	require.NoError(t, err)
	require.Contains(t, string(diagContent), "multiRef")
	require.Contains(t, string(diagContent), "walExpiries")

	walEntries, err := os.ReadDir(filepath.Join(captureDir, "wal"))
	require.NoError(t, err)
	require.NotEmpty(t, walEntries, "at least one WAL segment must have been copied")

	// Restarting the ingester against the same directory must not capture a second time,
	// even though WAL replay will report the same unknown reference again.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
	ing.closeAllTSDB()

	ing2, err := New(ingesterCfg, overrides, createAndStartRing(t, ingesterCfg.IngesterRing.ToRingConfig()), nil, nil, nil, prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing2))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), ing2)
		ing2.closeAllTSDB()
		ing2.subservicesWatcher.Close()
	})

	require.Len(t, listCaptureDirs(t, diagDir), 1, "a later restart must not create a second capture directory")
}

// unknownRefsMetricValue returns the value of cortex_ingester_tsdb_wal_replay_unknown_refs_total
// for the given user and type label, or 0 if no such series was exposed.
func unknownRefsMetricValue(t *testing.T, registry *prometheus.Registry, userID, refType string) float64 {
	t.Helper()

	families, err := registry.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() != "cortex_ingester_tsdb_wal_replay_unknown_refs_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			var gotUser, gotType string
			for _, pair := range metric.GetLabel() {
				switch pair.GetName() {
				case "user":
					gotUser = pair.GetValue()
				case "type":
					gotType = pair.GetValue()
				}
			}
			if gotUser == userID && gotType == refType {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func listCaptureDirs(t *testing.T, diagDir string) []string {
	t.Helper()

	entries, err := os.ReadDir(diagDir)
	require.NoError(t, err)

	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}
	return dirs
}
