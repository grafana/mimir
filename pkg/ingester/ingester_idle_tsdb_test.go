// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// TestIngester_ReopenedTSDBWithoutInOrderSamplesIsNotIdle verifies that a TSDB reopened with a head
// that has series but no in-order samples is not considered idle right away.
//
// This head state is reached when a partition becomes inactive: all series become non-owned, early
// compaction of non-owned series writes them with CompactSelectedSeries, and series with out-of-order
// data stay in the head. The idle compaction then checkpoints the WAL. After a restart, the in-order
// WAL samples are below the min valid time and blocks written by CompactSelectedSeries don't raise
// the head max time, so Head().MaxTime() stays math.MinInt64. Before the fix, lastUpdate
// was seeded from that value and, with shipping disabled, the idle-close check deleted the TSDB and
// its recent blocks within minutes of the restart.
//
// The idle compaction also writes the remaining in-order sample to a plain block, which would raise the
// head max time on reopen, so the test removes that block before the restart. The preconditions asserted
// after the restart make sure the test reaches the head state and cannot pass vacuously. If a Prometheus
// update changes the state, the test needs a new way to reach it, not weaker assertions.
//
// The second case has out-of-order data much older than the newest block, to check that
// lastUpdate follows the local blocks and not only the out-of-order samples in the head.
func TestIngester_ReopenedTSDBWithoutInOrderSamplesIsNotIdle(t *testing.T) {
	tests := map[string]struct {
		oooSeries            bool
		oooTimeWindow        time.Duration
		oooSampleAge         time.Duration
		closeIdleTSDBTimeout time.Duration
	}{
		"empty head": {
			oooTimeWindow:        time.Hour,
			oooSampleAge:         40 * time.Minute,
			closeIdleTSDBTimeout: 2 * time.Hour,
		},
		"recent out-of-order sample": {
			oooSeries:            true,
			oooTimeWindow:        time.Hour,
			oooSampleAge:         40 * time.Minute,
			closeIdleTSDBTimeout: 13 * time.Hour,
		},
		"out-of-order sample older than the close idle timeout": {
			oooSeries:            true,
			oooTimeWindow:        6 * time.Hour,
			oooSampleAge:         3 * time.Hour,
			closeIdleTSDBTimeout: 2 * time.Hour,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			testReopenedTSDBWithoutInOrderSamples(t, tc.oooSeries, tc.oooTimeWindow, tc.oooSampleAge, tc.closeIdleTSDBTimeout)
		})
	}
}

func testReopenedTSDBWithoutInOrderSamples(t *testing.T, oooSeries bool, oooTimeWindow, oooSampleAge, closeIdleTSDBTimeout time.Duration) {
	const numSeries = 3000 // Enough WAL data for the idle compaction to checkpoint the WAL.

	dataDir := t.TempDir()
	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = model.Duration(oooTimeWindow)

	newConfig := func() Config {
		cfg := defaultIngesterTestConfig(t)
		cfg.BlocksStorageConfig.TSDB.Dir = dataDir
		cfg.BlocksStorageConfig.TSDB.ShipInterval = 0
		cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBWhenShippingDisabled = true
		cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = closeIdleTSDBTimeout
		cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBInterval = time.Hour // The test runs idle checks itself.
		cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout = time.Hour
		cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour // The test runs compactions itself.
		cfg.BlocksStorageConfig.TSDB.WALSegmentSizeBytes = 32 * 1024
		return cfg
	}

	// The head keeps the series with out-of-order data, if any.
	expectedSeries := uint64(0)
	if oooSeries {
		expectedSeries = 1
	}

	now := time.Now()
	sampleTS := now.Add(-30 * time.Minute).UnixMilli()
	oooTS := now.Add(-oooSampleAge).UnixMilli()
	ctx := user.InjectOrgID(context.Background(), userID)

	// First ingester run.
	{
		i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, newConfig(), limits, nil, dataDir, prometheus.NewRegistry())
		require.NoError(t, err)
		startAndWaitHealthy(t, i, r)

		lbls := make([][]mimirpb.LabelAdapter, 0, numSeries)
		samples := make([]mimirpb.Sample, 0, numSeries)
		for n := 0; n < numSeries; n++ {
			lbls = append(lbls, mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, "test", "series", fmt.Sprintf("%05d", n))))
			samples = append(samples, mimirpb.Sample{TimestampMs: sampleTS, Value: float64(n)})
		}
		_, err = i.Push(ctx, mimirpb.ToWriteRequest(lbls, samples, nil, nil, mimirpb.API))
		require.NoError(t, err)

		if oooSeries {
			// A late sample older than the head min valid time but within the out-of-order window
			// for one series, which then has both in-order and out-of-order data.
			_, err = i.Push(ctx, mockWriteRequest(t, labels.FromStrings(model.MetricNameLabel, "test", "series", "00000"), 1, oooTS))
			require.NoError(t, err)
		}

		db := i.getTSDB(userID)
		require.NotNil(t, db)

		// Early compaction of non-owned series, as done by compactBlocksDueToNonOwnedSeries,
		// with every series non-owned.
		require.NoError(t, db.db.(prometheusEngine).db.CompactSelectedSeries(headSeriesRefsForTest(t, db)))
		require.Equal(t, expectedSeries, db.Head().NumSeries(), "only the series with out-of-order data should stay in the head")

		// Idle compaction: no in-order data is left, but the WAL is checkpointed.
		db.setLastUpdate(now.Add(-2 * time.Hour))
		i.compactBlocks(context.Background(), false, 0, nil)

		if oooSeries {
			require.Equal(t, oooTS, db.Head().TimeBounds().MaxOOOTime)
		}

		// The idle compaction also wrote the remaining in-order sample of that series to a plain block,
		// which would raise the head max time on reopen. Remove it, so that the local blocks only
		// have blocks that are skipped for that, like the ones written by CompactSelectedSeries.
		var removed int
		for _, b := range db.Blocks() {
			if len(b.Meta().Compaction.Hints) == 0 {
				removed++
				require.NoError(t, os.RemoveAll(b.Dir()))
			}
		}
		require.Equal(t, int(expectedSeries), removed)

		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
	}

	// Second ingester run on the same data directory.
	i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, newConfig(), limits, nil, dataDir, prometheus.NewRegistry())
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	db := i.getTSDB(userID)
	require.NotNil(t, db)
	dir := db.dir

	// Preconditions: the reopened head has no series, or only the one with out-of-order data and no in-order samples.
	// With an empty head, the WAL replay still sets the head max time from samples of the deleted series,
	// and createTSDB doesn't use it.
	require.Equal(t, expectedSeries, db.Head().NumSeries(), "only the out-of-order-only series should be replayed")
	if oooSeries {
		require.Equal(t, int64(math.MinInt64), db.Head().TimeBounds().MaxTime)
		require.Equal(t, oooTS, db.Head().TimeBounds().MaxOOOTime)
	}

	require.False(t, db.isIdle(time.Now(), closeIdleTSDBTimeout), "lastUpdate is older than the newest local data: %s", db.getLastUpdate())

	i.compactBlocks(context.Background(), false, 0, nil)
	require.NotEqual(t, tsdbIdleClosed, i.closeAndDeleteUserTSDBIfIdle(userID))
	_, err = os.Stat(dir)
	require.NoError(t, err, "local TSDB should not have been deleted")
}

func headSeriesRefsForTest(t *testing.T, db *userTSDB) []storage.SeriesRef {
	t.Helper()
	ir, err := db.Head().Index()
	require.NoError(t, err)
	defer func() { _ = ir.Close() }()

	p, err := ir.Postings(context.Background(), model.MetricNameLabel, "test")
	require.NoError(t, err)
	var refs []storage.SeriesRef
	for p.Next() {
		refs = append(refs, p.At())
	}
	require.NoError(t, p.Err())
	return refs
}
