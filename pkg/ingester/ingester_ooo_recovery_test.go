// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	downscaleTenantID        = "antithesis-test-downscale-8781"
	downscaleIngesterID      = "ingester-zone-a-0"
	downscaleDriverName      = "mimir_write_path/parallel_driver_query_during_downscale"
	downscaleFloatMetric     = "mimir_antithesis_test_sine_wave"
	downscaleHistogramMetric = "mimir_antithesis_test_histogram_sine_wave"

	// This OOO timestamp is from the exported WBL: 2026-09-07 09:47:39 UTC.
	// Neighboring timestamps below are fixture choices, not recovered requests.
	downscaleDelayedTimestamp int64 = 1788774459000
	downscaleInitialTimestamp       = downscaleDelayedTimestamp - 3000
	downscaleMiddleTimestamp        = downscaleDelayedTimestamp - 2000
	downscaleNewerTimestamp         = downscaleDelayedTimestamp + 1000
)

func TestIngester_FlushHandlerPreservesOOOAfterRestart(t *testing.T) {
	for _, scenario := range []struct {
		name                      string
		recoverCheckpointSnapshot bool
	}{
		{name: "completed_flush"},
		{name: "checkpoint_before_ooo_compaction", recoverCheckpointSnapshot: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dataDir := filepath.Join(t.TempDir(), "tsdb")
			expected := map[string]int{downscaleFloatMetric: 1, downscaleHistogramMetric: 1}
			ingester, _ := startIngester(t, dataDir)

			// 1. Model the earlier driver writes and repeated flushes around v1535–1611.
			// Each flush empties head, so the next write recreates identical labels with
			// new refs. Their definitions remain in older WAL segments. This gives three
			// generations, like 121/141/161 in the run, without hard-coding numeric IDs.
			pushSamples(t, ingester, "earlier invocation", prepareWrite(downscaleInitialTimestamp))
			require.Equal(t, int64(2), ingester.seriesCount.Load())
			flushIngester(t, ingester, 2)
			require.Zero(t, ingester.seriesCount.Load())
			pushSamples(t, ingester, "earlier invocation", prepareWrite(downscaleMiddleTimestamp))
			require.Equal(t, int64(2), ingester.seriesCount.Load())
			flushIngester(t, ingester, 1)
			require.Zero(t, ingester.seriesCount.Load())

			// 2. A/B are illustrative invocations of the same parallel driver, not
			// identities recovered from the logs. Neither adds a writer label.
			// A prepares its request first but is delayed; B arrives first. Sequential
			// delivery fixes that ordering without goroutines, sleeps, or network faults.
			writerA, writerB := "writer A", "writer B"
			delayedWrite := prepareWrite(downscaleDelayedTimestamp)
			newerWrite := prepareWrite(downscaleNewerTimestamp)
			pushSamples(t, ingester, writerB, newerWrite)
			pushSamples(t, ingester, writerA, delayedWrite)
			require.Equal(t, int64(2), ingester.seriesCount.Load())
			require.Equal(t, expected, querySamples(t, ingester))

			// 3. Fill WAL segments through Push so the next flush will checkpoint.
			// Restart before flushing: replay merges all three generations' refs in memory.
			// This corresponds to the successful replay at v1664.663.
			fillWAL(t, ingester)
			stopIngester(t, ingester)
			ingester, _ = startIngester(t, dataDir)
			require.Equal(t, expected, querySamples(t, ingester))

			// A second successful replay mirrors the recovery at v1715.300. The run
			// had a fault-injector kill at v1704.732; this test uses the shutdown API.
			stopIngester(t, ingester)
			ingester, checkpointLogger := startIngester(t, dataDir)
			require.Equal(t, expected, querySamples(t, ingester))

			// 4. Copy disk state synchronously after checkpointing, before the same
			// compaction worker proceeds to OOO compaction. No writes run concurrently.
			// Cycle 6 is the recorded flush at v1799.305; checkpointing followed at
			// v1800.339. We capture the dependency gap before OOO compaction starts.
			// The live flush then completes; its later changes cannot alter the copy.
			recoveryDir := dataDir
			if scenario.recoverCheckpointSnapshot {
				recoveryDir = filepath.Join(t.TempDir(), "tsdb")
				checkpointLogger.snapshotAtNextCheckpoint(dataDir, recoveryDir)
			}
			flushIngester(t, ingester, 6)
			require.Positive(t, checkpointLogger.checkpoints.Load(), "flush must checkpoint the WAL")
			if scenario.recoverCheckpointSnapshot {
				captured, err := checkpointLogger.snapshotResult()
				require.NoError(t, err)
				require.True(t, captured, "checkpoint snapshot must be captured before OOO compaction")
			}
			require.Equal(t, expected, querySamples(t, ingester), "completed live flush preserves both samples")
			stopIngester(t, ingester)

			// 5. Recover through Mimir from either the completed-flush directory or
			// the checkpoint snapshot. The latter models a stop at that boundary;
			// this test does not kill a process or test operating-system crash behavior.
			// This corresponds to the failing recovery at v1821.400. We do not reproduce
			// the EOF or an unfinished OOO block: the snapshot models an earlier boundary.
			restarted, recoveryLogger := startIngester(t, recoveryDir)
			recovered := querySamples(t, restarted)
			t.Logf("scenario=%s recovered=%v unknownOOORefWarnings=%d", scenario.name, recovered, recoveryLogger.unknownOOORefWarnings.Load())
			require.Equal(t, expected, recovered, "acknowledged OOO samples must survive flush and restart")
		})
	}
}

func startIngester(t *testing.T, dataDir string) (*Ingester, *oooCheckpointSnapshotLogger) {
	t.Helper()
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.InstanceID = downscaleIngesterID
	cfg.BlocksStorageConfig.TSDB.WALSegmentSizeBytes = 32768
	cfg.BlocksStorageConfig.TSDB.WALCompressionEnabled = false
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 0
	cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
	cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown = false
	cfg.BlocksStorageConfig.TSDB.MemorySnapshotOnShutdown = false
	limits := defaultLimitsTestConfig()
	limits.NativeHistogramsIngestionEnabled = true
	limits.OutOfOrderTimeWindow = model.Duration(time.Minute)
	overrides := validation.NewOverrides(limits, nil)
	ingester, ring, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, overrides, nil, dataDir, filepath.Join(filepath.Dir(dataDir), "bucket"), prometheus.NewRegistry())
	require.NoError(t, err)
	logger := &oooCheckpointSnapshotLogger{}
	// Tenant TSDB loggers are derived during startup or the first Push.
	ingester.logger = logger
	startAndWaitHealthy(t, ingester, ring)
	return ingester, logger
}

func stopIngester(t *testing.T, ingester *Ingester) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
}

func flushIngester(t *testing.T, ingester *Ingester, cycle int) {
	t.Helper()
	t.Logf("driver=%s actor=flush-caller event=downscale-query-flush-started cycle=%d ingester=%s", downscaleDriverName, cycle, downscaleIngesterID)
	response := httptest.NewRecorder()
	ingester.FlushHandler(response, httptest.NewRequest(http.MethodPost, "/ingester/flush?wait=true&tenant="+downscaleTenantID, nil))
	require.Equal(t, http.StatusNoContent, response.Code)
}

func querySamples(t *testing.T, ingester *Ingester) map[string]int {
	t.Helper()
	response := stream{ctx: user.InjectOrgID(context.Background(), downscaleTenantID)}
	require.NoError(t, ingester.QueryStream(&client.QueryRequest{
		StartTimestampMs:         downscaleDelayedTimestamp,
		EndTimestampMs:           downscaleDelayedTimestamp,
		Matchers:                 []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: "__name__", Value: "mimir_antithesis_test_(histogram_)?sine_wave"}},
		StreamingChunksBatchSize: 64,
	}, &response))
	matrix, err := client.StreamsToMatrixForTests(model.Time(downscaleDelayedTimestamp), model.Time(downscaleDelayedTimestamp), response.responses)
	require.NoError(t, err)
	found := map[string]int{}
	for _, series := range matrix {
		name := string(series.Metric[model.MetricNameLabel])
		require.Equal(t, model.LabelValue(downscaleTenantID), series.Metric["tenant"])
		require.Equal(t, model.LabelValue("0"), series.Metric["series_id"])
		for _, sample := range series.Values {
			require.Equal(t, downscaleFloatMetric, name)
			require.Equal(t, model.Time(downscaleDelayedTimestamp), sample.Timestamp)
			require.Equal(t, model.SampleValue(downscaleDelayedTimestamp), sample.Value)
			found[name]++
		}
		for _, sample := range series.Histograms {
			require.Equal(t, downscaleHistogramMetric, name)
			require.Equal(t, model.Time(downscaleDelayedTimestamp), sample.Timestamp)
			require.Equal(t, model.FloatString(1), sample.Histogram.Count)
			require.Equal(t, model.FloatString(0), sample.Histogram.Sum)
			found[name]++
		}
	}
	return found
}

// Both writers use the run's labels for series_id=0. One float and one histogram
// are enough to reproduce the two replay paths; payload values stay synthetic.
func prepareWrite(timestamp int64) *mimirpb.WriteRequest {
	return &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			{TimeSeries: &mimirpb.TimeSeries{
				Labels:  mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("__name__", downscaleFloatMetric, "tenant", downscaleTenantID, "series_id", "0")),
				Samples: []mimirpb.Sample{{TimestampMs: timestamp, Value: float64(timestamp)}},
			}},
			{TimeSeries: &mimirpb.TimeSeries{
				Labels:     mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("__name__", downscaleHistogramMetric, "tenant", downscaleTenantID, "series_id", "0")),
				Histograms: []mimirpb.Histogram{mimirpb.FromFloatHistogramToHistogramProto(timestamp, &histogram.FloatHistogram{Schema: 0, Count: 1, ZeroCount: 1, ZeroThreshold: 0.001})},
			}},
		},
	}
}

func pushSamples(t *testing.T, ingester *Ingester, writer string, request *mimirpb.WriteRequest) {
	t.Helper()
	// Read before Push, which may recycle the request's series buffers.
	timestamp := request.Timeseries[0].Samples[0].TimestampMs
	_, err := ingester.Push(user.InjectOrgID(context.Background(), downscaleTenantID), request)
	require.NoError(t, err)
	t.Logf("driver=%s actor=%s tenant=%s timestamp=%d acknowledged", downscaleDriverName, writer, downscaleTenantID, timestamp)
}

// Extra labels fill several 32 KiB segments without adding samples to the query.
// No direct TSDB or WAL APIs are used to make checkpointing eligible.
func fillWAL(t *testing.T, ingester *Ingester) {
	t.Helper()
	ctx := user.InjectOrgID(context.Background(), downscaleTenantID)
	for n := 0; n < 32; n++ {
		_, err := ingester.Push(ctx, writeRequestSingleSeries(
			labels.FromStrings("__name__", "wal_filler", "padding", strings.Repeat("x", 8192), "series_id", fmt.Sprint(n)),
			[]mimirpb.Sample{{TimestampMs: downscaleNewerTimestamp, Value: float64(n)}},
		))
		require.NoError(t, err)
	}
}

// The log event is synchronous with the compaction worker. Copying here freezes
// the recoverable disk state before OOO compaction can persist its head chunks.
// Snapshot fields are guarded because Log runs on an ingester goroutine.
type oooCheckpointSnapshotLogger struct {
	checkpoints           atomic.Int64
	unknownOOORefWarnings atomic.Int64
	snapshotMu            sync.Mutex
	snapshotSource        string
	snapshotDestination   string
	snapshotCaptured      bool
	snapshotErr           error
}

func (l *oooCheckpointSnapshotLogger) snapshotAtNextCheckpoint(source, destination string) {
	l.snapshotMu.Lock()
	defer l.snapshotMu.Unlock()
	l.snapshotSource = source
	l.snapshotDestination = destination
}

func (l *oooCheckpointSnapshotLogger) snapshotResult() (bool, error) {
	l.snapshotMu.Lock()
	defer l.snapshotMu.Unlock()
	return l.snapshotCaptured, l.snapshotErr
}

func (l *oooCheckpointSnapshotLogger) Log(keyvals ...interface{}) error {
	for n := 0; n+1 < len(keyvals); n += 2 {
		if keyvals[n] != "msg" {
			continue
		}
		switch keyvals[n+1] {
		case "Unknown series references for ooo WAL replay":
			l.unknownOOORefWarnings.Add(1)
		case "WAL checkpoint complete":
			l.checkpoints.Add(1)
			l.snapshotMu.Lock()
			if l.snapshotDestination != "" {
				l.snapshotErr = os.CopyFS(l.snapshotDestination, os.DirFS(l.snapshotSource))
				l.snapshotCaptured = l.snapshotErr == nil
				l.snapshotDestination = ""
			}
			l.snapshotMu.Unlock()
		}
	}
	return nil
}

func (*oooCheckpointSnapshotLogger) DebugEnabled() bool { return false }
