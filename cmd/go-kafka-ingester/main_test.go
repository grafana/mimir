// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/chunks"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/kafka"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/limits"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/record"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/segment"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/store"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

type stuckServer struct {
	stop chan struct{}
}

func (s *stuckServer) GracefulStop() { <-s.stop }
func (s *stuckServer) Stop()         { close(s.stop) }

type quickServer struct{ stopped bool }

func (s *quickServer) GracefulStop() {}
func (s *quickServer) Stop()         { s.stopped = true }

func TestGracefulShutdownGivesUpOnStuckRequests(t *testing.T) {
	started := time.Now()
	boundedGracefulStop(&stuckServer{stop: make(chan struct{})}, 50*time.Millisecond)
	require.GreaterOrEqual(t, time.Since(started), 50*time.Millisecond)
	// A shutdown whose requests finish in time isn't cut short.
	quick := &quickServer{}
	boundedGracefulStop(quick, time.Hour)
	require.False(t, quick.stopped)
}

func TestHeadCompactsAtOnceThenMoreOftenWhileReplayingLikeTheGoIngester(t *testing.T) {
	interval := 15 * time.Minute
	start := time.Now()
	schedule := &compactionSchedule{}
	require.True(t, schedule.due(start, interval, false))
	require.False(t, schedule.due(start.Add(29*time.Second), interval, false))
	require.True(t, schedule.due(start.Add(30*time.Second), interval, false))
	require.False(t, schedule.due(start.Add(60*time.Second), interval, true))
	require.True(t, schedule.due(start.Add(30*time.Second+interval), interval, true))
}

func TestCoverageOnlyMovesForward(t *testing.T) {
	path := filepath.Join(t.TempDir(), "coverage")
	require.NoError(t, raiseCoverage(path, 100))
	require.NoError(t, raiseCoverage(path, 50))
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "100", string(contents))
	require.NoError(t, raiseCoverage(path, 200))
	contents, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "200", string(contents))
}

func TestAPodWithoutAnOffsetReplaysItsRetentionByDefault(t *testing.T) {
	retention := optionalInt{value: 13 * 3600, set: true}
	latest := &kafka.StartOffset{Kind: kafka.StartLatest}
	require.Equal(t, bootstrapPlan{lookback: true, lookbackSeconds: 13 * 3600}, bootstrapStart(nil, optionalInt{}, retention))
	require.Equal(t, bootstrapPlan{offset: *latest}, bootstrapStart(latest, optionalInt{}, retention))
	require.Equal(t, bootstrapPlan{lookback: true, lookbackSeconds: 60}, bootstrapStart(latest, optionalInt{value: 60, set: true}, retention))
	require.Equal(t, bootstrapPlan{offset: kafka.StartOffset{Kind: kafka.StartEarliest}}, bootstrapStart(nil, optionalInt{}, optionalInt{}))
}

func TestEmptyPartitionIsReadyAtEarliestOffset(t *testing.T) {
	require.True(t, initialReplayComplete(kafka.StartOffset{Kind: kafka.StartEarliest}, 42, 42))
	require.False(t, initialReplayComplete(kafka.StartOffset{Kind: kafka.StartEarliest}, 41, 42))
	require.True(t, initialReplayComplete(kafka.StartOffset{Kind: kafka.StartAt, Offset: 42}, 41, 42))
}

func metricRequest(name string, timestampMs int64) record.DecodedRequest {
	return record.DecodedRequest{Series: []record.DecodedSeries{{
		Labels:  [][2]string{{"__name__", name}},
		Samples: []mimirpb.Sample{{TimestampMs: timestampMs, Value: 1}},
	}}}
}

func TestShutdownDuringRestoreKeepsTheHeadSnapshot(t *testing.T) {
	dataDir := t.TempDir()
	chunkDir := filepath.Join(dataDir, "chunks_head")
	sources := []source{{"topic", "broker"}}
	log, _, err := segment.Open(dataDir, 0, "topic", 0, segment.Retention{})
	require.NoError(t, err)
	request := metricRequest("metric", 1)
	s, err := store.NewWithShards(1000, store.Retention{}, chunkDir, 4, 2)
	require.NoError(t, err)
	require.NoError(t, log.Append(5, 1, "tenant", &request))
	require.NoError(t, s.Ingest("tenant", request))
	require.NoError(t, log.Close())
	offsets := []store.SnapshotOffset{{Offset: 5, HasOffset: true, TimestampMs: 1}}
	require.NoError(t, s.WriteSnapshot(offsets))
	require.NoError(t, s.Close())

	open := func(shutdown bool) startupStore {
		opened, err := openStore(dataDir, chunkDir, sources, 0, storeConfig{
			activeWindowMs:  1000,
			shards:          4,
			threads:         2,
			overrides:       limits.DefaultOverrides(),
			costAttribution: [2]int64{180_000, 1_200_000},
			flushSeries:     150,
			pusherShards:    store.DefaultPusherShards(),
		}, func() bool { return shutdown })
		require.NoError(t, err)
		return opened
	}
	require.True(t, open(true).stopped)
	require.FileExists(t, filepath.Join(chunkDir, "snapshot"))
	resumed := open(false)
	require.NotNil(t, resumed.logs, "the snapshot resumes")
	require.Equal(t, uint64(1), resumed.store.NumSeries("tenant"))
	offset, ok := resumed.logs[0].log.LastOffset()
	require.True(t, ok)
	require.Equal(t, int64(5), offset)
	require.Equal(t, offsets[0], resumed.logs[0].offset)
	require.NoError(t, resumed.logs[0].log.Close())
	require.NoError(t, resumed.store.Close())
	rebuilt := open(false)
	require.Nil(t, rebuilt.logs, "the snapshot was consumed")
	require.False(t, rebuilt.stopped)
	require.NoError(t, rebuilt.store.Close())
}

func testApplier(t *testing.T, dataDir string, s *store.Store) *applier {
	log, _, err := segment.Open(dataDir, 0, "topic", 0, segment.Retention{})
	require.NoError(t, err)
	return &applier{
		store:        s,
		consistency:  kafka.NewConsistency(0, 0, 1, time.Second),
		segmentLog:   log,
		fatal:        newSignals(),
		coveragePath: filepath.Join(dataDir, "coverage"),
		replayTarget: -1,
	}
}

func TestFetchedRecordsAreAppliedInOrderWhetherDecodedInlineOrInParallel(t *testing.T) {
	dataDir := t.TempDir()
	s := store.Default()
	a := testApplier(t, dataDir, s)
	// Every record writes the same series at its offset, so the order shows in its samples.
	fetchedAt := func(offset int64, padding int) applyCommand {
		payload, err := (&mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
			Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: "up"}, {Name: "padding", Value: strings.Repeat("x", padding)}},
			Samples: []mimirpb.Sample{{TimestampMs: offset, Value: float64(offset)}},
		}}}}).Marshal()
		require.NoError(t, err)
		return applyCommand{
			kind:          applyFetched,
			fetched:       kafka.RawRecord{Offset: offset, TimestampMs: offset, Tenant: "tenant", Version: 1, Payload: payload},
			highWatermark: offset + 1,
		}
	}
	var inline, parallel []applyCommand
	for offset := int64(1); offset <= 3; offset++ {
		inline = append(inline, fetchedAt(offset, 10))
	}
	for offset := int64(4); offset <= 12; offset++ {
		parallel = append(parallel, fetchedAt(offset, parallelPrepareBytes/8))
	}
	for _, batch := range [][]applyCommand{inline, parallel} {
		require.NoError(t, a.applyBatch(batch))
	}
	last, ok := a.segmentLog.LastOffset()
	require.True(t, ok)
	require.Equal(t, int64(12), last)
	selected, err := s.SelectChunks("tenant", -1<<63, 1<<63-1, nil)
	require.NoError(t, err)
	var timestamps []int64
	for _, series := range selected {
		for _, encoded := range series.Chunks {
			var chunk client.Chunk
			require.NoError(t, chunk.Unmarshal(encoded.Wire))
			samples, err := chunks.DecodeXOR(chunk.Data)
			require.NoError(t, err)
			for _, sample := range samples {
				timestamps = append(timestamps, sample.T)
			}
		}
	}
	// The two padding sizes are two series, each with its samples in offset order.
	var expected []int64
	for offset := int64(1); offset <= 12; offset++ {
		expected = append(expected, offset)
	}
	require.Equal(t, expected, timestamps)
	require.NoError(t, a.segmentLog.Close())
}

func TestAnUnknownLatestOffsetIsAStalledFetch(t *testing.T) {
	require.False(t, fetchStalled(10, nil, 10), "caught up")
	require.True(t, fetchStalled(11, nil, 10), "records to fetch")
	require.True(t, fetchStalled(-1, nil, 10), "unknown latest offset")
	require.True(t, fetchStalled(0, errors.New("watermarks"), 10))
}

func TestRejectedOffsetsNeverReachTheStoreOrLog(t *testing.T) {
	dataDir := t.TempDir()
	s := store.Default()
	a := testApplier(t, dataDir, s)
	recordAt := func(offset int64, metric string) applyCommand {
		request := metricRequest(metric, offset)
		return applyCommand{
			kind:          applyRecord,
			offset:        offset,
			timestampMs:   offset,
			highWatermark: offset + 1,
			tenant:        "tenant",
			keys:          segment.SeriesKeys("tenant", &request),
			request:       &request,
		}
	}
	require.NoError(t, a.applyBatch([]applyCommand{recordAt(1, "first"), recordAt(2, "second")}))
	require.Equal(t, uint64(2), s.NumSeries("tenant"))
	// A batch with any stale offset is rejected before its other records are applied.
	require.Error(t, a.applyBatch([]applyCommand{recordAt(3, "third"), recordAt(2, "duplicate")}))
	require.Equal(t, uint64(2), s.NumSeries("tenant"))
	last, _ := a.segmentLog.LastOffset()
	require.Equal(t, int64(2), last)
	require.NoError(t, a.segmentLog.Close())

	log, recovered, err := segment.Open(dataDir, 0, "topic", 0, segment.Retention{})
	require.NoError(t, err)
	last, _ = log.LastOffset()
	require.Equal(t, int64(2), last)
	var offsets []int64
	for _, recoveredRecord := range recovered {
		offsets = append(offsets, recoveredRecord.Offset)
	}
	require.Equal(t, []int64{1, 2}, offsets)
	require.NoError(t, log.Close())
}

func TestBooleanFlagsTakeTheirValueAsTheNextArgumentLikeClap(t *testing.T) {
	args, err := parseServeArgs([]string{
		"--brokers", "localhost:9092", "--topic", "ingest", "--partition", "3", "--data-dir", "/data",
		"--ingester.push-circuit-breaker.enabled", "true", "--kafka-tls",
		"--ingester.track-ingester-owned-series=true", "--retention-seconds", "60",
	})
	require.NoError(t, err)
	require.True(t, args.protection.PushCircuitBreakerEnabled)
	require.True(t, args.kafkaTLS)
	require.True(t, args.trackOwnedSeries)
	require.Equal(t, optionalInt{value: 60, set: true}, args.retentionSeconds)
	require.False(t, args.bootstrapLookbackSeconds.set)
	_, err = parseServeArgs([]string{"--topic", "ingest"})
	require.Error(t, err)
}
