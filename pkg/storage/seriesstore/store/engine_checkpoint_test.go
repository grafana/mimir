// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openCheckpointedEngine(t *testing.T, dir string, retentionMs int64) *Engine {
	e, err := OpenEngine(dir, "user", EngineOptions{Shards: 2, TimelyCompaction: true, RetentionMs: retentionMs, Checkpoints: true})
	require.NoError(t, err)
	return e
}

func appendSeries(t *testing.T, e *Engine, count int, ts int64) {
	app := e.Appender(context.Background())
	for n := range count {
		_, err := app.Append(0, promlabels.FromStrings("__name__", "m", "n", fmt.Sprint(n)), ts, float64(ts))
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
}

// More series than a snapshot writes in a batch, so it unlocks and paces between them.
const checkpointSeries = 3*snapshotBatch + 7

// An engine that crashed restores its last checkpoint, without what came after it, as often as it crashes again.
func TestEngineRestoresItsCheckpointAfterACrash(t *testing.T) {
	dir := t.TempDir()
	e := openCheckpointedEngine(t, dir, 0)
	appendSeries(t, e, checkpointSeries, 1000)
	appendSeries(t, e, checkpointSeries, 2000)
	require.NoError(t, e.Checkpoint(context.Background(), []byte("offsets"), 0.5))
	appendSeries(t, e, checkpointSeries, 3000)
	appendSeries(t, e, 1, 4000)

	for range 2 {
		crashed := openCheckpointedEngine(t, dir, 0)
		t.Cleanup(func() { _ = crashed.Close() })
		require.True(t, crashed.Restored())
		require.Equal(t, []byte("offsets"), crashed.RestoredCheckpoint())
		require.Equal(t, uint64(checkpointSeries), crashed.NumSeries())
		series, times := sampleTimes(t, crashed, "m")
		require.Equal(t, checkpointSeries, series)
		require.Len(t, times, 2*checkpointSeries)
		for _, ts := range times {
			require.LessOrEqual(t, ts, int64(2000))
		}
		e = crashed
	}

	// A clean shutdown's snapshot has no checkpoint, and is only restored once.
	appendSeries(t, e, checkpointSeries, 3000)
	require.NoError(t, e.Close())
	e = openCheckpointedEngine(t, dir, 0)
	require.True(t, e.Restored())
	require.Nil(t, e.RestoredCheckpoint())
	_, times := sampleTimes(t, e, "m")
	require.Len(t, times, 3*checkpointSeries)
	crashed := openCheckpointedEngine(t, dir, 0)
	t.Cleanup(func() { _ = e.Close(); _ = crashed.Close() })
	require.False(t, crashed.Restored())
}

// The cold blocks and chunk files retention drops after a checkpoint are what restoring it needs, until the next one.
func TestEngineCheckpointKeepsTheFilesItReferences(t *testing.T) {
	withExpiredBlock := func(t *testing.T) (*Engine, string) {
		dir := t.TempDir()
		e := openCheckpointedEngine(t, dir, 1)
		appendSeries(t, e, 10, 0)
		appendSeries(t, e, 10, 1)
		require.NoError(t, e.CompactHead(0, 1))
		require.NoError(t, e.Checkpoint(context.Background(), []byte("before retention"), 0))
		time.Sleep(5 * time.Millisecond)
		require.NoError(t, e.Compact(context.Background()))
		_, times := sampleTimes(t, e, "m")
		require.Empty(t, times, "the block is older than the retention")
		return e, dir
	}
	coldFiles := func(t *testing.T, dir string) int {
		cold := 0
		for shard := range 2 {
			files, err := os.ReadDir(coldDir(filepath.Join(dir, storeDirName), shard))
			require.NoError(t, err)
			cold += len(files)
		}
		return cold
	}

	t.Run("restored after a crash", func(t *testing.T) {
		e, dir := withExpiredBlock(t)
		crashed := openCheckpointedEngine(t, dir, 0)
		t.Cleanup(func() { _ = e.Close(); _ = crashed.Close() })
		require.Equal(t, []byte("before retention"), crashed.RestoredCheckpoint())
		_, times := sampleTimes(t, crashed, "m")
		require.Len(t, times, 20)
	})
	t.Run("removed by the next checkpoint", func(t *testing.T) {
		e, dir := withExpiredBlock(t)
		t.Cleanup(func() { _ = e.Close() })
		require.NotZero(t, coldFiles(t, dir))
		require.NoError(t, e.Checkpoint(context.Background(), []byte("after retention"), 0))
		require.Zero(t, coldFiles(t, dir))
	})
}

// A checkpoint that stops part way, like at shutdown, leaves the last one in place.
func TestEngineCanceledCheckpointKeepsTheLastOne(t *testing.T) {
	dir := t.TempDir()
	e := openCheckpointedEngine(t, dir, 0)
	appendSeries(t, e, checkpointSeries, 1000)
	require.NoError(t, e.Checkpoint(context.Background(), []byte("first"), 0))
	appendSeries(t, e, checkpointSeries, 2000)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, e.Checkpoint(ctx, []byte("second"), 0.5), context.Canceled)
	_, err := os.Stat(filepath.Join(dir, storeDirName, snapshotFileName+".tmp"))
	require.ErrorIs(t, err, os.ErrNotExist)

	crashed := openCheckpointedEngine(t, dir, 0)
	t.Cleanup(func() { _ = e.Close(); _ = crashed.Close() })
	require.Equal(t, []byte("first"), crashed.RestoredCheckpoint())
	_, times := sampleTimes(t, crashed, "m")
	require.Len(t, times, checkpointSeries)
}

// Checkpoints run while appends, queries and compactions go on: each restores every series it had, with the samples
// appended before it started and maybe some after.
func TestEngineCheckpointsWhileIngesting(t *testing.T) {
	dir := t.TempDir()
	e := openCheckpointedEngine(t, dir, 0)
	const writers, rounds, perWriter = 4, 100, snapshotBatch / 2
	var wg sync.WaitGroup
	var appended sync.Map
	for writer := range writers {
		wg.Go(func() {
			for round := range rounds {
				app := e.Appender(context.Background())
				for n := range perWriter {
					lset := promlabels.FromStrings("__name__", "m", "writer", fmt.Sprint(writer), "n", fmt.Sprint(n))
					_, err := app.Append(0, lset, int64(round)*15_000, float64(round))
					assert.NoError(t, err)
				}
				assert.NoError(t, app.Commit())
				appended.Store(writer, round)
			}
		})
	}
	done := make(chan struct{})
	var others sync.WaitGroup
	others.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			assert.NoError(t, e.Compact(context.Background()))
		}
	})
	checkpoints := 0
	var before map[int]int
	for checkpoints < 3 || before == nil {
		snapshot := map[int]int{}
		appended.Range(func(key, value any) bool {
			snapshot[key.(int)] = value.(int)
			return true
		})
		require.NoError(t, e.Checkpoint(context.Background(), fmt.Append(nil, checkpoints), 0.5))
		before = snapshot
		checkpoints++
	}
	wg.Wait()
	close(done)
	others.Wait()

	crashed := openCheckpointedEngine(t, dir, 0)
	t.Cleanup(func() { _ = e.Close(); _ = crashed.Close() })
	require.Equal(t, fmt.Append(nil, checkpoints-1), crashed.RestoredCheckpoint())
	q, err := crashed.ChunkQuerier(0, 1<<62)
	require.NoError(t, err)
	defer q.Close()
	perSeries := map[string]int{}
	set := q.Select(context.Background(), true, nil, promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "m"))
	for set.Next() {
		perSeries[set.At().Labels().Get("writer")+"/"+set.At().Labels().Get("n")] = len(mergedSamples(t, set.At()))
	}
	require.NoError(t, set.Err())
	for writer, round := range before {
		for n := range perWriter {
			require.GreaterOrEqual(t, perSeries[fmt.Sprintf("%d/%d", writer, n)], round+1, "writer %d series %d", writer, n)
		}
	}
}

// The engine's state has a hash for every series of every block: a large tenant's is more than the 64MiB a field may
// otherwise have.
func TestSnapshotRestoresAnEngineStateOverTheFieldLimit(t *testing.T) {
	dir := t.TempDir()
	s, err := NewWithShards(20*60*1000, Retention{}, dir, 1, 1)
	require.NoError(t, err)
	state := make([]byte, 65<<20)
	state[len(state)-1] = 7
	require.NoError(t, s.writeSnapshot(snapshotPlan{engineState: state}))
	require.NoError(t, s.Close())

	restored, err := Restore(20*60*1000, Retention{}, dir, 1)
	require.NoError(t, err)
	require.NotNil(t, restored, "the snapshot is valid")
	t.Cleanup(func() { _ = restored.Store.Close() })
	require.Equal(t, state, restored.EngineState)
}
