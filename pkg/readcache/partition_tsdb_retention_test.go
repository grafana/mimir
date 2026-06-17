// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// TestPartitionTSDB_LocalBlockRetention_DeletesOldBlocks verifies that
// the readcache-local retention knob is actually wired into the
// underlying Prometheus TSDB and that block deletion fires on the
// next CompactHead after a block ages out.
//
// Prometheus's BeyondTimeRetention compares each block's MaxTime
// against the newest block's MaxTime — a block is deletable when
// (newest.MaxTime - block.MaxTime) >= retention. So we set up two
// blocks separated by more than the configured retention and verify
// the older one disappears after the next compaction cycle.
//
// Regression test for the dead-config bug: before the fix,
// LocalBlockRetention was defined and validated in Config but never
// passed into tsdb.Options, leaving the readcache effectively
// unbounded (or bounded by the shared 13h ingester knob).
func TestPartitionTSDB_LocalBlockRetention_DeletesOldBlocks(t *testing.T) {
	const retention = 500 * time.Millisecond
	const blockRange = 100 * time.Millisecond

	cfg := newTestConfig(t, false, 0)
	cfg.LocalBlockRetention = retention
	// Shrink the TSDB block range so the head compacts into a tiny
	// block instead of a 2h one (the default). Without this the test
	// would need to span 2h+ of synthetic time to produce a second
	// block.
	cfg.BlocksStorage.TSDB.BlockRanges = mimir_tsdb.DurationList{blockRange}

	limits := validation.NewOverrides(validation.Limits{}, nil)

	p, err := openPartitionTSDB(
		"tenant-1",
		0,
		0, // epoch
		cfg.DataDir,
		cfg.BlocksStorage.TSDB,
		cfg.LocalBlockRetention,
		limits,
		0,
		nil, nil, nil,
		prometheus.NewRegistry(),
		log.NewNopLogger(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })

	// Helper: append one sample at the given timestamp and commit
	// so it becomes part of the head's [MinTime, MaxTime].
	appendAt := func(tsMs int64) {
		t.Helper()
		app := p.Appender(context.Background())
		_, err := app.Append(0, labels.FromStrings("__name__", "test", "ts", "v"+itoa(tsMs)), tsMs, float64(tsMs))
		require.NoError(t, err)
		require.NoError(t, app.Commit())
	}

	// Block 1: samples in [0, blockRange).
	appendAt(0)
	appendAt(blockRange.Milliseconds() - 1)
	require.NoError(t, p.CompactHead())

	// We should have one block now.
	require.Len(t, p.Blocks(), 1, "first CompactHead should have persisted exactly one block")
	firstBlockID := p.Blocks()[0].Meta().ULID

	// Block 2: samples in [retention+blockRange, retention+2*blockRange).
	// This is far enough past block 1 that
	//   newest.MaxTime - oldest.MaxTime >= retention,
	// so Prometheus's BeyondTimeRetention will mark block 1 deletable
	// on the next reloadBlocks (triggered by CompactHead).
	start := retention.Milliseconds() + blockRange.Milliseconds()
	appendAt(start)
	appendAt(start + blockRange.Milliseconds() - 1)
	require.NoError(t, p.CompactHead())

	// After the second CompactHead, reloadBlocks should have deleted
	// the original block — its MaxTime is more than `retention`
	// behind the new block's MaxTime.
	blocks := p.Blocks()
	require.Len(t, blocks, 1,
		"the older block must be deleted by time-retention after the second CompactHead; got %d blocks", len(blocks))
	assert.NotEqual(t, firstBlockID, blocks[0].Meta().ULID,
		"the surviving block should be the newer one, not the original")
}

// TestPartitionTSDB_LocalBlockRetention_Zero_DisablesRetention is the
// "opt-out" negative test: with LocalBlockRetention=0, the TSDB's
// time-retention is disabled and old blocks accumulate. This is the
// pre-fix legacy behavior we preserve for tests / operators who
// explicitly want unbounded local blocks.
//
// Mirrors the structure of the positive test so any change to one
// without the other is obviously asymmetric.
func TestPartitionTSDB_LocalBlockRetention_Zero_DisablesRetention(t *testing.T) {
	const blockRange = 100 * time.Millisecond

	cfg := newTestConfig(t, false, 0)
	cfg.LocalBlockRetention = 0
	cfg.BlocksStorage.TSDB.BlockRanges = mimir_tsdb.DurationList{blockRange}

	limits := validation.NewOverrides(validation.Limits{}, nil)
	p, err := openPartitionTSDB(
		"tenant-1",
		0,
		0, // epoch
		cfg.DataDir,
		cfg.BlocksStorage.TSDB,
		cfg.LocalBlockRetention,
		limits,
		0,
		nil, nil, nil,
		prometheus.NewRegistry(),
		log.NewNopLogger(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })

	appendAt := func(tsMs int64) {
		t.Helper()
		app := p.Appender(context.Background())
		_, err := app.Append(0, labels.FromStrings("__name__", "test", "ts", "v"+itoa(tsMs)), tsMs, float64(tsMs))
		require.NoError(t, err)
		require.NoError(t, app.Commit())
	}

	appendAt(0)
	appendAt(blockRange.Milliseconds() - 1)
	require.NoError(t, p.CompactHead())
	require.Len(t, p.Blocks(), 1)

	// Same temporal spacing as the positive test, but retention=0
	// disables BeyondTimeRetention so both blocks must survive.
	start := 10 * blockRange.Milliseconds()
	appendAt(start)
	appendAt(start + blockRange.Milliseconds() - 1)
	require.NoError(t, p.CompactHead())

	assert.Len(t, p.Blocks(), 2,
		"with LocalBlockRetention=0, time-retention must be disabled and both blocks must survive")
}

// itoa keeps the labels.FromStrings calls allocation-free without
// pulling in strconv at every call site — the test is hot enough
// (4 appends per case) that this saves noise in profiles.
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
