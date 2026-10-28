// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
)

// openRetainingPrometheus is openPrometheus with the block retention of Mimir's ingesters without shipping: a block
// the TSDB would delete is deleted once it was written longer than the retention period ago.
func openRetainingPrometheus(t *testing.T, opts differentialOptions, retention time.Duration) prometheusHead {
	promOpts := tsdb.DefaultOptions()
	promOpts.MinBlockDuration = chunkRangeMs
	promOpts.MaxBlockDuration = chunkRangeMs
	promOpts.NoLockfile = true
	promOpts.IsolationDisabled = true
	promOpts.OutOfOrderTimeWindow = opts.oooWindowMs
	promOpts.EnableOverlappingCompaction = false
	promOpts.RetentionDuration = retention.Milliseconds()
	var db *tsdb.DB
	promOpts.BlocksToDelete = func(blocks []*tsdb.Block) map[ulid.ULID]struct{} {
		deletable := map[ulid.ULID]struct{}{}
		// Opening the TSDB reloads its blocks, of which there are none.
		if db == nil {
			return deletable
		}
		deadline := time.Now().Add(-retention)
		for id := range tsdb.DefaultBlocksToDelete(db)(blocks) {
			if time.UnixMilli(int64(id.Time())).Before(deadline) {
				deletable[id] = struct{}{}
			}
		}
		return deletable
	}
	db, err := tsdb.Open(t.TempDir(), nil, nil, promOpts, nil)
	require.NoError(t, err)
	db.DisableCompactions()
	return prometheusHead{db}
}

// A tenant's out-of-order window may reach further back than the retention: the out-of-order samples older than it
// are in the ingester until their out-of-order head is compacted, and then for the retention period, like the blocks
// the Go ingester wrote for them, however old the samples are.
func TestEngineKeepsOutOfOrderSamplesOlderThanTheRetentionLikeTheIngester(t *testing.T) {
	const (
		day       = 24 * time.Hour
		retention = 2 * time.Second
	)
	opts := differentialOptions{oooWindowMs: (30 * day).Milliseconds()}
	dir := t.TempDir()
	open := func() *Engine {
		engine, err := OpenEngine(dir, "tenant", EngineOptions{
			Shards: 4, OutOfOrderTimeWindowMs: opts.oooWindowMs, RetentionMs: retention.Milliseconds(), SecondaryHashFunction: secondaryHash,
		})
		require.NoError(t, err)
		return engine
	}
	heads := [2]headUnderTest{openRetainingPrometheus(t, opts, retention), open()}
	t.Cleanup(func() {
		for _, head := range heads {
			_ = head.Close()
		}
	})
	now := (40 * day).Milliseconds()
	old := now - (20 * day).Milliseconds()
	appendRange := func(lset promlabels.Labels, from, to int64) {
		for _, head := range heads {
			app := head.Appender(context.Background())
			for ts := from; ts < to; ts += 60_000 {
				_, err := app.Append(0, lset, 0, ts, float64(ts), nil, nil, storage.AppendV2Options{})
				require.NoError(t, err)
			}
			require.NoError(t, app.Commit())
		}
	}
	each := func(do func(headUnderTest) error) {
		for _, head := range heads {
			require.NoError(t, do(head))
		}
	}
	compact := func(head headUnderTest) error { return head.Compact(context.Background()) }
	// The samples of the old range, which both have the same of.
	matcher := promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", ".+")
	oldSamples := func() int {
		want := querySamples(t, heads[0], old-day.Milliseconds(), old+day.Milliseconds(), nil, matcher)
		require.Equal(t, want, querySamples(t, heads[1], old-day.Milliseconds(), old+day.Milliseconds(), nil, matcher))
		count := 0
		for _, series := range want {
			count += len(series.samples)
		}
		return count
	}

	// A block of in-order samples, which expires first.
	appendRange(promlabels.FromStrings("__name__", "in_order"), now, now+3*time.Hour.Milliseconds())
	each(func(head headUnderTest) error { return head.CompactHead(now, now+2*time.Hour.Milliseconds()-1) })
	time.Sleep(retention * 6 / 10)

	// Old out-of-order samples, of the series and of one with no others.
	appendRange(promlabels.FromStrings("__name__", "in_order"), old, old+10*60_000)
	appendRange(promlabels.FromStrings("__name__", "out_of_order_only"), old, old+10*60_000)
	each(compact)
	require.Equal(t, 20, oldSamples(), "in the out-of-order head")
	each(func(head headUnderTest) error { return head.CompactOOOHead(context.Background()) })
	each(compact)
	require.Equal(t, 20, oldSamples(), "in a block written just now")
	// Across a restart too, which restores which chunks the compaction took.
	require.NoError(t, heads[1].Close())
	heads[1] = open()
	require.True(t, heads[1].(*Engine).Restored())
	require.Equal(t, 20, oldSamples(), "restored")

	time.Sleep(retention * 6 / 10)
	each(compact)
	require.Equal(t, 20, oldSamples(), "in a block written less than the retention ago, once the in-order one expired")

	time.Sleep(retention * 6 / 10)
	// The TSDB deletes blocks when it reloads them, which an out-of-order compaction does, even of nothing, and its
	// background loop every minute.
	each(func(head headUnderTest) error { return head.CompactOOOHead(context.Background()) })
	each(compact)
	require.Zero(t, oldSamples(), "in a block written longer than the retention ago")
}

// A block written again for its range, as idle eviction does each time series of the range leave, keeps when it was
// first written: retention counts from then, like the TSDB's block of the range written at head compaction.
func TestEngineBlockWrittenAgainKeepsItsCreation(t *testing.T) {
	e, err := OpenEngine(t.TempDir(), "user", EngineOptions{Shards: 2})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })
	const blockMax = 2 * 3_600_000
	require.True(t, e.recordBlock(0, blockMax, false, true, []uint64{1}))
	_, tenant := e.home()
	written := tenant.blocks[0].createdMs
	time.Sleep(5 * time.Millisecond)
	require.True(t, e.recordBlock(0, blockMax, false, true, []uint64{2}))
	_, tenant = e.home()
	require.Len(t, tenant.blocks, 1)
	require.Equal(t, []uint64{1, 2}, tenant.blocks[0].series)
	require.Equal(t, written, tenant.blocks[0].createdMs)
	require.Equal(t, int64(blockMax), expiredBefore(tenant.blocks, written+1), "expires from when it was first written")
}
