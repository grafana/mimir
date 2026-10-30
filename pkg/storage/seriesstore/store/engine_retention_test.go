// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
)

// openRetainingPrometheus is openPrometheus with the block retention of Mimir's ingesters without shipping: a block
// the TSDB would delete is deleted once it was written longer than the retention period ago. The TSDB reloads its blocks,
// which deletes them, every reload, like the ingester's background loop, or its default minute when zero.
func openRetainingPrometheus(t *testing.T, opts differentialOptions, retention, reload time.Duration) prometheusHead {
	promOpts := tsdb.DefaultOptions()
	promOpts.MinBlockDuration = chunkRangeMs
	promOpts.MaxBlockDuration = chunkRangeMs
	promOpts.NoLockfile = true
	promOpts.IsolationDisabled = true
	promOpts.OutOfOrderTimeWindow = opts.oooWindowMs
	promOpts.EnableOverlappingCompaction = false
	promOpts.RetentionDuration = retention.Milliseconds()
	if reload > 0 {
		promOpts.BlockReloadInterval = reload
	}
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
	heads := [2]headUnderTest{openRetainingPrometheus(t, opts, retention, 0), open()}
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

// An out-of-order block older than the retention whose data is newer than the cutoff stays recorded, and retention
// then has nothing to drop: compactions don't scan every series again for it.
func TestEngineRetentionRunsOnceForAnOutOfOrderBlockPastTheCutoff(t *testing.T) {
	const retention = time.Second
	e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{
		Shards: 4, OutOfOrderTimeWindowMs: (24 * time.Hour).Milliseconds(), RetentionMs: retention.Milliseconds(), SecondaryHashFunction: secondaryHash,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })
	hour := time.Hour.Milliseconds()
	appendRange := func(name string, from, to int64) {
		app := e.Appender(t.Context())
		for ts := from; ts < to; ts += 60_000 {
			_, err := app.Append(0, promlabels.FromStrings("__name__", name), 0, ts, float64(ts), nil, nil, storage.AppendV2Options{})
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
	}
	appendRange("in_order", 0, 3*hour)
	require.NoError(t, e.CompactHead(0, 2*hour-1))
	// Older than the series' newest sample, newer than the in-order block.
	appendRange("in_order", 2*hour+30*60_000+30_000, 2*hour+40*60_000)
	require.NoError(t, e.CompactOOOHead(t.Context()))
	time.Sleep(retention * 3 / 2)

	require.NoError(t, e.Compact(t.Context()))
	kept := e.store.retained.Load()
	require.Equal(t, 2*hour, kept.cutoff, "the in-order block expired")
	_, tenant := e.home()
	require.True(t, slices.ContainsFunc(tenant.blocks, func(b emulatedBlock) bool { return b.outOfOrder }), "the out-of-order block stays recorded")
	for range 3 {
		require.NoError(t, e.Compact(t.Context()))
		require.Same(t, kept, e.store.retained.Load(), "nothing changed for retention to drop")
	}
}

// A block written again for its range keeps when it was first written: retention counts from then, like the TSDB's
// block of the range written at head compaction. The blocks of the series the engine evicts while idle aren't the
// TSDB's, and never expire the data of every series: theirs goes with the range's block.
func TestEngineBlockWrittenAgainKeepsItsCreation(t *testing.T) {
	e, err := OpenEngine(t.TempDir(), "user", EngineOptions{Shards: 2})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })
	const blockMax = 2 * 3_600_000
	require.True(t, e.recordBlock(0, blockMax, blockMax, blockInOrder, []uint64{1}))
	_, tenant := e.home()
	written := tenant.blocks[0].createdMs
	time.Sleep(5 * time.Millisecond)
	require.True(t, e.recordBlock(0, blockMax, blockMax, blockInOrder, []uint64{2}))
	_, tenant = e.home()
	require.Len(t, tenant.blocks, 1)
	require.Equal(t, []uint64{1, 2}, tenant.blocks[0].series)
	require.Equal(t, written, tenant.blocks[0].createdMs)
	require.Equal(t, int64(blockMax), expiredBefore(tenant.blocks, written+1), "expires from when it was first written")

	require.True(t, e.recordBlock(blockMax, 2*blockMax, 2*blockMax, blockIdleEvicted, []uint64{3}))
	_, tenant = e.home()
	require.Len(t, tenant.blocks, 2)
	require.Equal(t, int64(blockMax), expiredBefore(tenant.blocks, nowMs()+1), "an idle eviction's block expires no series' data")
}

// The samples of a range expire like the ingester's: with the TSDB's block of the range, written once the head's samples
// are half a range past it, and not with the blocks the engine writes before that. A series the engine evicts while it is
// idle, which the TSDB's head keeps until it compacts the range, keeps its samples until then; a series an early
// compaction moved out of the head, as the ingester does with the series a partition no longer owns, loses only the
// samples it had then when that block expires, and the series the block doesn't hold keep theirs. Across a restart too.
// The heads go on writing blocks, as an ingester's always have a newer block than one the retention deletes.
func TestEngineRetainsTheSamplesOfIdleAndEarlyCompactedSeriesLikeTheIngester(t *testing.T) {
	// Blocks are written step seconds apart, and each check falls between two blocks' expiries, past the TSDB's reload.
	const (
		retention = 10 * time.Second
		step      = 3 * time.Second
	)
	minute := time.Minute.Milliseconds()
	dir := t.TempDir()
	open := func() *Engine {
		engine, err := OpenEngine(dir, "tenant", EngineOptions{
			Shards: 4, RetentionMs: retention.Milliseconds(), IdleEvictionMs: 30 * minute, SecondaryHashFunction: secondaryHash,
		})
		require.NoError(t, err)
		return engine
	}
	heads := [2]headUnderTest{openRetainingPrometheus(t, differentialOptions{}, retention, tsdb.MinBlockReloadInterval), open()}
	t.Cleanup(func() {
		for _, head := range heads {
			_ = head.Close()
		}
	})
	active := promlabels.FromStrings("__name__", "active")
	idle := promlabels.FromStrings("__name__", "idle")
	// Evicted in the second range, its cold block holds samples on both sides of the first range's end.
	idleLater := promlabels.FromStrings("__name__", "idle_later")
	nonOwned := promlabels.FromStrings("__name__", "non_owned")
	refs := [2]map[string]storage.SeriesRef{{}, {}}
	// Each series' samples in [from, its end), a minute apart, the series' samples of a minute together like a scrape's.
	appendRange := func(from int64, ends map[*promlabels.Labels]int64) {
		for index, head := range heads {
			app := head.Appender(context.Background())
			for ts := from; ; ts += minute {
				appended := false
				for lset, end := range ends {
					if ts >= end {
						continue
					}
					appended = true
					ref, err := app.Append(refs[index][lset.String()], *lset, 0, ts, float64(ts), nil, nil, storage.AppendV2Options{})
					require.NoError(t, err, "head %d, %s at %d", index, lset, ts)
					refs[index][lset.String()] = ref
				}
				if !appended {
					break
				}
			}
			require.NoError(t, app.Commit())
		}
	}
	each := func(do func(int, headUnderTest) error) {
		for index, head := range heads {
			require.NoError(t, do(index, head))
		}
	}
	compact := func(_ int, head headUnderTest) error { return head.Compact(context.Background()) }
	// The TSDB deletes blocks when it reloads them, every second here: retention is compared once it reloaded since.
	reloaded := func() { time.Sleep(tsdb.MinBlockReloadInterval * 11 / 10) }
	matcher := promlabels.MustNewMatcher(promlabels.MatchRegexp, "__name__", ".+")
	same := func(name string) []queriedSeries {
		want := querySamples(t, heads[0], 0, 12*time.Hour.Milliseconds(), nil, matcher)
		if got := querySamples(t, heads[1], 0, 12*time.Hour.Milliseconds(), nil, matcher); !reflect.DeepEqual(want, got) {
			for side, series := range [][]queriedSeries{want, got} {
				for _, s := range series {
					t.Logf("%s side %d: %s from %d, %d samples", name, side, s.labels, s.samples[0].t/minute, len(s.samples))
				}
			}
		}
		require.Equal(t, want, querySamples(t, heads[1], 0, 12*time.Hour.Milliseconds(), nil, matcher), name)
		return want
	}
	firstOf := func(series []queriedSeries, name string) (int64, bool) {
		for _, s := range series {
			if s.labels == promlabels.FromStrings("__name__", name).String() && len(s.samples) > 0 {
				return s.samples[0].t, true
			}
		}
		return 0, false
	}
	live := map[*promlabels.Labels]int64{&idleLater: 150 * minute}
	writeUntil := func(from, to int64) {
		live[&active], live[&nonOwned] = to, to
		appendRange(from, live)
		each(compact)
	}

	// The first range's first hour and a half: the idle series stops after half an hour, which the engine evicts.
	appendRange(0, map[*promlabels.Labels]int64{&idle: 30 * minute, &idleLater: 90 * minute, &active: 90 * minute, &nonOwned: 90 * minute})
	each(compact)
	// The ingester's early compaction of the series the partition no longer owns, which then come back.
	each(func(index int, head headUnderTest) error {
		return head.CompactSelectedSeries([]storage.SeriesRef{refs[index][nonOwned.String()]})
	})
	same("after the early compaction")
	time.Sleep(step)
	// Past half a range after the first one: the head compaction writes the first range's block.
	writeUntil(90*minute, 210*minute)
	same("after the first range's head compaction")
	time.Sleep(step)
	writeUntil(210*minute, 330*minute)
	same("after the second range's head compaction")

	// The early compaction's block expired, the first range's not yet.
	time.Sleep(retention - 2*step + step/3)
	each(compact)
	reloaded()
	got := same("once the early compaction's block expired")
	first, ok := firstOf(got, "active")
	require.True(t, ok)
	require.Zero(t, first, "the active series keeps the first range")
	first, ok = firstOf(got, "idle")
	require.True(t, ok, "the idle series is in the range's block")
	require.Zero(t, first)
	first, ok = firstOf(got, "non_owned")
	require.True(t, ok)
	require.Equal(t, 90*minute, first, "the series the early compaction moved lost what it had then")

	// Across a restart, which restores the blocks, and gives the series refs of its own.
	require.NoError(t, heads[1].Close())
	heads[1] = open()
	refs[1] = map[string]storage.SeriesRef{}
	each(compact)
	same("restored")

	// The first range's block expired, the second's not yet.
	time.Sleep(step / 3)
	writeUntil(330*minute, 450*minute)
	reloaded()
	got = same("once the first range's block expired")
	_, ok = firstOf(got, "idle")
	require.False(t, ok, "the idle series went with the range's block")
	for _, name := range []string{"active", "non_owned"} {
		first, ok = firstOf(got, name)
		require.True(t, ok)
		require.Equal(t, 2*time.Hour.Milliseconds(), first, name)
	}

	// Across a restart again, queried before any compaction: what expired stays gone.
	require.NoError(t, heads[1].Close())
	heads[1] = open()
	same("restored once the first range's block expired")

	// Nor does it stay on disk: the cold blocks are written again without it.
	each(compact)
	engine := heads[1].(*Engine)
	cutoff := engine.store.retained.Load().cutoff
	require.Equal(t, 2*time.Hour.Milliseconds(), cutoff)
	for _, shard := range engine.store.shards {
		for _, block := range shard.cold.blocks {
			for _, table := range block.tenants {
				for index := range table.seriesCount {
					series := block.seriesIn(table, index)
					for _, meta := range chunkList(series.chunks).toSlice() {
						require.GreaterOrEqual(t, meta.MaxTime, cutoff, "%s", series.labels())
					}
				}
			}
		}
	}
}
