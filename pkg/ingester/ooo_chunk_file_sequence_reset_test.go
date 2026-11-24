// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package ingester

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
)

// TestOOOSamplesAfterChunkFileSequenceReset checks that OOO samples appended after
// a restart stay queryable, and survive head GC and OOO compaction, even when
// head truncation had deleted every m-mapped chunk file in between.
// See https://github.com/prometheus/prometheus/issues/20054.
//
// An OOO compaction remembers the last m-mapped chunk ref it covered. From then
// on, queries ignore OOO chunks at or below that ref and head GC drops them,
// since the compaction is assumed to have persisted them. That is only correct
// if chunk refs never go backwards, so the chunk file numbering must not restart
// after all files are deleted: new chunks would otherwise reuse refs at or below
// the remembered one, and be hidden and then dropped although they were never
// compacted.
//
// The ingester runs both orderings covered here: forced head compaction
// (userTSDB.compactHead) compacts the in-order head before the OOO head, while
// early compaction of non-owned series (compactBlocksDueToNonOwnedSeries)
// compacts the OOO head first. The test drives the TSDB directly, since the
// TSDB's own tests aren't vendored and so don't run as part of Mimir's.
func TestOOOSamplesAfterChunkFileSequenceReset(t *testing.T) {
	const minute = int64(time.Minute / time.Millisecond)
	// Samples appended after the restart, in minutes.
	wantMinutes := []int64{100, 101, 102, 103, 104}

	for _, tc := range []struct {
		name string
		// Whether the in-order head is compacted, which also runs head GC,
		// before the OOO head is compacted.
		headGCFirst bool
	}{
		{name: "head GC before OOO compaction", headGCFirst: true},
		{name: "OOO compaction before head GC", headGCFirst: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := tsdb.DefaultOptions()
			opts.OutOfOrderTimeWindow = (2 * time.Hour).Milliseconds()
			// Make the appended samples fill an OOO chunk, so that it gets m-mapped
			// without having to append many samples.
			opts.OutOfOrderCapMax = 4
			// Write chunks synchronously, to make the chunk files deterministic.
			opts.HeadChunksWriteQueueSize = 0

			lbls := labels.FromStrings(model.MetricNameLabel, "test_metric")
			openDB := func() *tsdb.DB {
				db, err := tsdb.Open(dir, nil, nil, opts, nil)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, db.Close()) })
				db.DisableCompactions()
				return db
			}
			appendMinutes := func(db *tsdb.DB, minutes ...int64) {
				app := db.Appender(t.Context())
				for _, m := range minutes {
					_, err := app.Append(0, lbls, m*minute, float64(m))
					require.NoError(t, err)
				}
				require.NoError(t, app.Commit())
			}
			queryMinutes := func(db *tsdb.DB) []int64 {
				q, err := db.Querier(wantMinutes[0]*minute, wantMinutes[len(wantMinutes)-1]*minute)
				require.NoError(t, err)
				defer func() { require.NoError(t, q.Close()) }()

				var got []int64
				ss := q.Select(t.Context(), false, nil, labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_metric"))
				for ss.Next() {
					it := ss.At().Iterator(nil)
					for it.Next() == chunkenc.ValFloat {
						ts, _ := it.At()
						got = append(got, ts/minute)
					}
					require.NoError(t, it.Err())
				}
				require.NoError(t, ss.Err())
				return got
			}
			chunkFiles := func() []os.DirEntry {
				entries, err := os.ReadDir(filepath.Join(dir, "chunks_head"))
				require.NoError(t, err)
				return entries
			}

			// First run: m-map an OOO chunk by compacting the OOO head, which also
			// makes the compaction remember that chunk's ref.
			db := openDB()
			appendMinutes(db, 150) // In-order, to move the head forward.
			appendMinutes(db, 90)  // Out-of-order.
			require.NoError(t, db.CompactOOOHead(t.Context()))
			require.Len(t, chunkFiles(), 1)
			// Close is safe to repeat when the registered cleanup runs.
			require.NoError(t, db.Close())

			// Second run: a restart forgets what was compacted, but keeps the chunk
			// files, and has no active chunk file until the next chunk is written.
			db = openDB()

			// The reloaded OOO chunk is compacted again, and head truncation then
			// finds nothing it needs to keep, so it deletes every chunk file.
			require.NoError(t, db.CompactOOOHead(t.Context()))
			require.Empty(t, chunkFiles(), "expected all chunk files to be deleted")

			// Appending enough OOO samples to fill a chunk m-maps it into a new
			// chunk file. Compacting the in-order head or the OOO head afterwards
			// is what must not touch these samples, since they haven't been
			// compacted.
			appendMinutes(db, wantMinutes...)
			require.NotEmpty(t, chunkFiles(), "expected the OOO chunk to be m-mapped")
			require.Equal(t, wantMinutes, queryMinutes(db), "after appending")

			if tc.headGCFirst {
				h := db.Head()
				require.NoError(t, db.CompactHead(tsdb.NewRangeHead(h, h.MinTime(), h.MaxTime())))
			}
			require.NoError(t, db.CompactOOOHead(t.Context()))
			require.Equal(t, wantMinutes, queryMinutes(db), "after compaction")
		})
	}
}
