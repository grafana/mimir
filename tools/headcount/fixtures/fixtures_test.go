// SPDX-License-Identifier: AGPL-3.0-only

package fixtures

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestCopyTree(t *testing.T) {
	src := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(src, "block-1"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "block-1", "meta.json"), []byte(`{"a":1}`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "top-level"), []byte("x"), 0o644))

	dst := filepath.Join(t.TempDir(), "copy")
	require.NoError(t, copyTree(src, dst))

	got, err := os.ReadFile(filepath.Join(dst, "block-1", "meta.json"))
	require.NoError(t, err)
	require.Equal(t, `{"a":1}`, string(got))

	got, err = os.ReadFile(filepath.Join(dst, "top-level"))
	require.NoError(t, err)
	require.Equal(t, "x", string(got))
}

func TestCopyTree_IndependentOfSource(t *testing.T) {
	src := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(src, "f"), []byte("original"), 0o644))

	dst := filepath.Join(t.TempDir(), "copy")
	require.NoError(t, copyTree(src, dst))
	require.NoError(t, os.WriteFile(filepath.Join(src, "f"), []byte("mutated"), 0o644))

	got, err := os.ReadFile(filepath.Join(dst, "f"))
	require.NoError(t, err)
	require.Equal(t, "original", string(got), "the copy must not alias the source file")
}

func TestFreePort_ReturnsDistinctUsablePorts(t *testing.T) {
	a, err := freePort()
	require.NoError(t, err)
	b, err := freePort()
	require.NoError(t, err)
	require.NotEqual(t, a, b)
	require.Greater(t, a, 0)
}

// writeFakeBlock creates a block directory with just enough of a meta.json
// for blockLevels to read: no index or chunks, since these tests only
// exercise the level bookkeeping, not real block contents.
func writeFakeBlock(t *testing.T, bucketDir string, level int) ulid.ULID {
	id, err := tryWriteFakeBlock(bucketDir, level)
	require.NoError(t, err)
	return id
}

// tryWriteFakeBlock is writeFakeBlock for a goroutine other than the test's,
// which must not call require.
func tryWriteFakeBlock(bucketDir string, level int) (ulid.ULID, error) {
	id := ulid.MustNew(ulid.Now(), rand.Reader)
	dir := filepath.Join(bucketDir, "anonymous", id.String())
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return id, err
	}
	meta := block.Meta{
		BlockMeta: tsdb.BlockMeta{ULID: id, Version: block.TSDBVersion1, Compaction: tsdb.BlockMetaCompaction{Level: level}},
		Thanos:    block.ThanosMeta{Version: block.ThanosVersion1},
	}
	return id, meta.WriteToDir(log.NewNopLogger(), dir)
}

func TestBlockSetSignature_ChangesWhenBlocksChange(t *testing.T) {
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(bucket, "anonymous"), 0o755))

	sig1, err := blockSetSignature(bucket)
	require.NoError(t, err)

	writeFakeBlock(t, bucket, 1)
	sig2, err := blockSetSignature(bucket)
	require.NoError(t, err)
	require.NotEqual(t, sig1, sig2)

	sig3, err := blockSetSignature(bucket)
	require.NoError(t, err)
	require.Equal(t, sig2, sig3, "signature must be stable when nothing changed")
}

func TestCheckBlockLevels(t *testing.T) {
	newBucket := func(t *testing.T, levels ...int) string {
		bucket := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(bucket, "anonymous"), 0o755))
		for _, l := range levels {
			writeFakeBlock(t, bucket, l)
		}
		return bucket
	}

	t.Run("handover shape: sources plus a compacted block", func(t *testing.T) {
		require.NoError(t, checkBlockLevels(newBucket(t, 1, 4), true))
		require.Error(t, checkBlockLevels(newBucket(t, 1, 4), false), "must reject: level-1 blocks are present but not wanted")
	})

	t.Run("compacted shape: no sources left", func(t *testing.T) {
		require.NoError(t, checkBlockLevels(newBucket(t, 4), false))
		require.Error(t, checkBlockLevels(newBucket(t, 4), true), "must reject: level-1 blocks are wanted but absent")
	})

	t.Run("nothing compacted yet", func(t *testing.T) {
		require.Error(t, checkBlockLevels(newBucket(t, 1), true))
		require.Error(t, checkBlockLevels(newBucket(t, 1), false))
	})
}

func TestWaitForCompaction(t *testing.T) {
	t.Run("returns once the block set stabilizes at the wanted shape", func(t *testing.T) {
		bucket := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(bucket, "anonymous"), 0o755))
		writeFakeBlock(t, bucket, 1)

		done := make(chan error, 1)
		go func() {
			// The level-1 block written above is never removed, so the
			// final shape has sources present: wantSources=true.
			done <- waitForCompaction(bucket, true, 200*time.Millisecond, 20*time.Millisecond, 5*time.Second)
		}()

		time.Sleep(50 * time.Millisecond)
		writeFakeBlock(t, bucket, 4) // simulates the compactor producing its output.

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("waitForCompaction did not return")
		}
	})

	t.Run("does not mistake an unchanging but incomplete block set for a finished one", func(t *testing.T) {
		// Regression test: right after the compactor starts, the block set
		// is just its unchanged input, which trivially looks stable for as
		// long as the compactor takes to produce anything. A bucket stuck
		// at level 1 only, however long it stays that way, must never be
		// reported as done.
		bucket := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(bucket, "anonymous"), 0o755))
		writeFakeBlock(t, bucket, 1)

		err := waitForCompaction(bucket, false, 100*time.Millisecond, 20*time.Millisecond, 400*time.Millisecond)
		require.Error(t, err)
	})

	t.Run("times out if the block set keeps changing", func(t *testing.T) {
		bucket := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(bucket, "anonymous"), 0o755))

		// The writer is joined before the test returns, so it never writes
		// into the temp dir while the cleanup removes it, and it reports
		// its errors over a channel instead of calling require.
		stop := make(chan struct{})
		writeErr := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := tryWriteFakeBlock(bucket, 1); err != nil {
					writeErr <- err
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()

		err := waitForCompaction(bucket, false, 300*time.Millisecond, 20*time.Millisecond, 500*time.Millisecond)
		close(stop)
		wg.Wait()
		select {
		case werr := <-writeErr:
			require.NoError(t, werr, "the writer failed, so the block set may not have kept changing")
		default:
		}
		require.Error(t, err)
	})
}
