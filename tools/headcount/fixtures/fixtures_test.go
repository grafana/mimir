// SPDX-License-Identifier: AGPL-3.0-only

package fixtures

import (
	"crypto/rand"
	"os"
	"path/filepath"
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
	id := ulid.MustNew(ulid.Now(), rand.Reader)
	dir := filepath.Join(bucketDir, "anonymous", id.String())
	require.NoError(t, os.MkdirAll(dir, 0o755))
	meta := block.Meta{
		BlockMeta: tsdb.BlockMeta{ULID: id, Version: block.TSDBVersion1, Compaction: tsdb.BlockMetaCompaction{Level: level}},
		Thanos:    block.ThanosMeta{Version: block.ThanosVersion1},
	}
	require.NoError(t, meta.WriteToDir(log.NewNopLogger(), dir))
	return id
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

func TestWaitForStableBlockSet(t *testing.T) {
	t.Run("returns once the block set stops changing", func(t *testing.T) {
		bucket := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(bucket, "anonymous"), 0o755))

		done := make(chan error, 1)
		go func() {
			done <- waitForStableBlockSet(bucket, 200*time.Millisecond, 20*time.Millisecond, 5*time.Second)
		}()

		time.Sleep(50 * time.Millisecond)
		writeFakeBlock(t, bucket, 1) // a change during the stability window must reset the clock.

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("waitForStableBlockSet did not return")
		}
	})

	t.Run("times out if the block set keeps changing", func(t *testing.T) {
		bucket := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(bucket, "anonymous"), 0o755))

		stop := make(chan struct{})
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
					writeFakeBlock(t, bucket, 1)
					time.Sleep(10 * time.Millisecond)
				}
			}
		}()
		defer close(stop)

		err := waitForStableBlockSet(bucket, 300*time.Millisecond, 20*time.Millisecond, 500*time.Millisecond)
		require.Error(t, err)
	})
}
