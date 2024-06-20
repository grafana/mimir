// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/stretchr/testify/require"
)

func TestSnapshotter_PersistAndRestoreLoadedBlocks(t *testing.T) {
	tmpDir := t.TempDir()

	usedAt := time.Now()
	testBlockID := ulid.MustNew(ulid.Now(), rand.Reader)

	origBlocks := map[ulid.ULID]int64{
		testBlockID: usedAt.UnixMilli(),
	}
	testBlocksLoader := testBlocksLoaderFunc(func() map[ulid.ULID]int64 { return origBlocks })

	config := SnapshotterConfig{
		Path:   tmpDir,
		UserID: "anonymous",
	}

	// First instance persists the original snapshot.
	s1 := NewSnapshotter(log.NewNopLogger(), config)
	err := s1.PersistLoadedBlocks(testBlocksLoader)
	require.NoError(t, err)

	persistedFile := filepath.Join(tmpDir, lazyLoadedHeadersListFileName)
	data, err := os.ReadFile(persistedFile)
	require.NoError(t, err)

	expected := fmt.Sprintf(`{"index_header_last_used_time":{"%s":%d},"user_id":"anonymous"}`, testBlockID, usedAt.UnixMilli())
	require.JSONEq(t, expected, string(data))

	// Another instance restores the snapshot persisted earlier.
	s2 := NewSnapshotter(log.NewNopLogger(), config)

	restoredBlocks := s2.RestoreLoadedBlocks()
	require.Equal(t, origBlocks, restoredBlocks)
}

func TestSnapshotter_StartStop(t *testing.T) {
	t.Run("stop after start", func(t *testing.T) {
		tmpDir := t.TempDir()

		testBlocksLoader := testBlocksLoaderFunc(func() map[ulid.ULID]int64 {
			// We don't care about the content of the index header in this test.
			return map[ulid.ULID]int64{
				ulid.MustNew(ulid.Now(), rand.Reader): time.Now().UnixMilli(),
			}
		})

		config := SnapshotterConfig{
			Path:   tmpDir,
			UserID: "anonymous",
		}
		s := NewSnapshotter(log.NewNopLogger(), config)

		s.Start(context.Background(), testBlocksLoader)
		s.Stop()

		persistedFile := filepath.Join(tmpDir, lazyLoadedHeadersListFileName)
		data, err := os.ReadFile(persistedFile)
		require.NoError(t, err)
		require.NotEmpty(t, data)
	})

	t.Run("stop but no start", func(t *testing.T) {
		tmpDir := t.TempDir()

		config := SnapshotterConfig{
			Path:   tmpDir,
			UserID: "anonymous",
		}
		s := NewSnapshotter(log.NewNopLogger(), config)

		// Nothing was started but an attempt to stop shouldn't hang.
		s.Stop()

		persistedFile := filepath.Join(tmpDir, lazyLoadedHeadersListFileName)
		_, err := os.ReadFile(persistedFile)
		require.ErrorIs(t, err, os.ErrNotExist)
	})
}

type testBlocksLoaderFunc func() map[ulid.ULID]int64

func (f testBlocksLoaderFunc) LoadedBlocks() map[ulid.ULID]int64 {
	return f()
}
