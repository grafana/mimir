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
	"github.com/grafana/dskit/services"
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
	s1 := NewSnapshotter(log.NewNopLogger(), config, testBlocksLoader)
	err := s1.PersistLoadedBlocks()
	require.NoError(t, err)

	persistedFile := filepath.Join(tmpDir, lazyLoadedHeadersListFileName)
	data, err := os.ReadFile(persistedFile)
	require.NoError(t, err)

	expected := fmt.Sprintf(`{"index_header_last_used_time":{"%s":%d},"user_id":"anonymous"}`, testBlockID, usedAt.UnixMilli())
	require.JSONEq(t, expected, string(data))

	restoredBlocks, err := RestoreLoadedBlocks(config.Path)
	require.Equal(t, origBlocks, restoredBlocks)
	require.NoError(t, err)
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
			Path:            tmpDir,
			UserID:          "anonymous",
			PersistInterval: 100 * time.Millisecond,
		}
		s := NewSnapshotter(log.NewNopLogger(), config, testBlocksLoader)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
		time.Sleep(config.PersistInterval * 2) // give enough time to persist the snapshot
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), s))

		persistedFile := filepath.Join(tmpDir, lazyLoadedHeadersListFileName)
		data, err := os.ReadFile(persistedFile)
		require.NoError(t, err)
		require.NotEmpty(t, data)
	})
}

type testBlocksLoaderFunc func() map[ulid.ULID]int64

func (f testBlocksLoaderFunc) LoadedBlocks() map[ulid.ULID]int64 {
	return f()
}
