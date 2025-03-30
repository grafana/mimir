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
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

func TestSnapshotter_PersistAndRestoreLoadedBlocks(t *testing.T) {
	tmpDir := t.TempDir()

	testBlockID := ulid.MustNew(ulid.Now(), rand.Reader)

	origBlocks := []ulid.ULID{testBlockID}

	testBlocksLoader := testBlocksLoaderFunc(func() []ulid.ULID { return origBlocks })

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

	expected := fmt.Sprintf(`{"index_header_last_used_time":{"%s":1},"user_id":"anonymous"}`, testBlockID)
	require.JSONEq(t, expected, string(data))

	restoredBlocks, err := RestoreLoadedBlocks(config.Path)
	require.Equal(t, map[ulid.ULID]struct{}{testBlockID: {}}, restoredBlocks)
	require.NoError(t, err)
}

func TestSnapshotter_ChecksumOptimization(t *testing.T) {
	tmpDir := t.TempDir()

	firstBlockID := ulid.MustNew(ulid.Now(), rand.Reader)

	origBlocks := []ulid.ULID{firstBlockID}
	testBlocksLoader := testBlocksLoaderFunc(func() []ulid.ULID { return origBlocks })

	config := SnapshotterConfig{
		Path:   tmpDir,
		UserID: "anonymous",
	}

	// Create snapshotter and persist data
	s := NewSnapshotter(log.NewNopLogger(), config, testBlocksLoader)

	err := s.PersistLoadedBlocks()
	require.NoError(t, err)

	// Verify the content of the file using RestoreLoadedBlocks
	restoredBlocks, err := RestoreLoadedBlocks(config.Path)
	require.NoError(t, err)
	require.Equal(t, map[ulid.ULID]struct{}{firstBlockID: {}}, restoredBlocks, "Restored blocks should match original blocks")

	// Get file info after first write
	persistedFile := filepath.Join(tmpDir, lazyLoadedHeadersListFileName)
	firstStat, err := os.Stat(persistedFile)
	require.NoError(t, err)
	firstModTime := firstStat.ModTime()

	// Wait a moment to ensure modification time would be different if file is written
	time.Sleep(10 * time.Millisecond)

	// Call persist again with the same data
	err = s.PersistLoadedBlocks()
	require.NoError(t, err)

	// Get file info after second write attempt
	secondStat, err := os.Stat(persistedFile)
	require.NoError(t, err)
	secondModTime := secondStat.ModTime()

	// File should not have been modified since the data hasn't changed
	require.Equal(t, firstModTime, secondModTime, "File was modified even though data hasn't changed")

	// Verify the content has not changed using RestoreLoadedBlocks
	restoredBlocksAfterSecondPersist, err := RestoreLoadedBlocks(config.Path)
	require.NoError(t, err)
	require.Equal(t, map[ulid.ULID]struct{}{firstBlockID: {}}, restoredBlocksAfterSecondPersist, "Restored blocks should match original blocks")

	// Now change the data and persist again
	secondBlockID := ulid.MustNew(ulid.Now(), rand.Reader)
	newBlocks := []ulid.ULID{firstBlockID, secondBlockID}

	// Create a new loader with updated data
	updatedBlocksLoader := testBlocksLoaderFunc(func() []ulid.ULID { return newBlocks })
	s.bl = updatedBlocksLoader

	// Wait a moment to ensure modification time would be different if file is written
	time.Sleep(10 * time.Millisecond)

	// Persist the new data
	err = s.PersistLoadedBlocks()
	require.NoError(t, err)

	// Get file info after third write
	thirdStat, err := os.Stat(persistedFile)
	require.NoError(t, err)
	thirdModTime := thirdStat.ModTime()

	// File should have been modified since the data has changed
	require.NotEqual(t, secondModTime, thirdModTime, "File was not modified even though data has changed")

	// Verify the content has changed using RestoreLoadedBlocks
	restoredBlocksAfterThirdPersist, err := RestoreLoadedBlocks(config.Path)
	require.NoError(t, err)
	expectedBlocks := map[ulid.ULID]struct{}{
		firstBlockID:  {},
		secondBlockID: {},
	}
	require.Equal(t, expectedBlocks, restoredBlocksAfterThirdPersist, "Restored blocks should match new blocks")
}

func TestSnapshotter_StartStop(t *testing.T) {
	t.Run("stop after start", func(t *testing.T) {
		tmpDir := t.TempDir()

		testBlocksLoader := testBlocksLoaderFunc(func() []ulid.ULID {
			// We don't care about the content of the index header in this test.
			return []ulid.ULID{
				ulid.MustNew(ulid.Now(), rand.Reader),
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

type testBlocksLoaderFunc func() []ulid.ULID

func (f testBlocksLoaderFunc) LoadedBlocks() []ulid.ULID {
	return f()
}
