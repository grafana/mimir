// SPDX-License-Identifier: AGPL-3.0-only

package bucketindex

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInProgressIndex_WriteAndRead(t *testing.T) {
	logger := log.NewNopLogger()
	tmpDir := t.TempDir()
	userID := "test-user"

	idx := &Index{
		Version:   IndexVersion2,
		UpdatedAt: time.Now().Unix(),
		Blocks: []*Block{
			{
				ID:      ulid.MustNew(1, nil),
				MinTime: 1000,
				MaxTime: 2000,
			},
		},
		BlockDeletionMarks: []*BlockDeletionMark{
			{
				ID:           ulid.MustNew(2, nil),
				DeletionTime: time.Now().Unix(),
			},
		},
	}

	inProgressIdx := &InProgressIndex{
		Index:         idx,
		BaseUpdatedAt: 12345,
		CreatedAt:     time.Now().Unix(),
	}

	// Write the index.
	err := WriteInProgressIndex(tmpDir, userID, inProgressIdx, logger)
	require.NoError(t, err)

	// Verify the file exists.
	path := inProgressIndexPath(tmpDir, userID)
	_, err = os.Stat(path)
	require.NoError(t, err)

	// Read the index back.
	readIdx, err := ReadInProgressIndex(tmpDir, userID, logger)
	require.NoError(t, err)
	require.NotNil(t, readIdx)

	// Verify the content.
	assert.Equal(t, inProgressIdx.BaseUpdatedAt, readIdx.BaseUpdatedAt)
	assert.Equal(t, inProgressIdx.CreatedAt, readIdx.CreatedAt)
	assert.Equal(t, len(inProgressIdx.Index.Blocks), len(readIdx.Index.Blocks))
	assert.Equal(t, len(inProgressIdx.Index.BlockDeletionMarks), len(readIdx.Index.BlockDeletionMarks))
	assert.Equal(t, inProgressIdx.Index.Version, readIdx.Index.Version)
}

func TestInProgressIndex_ReadNonExistent(t *testing.T) {
	logger := log.NewNopLogger()
	tmpDir := t.TempDir()
	userID := "test-user"

	// Read a non-existent index.
	readIdx, err := ReadInProgressIndex(tmpDir, userID, logger)
	require.NoError(t, err)
	assert.Nil(t, readIdx)
}

func TestInProgressIndex_Delete(t *testing.T) {
	logger := log.NewNopLogger()
	tmpDir := t.TempDir()
	userID := "test-user"

	idx := &Index{
		Version:   IndexVersion2,
		UpdatedAt: time.Now().Unix(),
	}

	inProgressIdx := &InProgressIndex{
		Index:         idx,
		BaseUpdatedAt: 12345,
		CreatedAt:     time.Now().Unix(),
	}

	// Write the index.
	err := WriteInProgressIndex(tmpDir, userID, inProgressIdx, logger)
	require.NoError(t, err)

	// Verify it exists.
	path := inProgressIndexPath(tmpDir, userID)
	_, err = os.Stat(path)
	require.NoError(t, err)

	// Delete it.
	err = DeleteInProgressIndex(tmpDir, userID, logger)
	require.NoError(t, err)

	// Verify it's gone.
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))

	// Deleting again should not error.
	err = DeleteInProgressIndex(tmpDir, userID, logger)
	require.NoError(t, err)
}

func TestInProgressIndex_EmptyDataDir(t *testing.T) {
	logger := log.NewNopLogger()
	userID := "test-user"

	idx := &Index{
		Version:   IndexVersion2,
		UpdatedAt: time.Now().Unix(),
	}

	inProgressIdx := &InProgressIndex{
		Index:         idx,
		BaseUpdatedAt: 12345,
		CreatedAt:     time.Now().Unix(),
	}

	// Write with empty dataDir should succeed but do nothing.
	err := WriteInProgressIndex("", userID, inProgressIdx, logger)
	require.NoError(t, err)

	// Read with empty dataDir should return nil.
	readIdx, err := ReadInProgressIndex("", userID, logger)
	require.NoError(t, err)
	assert.Nil(t, readIdx)

	// Delete with empty dataDir should succeed but do nothing.
	err = DeleteInProgressIndex("", userID, logger)
	require.NoError(t, err)
}

func TestInProgressIndex_CorruptedFile(t *testing.T) {
	logger := log.NewNopLogger()
	tmpDir := t.TempDir()
	userID := "test-user"

	// Create a corrupted file.
	path := inProgressIndexPath(tmpDir, userID)
	err := os.MkdirAll(filepath.Dir(path), 0755)
	require.NoError(t, err)

	err = os.WriteFile(path, []byte("corrupted data"), 0644)
	require.NoError(t, err)

	// Reading the corrupted file should return an error.
	readIdx, err := ReadInProgressIndex(tmpDir, userID, logger)
	require.Error(t, err)
	assert.Nil(t, readIdx)
}

func TestInProgressIndex_AtomicWrite(t *testing.T) {
	logger := log.NewNopLogger()
	tmpDir := t.TempDir()
	userID := "test-user"

	idx := &Index{
		Version:   IndexVersion2,
		UpdatedAt: time.Now().Unix(),
	}

	inProgressIdx := &InProgressIndex{
		Index:         idx,
		BaseUpdatedAt: 12345,
		CreatedAt:     time.Now().Unix(),
	}

	// Write the index.
	err := WriteInProgressIndex(tmpDir, userID, inProgressIdx, logger)
	require.NoError(t, err)

	// Verify the temporary file was cleaned up.
	path := inProgressIndexPath(tmpDir, userID)
	tmpPath := path + ".tmp"
	_, err = os.Stat(tmpPath)
	require.True(t, os.IsNotExist(err), "temporary file should be cleaned up")

	// Verify the final file exists.
	_, err = os.Stat(path)
	require.NoError(t, err)
}
