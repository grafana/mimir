// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testPartitionID = 1

func TestOffsetFile_ReadWrite(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "partition-1.offset")

	logger := log.NewNopLogger()
	offsetFile := newOffsetFile(filePath, testPartitionID, logger)

	// Test reading from non-existent file
	offset, exists := offsetFile.Read()
	assert.False(t, exists)
	assert.Equal(t, int64(0), offset)

	// Test writing offset
	err := offsetFile.Write(12345)
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(filePath)
	require.NoError(t, err)

	// Test reading the written offset
	offset, exists = offsetFile.Read()
	assert.True(t, exists)
	assert.Equal(t, int64(12345), offset)

	// Test overwriting with a new offset
	err = offsetFile.Write(67890)
	require.NoError(t, err)

	offset, exists = offsetFile.Read()
	assert.True(t, exists)
	assert.Equal(t, int64(67890), offset)
}

func TestOffsetFile_WriteCreatesDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	// Use a nested path that doesn't exist
	filePath := filepath.Join(tmpDir, "nested", "path", "partition-1.offset")

	logger := log.NewNopLogger()
	offsetFile := newOffsetFile(filePath, testPartitionID, logger)

	// Write should create all parent directories
	err := offsetFile.Write(1)
	require.NoError(t, err)

	// Verify the file was created
	offset, exists := offsetFile.Read()
	assert.True(t, exists)
	assert.Equal(t, int64(1), offset)
}

func TestOffsetFile_ReadInvalidContent(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "invalid.offset")

	logger := log.NewNopLogger()
	offsetFile := newOffsetFile(filePath, testPartitionID, logger)

	// Write invalid JSON to the file
	err := os.WriteFile(filePath, []byte("not-valid-json"), 0644)
	require.NoError(t, err)

	// Reading should return false and 0
	offset, exists := offsetFile.Read()
	assert.False(t, exists)
	assert.Equal(t, int64(0), offset)
}

func TestOffsetFile_ReadEmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "empty.offset")

	logger := log.NewNopLogger()
	offsetFile := newOffsetFile(filePath, testPartitionID, logger)

	// Write empty file (unmarshal yields Version=0, which is not a valid version)
	err := os.WriteFile(filePath, []byte{}, 0644)
	require.NoError(t, err)

	offset, exists := offsetFile.Read()
	assert.False(t, exists)
	assert.Equal(t, int64(0), offset)
}

func TestOffsetFile_ReadWrongPartitionID(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "partition.offset")

	logger := log.NewNopLogger()
	// Create file for partition 1
	writer := newOffsetFile(filePath, 1, logger)
	require.NoError(t, writer.Write(42))

	// Read with different partition ID
	reader := newOffsetFile(filePath, 2, logger)
	offset, exists := reader.Read()
	assert.False(t, exists)
	assert.Equal(t, int64(0), offset)
}

func TestOffsetFile_ReadWrongVersion(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "version.offset")

	logger := log.NewNopLogger()
	offsetFile := newOffsetFile(filePath, testPartitionID, logger)

	// Write valid structure but with unknown version
	data := offsetFileData{Version: 2, PartitionID: testPartitionID, Offset: 0}
	jsonBytes, err := json.Marshal(data)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filePath, jsonBytes, 0644))

	offset, exists := offsetFile.Read()
	assert.False(t, exists)
	assert.Equal(t, int64(0), offset)
}

func TestOffsetFile_NilWhenEmpty(t *testing.T) {
	logger := log.NewNopLogger()

	// Creating with empty path should return nil
	offsetFile := newOffsetFile("", testPartitionID, logger)
	assert.Nil(t, offsetFile)

	// Operations on nil should be safe
	offset, exists := offsetFile.Read()
	assert.False(t, exists)
	assert.Equal(t, int64(0), offset)

	err := offsetFile.Write(123)
	assert.NoError(t, err)
}

func TestOffsetFile_AtomicWrite(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "atomic.offset")

	logger := log.NewNopLogger()
	offsetFile := newOffsetFile(filePath, testPartitionID, logger)

	// Write initial offset
	err := offsetFile.Write(100)
	require.NoError(t, err)

	// Verify no .tmp file remains
	tmpFile := filePath + ".tmp"
	_, err = os.Stat(tmpFile)
	assert.True(t, os.IsNotExist(err), "temporary file should not exist after write")

	// Verify final file exists with correct content
	offset, exists := offsetFile.Read()
	assert.True(t, exists)
	assert.Equal(t, int64(100), offset)
}

func TestOffsetFile_Concurrent(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "concurrent.offset")

	logger := log.NewNopLogger()
	offsetFile := newOffsetFile(filePath, testPartitionID, logger)

	// Write and read concurrently (the mutex should protect against races)
	done := make(chan bool)

	go func() {
		for i := 0; i < 100; i++ {
			_ = offsetFile.Write(int64(i))
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			_, _ = offsetFile.Read()
		}
		done <- true
	}()

	<-done
	<-done

	// Verify the file is in a valid state
	offset, exists := offsetFile.Read()
	assert.True(t, exists)
	assert.GreaterOrEqual(t, offset, int64(0))
	assert.Less(t, offset, int64(100))
}
