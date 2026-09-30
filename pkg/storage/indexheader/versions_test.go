// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestIndexHeaderFilename(t *testing.T) {
	require.Equal(t, block.IndexHeaderFilename, indexHeaderFilename(BinaryFormatV1))
	require.Equal(t, "index-header-v2", indexHeaderFilename(BinaryFormatV2))
	require.Equal(t, block.IndexHeaderFilenameV2, indexHeaderFilename(BinaryFormatV2))
	// A hypothetical future version follows the same "-vN" convention without any code change.
	require.Equal(t, "index-header-v3", indexHeaderFilename(3))
}

func TestIndexHeadersOnDisk(t *testing.T) {
	t.Run("empty directory", func(t *testing.T) {
		dir := t.TempDir()
		headers, err := IndexHeadersOnDisk(dir)
		require.NoError(t, err)
		require.Empty(t, headers)
	})

	t.Run("parse v1, v2, and vN (unsupported) by filename", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "index-header"), "v1 content")
		writeFile(t, filepath.Join(dir, "index-header-v2"), "v2 content")
		writeFile(t, filepath.Join(dir, "index-header-v9"), "unknown future content")

		headers, err := IndexHeadersOnDisk(dir)
		require.NoError(t, err)

		versions := make(map[int]string, len(headers))
		for _, h := range headers {
			versions[h.Version] = h.Path
		}
		require.Equal(t, map[int]string{
			1: filepath.Join(dir, "index-header"),
			2: filepath.Join(dir, "index-header-v2"),
			9: filepath.Join(dir, "index-header-v9"),
		}, versions)
	})

	t.Run("ignore non-matching filenames", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, block.SparseIndexHeaderFilename), "sparse")
		writeFile(t, filepath.Join(dir, "indexheader"), "missing the dash")
		writeFile(t, filepath.Join(dir, "index-header-vN"), "not a number")
		writeFile(t, filepath.Join(dir, "index-header-v2-extra"), "trailing junk")
		writeFile(t, filepath.Join(dir, "meta.json"), "{}")

		headers, err := IndexHeadersOnDisk(dir)
		require.NoError(t, err)
		require.Empty(t, headers)
	})

	t.Run("ignore .tmp files", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "index-header.tmp"), "partial")
		writeFile(t, filepath.Join(dir, "index-header-v2.tmp"), "partial")

		headers, err := IndexHeadersOnDisk(dir)
		require.NoError(t, err)
		require.Empty(t, headers)
	})

	t.Run("ignore directories matching index-header filename pattern", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.Mkdir(filepath.Join(dir, "index-header-v2"), os.ModePerm))

		headers, err := IndexHeadersOnDisk(dir)
		require.NoError(t, err)
		require.Empty(t, headers)
	})
}

func TestRemoveIndexHeaders(t *testing.T) {
	testCases := []struct {
		name                   string
		extantFiles            []string
		filesOutsideBlockDir   []string
		toRemove               func(blockDir, outsideDir string) []OnDiskIndexHeader
		wantErr                bool
		expectedRemainingFiles []string
	}{
		{
			name:        "remove given headers only",
			extantFiles: []string{"index-header", "index-header-v2", "index-header-v9", block.SparseIndexHeaderFilename},
			toRemove: func(blockDir, _ string) []OnDiskIndexHeader {
				return []OnDiskIndexHeader{
					{Version: BinaryFormatV1, Path: filepath.Join(blockDir, "index-header")},
					{Version: 9, Path: filepath.Join(blockDir, "index-header-v9")},
				}
			},
			expectedRemainingFiles: []string{"index-header-v2", block.SparseIndexHeaderFilename},
		},
		{
			name:        "remove nothing",
			extantFiles: []string{"index-header-v2"},
			toRemove: func(_, _ string) []OnDiskIndexHeader {
				return nil
			},
			expectedRemainingFiles: []string{"index-header-v2"},
		},
		{
			name:        "remove a header doesn't exist",
			extantFiles: nil,
			toRemove: func(blockDir, _ string) []OnDiskIndexHeader {
				return []OnDiskIndexHeader{{Version: BinaryFormatV1, Path: filepath.Join(blockDir, "index-header")}}
			},
			expectedRemainingFiles: nil,
		},
		{
			name:                 "remove a path outside of blockDir",
			filesOutsideBlockDir: []string{"index-header-v2"},
			toRemove: func(_, outsideDir string) []OnDiskIndexHeader {
				return []OnDiskIndexHeader{{Version: BinaryFormatV2, Path: filepath.Join(outsideDir, "index-header-v2")}}
			},
			wantErr:                true,
			expectedRemainingFiles: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			blockDir := t.TempDir()
			outsideDir := t.TempDir()

			for _, f := range tc.extantFiles {
				writeFile(t, filepath.Join(blockDir, f), "content")
			}
			for _, f := range tc.filesOutsideBlockDir {
				writeFile(t, filepath.Join(outsideDir, f), "content")
			}

			err := removeIndexHeaders(blockDir, tc.toRemove(blockDir, outsideDir)...)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			entries, err := os.ReadDir(blockDir)
			require.NoError(t, err)
			remaining := make([]string, 0, len(entries))
			for _, e := range entries {
				remaining = append(remaining, e.Name())
			}
			require.ElementsMatch(t, tc.expectedRemainingFiles, remaining)

			for _, f := range tc.filesOutsideBlockDir {
				_, statErr := os.Stat(filepath.Join(outsideDir, f))
				require.NoError(t, statErr, "files outside blockDir must never be removed")
			}
		})
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), os.ModePerm))
}
