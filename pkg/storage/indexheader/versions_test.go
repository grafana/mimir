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

func TestRemoveOtherIndexHeaderVersions(t *testing.T) {
	t.Run("removes every other version, including unsupported ones", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "index-header"), "v1")
		writeFile(t, filepath.Join(dir, "index-header-v2"), "v2")
		writeFile(t, filepath.Join(dir, "index-header-v9"), "unknown")
		writeFile(t, filepath.Join(dir, block.SparseIndexHeaderFilename), "sparse, must survive")

		require.NoError(t, removeOtherIndexHeaderVersions(dir, BinaryFormatV2))

		headers, err := IndexHeadersOnDisk(dir)
		require.NoError(t, err)
		require.Len(t, headers, 1)
		require.Equal(t, BinaryFormatV2, headers[0].Version)

		_, err = os.Stat(filepath.Join(dir, block.SparseIndexHeaderFilename))
		require.NoError(t, err, "sparse-index-header must never be touched by the index-header sweep")
	})

	t.Run("kept version stays on disk", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "index-header-v2"), "v2")

		require.NoError(t, removeOtherIndexHeaderVersions(dir, BinaryFormatV2))

		headers, err := IndexHeadersOnDisk(dir)
		require.NoError(t, err)
		require.Len(t, headers, 1)
	})

	t.Run("no index-header on disk", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, removeOtherIndexHeaderVersions(dir, BinaryFormatV1))

		headers, err := IndexHeadersOnDisk(dir)
		require.NoError(t, err)
		require.Empty(t, headers)
	})
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), os.ModePerm))
}
