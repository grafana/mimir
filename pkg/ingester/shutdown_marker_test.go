// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShutdownMarker_Create(t *testing.T) {
	t.Run("directory does not exist", func(t *testing.T) {
		m := NewShutdownMarker(path.Join(t.TempDir(), "does-not-exist", "marker.txt"))
		require.Error(t, m.Create())
	})

	t.Run("file already exists as directory", func(t *testing.T) {
		dir := path.Join(t.TempDir(), "exists", "marker.txt")

		require.NoError(t, os.MkdirAll(dir, 0777))

		m := NewShutdownMarker(dir)
		require.Error(t, m.Create())
	})

	t.Run("file already exists", func(t *testing.T) {
		dir := path.Join(t.TempDir(), "exists")
		full := path.Join(dir, "marker.txt")

		require.NoError(t, os.MkdirAll(dir, 0777))
		require.NoError(t, os.WriteFile(full, []byte{}, 0666))

		m := NewShutdownMarker(full)
		require.NoError(t, m.Create())
	})

	t.Run("file does exist", func(t *testing.T) {
		dir := path.Join(t.TempDir(), "exists")
		full := path.Join(dir, "marker.txt")

		require.NoError(t, os.MkdirAll(dir, 0777))

		m := NewShutdownMarker(full)
		require.NoError(t, m.Create())
	})
}

func TestShutdownMarker_Exists(t *testing.T) {
	t.Run("file exists", func(t *testing.T) {
		dir := path.Join(t.TempDir(), "exists")
		full := path.Join(dir, "marker.txt")

		require.NoError(t, os.MkdirAll(dir, 0777))
		require.NoError(t, os.WriteFile(full, []byte{}, 0400))

		m := NewShutdownMarker(full)
		exists, err := m.Exists()

		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("file does not exist", func(t *testing.T) {
		dir := path.Join(t.TempDir(), "exists")
		full := path.Join(dir, "marker.txt")

		require.NoError(t, os.MkdirAll(dir, 0777))

		m := NewShutdownMarker(full)
		exists, err := m.Exists()

		require.False(t, exists)
		require.NoError(t, err)
	})
}
