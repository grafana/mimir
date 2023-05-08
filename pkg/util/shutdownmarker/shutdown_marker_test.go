package shutdownmarker

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShutdownMarker_GetPath(t *testing.T) {
	dir := "/a/b/c"
	expectedPath := filepath.Join(dir, shutdownMarkerFilename)
	require.Equal(t, expectedPath, GetPath(dir))
}

func TestShutdownMarker_Create(t *testing.T) {
	dir := t.TempDir()
	shutdownMarkerPath := GetPath(dir)
	exists, err := Exists(shutdownMarkerPath)
	require.Nil(t, err)
	require.False(t, exists)

	err = Create(shutdownMarkerPath)
	require.Nil(t, err)

	exists, err = Exists(shutdownMarkerPath)
	require.Nil(t, err)
	require.True(t, exists)
}

func TestShutdownMarker_Remove(t *testing.T) {
	dir := t.TempDir()
	shutdownMarkerPath := GetPath(dir)
	exists, err := Exists(shutdownMarkerPath)
	require.Nil(t, err)
	require.False(t, exists)

	err = Create(shutdownMarkerPath)
	exists, err = Exists(shutdownMarkerPath)
	require.Nil(t, err)
	require.True(t, exists)

	err = Remove(shutdownMarkerPath)
	exists, err = Exists(shutdownMarkerPath)
	require.Nil(t, err)
	require.False(t, exists)
}
