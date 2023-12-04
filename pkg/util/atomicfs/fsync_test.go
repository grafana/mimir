// SPDX-License-Identifier: AGPL-3.0-only

package atomicfs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test")
	wantData := "test1"
	if err := CreateFile(path, strings.NewReader(wantData)); err != nil {
		t.Errorf("CreateFile() error = %v, wantErr %v", err, false)
	}
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	require.Equal(t, wantData, string(data))
}

func TestCreateFileAndMove(t *testing.T) {
	tmpPath := filepath.Join(t.TempDir(), "test")
	finalPath := filepath.Join(t.TempDir(), "testFinal")
	wantData := "test1"
	err := CreateFileAndMove(tmpPath, finalPath, strings.NewReader(wantData))
	require.NoError(t, err)
	_, err = os.ReadFile(tmpPath)
	require.Error(t, err, "we expect error because the file in tmpPath should already been removed")

	data, err := os.ReadFile(finalPath)
	require.NoError(t, err)

	require.Equal(t, wantData, string(data))
}
