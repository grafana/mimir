// SPDX-License-Identifier: AGPL-3.0-only

package atomicfs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test")
	wantData := "test1"
	if err := CreateFile(path, strings.NewReader(wantData)); err != nil {
		t.Errorf("CreateFile() error = %v, wantErr %v", err, false)
	}
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	require.Equal(t, wantData, string(data))
}
