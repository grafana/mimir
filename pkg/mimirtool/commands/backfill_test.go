// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockList_SetCleansPath(t *testing.T) {
	dir := t.TempDir()

	var l blockList
	require.NoError(t, l.Set(dir+"/"))
	assert.Equal(t, blockList{filepath.Clean(dir)}, l)
}
