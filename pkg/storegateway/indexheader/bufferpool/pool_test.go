// SPDX-License-Identifier: AGPL-3.0-only

package bufferpool

import (
	"io"
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const testFile = "./testdata/file.bin" // 5000 bytes

func TestPool_LoadPages(t *testing.T) {
	p := New(prometheus.DefaultRegisterer, 10, 100)

	f, err := os.OpenFile(testFile, openFileFlags, os.ModePerm)
	require.NoError(t, err)

	// load 1s pageset
	rs0, err := p.LoadPages(f, 0, 5)
	require.NoError(t, err)
	require.NotNil(t, rs0)
	require.Equal(t, 5, rs0.Size())
	require.Len(t, p.inUse, 5)
	require.Len(t, p.evictCandidates, 0)

	// load 2nd pageset
	rs1, err := p.LoadPages(f, 5, 5)
	require.NoError(t, err)
	require.NotNil(t, rs1)
	require.Equal(t, 5, rs1.Size())
	require.Len(t, p.inUse, 10)
	require.Len(t, p.evictCandidates, 0)

	// fail on loading last page...
	rs2, err := p.LoadPages(f, 49, 1)
	require.Equal(t, errNoBufferPages, err)
	require.Nil(t, rs2)

	// unload 1st pageset
	rs0.Close()
	require.Len(t, p.inUse, 5)
	require.Len(t, p.evictCandidates, 5)

	// try loading last page
	rs2, err = p.LoadPages(f, 49, 1)
	require.NoError(t, err)
	require.NotNil(t, rs2)
	require.Equal(t, 1, rs2.Size())

	// load out-of-bound page
	rs3, err := p.LoadPages(f, 50, 1)
	require.Nil(t, rs3)
	require.Equal(t, io.EOF, err)
}
