// SPDX-License-Identifier: AGPL-3.0-only
package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllocateBallast(t *testing.T) {
	for i := 0; i < 20; i++ {
		size := i * 1024 * 1024

		b := AllocateBallast(size).([][]byte)

		totalSize := 0
		for _, bs := range b {
			totalSize += len(bs)
		}
		require.Equal(t, size, totalSize)
	}
}
