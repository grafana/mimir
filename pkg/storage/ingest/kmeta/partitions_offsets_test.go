// SPDX-License-Identifier: AGPL-3.0-only

package kmeta

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSingleClusterPartitionOffsets(t *testing.T) {
	assert.Equal(t, PartitionOffsets{7}, NewSingleClusterPartitionOffsets(7))
}

func TestNewMultiClusterPartitionOffsets(t *testing.T) {
	assert.Equal(t, PartitionOffsets{10, 20, -1}, NewMultiClusterPartitionOffsets([]int64{10, 20, -1}))
}
