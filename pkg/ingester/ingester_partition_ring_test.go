// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompartmentPartitionRingKey(t *testing.T) {
	assert.Equal(t, "ingester-partitions-compartment-0", CompartmentPartitionRingKey(0))
	assert.Equal(t, "ingester-partitions-compartment-1", CompartmentPartitionRingKey(1))
	assert.Equal(t, "ingester-partitions-compartment-12", CompartmentPartitionRingKey(12))
}
