// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadCompartmentRingKeyAndName(t *testing.T) {
	assert.Equal(t, "readcomp-0-ingester-partitions", ReadCompartmentRingKey(0, "ingester-partitions"))
	assert.Equal(t, "readcomp-3-ingester-partitions", ReadCompartmentRingKey(3, "ingester-partitions"))
	assert.Equal(t, "readcomp-0-partition-ring", ReadCompartmentRingName(0, "partition-ring"))
	assert.Equal(t, "readcomp-2-partition-ring", ReadCompartmentRingName(2, "partition-ring"))
}
