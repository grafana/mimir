// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadCompartmentRingKeyAndName(t *testing.T) {
	assert.Equal(t, "rc-0-ingester-partitions", ReadCompartmentRingKey(0, "ingester-partitions"))
	assert.Equal(t, "rc-3-ingester-partitions", ReadCompartmentRingKey(3, "ingester-partitions"))
	assert.Equal(t, "rc-0-partition-ring", ReadCompartmentRingName(0, "partition-ring"))
	assert.Equal(t, "rc-2-partition-ring", ReadCompartmentRingName(2, "partition-ring"))
}
