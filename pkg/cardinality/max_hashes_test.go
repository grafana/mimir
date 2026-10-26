// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashesNeeded(t *testing.T) {
	n, ok := HashesNeeded("store-gateway: " + MaxHashesRefusal(1234, 1000))
	assert.True(t, ok)
	assert.Equal(t, int64(1234), n)
	_, ok = HashesNeeded("the request would read about 5000 index bytes, more than max_index_bytes 1000")
	assert.False(t, ok)
}
