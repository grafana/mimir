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

func TestHashBudget(t *testing.T) {
	assert.Equal(t, int64(256<<20/HashBytes), HashBudget(256<<20))
	assert.Equal(t, int64(1), HashBudget(1), "a set limit allows at least one hash")
	assert.Zero(t, HashBudget(0), "no limit")
}

func TestBoundMaxHashes(t *testing.T) {
	assert.Equal(t, int64(0), BoundMaxHashes(0, 0), "no budget keeps an unbounded request")
	assert.Equal(t, int64(10), BoundMaxHashes(10, 0), "no budget keeps the request's bound")
	assert.Equal(t, int64(100), BoundMaxHashes(0, 100), "an unbounded request gets the budget")
	assert.Equal(t, int64(10), BoundMaxHashes(10, 100), "a smaller request is kept")
	assert.Equal(t, int64(100), BoundMaxHashes(1000, 100), "a larger request is cut")
}
