// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBoundMaxIndexBytes(t *testing.T) {
	for name, tc := range map[string]struct {
		asked, limit, want int64
	}{
		"no limit keeps an unbounded request": {asked: 0, limit: 0, want: 0},
		"no limit keeps the request's bound":  {asked: 10, limit: 0, want: 10},
		"an unbounded request gets the limit": {asked: 0, limit: 100, want: 100},
		"a smaller request is kept":           {asked: 10, limit: 100, want: 10},
		"a larger request is cut":             {asked: 1000, limit: 100, want: 100},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, BoundMaxIndexBytes(tc.asked, tc.limit))
		})
	}
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
