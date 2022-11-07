// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSafeQueryStats_merge(t *testing.T) {
	first := newSafeQueryStats()
	first.update(func(stats *queryStats) {
		stats.blocksQueried = 1
	})

	second := newSafeQueryStats()
	second.update(func(stats *queryStats) {
		stats.blocksQueried = 2
	})

	merged := first.merge(second.export())

	// Ensure first is not modified in-place.
	assert.Equal(t, 1, first.export().blocksQueried)

	// Ensure the returned merged stats are correct.
	assert.Equal(t, 3, merged.export().blocksQueried)
}

func TestSafeQueryStats_export(t *testing.T) {
	orig := newSafeQueryStats()
	orig.update(func(stats *queryStats) {
		stats.blocksQueried = 10
	})

	exported := orig.export()
	assert.Equal(t, 10, exported.blocksQueried)

	orig.update(func(stats *queryStats) {
		stats.blocksQueried = 20
	})

	assert.Equal(t, 20, orig.unsafeStats.blocksQueried)
	assert.Equal(t, 10, exported.blocksQueried)
}
