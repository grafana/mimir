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

	first.merge(second.export())

	// Ensure first is modified in-place.
	assert.Equal(t, 3, first.export().blocksQueried)

	// Ensure second is not modified.
	assert.Equal(t, 2, second.export().blocksQueried)
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
