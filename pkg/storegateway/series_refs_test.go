// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"sort"
	"testing"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
)

func TestSeriesChunkRef_Compare(t *testing.T) {
	input := []seriesChunkRef{
		{blockID: ulid.MustNew(0, nil), minTime: 2, maxTime: 5},
		{blockID: ulid.MustNew(1, nil), minTime: 1, maxTime: 5},
		{blockID: ulid.MustNew(2, nil), minTime: 1, maxTime: 3},
		{blockID: ulid.MustNew(3, nil), minTime: 4, maxTime: 7},
		{blockID: ulid.MustNew(4, nil), minTime: 3, maxTime: 6},
	}

	expected := []seriesChunkRef{
		{blockID: ulid.MustNew(2, nil), minTime: 1, maxTime: 3},
		{blockID: ulid.MustNew(1, nil), minTime: 1, maxTime: 5},
		{blockID: ulid.MustNew(0, nil), minTime: 2, maxTime: 5},
		{blockID: ulid.MustNew(4, nil), minTime: 3, maxTime: 6},
		{blockID: ulid.MustNew(3, nil), minTime: 4, maxTime: 7},
	}

	sort.Slice(input, func(i, j int) bool {
		return input[i].Compare(input[j]) > 0
	})

	assert.Equal(t, expected, input)
}
