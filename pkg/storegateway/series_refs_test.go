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
		{BlockID: ulid.MustNew(0, nil), MinTime: 2, MaxTime: 5},
		{BlockID: ulid.MustNew(1, nil), MinTime: 1, MaxTime: 5},
		{BlockID: ulid.MustNew(2, nil), MinTime: 1, MaxTime: 3},
		{BlockID: ulid.MustNew(3, nil), MinTime: 4, MaxTime: 7},
		{BlockID: ulid.MustNew(4, nil), MinTime: 3, MaxTime: 6},
	}

	expected := []seriesChunkRef{
		{BlockID: ulid.MustNew(2, nil), MinTime: 1, MaxTime: 3},
		{BlockID: ulid.MustNew(1, nil), MinTime: 1, MaxTime: 5},
		{BlockID: ulid.MustNew(0, nil), MinTime: 2, MaxTime: 5},
		{BlockID: ulid.MustNew(4, nil), MinTime: 3, MaxTime: 6},
		{BlockID: ulid.MustNew(3, nil), MinTime: 4, MaxTime: 7},
	}

	sort.Slice(input, func(i, j int) bool {
		return input[i].Compare(input[j]) > 0
	})

	assert.Equal(t, expected, input)
}
