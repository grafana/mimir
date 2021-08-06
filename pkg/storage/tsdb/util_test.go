// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/util_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"crypto/rand"
	"testing"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
)

func TestHashBlockID(t *testing.T) {
	tests := []struct {
		first         ulid.ULID
		second        ulid.ULID
		expectedEqual bool
	}{
		{
			first:         ulid.MustNew(10, nil),
			second:        ulid.MustNew(10, nil),
			expectedEqual: true,
		},
		{
			first:         ulid.MustNew(10, nil),
			second:        ulid.MustNew(20, nil),
			expectedEqual: false,
		},
		{
			first:         ulid.MustNew(10, rand.Reader),
			second:        ulid.MustNew(10, rand.Reader),
			expectedEqual: false,
		},
	}

	for _, testCase := range tests {
		firstHash := HashBlockID(testCase.first)
		secondHash := HashBlockID(testCase.second)
		assert.Equal(t, testCase.expectedEqual, firstHash == secondHash)
	}
}
