// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/util/pool/pool_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package pool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func makeFunc(size int) []int {
	return make([]int, 0, size)
}

func TestPool(t *testing.T) {
	testPool := NewBucketedPool(1, 8, 2, makeFunc)
	cases := []struct {
		size        int
		expectedCap int
	}{
		{
			size:        -1,
			expectedCap: 1,
		},
		{
			size:        3,
			expectedCap: 4,
		},
		{
			size:        10,
			expectedCap: 10,
		},
	}
	for _, c := range cases {
		ret := testPool.Get(c.size)
		require.Equal(t, c.expectedCap, cap(ret))
		testPool.Put(ret)
	}
}
