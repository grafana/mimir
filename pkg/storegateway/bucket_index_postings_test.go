// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBigEndianPostingsCount(t *testing.T) {
	const count = 1000
	raw := make([]byte, count*4)

	for ix := 0; ix < count; ix++ {
		binary.BigEndian.PutUint32(raw[4*ix:], rand.Uint32())
	}

	p := newBigEndianPostings(raw)
	assert.Equal(t, count, p.length())

	c := 0
	for p.Next() {
		c++
	}
	assert.Equal(t, count, c)
}
