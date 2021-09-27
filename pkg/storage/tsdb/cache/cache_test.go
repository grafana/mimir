// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cache/cache_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package cache

import (
	"encoding/base64"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"golang.org/x/crypto/blake2b"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestCacheKey_string(t *testing.T) {
	t.Parallel()

	uid := ulid.MustNew(1, nil)

	tests := map[string]struct {
		key      cacheKey
		expected string
	}{
		"should stringify postings cache key": {
			key: cacheKey{uid, cacheKeyPostings(labels.Label{Name: "foo", Value: "bar"})},
			expected: func() string {
				hash := blake2b.Sum256([]byte("foo:bar"))
				encodedHash := base64.RawURLEncoding.EncodeToString(hash[0:])

				return fmt.Sprintf("P:%s:%s", uid.String(), encodedHash)
			}(),
		},
		"should stringify series cache key": {
			key:      cacheKey{uid, cacheKeySeries(12345)},
			expected: fmt.Sprintf("S:%s:12345", uid.String()),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual := testData.key.string()
			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestCacheKey_string_ShouldGuaranteeReasonablyShortKeyLength(t *testing.T) {
	t.Parallel()

	uid := ulid.MustNew(1, nil)

	tests := map[string]struct {
		keys        []cacheKey
		expectedLen int
	}{
		"should guarantee reasonably short key length for postings": {
			expectedLen: 72,
			keys: []cacheKey{
				{uid, cacheKeyPostings(labels.Label{Name: "a", Value: "b"})},
				{uid, cacheKeyPostings(labels.Label{Name: strings.Repeat("a", 100), Value: strings.Repeat("a", 1000)})},
			},
		},
		"should guarantee reasonably short key length for series": {
			expectedLen: 49,
			keys: []cacheKey{
				{uid, cacheKeySeries(math.MaxUint64)},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for _, key := range testData.keys {
				assert.Equal(t, testData.expectedLen, len(key.string()))
			}
		})
	}
}

func BenchmarkCacheKey_string_Postings(b *testing.B) {
	uid := ulid.MustNew(1, nil)
	key := cacheKey{uid, cacheKeyPostings(labels.Label{Name: strings.Repeat("a", 100), Value: strings.Repeat("a", 1000)})}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key.string()
	}
}

func BenchmarkCacheKey_string_Series(b *testing.B) {
	uid := ulid.MustNew(1, nil)
	key := cacheKey{uid, cacheKeySeries(math.MaxUint64)}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key.string()
	}
}
