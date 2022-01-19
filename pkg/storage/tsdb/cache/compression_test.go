// SPDX-License-Identifier: AGPL-3.0-only

package cache

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
)

func TestCompressionConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		cfg      CompressionConfig
		expected error
	}{
		"should pass with default config": {
			cfg: CompressionConfig{},
		},
		"should pass with snappy compression": {
			cfg: CompressionConfig{
				Compression: "snappy",
			},
		},
		"should fail with unsupported compression": {
			cfg: CompressionConfig{
				Compression: "unsupported",
			},
			expected: errUnsupportedCompression,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.cfg.Validate())
		})
	}
}

func TestSnappyCache(t *testing.T) {
	ctx := context.Background()
	backend := NewMockCache()
	c := NewSnappy(backend, log.NewNopLogger())

	t.Run("Fetch() should return empty results if no key has been found", func(t *testing.T) {
		assert.Empty(t, c.Fetch(ctx, []string{"a", "b", "c"}))
	})

	t.Run("Fetch() should return previously set keys", func(t *testing.T) {
		expected := map[string][]byte{
			"a": []byte("value-a"),
			"b": []byte("value-b"),
		}

		c.Store(ctx, expected, 0)
		assert.Equal(t, expected, c.Fetch(ctx, []string{"a", "b", "c"}))
	})

	t.Run("Fetch() should skip entries failing to decode", func(t *testing.T) {
		c.Store(ctx, map[string][]byte{"a": []byte("value-a")}, 0)
		backend.Store(ctx, map[string][]byte{"b": []byte("value-b")}, 0)

		expected := map[string][]byte{
			"a": []byte("value-a"),
		}
		assert.Equal(t, expected, c.Fetch(ctx, []string{"a", "b", "c"}))
	})
}
