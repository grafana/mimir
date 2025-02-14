// SPDX-License-Identifier: AGPL-3.0-only

package math

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMedianFilter(t *testing.T) {
	t.Run("not full window", func(t *testing.T) {
		filter := NewMedianFilter(3)
		median := filter.Add(5.0)
		assert.Equal(t, 5.0, median)
		assert.Equal(t, 0.0, filter.Median())
	})

	t.Run("full window", func(t *testing.T) {
		filter := NewMedianFilter(3)
		assert.Equal(t, 1.0, filter.Add(1.0))
		assert.Equal(t, 2.0, filter.Add(2.0))
		assert.Equal(t, 2.0, filter.Add(3.0))
		assert.Equal(t, 2.0, filter.Median())
	})

	t.Run("rolling median", func(t *testing.T) {
		filter := NewMedianFilter(3)
		filter.Add(1.0)
		filter.Add(2.0)
		filter.Add(3.0)
		median := filter.Add(4.0)
		assert.Equal(t, 3.0, median)
	})

	t.Run("unsorted input", func(t *testing.T) {
		filter := NewMedianFilter(5)
		filter.Add(5.0)
		filter.Add(2.0)
		filter.Add(8.0)
		filter.Add(1.0)
		median := filter.Add(9.0)
		assert.Equal(t, 5.0, median)
	})
}
