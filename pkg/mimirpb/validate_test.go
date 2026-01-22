// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLabelsAreSorted(t *testing.T) {
	t.Parallel()

	t.Run("sorted", func(t *testing.T) {
		assert.True(t, LabelsAreUniqueSorted([]LabelAdapter{
			{Name: "a", Value: "z"},
			{Name: "b", Value: "y"},
			{Name: "c", Value: "x"},
		}))
	})

	t.Run("not sorted", func(t *testing.T) {
		assert.False(t, LabelsAreUniqueSorted([]LabelAdapter{
			{Name: "a", Value: "z"},
			{Name: "c", Value: "x"},
			{Name: "b", Value: "y"},
		}))
	})

	t.Run("not unique", func(t *testing.T) {
		assert.False(t, LabelsAreUniqueSorted([]LabelAdapter{
			{Name: "a", Value: "z"},
			{Name: "b", Value: "y"},
			{Name: "b", Value: "y"},
			{Name: "c", Value: "x"},
		}))
	})

	t.Run("single is ok", func(t *testing.T) {
		assert.True(t, LabelsAreUniqueSorted([]LabelAdapter{
			{Name: "a", Value: "z"},
		}))
	})

	t.Run("empty is ok", func(t *testing.T) {
		assert.True(t, LabelsAreUniqueSorted([]LabelAdapter{
			{Name: "a", Value: "z"},
		}))
	})
}
