// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLabelsAreSorted(t *testing.T) {
	t.Parallel()

	t.Run("sorted", func(t *testing.T) {
		assert.True(t, AreLabelNamesSortedAndUnique([]LabelAdapter{
			{Name: BorrowString("a"), Value: BorrowString("z")},
			{Name: BorrowString("b"), Value: BorrowString("y")},
			{Name: BorrowString("c"), Value: BorrowString("x")},
		}))
	})

	t.Run("not sorted", func(t *testing.T) {
		assert.False(t, AreLabelNamesSortedAndUnique([]LabelAdapter{
			{Name: BorrowString("a"), Value: BorrowString("z")},
			{Name: BorrowString("c"), Value: BorrowString("x")},
			{Name: BorrowString("b"), Value: BorrowString("y")},
		}))
	})

	t.Run("not unique", func(t *testing.T) {
		assert.False(t, AreLabelNamesSortedAndUnique([]LabelAdapter{
			{Name: BorrowString("a"), Value: BorrowString("z")},
			{Name: BorrowString("b"), Value: BorrowString("y")},
			{Name: BorrowString("b"), Value: BorrowString("y")},
			{Name: BorrowString("c"), Value: BorrowString("x")},
		}))
	})

	t.Run("single is ok", func(t *testing.T) {
		assert.True(t, AreLabelNamesSortedAndUnique([]LabelAdapter{
			{Name: BorrowString("a"), Value: BorrowString("z")},
		}))
	})

	t.Run("empty is ok", func(t *testing.T) {
		assert.True(t, AreLabelNamesSortedAndUnique([]LabelAdapter{}))
	})
}
