// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatchersStringer(t *testing.T) {
	t.Run("One matcher", func(t *testing.T) {
		m, err := labels.NewMatcher(labels.MatchEqual, "name", "value")
		require.NoError(t, err)
		matchers := []*labels.Matcher{
			m,
		}

		got := MatchersStringer(matchers).String()

		assert.Equal(t, `name="value"`, got)
	})

	t.Run("Two matchers", func(t *testing.T) {
		m1, err := labels.NewMatcher(labels.MatchEqual, "name1", "value1")
		require.NoError(t, err)
		m2, err := labels.NewMatcher(labels.MatchEqual, "name2", "value2")
		require.NoError(t, err)
		matchers := []*labels.Matcher{
			m1, m2,
		}

		got := MatchersStringer(matchers).String()

		assert.Equal(t, `name1="value1",name2="value2"`, got)
	})
}
