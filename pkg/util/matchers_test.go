// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiMatchersStringer(t *testing.T) {
	t.Run("One nested matcher", func(t *testing.T) {
		m1, err := labels.NewMatcher(labels.MatchEqual, "env", "dev")
		require.NoError(t, err)
		m2, err := labels.NewMatcher(labels.MatchEqual, "class", "public")
		require.NoError(t, err)

		matchers := [][]*labels.Matcher{
			{m1, m2},
		}

		s := MultiMatchersStringer(matchers).String()
		assert.Equal(t, `{env="dev",class="public"}`, s)
	})

	t.Run("Two nested matchers", func(t *testing.T) {
		m1, err := labels.NewMatcher(labels.MatchEqual, "env", "dev")
		require.NoError(t, err)
		m2, err := labels.NewMatcher(labels.MatchEqual, "class", "public")
		require.NoError(t, err)

		m3, err := labels.NewMatcher(labels.MatchEqual, "env", "prd")
		require.NoError(t, err)
		m4, err := labels.NewMatcher(labels.MatchEqual, "user", "admin")
		require.NoError(t, err)

		matchers := [][]*labels.Matcher{
			{m1, m2},
			{m3, m4},
		}

		s := MultiMatchersStringer(matchers).String()
		assert.Equal(t, `{env="dev",class="public"},{env="prd",user="admin"}`, s)
	})
}

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

func TestMergeMatchers(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    []*labels.Matcher
		expected []*labels.Matcher
	}{
		{
			name:     "empty input",
			input:    nil,
			expected: nil,
		},
		{
			name: "one equals matcher",
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "x"),
			},
			expected: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "x"),
			},
		},
		{
			name: "one not equals matcher",
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, "foo", "x"),
			},
			expected: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, "foo", "x"),
			},
		},
		{
			name: "one not regexp matcher",
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotRegexp, "foo", "x.+"),
			},
			expected: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotRegexp, "foo", "x.+"),
			},
		},
		{
			name: "matchers for different names",
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "x"),
				labels.MustNewMatcher(labels.MatchEqual, "bar", "1"),
			},
			expected: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "bar", "1"),
				labels.MustNewMatcher(labels.MatchEqual, "foo", "x"),
			},
		},
		{
			name: "matchers for same label that are not merged",
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "foo", "x"),
				labels.MustNewMatcher(labels.MatchNotEqual, "foo", "y"),
				labels.MustNewMatcher(labels.MatchEqual, "bar", "1"),
			},
			expected: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "bar", "1"),
				labels.MustNewMatcher(labels.MatchEqual, "foo", "x"),
				labels.MustNewMatcher(labels.MatchNotEqual, "foo", "y"),
			},
		},
		{
			name: "merged not equal matchers for same label",
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, "foo", "x"),
				labels.MustNewMatcher(labels.MatchNotEqual, "foo", "y"),
				labels.MustNewMatcher(labels.MatchEqual, "bar", "1"),
			},
			expected: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "bar", "1"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "foo", "x|y"),
			},
		},
		{
			name: "merged escaped not equal matchers for two different labels",
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "moo", ""),
				labels.MustNewMatcher(labels.MatchNotEqual, "foo", "x"),
				labels.MustNewMatcher(labels.MatchNotEqual, "foo", "y"),
				labels.MustNewMatcher(labels.MatchEqual, "bar", "1"),
				labels.MustNewMatcher(labels.MatchNotEqual, "baz", ".*"),
				labels.MustNewMatcher(labels.MatchNotEqual, "baz", "[1-2]{2}"),
			},
			expected: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "bar", "1"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "baz", `\.\*|\[1-2\]\{2\}`),
				labels.MustNewMatcher(labels.MatchNotRegexp, "foo", "x|y"),
				labels.MustNewMatcher(labels.MatchEqual, "moo", ""),
			},
		},
		{
			name: "merged not regexp matchers for same label",
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotRegexp, "foo", "x.+"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "foo", ".+y"),
				labels.MustNewMatcher(labels.MatchEqual, "bar", "1"),
			},
			expected: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "bar", "1"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "foo", ".+y|x.+"),
			},
		},
		{
			name: "merged not regexp with not equal",
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotRegexp, "foo", "x.+"),
				labels.MustNewMatcher(labels.MatchNotEqual, "foo", "|"),
				labels.MustNewMatcher(labels.MatchEqual, "bar", "1"),
			},
			expected: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "bar", "1"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "foo", `\||x.+`),
			},
		},
		{
			name: "merged not regexp with not equal slash",
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotRegexp, "foo", "x.+"),
				labels.MustNewMatcher(labels.MatchNotEqual, "foo", `\`),
				labels.MustNewMatcher(labels.MatchEqual, "bar", "1"),
			},
			expected: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "bar", "1"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "foo", `\\|x.+`),
			},
		},
		{
			name: "merged not empty label with regexp not empty label deduplicates condition",
			input: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotRegexp, "foo", ".+"),
				labels.MustNewMatcher(labels.MatchNotEqual, "foo", ""),
				labels.MustNewMatcher(labels.MatchEqual, "bar", "1"),
			},
			expected: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "bar", "1"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "foo", `.+`),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := MergeMatchers(tc.input, log.NewNopLogger())
			// We use MatcherStringer to compare as its easier to debug the test than seeing slices of references to label matchers.
			require.Equal(t, MatchersStringer(tc.expected).String(), MatchersStringer(got).String())
		})
	}
}
