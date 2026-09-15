// SPDX-License-Identifier: AGPL-3.0-only

package searchexpr

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePrecedence(t *testing.T) {
	for _, test := range []struct {
		name  string
		input string
		want  Expr
	}{
		{
			name:  "term",
			input: "cortex",
			want:  Term{Value: "cortex"},
		},
		{
			name:  "AND binds tighter than OR",
			input: "cortex AND rule_evaluation_failures OR loki",
			want: Or{
				Left:  And{Left: Term{Value: "cortex"}, Right: Term{Value: "rule_evaluation_failures"}},
				Right: Term{Value: "loki"},
			},
		},
		{
			name:  "parentheses override precedence",
			input: "(cortex OR loki) AND rule_evaluation_failures",
			want: And{
				Left:  Or{Left: Term{Value: "cortex"}, Right: Term{Value: "loki"}},
				Right: Term{Value: "rule_evaluation_failures"},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertParse(t, test.input, test.want)
		})
	}
}

func TestParseOperatorsAndQuotedTerms(t *testing.T) {
	for _, test := range []struct {
		name  string
		input string
		want  Expr
	}{
		{
			name:  "operators are case insensitive",
			input: "cortex and loki Or envoy",
			want: Or{
				Left:  And{Left: Term{Value: "cortex"}, Right: Term{Value: "loki"}},
				Right: Term{Value: "envoy"},
			},
		},
		{
			name:  "chains AND left to right",
			input: "cortex AND loki AND envoy",
			want: And{
				Left:  And{Left: Term{Value: "cortex"}, Right: Term{Value: "loki"}},
				Right: Term{Value: "envoy"},
			},
		},
		{
			name:  "chains OR left to right",
			input: "cortex OR loki OR envoy",
			want: Or{
				Left:  Or{Left: Term{Value: "cortex"}, Right: Term{Value: "loki"}},
				Right: Term{Value: "envoy"},
			},
		},
		{
			name:  "quoted operator is a term",
			input: `"AND" OR "rule evaluation failures"`,
			want:  Or{Left: Term{Value: "AND"}, Right: Term{Value: "rule evaluation failures"}},
		},
		{
			name:  "quoted term supports escapes",
			input: `"cortex \"ruler\""`,
			want:  Term{Value: `cortex "ruler"`},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertParse(t, test.input, test.want)
		})
	}
}

func assertParse(t *testing.T, input string, want Expr) {
	t.Helper()
	got, err := Parse(input)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestParseRejectsInvalidExpressions(t *testing.T) {
	for _, input := range []string{
		"",
		"AND cortex",
		"cortex OR",
		"cortex rule_evaluation_failures",
		"(cortex OR loki",
		"(",
		"cortex AND )",
		"cortex)",
		`""`,
		`"unterminated`,
		"\"trailing escape\\",
	} {
		t.Run(input, func(t *testing.T) {
			_, err := Parse(input)
			require.Error(t, err)
			assert.True(t, strings.HasPrefix(err.Error(), "search expression:"))
		})
	}
}

func TestTokenDescribe(t *testing.T) {
	for _, test := range []struct {
		token token
		want  string
	}{
		{token: token{kind: tokenEOF}, want: "end of expression"},
		{token: token{kind: tokenTerm, value: "cortex"}, want: `term "cortex"`},
		{token: token{kind: tokenAnd}, want: "AND"},
		{token: token{kind: tokenOr}, want: "OR"},
		{token: token{kind: tokenLeftParen}, want: "("},
		{token: token{kind: tokenRightParen}, want: ")"},
		{token: token{kind: tokenKind(99)}, want: "token"},
	} {
		assert.Equal(t, test.want, test.token.describe())
	}
}

func TestExprNodesImplementExpr(t *testing.T) {
	Term{}.expr()
	And{}.expr()
	Or{}.expr()
}
