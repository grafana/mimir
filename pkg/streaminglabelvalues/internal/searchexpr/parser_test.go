// SPDX-License-Identifier: AGPL-3.0-only

package searchexpr

import (
	"strings"
	"testing"
	"unsafe"

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
		{
			name:  "NOT binds tighter than AND",
			input: "NOT cortex AND loki",
			want: And{
				Left:  Not{Expr: Term{Value: "cortex"}},
				Right: Term{Value: "loki"},
			},
		},
		{
			name:  "parenthesized NOT applies to the whole group",
			input: "NOT (cortex OR loki)",
			want: Not{Expr: Or{
				Left:  Term{Value: "cortex"},
				Right: Term{Value: "loki"},
			}},
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
			name:  "quoted keywords are terms",
			input: `"AND" AND "OR" OR "NOT"`,
			want: Or{
				Left:  And{Left: Term{Value: "AND"}, Right: Term{Value: "OR"}},
				Right: Term{Value: "NOT"},
			},
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

func TestParseResultDoesNotAliasInput(t *testing.T) {
	// Term values must own their memory. A term that points into the parser
	// input keeps the whole expression alive, and if the input ever references a
	// pooled request buffer it could outlive that buffer. Cover every path that
	// produces a Term value: a bare term, a quoted term, and a quoted term
	// containing an escape.
	input := strings.Join([]string{
		"cortex", "AND", `"quoted term"`, "OR", "NOT", `"esc\"aped"`, "OR", "deprecated",
	}, " ")
	expr, err := Parse(input)
	require.NoError(t, err)

	terms := collectTermValues(expr)
	require.Equal(t, []string{"cortex", "quoted term", `esc"aped`, "deprecated"}, terms)

	inputStart := uintptr(unsafe.Pointer(unsafe.StringData(input)))
	inputEnd := inputStart + uintptr(len(input))
	for _, term := range terms {
		termStart := uintptr(unsafe.Pointer(unsafe.StringData(term)))
		assert.False(t, termStart >= inputStart && termStart < inputEnd, "term %q aliases the parser input", term)
	}
}

// collectTermValues returns every Term value in expr, left to right.
func collectTermValues(expr Expr) []string {
	switch expr := expr.(type) {
	case Term:
		return []string{expr.Value}
	case And:
		return append(collectTermValues(expr.Left), collectTermValues(expr.Right)...)
	case Or:
		return append(collectTermValues(expr.Left), collectTermValues(expr.Right)...)
	case Not:
		return collectTermValues(expr.Expr)
	default:
		return nil
	}
}

func assertParse(t *testing.T, input string, want Expr) {
	t.Helper()
	got, err := Parse(input)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestParseRejectsInvalidExpressions(t *testing.T) {
	for _, test := range []struct {
		name  string
		input string
		want  string
	}{
		{name: "empty expression", input: "", want: "search expression: expected term or (, got end of expression at position 0"},
		{name: "leading AND", input: "AND cortex", want: "search expression: expected term or (, got AND at position 0"},
		{name: "leading OR", input: "OR cortex", want: "search expression: expected term or (, got OR at position 0"},
		{name: "bare NOT", input: "NOT", want: "search expression: expected term or (, got end of expression at position 3"},
		{name: "trailing AND", input: "cortex AND", want: "search expression: expected term or (, got end of expression at position 10"},
		{name: "trailing OR", input: "cortex OR", want: "search expression: expected term or (, got end of expression at position 9"},
		{name: "trailing NOT", input: "cortex AND NOT", want: "search expression: expected term or (, got end of expression at position 14"},
		{name: "repeated AND", input: "cortex AND AND loki", want: "search expression: expected term or (, got AND at position 11"},
		{name: "repeated OR", input: "cortex OR OR loki", want: "search expression: expected term or (, got OR at position 10"},
		{name: "NOT before close parenthesis", input: "NOT )", want: "search expression: expected term or (, got ) at position 4"},
		{name: "implicit adjacent terms", input: "cortex rule_evaluation_failures", want: `search expression: unexpected term "rule_evaluation_failures" at position 7`},
		{name: "implicit term before group", input: "cortex (loki OR envoy)", want: "search expression: unexpected ( at position 7"},
		{name: "empty group", input: "()", want: "search expression: expected term or (, got ) at position 1"},
		{name: "whitespace-only group", input: "(  )", want: "search expression: expected term or (, got ) at position 3"},
		{name: "missing closing parenthesis", input: "(cortex OR loki", want: "search expression: expected ), got end of expression at position 15"},
		{name: "missing group operand", input: "(", want: "search expression: expected term or (, got end of expression at position 1"},
		{name: "operator before close parenthesis", input: "cortex AND )", want: "search expression: expected term or (, got ) at position 11"},
		{name: "extra closing parenthesis", input: "cortex)", want: "search expression: unexpected ) at position 6"},
		{name: "operator in nested group", input: "cortex AND (loki OR)", want: "search expression: expected term or (, got ) at position 19"},
		{name: "adjacent quoted terms", input: `"cortex""loki"`, want: `search expression: unexpected term "loki" at position 8`},
		{name: "empty quoted term", input: `""`, want: "search expression: empty quoted term at position 0"},
		{name: "unterminated quoted term", input: `"unterminated`, want: "search expression: unterminated quoted term at position 0"},
		{name: "trailing quote escape", input: "\"trailing escape\\", want: "search expression: unterminated quoted term at position 0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse(test.input)
			require.EqualError(t, err, test.want)
		})
	}
}

func TestParseReportsOffendingTokenPosition(t *testing.T) {
	for _, test := range []struct {
		input string
		want  string
	}{
		{input: "AND foo", want: "search expression: expected term or (, got AND at position 0"},
		{input: "OR foo", want: "search expression: expected term or (, got OR at position 0"},
		{input: ") foo", want: "search expression: expected term or (, got ) at position 0"},
	} {
		t.Run(test.input, func(t *testing.T) {
			_, err := Parse(test.input)
			require.EqualError(t, err, test.want)
		})
	}
}

func TestParseRejectsQuoteInsideBareTerm(t *testing.T) {
	for _, test := range []struct {
		input string
		want  string
	}{
		{input: `NOT"foo"`, want: "search expression: unexpected quote at position 3"},
		{input: `a"`, want: "search expression: unexpected quote at position 1"},
		{input: `foo"bar baz"`, want: "search expression: unexpected quote at position 3"},
	} {
		t.Run(test.input, func(t *testing.T) {
			_, err := Parse(test.input)
			require.EqualError(t, err, test.want)
		})
	}
}

func TestTokenizeBoundsCapacityHint(t *testing.T) {
	tokens, err := tokenize(strings.Repeat(" ", maxExpressionBytes))
	require.NoError(t, err)
	require.Len(t, tokens, 1)
	assert.LessOrEqual(t, cap(tokens), 2*maxExpressionTerms+2)
}

func TestParseResourceLimits(t *testing.T) {
	t.Run("expression bytes", func(t *testing.T) {
		_, err := Parse("a" + strings.Repeat(" ", maxExpressionBytes))
		require.EqualError(t, err, "search expression: length 4097 exceeds maximum of 4096 bytes")
	})

	t.Run("term leaves", func(t *testing.T) {
		terms := make([]string, maxExpressionTerms+1)
		for i := range terms {
			terms[i] = "term"
		}
		_, err := Parse(strings.Join(terms, " AND "))
		require.EqualError(t, err, "search expression: term count exceeds maximum of 32")
	})

	t.Run("quoted term leaves", func(t *testing.T) {
		terms := make([]string, maxExpressionTerms+1)
		for i := range terms {
			terms[i] = `"term"`
		}
		_, err := Parse(strings.Join(terms, " AND "))
		require.EqualError(t, err, "search expression: term count exceeds maximum of 32")
	})

	for _, test := range []struct {
		name  string
		input string
	}{
		{name: "parentheses", input: strings.Repeat("(", maxExpressionNestingDepth+1) + "foo" + strings.Repeat(")", maxExpressionNestingDepth+1)},
		{name: "NOT", input: strings.Repeat("NOT ", maxExpressionNestingDepth+1) + "foo"},
		{name: "mixed", input: strings.Repeat("NOT (", maxExpressionNestingDepth/2+1) + "foo" + strings.Repeat(")", maxExpressionNestingDepth/2+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse(test.input)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "nesting depth exceeds maximum of 16")
		})
	}
}

func TestParseAcceptsResourceLimitBoundaries(t *testing.T) {
	_, err := Parse("foo" + strings.Repeat(" ", maxExpressionBytes-len("foo")))
	require.NoError(t, err)

	terms := make([]string, maxExpressionTerms)
	for i := range terms {
		terms[i] = "term"
	}
	_, err = Parse(strings.Join(terms, " AND "))
	require.NoError(t, err)

	_, err = Parse(strings.Repeat("(", maxExpressionNestingDepth) + "foo" + strings.Repeat(")", maxExpressionNestingDepth))
	require.NoError(t, err)

	_, err = Parse(strings.Repeat("NOT ", maxExpressionNestingDepth) + "foo")
	require.NoError(t, err)

	// The maximum term count and nesting depth may be combined. This is the
	// deepest AST Parse can legally produce and documents the derivation of
	// maxCompiledExpressionDepth.
	deepTerms := make([]string, maxExpressionTerms)
	for i := range deepTerms {
		deepTerms[i] = "term"
	}
	expr, err := Parse(strings.Repeat("NOT ", maxExpressionNestingDepth) + strings.Join(deepTerms, " AND "))
	require.NoError(t, err)
	require.NoError(t, Validate(expr))
}

func TestTokenDescribe(t *testing.T) {
	for _, test := range []struct {
		token token
		want  string
	}{
		{token: token{kind: tokenEOF}, want: "end of expression"},
		{token: token{kind: tokenTerm, value: "cortex"}, want: `term "cortex"`},
		{token: token{kind: tokenNot}, want: "NOT"},
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
	Not{}.expr()
}

func FuzzParse(f *testing.F) {
	for _, input := range []string{
		"",
		"cortex",
		"cortex AND rule_evaluation_failures OR loki",
		"loki AND NOT cortex",
		"(cortex OR loki) AND rule_evaluation_failures",
		`"AND" AND "OR" OR "NOT"`,
		`"unterminated`,
		"cortex AND )",
		"\x00",
		"\xff",
		"((((((cortex))))))",
		strings.Repeat("NOT ", maxExpressionNestingDepth+1) + "cortex",
	} {
		f.Add(input)
	}

	f.Fuzz(func(t *testing.T, input string) {
		expr, err := Parse(input)
		if err != nil {
			return
		}
		require.NotNil(t, expr)
		assertWellFormedExpr(t, expr)
	})
}

func assertWellFormedExpr(t *testing.T, expr Expr) {
	t.Helper()
	switch expr := expr.(type) {
	case Term:
		assert.NotEmpty(t, expr.Value)
	case And:
		require.NotNil(t, expr.Left)
		require.NotNil(t, expr.Right)
		assertWellFormedExpr(t, expr.Left)
		assertWellFormedExpr(t, expr.Right)
	case Or:
		require.NotNil(t, expr.Left)
		require.NotNil(t, expr.Right)
		assertWellFormedExpr(t, expr.Left)
		assertWellFormedExpr(t, expr.Right)
	case Not:
		require.NotNil(t, expr.Expr)
		assertWellFormedExpr(t, expr.Expr)
	default:
		t.Fatalf("unexpected expression type %T", expr)
	}
}
