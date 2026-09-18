// SPDX-License-Identifier: AGPL-3.0-only

package searchexpr

import (
	"fmt"

	"github.com/prometheus/prometheus/storage"
)

// TermFilterFactory constructs a leaf matcher for one expression term.
// Compile owns boolean composition and reports whether the leaf is under an
// effective NOT; the caller owns the matching and scoring behavior for each
// polarity.
type TermFilterFactory func(term string, negated bool) (storage.Filter, error)

// Compile is the safe direct entry point for an expression AST: it validates
// the AST and converts it into a Prometheus storage.Filter. Callers that have
// already validated an immutable AST may use CompileValidated.
//
// Negation affects acceptance but does not contribute a relevance score.
// Compile pushes negation to term leaves while it walks the AST, applying
// double-negation elimination and De Morgan's laws. This preserves positive
// scores for expressions such as NOT NOT foo and foo AND NOT deprecated.
// The returned filter inherits the concurrency guarantees of the filters
// returned by newTermFilter.
func Compile(expr Expr, newTermFilter TermFilterFactory) (storage.Filter, error) {
	if expr == nil {
		return nil, fmt.Errorf("search expression: cannot compile a nil expression")
	}
	if newTermFilter == nil {
		return nil, fmt.Errorf("search expression: term filter factory is nil")
	}
	if err := Validate(expr); err != nil {
		return nil, err
	}
	return CompileValidated(expr, newTermFilter)
}

// CompileValidated converts an expression that has already passed Validate
// into a Prometheus storage.Filter without walking the AST a second time. It
// is intended for callers that parse and validate once before building
// request-local filters. The caller must have called Validate; this function
// deliberately performs no depth or resource guard. The returned filter
// inherits the concurrency guarantees of the filters returned by
// newTermFilter.
func CompileValidated(expr Expr, newTermFilter TermFilterFactory) (storage.Filter, error) {
	if expr == nil {
		return nil, fmt.Errorf("search expression: cannot compile a nil expression")
	}
	if newTermFilter == nil {
		return nil, fmt.Errorf("search expression: term filter factory is nil")
	}
	root, err := compileEvaluator(expr, false, newTermFilter)
	if err != nil {
		return nil, err
	}
	return &compiledFilter{root: root}, nil
}

// Validate checks that an expression is safe to use for search. Every
// accepting path must require a positive term, so exclusion-only expressions
// cannot become near-match-all scans with no meaningful relevance anchor.
// Validate also bounds ASTs constructed directly instead of through Parse.
func Validate(expr Expr) error {
	if expr == nil {
		return fmt.Errorf("search expression: cannot validate a nil expression")
	}
	anchored, err := validateExpression(expr, false, 1, new(int))
	if err != nil {
		return err
	}
	if !anchored {
		return fmt.Errorf("search expression: every accepting path must require a positive term")
	}
	return nil
}

type evalResult struct {
	accepted bool
	score    float64
	scored   bool
}

type evaluator interface {
	evaluate(value string) evalResult
}

type compiledFilter struct {
	root evaluator
}

var _ storage.Filter = (*compiledFilter)(nil)

func (f *compiledFilter) Accept(value string) (bool, float64) {
	result := f.root.evaluate(value)
	if !result.accepted {
		return false, 0
	}
	if !result.scored {
		return true, 0
	}
	return true, result.score
}

type termEvaluator struct {
	filter  storage.Filter
	negated bool
}

func (e *termEvaluator) evaluate(value string) evalResult {
	accepted, score := e.filter.Accept(value)
	if e.negated {
		return evalResult{accepted: !accepted}
	}
	if !accepted {
		return evalResult{}
	}
	return evalResult{accepted: true, score: score, scored: true}
}

type andEvaluator struct {
	left  evaluator
	right evaluator
}

func (e *andEvaluator) evaluate(value string) evalResult {
	left := e.left.evaluate(value)
	if !left.accepted {
		return evalResult{}
	}
	right := e.right.evaluate(value)
	if !right.accepted {
		return evalResult{}
	}
	// An AND is only as relevant as its weakest required positive term. Using
	// the lower score prevents a partial match from ranking above that term.
	return combineScores(left, right, lowerScore)
}

type orEvaluator struct {
	left  evaluator
	right evaluator
}

func (e *orEvaluator) evaluate(value string) evalResult {
	left := e.left.evaluate(value)
	if left.accepted && left.scored && left.score >= 1 {
		return left
	}
	right := e.right.evaluate(value)

	switch {
	case !left.accepted:
		return right
	case !right.accepted:
		return left
	default:
		return combineScores(left, right, higherScore)
	}
}

func lowerScore(left, right float64) float64 {
	if left < right {
		return left
	}
	return right
}

func higherScore(left, right float64) float64 {
	if left > right {
		return left
	}
	return right
}

func combineScores(left, right evalResult, combine func(float64, float64) float64) evalResult {
	switch {
	case left.scored && right.scored:
		return evalResult{accepted: true, score: combine(left.score, right.score), scored: true}
	case left.scored:
		return evalResult{accepted: true, score: left.score, scored: true}
	case right.scored:
		return evalResult{accepted: true, score: right.score, scored: true}
	default:
		return evalResult{accepted: true}
	}
}

// validateExpression checks ASTs supplied directly to Compile as well as ASTs
// produced by Parse. It also enforces the search-specific positive-anchor
// rule: every path through an OR must require at least one positive term.
// This prevents exclusion-only branches from turning a search into a
// near-match-all scan with no meaningful relevance score.
func validateExpression(expr Expr, negated bool, depth int, termCount *int) (bool, error) {
	if depth > maxCompiledExpressionDepth {
		return false, fmt.Errorf("search expression: AST depth exceeds maximum of %d", maxCompiledExpressionDepth)
	}
	validateChildren := func(left, right Expr) (bool, bool, error) {
		return mapChildren(left, right, func(expr Expr) (bool, error) {
			return validateExpression(expr, negated, depth+1, termCount)
		})
	}

	switch expr := expr.(type) {
	case Term:
		if expr.Value == "" {
			return false, fmt.Errorf("search expression: cannot compile an empty term")
		}
		(*termCount)++
		if *termCount > maxExpressionTerms {
			return false, fmt.Errorf("search expression: term count exceeds maximum of %d", maxExpressionTerms)
		}
		return !negated, nil

	case And:
		left, right, err := validateChildren(expr.Left, expr.Right)
		if err != nil {
			return false, err
		}
		if negated {
			return left && right, nil
		}
		return left || right, nil

	case Or:
		left, right, err := validateChildren(expr.Left, expr.Right)
		if err != nil {
			return false, err
		}
		if negated {
			return left || right, nil
		}
		return left && right, nil

	case Not:
		if expr.Expr == nil {
			return false, fmt.Errorf("search expression: cannot compile NOT with a nil operand")
		}
		return validateExpression(expr.Expr, !negated, depth+1, termCount)

	default:
		return false, fmt.Errorf("search expression: cannot compile node %T", expr)
	}
}

func compileEvaluator(expr Expr, negated bool, newTermFilter TermFilterFactory) (evaluator, error) {
	compileChildren := func(left, right Expr) (evaluator, evaluator, error) {
		return mapChildren(left, right, func(expr Expr) (evaluator, error) {
			return compileEvaluator(expr, negated, newTermFilter)
		})
	}

	switch expr := expr.(type) {
	case Term:
		if expr.Value == "" {
			return nil, fmt.Errorf("search expression: cannot compile an empty term")
		}
		filter, err := newTermFilter(expr.Value, negated)
		if err != nil {
			return nil, fmt.Errorf("search expression: compile term %q: %w", expr.Value, err)
		}
		if filter == nil {
			return nil, fmt.Errorf("search expression: term filter factory returned nil for %q", expr.Value)
		}
		return &termEvaluator{filter: filter, negated: negated}, nil

	case And:
		left, right, err := compileChildren(expr.Left, expr.Right)
		if err != nil {
			return nil, err
		}
		if negated {
			return &orEvaluator{left: left, right: right}, nil
		}
		return &andEvaluator{left: left, right: right}, nil

	case Or:
		left, right, err := compileChildren(expr.Left, expr.Right)
		if err != nil {
			return nil, err
		}
		if negated {
			return &andEvaluator{left: left, right: right}, nil
		}
		return &orEvaluator{left: left, right: right}, nil

	case Not:
		if expr.Expr == nil {
			return nil, fmt.Errorf("search expression: cannot compile NOT with a nil operand")
		}
		return compileEvaluator(expr.Expr, !negated, newTermFilter)

	default:
		return nil, fmt.Errorf("search expression: cannot compile node %T", expr)
	}
}

func mapChildren[T any](leftExpr, rightExpr Expr, mapChild func(Expr) (T, error)) (T, T, error) {
	var zero T
	if leftExpr == nil || rightExpr == nil {
		return zero, zero, fmt.Errorf("search expression: cannot compile a binary expression with a nil operand")
	}
	left, err := mapChild(leftExpr)
	if err != nil {
		return zero, zero, err
	}
	right, err := mapChild(rightExpr)
	if err != nil {
		return zero, zero, err
	}
	return left, right, nil
}
