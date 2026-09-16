// SPDX-License-Identifier: AGPL-3.0-only

package searchexpr

import "fmt"

// Filter is the execution contract produced by Compile. It deliberately
// matches Prometheus's storage.Filter without importing the storage package.
type Filter interface {
	Accept(value string) (accepted bool, score float64)
}

// TermFilterFactory constructs the existing leaf matcher for one expression
// term. Compile owns boolean composition; the caller owns term-matching and
// scoring behavior.
type TermFilterFactory func(term string) (Filter, error)

// Compile converts an expression AST into an executable filter.
//
// Negation affects acceptance but does not contribute a relevance score.
// Compile pushes negation to term leaves while it walks the AST, applying
// double-negation elimination and De Morgan's laws. This preserves positive
// scores for expressions such as NOT NOT foo and foo AND NOT deprecated.
func Compile(expr Expr, newTermFilter TermFilterFactory) (Filter, error) {
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
	filter  Filter
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

func compileEvaluator(expr Expr, negated bool, newTermFilter TermFilterFactory) (evaluator, error) {
	switch expr := expr.(type) {
	case Term:
		if expr.Value == "" {
			return nil, fmt.Errorf("search expression: cannot compile an empty term")
		}
		filter, err := newTermFilter(expr.Value)
		if err != nil {
			return nil, fmt.Errorf("search expression: compile term %q: %w", expr.Value, err)
		}
		if filter == nil {
			return nil, fmt.Errorf("search expression: term filter factory returned nil for %q", expr.Value)
		}
		return &termEvaluator{filter: filter, negated: negated}, nil

	case And:
		left, right, err := compileChildren(expr.Left, expr.Right, negated, newTermFilter)
		if err != nil {
			return nil, err
		}
		if negated {
			return &orEvaluator{left: left, right: right}, nil
		}
		return &andEvaluator{left: left, right: right}, nil

	case Or:
		left, right, err := compileChildren(expr.Left, expr.Right, negated, newTermFilter)
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

func compileChildren(leftExpr, rightExpr Expr, negated bool, newTermFilter TermFilterFactory) (evaluator, evaluator, error) {
	if leftExpr == nil || rightExpr == nil {
		return nil, nil, fmt.Errorf("search expression: cannot compile a binary expression with a nil operand")
	}
	left, err := compileEvaluator(leftExpr, negated, newTermFilter)
	if err != nil {
		return nil, nil, err
	}
	right, err := compileEvaluator(rightExpr, negated, newTermFilter)
	if err != nil {
		return nil, nil, err
	}
	return left, right, nil
}
