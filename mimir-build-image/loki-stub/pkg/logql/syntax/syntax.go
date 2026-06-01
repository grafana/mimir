// SPDX-License-Identifier: AGPL-3.0-only
// Package syntax is a minimal stand-in for
// github.com/grafana/loki/v3/pkg/logql/syntax. It provides only the symbols
// referenced by github.com/grafana/dashboard-linter
// (lint/rule_target_logql_auto.go): the Expr type, ParseExpr and the handful of
// node types its Inspect helper type-switches on.
//
// dashboard-linter's only LogQL rule, "target-logql-auto-rule", checks that Loki
// targets use $__auto for range vectors instead of a fixed duration. It does so
// by parsing the query and walking the AST for LogRange nodes whose interval is
// not the auto sentinel, while independently checking the raw expression for the
// literal "$__auto". Reproducing just that behaviour lets us keep the lint rule
// working (Mimir's slow-queries dashboard does have Loki panels) without
// depending on the full Loki module and its enormous transitive dependency tree.
// See ../../../../go.mod for the replace directive that wires this in.
package syntax

import (
	"regexp"
	"time"
)

// Expr is the LogQL expression type. It is an alias for any: callers only
// type-switch on the concrete node types below, they never invoke methods.
type Expr = any

// rangeSelector matches a LogQL range selector such as "[5m]" or the expanded
// "$__auto" sentinel "[12345ms]". The duration is captured for ParseExpr.
var rangeSelector = regexp.MustCompile(`\[\s*([^\]]+?)\s*\]`)

// ParseExpr is a minimal LogQL parser: it returns a single LogRange node when
// the expression contains a range selector, with the selector's duration as the
// interval. That is everything dashboard-linter's Inspect walk looks at. The
// expanded "$__auto" sentinel (12345ms) parses to a duration equal to the rule's
// auto duration, so $__auto queries are not flagged; a fixed duration such as 5m
// is, exactly as the real parser would drive the rule. Durations the standard
// library cannot parse fall back to 0, which the rule treats as fixed.
func ParseExpr(expr string) (Expr, error) {
	m := rangeSelector.FindStringSubmatch(expr)
	if m == nil {
		return nil, nil
	}
	d, _ := time.ParseDuration(m[1])
	return &LogRange{Interval: d}, nil
}

// The node types below mirror the subset of loki/v3 logql/syntax that
// dashboard-linter's Inspect helper type-switches on. Only LogRange is ever
// instantiated (by ParseExpr); the rest exist so the type switch compiles.

type LogRange struct {
	Left     Expr
	Interval time.Duration
}

type BinOpExpr struct {
	SampleExpr Expr
	RHS        Expr
}

type RangeAggregationExpr struct {
	Left Expr
}

type VectorAggregationExpr struct {
	Left Expr
}

type LabelReplaceExpr struct {
	Left Expr
}

type PipelineExpr struct {
	Left        Expr
	MultiStages []Expr
}
