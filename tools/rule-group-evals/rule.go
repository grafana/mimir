package main

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
)

type Rule struct {
	RuleName              string  `json:"name"` // name of the series if type == recording
	Type                  string  `json:"type"` // "alerting" or "recording"
	EvaluationTimeSeconds float64 `json:"evaluationTime"`
	RuleQuery             string  `json:"query"`

	noDependentRules  bool
	noDependencyRules bool
}

func (r *Rule) Name() string {
	return r.RuleName
}

func (r *Rule) Labels() labels.Labels {
	panic("Rule.Labels() is not supported")
}

func (r *Rule) Eval(ctx context.Context, queryOffset time.Duration, evaluationTime time.Time, queryFunc rules.QueryFunc, externalURL *url.URL, limit int) (promql.Vector, error) {
	panic("Rule.Eval() is not supported")
}

func (r *Rule) String() string {
	panic("Rule.String() is not supported")
}

func (r *Rule) Query() parser.Expr {
	query, err := parser.ParseExpr(r.RuleQuery)
	if err != nil {
		panic(fmt.Sprintf("failed to parse query: %v", err))
	}

	return query
}

func (r *Rule) SetLastError(error) {
	panic("Rule.SetLastError() is not supported")
}

func (r *Rule) LastError() error {
	panic("Rule.LastError() is not supported")
}

func (r *Rule) SetHealth(rules.RuleHealth) {
	panic("Rule.SetHealth() is not supported")
}

func (r *Rule) Health() rules.RuleHealth {
	panic("Rule.Health() is not supported")
}

func (r *Rule) SetEvaluationDuration(time.Duration) {
	panic("Rule.SetEvaluationDuration() is not supported")
}

func (r *Rule) GetEvaluationDuration() time.Duration {
	return time.Duration(r.EvaluationTimeSeconds * float64(time.Second))
}

func (r *Rule) SetEvaluationTimestamp(time.Time) {
	panic("Rule.SetEvaluationTimestamp() is not supported")
}

func (r *Rule) GetEvaluationTimestamp() time.Time {
	panic("Rule.GetEvaluationTimestamp() is not supported")
}

func (r *Rule) SetNoDependentRules(v bool) {
	r.noDependentRules = v
}

func (r *Rule) NoDependentRules() bool {
	return r.noDependentRules
}

func (r *Rule) SetNoDependencyRules(v bool) {
	r.noDependencyRules = v
}

func (r *Rule) NoDependencyRules() bool {
	return r.noDependencyRules
}
