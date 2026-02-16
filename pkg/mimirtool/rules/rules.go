// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/rules/rules.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package rules

import (
	"fmt"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/mimirtool/config"
	"github.com/grafana/mimir/pkg/mimirtool/rules/rwrulefmt"
)

// RuleNamespace is used to parse a slightly modified prometheus
// rule file format, if no namespace is set, the default namespace
// is used. Namespace is functionally the same as a file name.
type RuleNamespace struct {
	// Namespace field only exists for setting namespace in namespace body instead of file name
	Namespace string `yaml:"namespace,omitempty"`
	Filepath  string `yaml:"-"`

	Groups []rwrulefmt.RuleGroup `yaml:"groups"`
}

// LintExpressions runs the `expr` from a rule through the PromQL or LogQL parser and
// compares its output. If it differs from the parser, it uses the parser's instead.
func (r RuleNamespace) LintExpressions(backend string, logger log.Logger) (int, int, error) {
	var parseFn func(string) (fmt.Stringer, error)
	var queryLanguage string

	switch backend {
	case MimirBackend:
		queryLanguage = "PromQL"
		p := config.CreateParser()
		parseFn = func(s string) (fmt.Stringer, error) {
			return p.ParseExpr(s)
		}
	default:
		return 0, 0, errInvalidBackend
	}

	// `count` represents the number of rules we evalated.
	// `mod` represents the number of rules linted.
	var count, mod int
	for i, group := range r.Groups {
		for j, rule := range group.Rules {
			level.Debug(logger).Log("msg", "linting expression", "language", queryLanguage, "rule", getRuleName(rule))
			exp, err := parseFn(rule.Expr)
			if err != nil {
				return count, mod, err
			}

			count++
			if rule.Expr != exp.String() {
				level.Debug(logger).Log("msg", "expression differs", "rule", getRuleName(rule), "currentExpr", rule.Expr, "afterExpr", exp.String())

				mod++
				r.Groups[i].Rules[j].Expr = exp.String()
			}
		}
	}

	return count, mod, nil
}

// CheckRecordingRules checks that recording rules have at least one colon in their name, this is based
// on the recording rules best practices here: https://prometheus.io/docs/practices/rules/
// Returns the number of rules that don't match the requirements.
func (r RuleNamespace) CheckRecordingRules(strict bool, logger log.Logger) int {
	var name string
	var count int
	reqChunks := 2
	if strict {
		reqChunks = 3
	}
	for _, group := range r.Groups {
		for _, rule := range group.Rules {
			// Assume if there is a rule.Record that this is a recording rule.
			if rule.Record == "" {
				continue
			}
			name = rule.Record
			level.Debug(logger).Log("msg", "linting recording rule name", "rule", name)
			chunks := strings.Split(name, ":")
			if len(chunks) < reqChunks {
				count++
				level.Error(logger).Log("msg", "bad recording rule name", "rule", getRuleName(rule), "ruleGroup", group.Name, "file", r.Filepath, "error", "recording rule name does not match level:metric:operation format, must contain at least one colon")
			}
		}
	}
	return count
}

// AggregateBy modifies the aggregation rules in groups to include a given Label.
// If the applyTo function is provided, the aggregation is applied only to rules
// for which the applyTo function returns true.
func (r RuleNamespace) AggregateBy(label string, applyTo func(group rwrulefmt.RuleGroup, rule rulefmt.Rule) bool, logger log.Logger) (int, int, error) {
	// `count` represents the number of rules we evaluated.
	// `mod` represents the number of rules we modified - a modification can either be a lint or adding the
	// label in the aggregation.
	var count, mod int

	p := config.CreateParser()

	for i, group := range r.Groups {
		for j, rule := range group.Rules {
			// Skip it if the applyTo function returns false.
			if applyTo != nil && !applyTo(group, rule) {
				level.Debug(logger).Log("msg", "skipped", "group", group.Name, "rule", getRuleName(rule))

				count++
				continue
			}

			level.Debug(logger).Log("msg", "evaluating...", "rule", getRuleName(rule))
			exp, err := p.ParseExpr(rule.Expr)
			if err != nil {
				return count, mod, err
			}

			count++
			// Given inspect will help us traverse every node in the AST, Let's create the
			// function that will modify the labels.
			f := exprNodeInspectorFunc(rule, label, logger)
			parser.Inspect(exp, f)

			// Only modify the ones that actually changed.
			if rule.Expr != exp.String() {
				level.Debug(logger).Log("msg", "expression differs", "rule", getRuleName(rule), "currentExpr", rule.Expr, "afterExpr", exp.String())
				mod++
				r.Groups[i].Rules[j].Expr = exp.String()
			}
		}
	}

	return count, mod, nil
}

// exprNodeInspectorFunc returns a PromQL inspector.
// It modifies most PromQL expressions to include a given label.
func exprNodeInspectorFunc(rule rulefmt.Rule, label string, logger log.Logger) func(node parser.Node, path []parser.Node) error {
	return func(node parser.Node, _ []parser.Node) error {
		var err error
		switch n := node.(type) {
		case *parser.AggregateExpr:
			err = prepareAggregationExpr(n, label, getRuleName(rule), logger)
		case *parser.BinaryExpr:
			err = prepareBinaryExpr(n, label, getRuleName(rule), logger)
		default:
			return err
		}

		return err
	}
}

func prepareAggregationExpr(e *parser.AggregateExpr, label string, ruleName string, logger log.Logger) error {
	// If the aggregation is about dropping labels (e.g. without), we don't want to modify
	// this expression. Omission as long as it is not the cluster label will include it.
	// TODO: We probably want to check whenever the label we're trying to include is included in the omission.
	if e.Without {
		return nil
	}

	for _, lbl := range e.Grouping {
		// It already has the label we want to aggregate by.
		if lbl == label {
			return nil
		}
	}

	level.Debug(logger).Log("msg", "aggregation without label, adding", "label", label, "rule", ruleName, "lbls", strings.Join(e.Grouping, ", "))

	e.Grouping = append(e.Grouping, label)
	return nil
}

func prepareBinaryExpr(e *parser.BinaryExpr, label string, rule string, logger log.Logger) error {
	if e.VectorMatching == nil {
		return nil
	}

	if !e.VectorMatching.On {
		return nil
	}

	// Skip if the aggregation label is already present in the on() clause.
	for _, lbl := range e.VectorMatching.MatchingLabels {
		if lbl == label {
			return nil
		}
	}

	// Skip if the aggregation label is already present in the group_left/right() clause.
	for _, lbl := range e.VectorMatching.Include {
		if lbl == label {
			return nil
		}
	}

	level.Debug(logger).Log("msg", "binary expression without label, adding", "label", label, "rule", rule, "lbls", strings.Join(e.VectorMatching.MatchingLabels, ", "))

	e.VectorMatching.MatchingLabels = append(e.VectorMatching.MatchingLabels, label)
	return nil
}

// Validate each rule in the rule namespace is valid
func (r RuleNamespace) Validate(groupNodes []rulefmt.RuleGroupNode, scheme model.ValidationScheme) []error {
	set := map[string]struct{}{}
	var errs []error

	for i, g := range r.Groups {
		if g.Name == "" {
			errs = append(errs, fmt.Errorf("group name should not be empty"))
		}

		if _, ok := set[g.Name]; ok {
			errs = append(
				errs,
				fmt.Errorf("group name: %q is repeated in the same namespace", g.Name),
			)
		}

		set[g.Name] = struct{}{}

		errs = append(errs, ValidateRuleGroup(g, groupNodes[i], scheme)...)
	}

	return errs
}

// ValidateRuleGroup validates a rulegroup
func ValidateRuleGroup(g rwrulefmt.RuleGroup, node rulefmt.RuleGroupNode, scheme model.ValidationScheme) []error {
	var errs []error
	p := config.CreateParser()
	for i, r := range g.Rules {
		for _, err := range r.Validate(node.Rules[i], scheme, p) {
			var ruleName string
			if r.Alert != "" {
				ruleName = r.Alert
			} else {
				ruleName = r.Record
			}
			errs = append(errs, &rulefmt.Error{
				Group:    g.Name,
				Rule:     i,
				RuleName: ruleName,
				Err:      err,
			})
		}
	}

	return errs
}

func getRuleName(r rulefmt.Rule) string {
	if r.Record != "" {
		return r.Record
	}

	return r.Alert
}
