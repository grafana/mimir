package rules

import (
	"fmt"
	"strings"

	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/promql"
	log "github.com/sirupsen/logrus"
)

// RuleNamespace is used to parse a slightly modified prometheus
// rule file format, if no namespace is set, the default namespace
// is used. Namespace is functionally the same as a file name.
type RuleNamespace struct {
	// Namespace field only exists for setting namespace in namespace body instead of file name
	Namespace string `yaml:"namespace,omitempty"`
	Filepath  string `yaml:"-"`

	Groups []rulefmt.RuleGroup `yaml:"groups"`
}

// LintPromQLExpressions runs the `expr` from a rule through the PromQL parser and
// compares its output. if it differs from the parser, it uses the parser's instead.
func (r RuleNamespace) LintPromQLExpressions() (int, int, error) {
	// `count` represents the number of rules we evalated.
	// `mod` represents the number of rules linted.
	var count, mod int
	for i, group := range r.Groups {
		for j, rule := range group.Rules {
			log.WithFields(log.Fields{"rule": getRuleName(rule)}).Debugf("linting PromQL")
			exp, err := promql.ParseExpr(rule.Expr)
			if err != nil {
				return count, mod, err
			}

			count++
			if rule.Expr != exp.String() {
				log.WithFields(log.Fields{
					"rule":        getRuleName(rule),
					"currentExpr": rule.Expr,
					"afterExpr":   exp.String(),
				}).Debugf("expression differs")

				mod++
				r.Groups[i].Rules[j].Expr = exp.String()
			}
		}
	}

	return count, mod, nil
}

// AggregateBy modifies the aggregation rules in groups to include a given Label.
func (r RuleNamespace) AggregateBy(label string) (int, int, error) {
	// `count` represents the number of rules we evalated.
	// `mod` represents the number of rules we modified - a modification can either be a lint or adding the
	// label in the aggregation.
	var count, mod int

	for i, group := range r.Groups {
		for j, rule := range group.Rules {
			log.WithFields(log.Fields{"rule": getRuleName(rule)}).Debugf("evaluating...")
			exp, err := promql.ParseExpr(rule.Expr)
			if err != nil {
				return count, mod, err
			}

			count++
			// Given inspect will help us traverse every node in the AST, Let's create the
			// function that will modify the labels.
			f := exprNodeInspectorFunc(rule, label)
			promql.Inspect(exp, f)

			// Only modify the ones that actually changed.
			if rule.Expr != exp.String() {
				log.WithFields(log.Fields{
					"rule":        getRuleName(rule),
					"currentExpr": rule.Expr,
					"afterExpr":   exp.String(),
				}).Debugf("expression differs")
				mod++
				r.Groups[i].Rules[j].Expr = exp.String()
			}
		}
	}

	return count, mod, nil
}

// exprNodeInspectorFunc returns a PromQL inspector.
// It modifies most PromQL aggregations to include a given label.
func exprNodeInspectorFunc(rule rulefmt.Rule, label string) func(node promql.Node, path []promql.Node) error {
	return func(node promql.Node, path []promql.Node) error {
		aggregation, ok := node.(*promql.AggregateExpr)
		if !ok {
			return nil
		}

		// If the aggregation is about dropping labels (e.g. without), we don't want to modify
		// this expression. Omission as long as it is not the cluster label will include it.
		// TODO: We probably want to check whenever the label we're trying to include is included in the omission.
		if aggregation.Without {
			return nil
		}

		for _, lbl := range aggregation.Grouping {
			if lbl == label {
				return nil
			}
		}

		log.WithFields(
			log.Fields{"rule": getRuleName(rule), "lbls": strings.Join(aggregation.Grouping, ", ")},
		).Debugf("aggregation without '%s' label, adding.", label)

		aggregation.Grouping = append(aggregation.Grouping, label)
		return nil
	}
}

// Validate each rule in the rule namespace is valid
func (r RuleNamespace) Validate() []error {
	set := map[string]struct{}{}
	var errs []error

	for _, g := range r.Groups {
		if g.Name == "" {
			errs = append(errs, fmt.Errorf("Groupname should not be empty"))
		}

		if _, ok := set[g.Name]; ok {
			errs = append(
				errs,
				fmt.Errorf("groupname: \"%s\" is repeated in the same namespace", g.Name),
			)
		}

		set[g.Name] = struct{}{}

		errs = append(errs, ValidateRuleGroup(g)...)
	}

	return errs
}

// ValidateRuleGroup validates a rulegroup
func ValidateRuleGroup(g rulefmt.RuleGroup) []error {
	var errs []error
	for i, r := range g.Rules {
		for _, err := range r.Validate() {
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
