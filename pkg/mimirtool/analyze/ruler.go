// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/analyse/ruler.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package analyze

import (
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/pkg/mimirtool/rules/rwrulefmt"
)

type MetricsInRuler struct {
	MetricsUsed    model.LabelValues   `json:"metricsUsed"`
	OverallMetrics map[string]struct{} `json:"-"`
	RuleGroups     []RuleGroupMetrics  `json:"ruleGroups"`
}

type RuleGroupMetrics struct {
	Namespace   string   `json:"namspace"`
	GroupName   string   `json:"name"`
	Metrics     []string `json:"metrics"`
	ParseErrors []string `json:"parse_errors"`
}

func ParseMetricsInRuleGroup(mir *MetricsInRuler, group rwrulefmt.RuleGroup, ns string) error {
	var (
		ruleMetrics = make(map[string]struct{})
		refMetrics  = make(map[string]struct{})
		parseErrors []error
	)

	for _, rule := range group.Rules {
		if rule.Record.Value != "" {
			ruleMetrics[rule.Record.Value] = struct{}{}
		}

		query := rule.Expr.Value
		expr, err := parser.ParseExpr(query)
		if err != nil {
			parseErrors = append(parseErrors, errors.Wrapf(err, "query=%v", query))
			log.Debugln("msg", "promql parse error", "err", err, "query", query)
			continue
		}

		parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
			if n, ok := node.(*parser.VectorSelector); ok {
				refMetrics[n.Name] = struct{}{}
			}

			return nil
		})
	}

	var metricsInGroup []string
	var parseErrs []string

	for metric := range refMetrics {
		if metric == "" {
			continue
		}
		metricsInGroup = append(metricsInGroup, metric)
		mir.OverallMetrics[metric] = struct{}{}
	}
	slices.Sort(metricsInGroup)

	for _, err := range parseErrors {
		parseErrs = append(parseErrs, err.Error())
	}

	mir.RuleGroups = append(mir.RuleGroups, RuleGroupMetrics{
		Namespace:   ns,
		GroupName:   group.Name,
		Metrics:     metricsInGroup,
		ParseErrors: parseErrs,
	})

	return nil
}
