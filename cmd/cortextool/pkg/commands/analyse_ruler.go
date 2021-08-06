package commands

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql/parser"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/cortex-tools/pkg/analyse"
	"github.com/grafana/cortex-tools/pkg/client"
	"github.com/grafana/cortex-tools/pkg/rules/rwrulefmt"
)

type RulerAnalyseCommand struct {
	ClientConfig client.Config
	cli          *client.CortexClient
	outputFile   string
}

func (cmd *RulerAnalyseCommand) run(k *kingpin.ParseContext) error {
	output := &analyse.MetricsInRuler{}
	output.OverallMetrics = make(map[string]struct{})

	cli, err := client.New(cmd.ClientConfig)
	if err != nil {
		return err
	}

	cmd.cli = cli
	rules, err := cmd.cli.ListRules(context.Background(), "")
	if err != nil {
		log.Fatalf("unable to read rules from cortex, %v", err)
	}

	for ns := range rules {
		for _, rg := range rules[ns] {
			err := parseMetricsInRuleGroup(output, rg, ns)
			if err != nil {
				log.Fatalf("metrics parse error %v", err)
			}
		}
	}

	err = writeOutRuleMetrics(output, cmd.outputFile)
	if err != nil {
		return err
	}

	return nil
}

func parseMetricsInRuleGroup(mir *analyse.MetricsInRuler, group rwrulefmt.RuleGroup, ns string) error {
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

		parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
			if n, ok := node.(*parser.VectorSelector); ok {
				refMetrics[n.Name] = struct{}{}
			}

			return nil
		})
	}

	// remove defined recording rule metrics in same RG
	for ruleMetric := range ruleMetrics {
		delete(refMetrics, ruleMetric)
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
	sort.Strings(metricsInGroup)

	for _, err := range parseErrors {
		parseErrs = append(parseErrs, err.Error())
	}

	mir.RuleGroups = append(mir.RuleGroups, analyse.RuleGroupMetrics{
		Namespace:   ns,
		GroupName:   group.Name,
		Metrics:     metricsInGroup,
		ParseErrors: parseErrs,
	})

	return nil
}

func writeOutRuleMetrics(mir *analyse.MetricsInRuler, outputFile string) error {
	var metricsUsed []string
	for metric := range mir.OverallMetrics {
		metricsUsed = append(metricsUsed, metric)
	}
	sort.Strings(metricsUsed)

	mir.MetricsUsed = metricsUsed
	out, err := json.MarshalIndent(mir, "", "  ")
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(outputFile, out, os.FileMode(int(0666))); err != nil {
		return err
	}

	return nil
}
