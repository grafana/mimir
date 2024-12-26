// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse_ruler.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	log "github.com/sirupsen/logrus"

	"github.com/grafana/mimir/pkg/mimirtool/analyze"
	"github.com/grafana/mimir/pkg/mimirtool/client"
)

type UnexpectedLabelsAnalyzeCommand struct {
	ClientConfig client.Config
	trace        bool
	outputFile   string
}

func (cmd *UnexpectedLabelsAnalyzeCommand) run(_ *kingpin.ParseContext) error {
	_, err := AnalyzeUnexpectedLabels(cmd.ClientConfig, cmd.trace)
	if err != nil {
		return err
	}
	return nil
}

func AnalyzeUnexpectedLabels(c client.Config, trace bool) (map[string]string, error) {
	tracef := func(string, ...interface{}) {}
	if trace {
		tracef = func(format string, args ...interface{}) { fmt.Fprintf(os.Stderr, "TRACE: "+format, args...) }
	}
	output := &analyze.MetricsInRuler{}
	output.OverallMetrics = make(map[string]struct{})

	cli, err := client.New(c)
	if err != nil {
		return nil, err
	}

	rules, err := cli.ListRules(context.Background(), "")
	if err != nil {
		return nil, errors.Wrap(err, "Unable to read rules from Grafana Mimir")
	}

	var queries []string
	for ns := range rules {
		for _, rg := range rules[ns] {
			for _, r := range rg.Rules {
				if r.Record.Value != "grafana_slo_sli_1d" {
					continue
				}
				sloUUID, ok := r.Labels["grafana_slo_uuid"]
				if !ok {
					continue
				}

				tracef("grafana_slo_uuid=%s", sloUUID)

				var parseErrors []error
				query := r.Expr.Value
				expr, err := parser.ParseExpr(query)
				if err != nil {
					parseErrors = append(parseErrors, errors.Wrapf(err, "query=%v", query))
					log.Debugln("msg", "promql parse error", "err", err, "query", query)
					continue
				}

				var vs *parser.VectorSelector
				matchers := map[string]*labels.Matcher{}
				parser.Inspect(expr, func(node parser.Node, parent []parser.Node) error {
					if vs != nil {
						return nil
					}

					if n, ok := node.(*parser.VectorSelector); ok {
						var grouping []string
						var grouped bool
						for _, p := range parent {
							switch p := p.(type) {
							case *parser.AggregateExpr:
								if len(p.Grouping) == 0 {
									// all labels are aggregated, so we can't see them on the rule.
									tracef("all agg")
									return nil
								}
								tracef("grouping: %s by (%s)", p.Op, strings.Join(p.Grouping, ","))
								if grouped {
									grouping = intersect(grouping, p.Grouping)
								} else {
									grouping = p.Grouping
									grouped = true
								}
							}
						}
						vs = n
						for _, m := range n.LabelMatchers {
							if m.Type != labels.MatchNotRegexp {
								continue
							}
							if grouped && !slices.Contains(grouping, m.Name) {
								tracef("%s not in grouping", m.Name)
								continue
							}
							matchers[m.String()] = m
						}
					}
					return nil
				})
				if vs == nil || len(matchers) == 0 {
					tracef("no vector selector\n\n")
					continue
				}
				fmt.Printf("\n")
				fmt.Printf("grafana_slo_uuid=%s\n", sloUUID)
				fmt.Printf("analyzed selector: %s\n----\n", vs)

				for _, m := range matchers {
					m.Type = labels.MatchRegexp
					q := &parser.VectorSelector{
						Name: r.Record.Value,
						LabelMatchers: []*labels.Matcher{
							labels.MustNewMatcher(labels.MatchEqual, "grafana_slo_uuid", sloUUID),
							m,
						},
					}
					query := fmt.Sprintf("present_over_time(%s[30m])", q)
					queries = append(queries, query)
					fmt.Printf("  %s\n", query)
				}
				fmt.Printf("\n")
			}
		}
	}
	fmt.Printf("ALL:\n======================\ncount by (grafana_slo_uuid) (\n%s\n)\n", strings.Join(queries, "\nor\n"))

	return map[string]string{}, nil
}

func intersect(a []string, b []string) []string {
	m := map[string]struct{}{}
	for _, s := range a {
		m[s] = struct{}{}
	}
	var out []string
	for _, s := range b {
		if _, ok := m[s]; ok {
			out = append(out, s)
		}
	}
	return out
}

func writeOutAny(anything any, outputFile string) error {
	out, err := json.MarshalIndent(anything, "", "  ")
	if err != nil {
		return err
	}
	if outputFile == "-" {
		_, err := os.Stdout.Write(out)
		return err
	}

	if err := os.WriteFile(outputFile, out, os.FileMode(int(0o666))); err != nil {
		return err
	}

	return nil
}
