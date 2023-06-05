// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse_ruler.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"encoding/json"
	"os"
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/pkg/mimirtool/analyze"
	"github.com/grafana/mimir/pkg/mimirtool/client"
)

type RulerAnalyzeCommand struct {
	ClientConfig client.Config
	outputFile   string
}

func (cmd *RulerAnalyzeCommand) run(_ *kingpin.ParseContext) error {
	output, err := AnalyzeRuler(cmd.ClientConfig)
	if err != nil {
		return err
	}

	err = writeOutRuleMetrics(output, cmd.outputFile)
	if err != nil {
		return err
	}

	return nil
}

// AnalyzeRuler analyze Mimir's ruler and return the list metrics used in them.
func AnalyzeRuler(c client.Config) (*analyze.MetricsInRuler, error) {
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

	for ns := range rules {
		for _, rg := range rules[ns] {
			err := analyze.ParseMetricsInRuleGroup(output, rg, ns)
			if err != nil {
				return nil, errors.Wrap(err, "metrics parse error")

			}
		}
	}
	return output, nil
}

func writeOutRuleMetrics(mir *analyze.MetricsInRuler, outputFile string) error {
	var metricsUsed model.LabelValues
	for metric := range mir.OverallMetrics {
		metricsUsed = append(metricsUsed, model.LabelValue(metric))
	}
	sort.Sort(metricsUsed)

	mir.MetricsUsed = metricsUsed
	out, err := json.MarshalIndent(mir, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(outputFile, out, os.FileMode(int(0o666))); err != nil {
		return err
	}

	return nil
}
