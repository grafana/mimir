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

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/mimirtool/analyze"
	"github.com/grafana/mimir/pkg/mimirtool/client"
)

type RulerAnalyzeCommand struct {
	ClientConfig client.Config
	outputFile   string

	logger log.Logger
}

func (cmd *RulerAnalyzeCommand) run(_ *kingpin.ParseContext) error {
	output, err := AnalyzeRuler(cmd.ClientConfig, cmd.logger)
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
func AnalyzeRuler(c client.Config, logger log.Logger) (*analyze.MetricsInRuler, error) {
	output := &analyze.MetricsInRuler{}
	output.OverallMetrics = make(map[string]struct{})

	cli, err := client.New(c, logger)
	if err != nil {
		return nil, err
	}

	rules, err := cli.ListRules(context.Background(), "")
	if err != nil {
		return nil, errors.Wrap(err, "Unable to read rules from Grafana Mimir")

	}

	for ns := range rules {
		for _, rg := range rules[ns] {
			err := analyze.ParseMetricsInRuleGroup(output, rg, ns, logger)
			if err != nil {
				return nil, errors.Wrap(err, "metrics parse error")

			}
		}
	}

	var metricsUsed model.LabelValues
	for metric := range output.OverallMetrics {
		metricsUsed = append(metricsUsed, model.LabelValue(metric))
	}
	sort.Sort(metricsUsed)
	output.MetricsUsed = metricsUsed

	return output, nil
}

func writeOutRuleMetrics(mir *analyze.MetricsInRuler, outputFile string) error {
	out, err := json.MarshalIndent(mir, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(outputFile, out, os.FileMode(int(0o666))); err != nil {
		return err
	}

	return nil
}
