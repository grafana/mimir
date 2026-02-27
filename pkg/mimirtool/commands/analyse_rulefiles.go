// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse_rulefiles.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"sort"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/mimirtool/analyze"
	"github.com/grafana/mimir/pkg/mimirtool/rules"
	"github.com/grafana/mimir/pkg/mimirtool/util"
)

type RuleFileAnalyzeCommand struct {
	RuleFilesList               []string
	outputFile                  string
	enableExperimentalFunctions bool

	logger log.Logger
}

func (cmd *RuleFileAnalyzeCommand) run(_ *kingpin.ParseContext) error {
	// TODO: Get scheme from CLI flag.
	output, err := AnalyzeRuleFiles(cmd.RuleFilesList, model.LegacyValidation, util.CreatePromQLParser(cmd.enableExperimentalFunctions), cmd.logger)
	if err != nil {
		return err
	}

	err = writeOutRuleMetrics(output, cmd.outputFile)
	if err != nil {
		return err
	}

	return nil
}

// AnalyzeRuleFiles analyze rules files and return the list metrics used in them.
func AnalyzeRuleFiles(ruleFiles []string, scheme model.ValidationScheme, promqlParser parser.Parser, logger log.Logger) (*analyze.MetricsInRuler, error) {
	output := &analyze.MetricsInRuler{}
	output.OverallMetrics = make(map[string]struct{})

	nss, err := rules.ParseFiles(rules.MimirBackend, ruleFiles, scheme, promqlParser, logger)
	if err != nil {
		return nil, errors.Wrap(err, "analyze operation unsuccessful, unable to parse rules files")
	}

	for _, ns := range nss {
		for _, group := range ns.Groups {
			err := analyze.ParseMetricsInRuleGroup(output, group, ns.Namespace, promqlParser, logger)
			if err != nil {
				return nil, err
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
