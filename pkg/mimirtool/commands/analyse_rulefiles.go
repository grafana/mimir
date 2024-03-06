// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse_rulefiles.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"sort"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/mimirtool/analyze"
	"github.com/grafana/mimir/pkg/mimirtool/rules"
)

type RuleFileAnalyzeCommand struct {
	RuleFilesList []string
	outputFile    string
}

func (cmd *RuleFileAnalyzeCommand) run(_ *kingpin.ParseContext) error {

	output, err := AnalyzeRuleFiles(cmd.RuleFilesList)
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
func AnalyzeRuleFiles(ruleFiles []string) (*analyze.MetricsInRuler, error) {
	output := &analyze.MetricsInRuler{}
	output.OverallMetrics = make(map[string]struct{})

	nss, err := rules.ParseFiles(rules.MimirBackend, ruleFiles)
	if err != nil {
		return nil, errors.Wrap(err, "analyze operation unsuccessful, unable to parse rules files")
	}

	for _, ns := range nss {
		for _, group := range ns.Groups {
			err := analyze.ParseMetricsInRuleGroup(output, group, ns.Namespace)
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
