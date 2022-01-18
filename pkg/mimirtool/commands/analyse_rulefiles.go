// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse_rulefiles.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/pkg/mimirtool/analyse"
	"github.com/grafana/mimir/pkg/mimirtool/rules"
)

type RuleFileAnalyseCommand struct {
	RuleFilesList []string
	outputFile    string
}

func (cmd *RuleFileAnalyseCommand) run(k *kingpin.ParseContext) error {

	output := &analyse.MetricsInRuler{}
	output.OverallMetrics = make(map[string]struct{})

	nss, err := rules.ParseFiles("cortex", cmd.RuleFilesList)
	if err != nil {
		return errors.Wrap(err, "analyse operation unsuccessful, unable to parse rules files")
	}

	for _, ns := range nss {
		for _, group := range ns.Groups {
			err := analyse.ParseMetricsInRuleGroup(output, group, ns.Namespace)
			if err != nil {
				return err
			}
		}
	}

	err = writeOutRuleMetrics(output, cmd.outputFile)
	if err != nil {
		return err
	}

	return nil
}
