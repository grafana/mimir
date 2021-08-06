package commands

import (
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/cortex-tools/pkg/analyse"
	"github.com/grafana/cortex-tools/pkg/rules"
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
			err := parseMetricsInRuleGroup(output, group, ns.Namespace)
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
