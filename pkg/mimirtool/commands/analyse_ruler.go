// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse_ruler.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"sort"

	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/pkg/mimirtool/analyse"
	"github.com/grafana/mimir/pkg/mimirtool/client"
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
		log.Fatalf("unable to read rules from mimir, %v", err)
	}

	for ns := range rules {
		for _, rg := range rules[ns] {
			err := analyse.ParseMetricsInRuleGroup(output, rg, ns)
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
