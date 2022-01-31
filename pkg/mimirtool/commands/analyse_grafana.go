// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse_grafana.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"

	"github.com/grafana-tools/sdk"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/pkg/mimirtool/analyse"
)

type GrafanaAnalyseCommand struct {
	address     string
	apiKey      string
	readTimeout time.Duration

	outputFile string
}

func (cmd *GrafanaAnalyseCommand) run(k *kingpin.ParseContext) error {
	output := &analyse.MetricsInGrafana{}
	output.OverallMetrics = make(map[string]struct{})

	ctx, cancel := context.WithTimeout(context.Background(), cmd.readTimeout)
	defer cancel()

	c, err := sdk.NewClient(cmd.address, cmd.apiKey, sdk.DefaultHTTPClient)
	if err != nil {
		return err
	}

	boardLinks, err := c.SearchDashboards(ctx, "", false)
	if err != nil {
		return err
	}

	for _, link := range boardLinks {
		board, _, err := c.GetDashboardByUID(ctx, link.UID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s for %s %s\n", err, link.UID, link.Title)
			continue
		}
		analyse.ParseMetricsInBoard(output, board)
	}

	err = writeOut(output, cmd.outputFile)
	if err != nil {
		return err
	}

	return nil
}

func writeOut(mig *analyse.MetricsInGrafana, outputFile string) error {
	var metricsUsed []string
	for metric := range mig.OverallMetrics {
		metricsUsed = append(metricsUsed, metric)
	}
	sort.Strings(metricsUsed)

	mig.MetricsUsed = metricsUsed
	out, err := json.MarshalIndent(mig, "", "  ")
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(outputFile, out, os.FileMode(int(0666))); err != nil {
		return err
	}

	return nil
}
