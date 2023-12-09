// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse_dashboards.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/mimirtool/analyze"
	"github.com/grafana/mimir/pkg/mimirtool/minisdk"
)

type DashboardAnalyzeCommand struct {
	DashFilesList []string
	outputFile    string
}

func (cmd *DashboardAnalyzeCommand) run(_ *kingpin.ParseContext) error {
	output, err := AnalyzeDashboards(cmd.DashFilesList)
	if err != nil {
		return err
	}

	err = writeOut(output, cmd.outputFile)
	if err != nil {
		return err
	}
	return nil
}

// AnalyzeDashboards analyze the given list of dashboard files and return the list metrics used in them.
func AnalyzeDashboards(dashFilesList []string) (*analyze.MetricsInGrafana, error) {
	output := &analyze.MetricsInGrafana{}
	output.OverallMetrics = make(map[string]struct{})

	for _, file := range dashFilesList {
		var board minisdk.Board
		buf, err := loadFile(file)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(buf, &board); err != nil {
			fmt.Fprintf(os.Stderr, "%s for %s\n", err, file)
			continue
		}
		analyze.ParseMetricsInBoard(output, board)
	}

	var metricsUsed model.LabelValues
	for metric := range output.OverallMetrics {
		metricsUsed = append(metricsUsed, model.LabelValue(metric))
	}
	sort.Sort(metricsUsed)
	output.MetricsUsed = metricsUsed

	return output, nil
}

func loadFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	filesize := fileinfo.Size()
	buffer := make([]byte, filesize)

	_, err = file.Read(buffer)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}
