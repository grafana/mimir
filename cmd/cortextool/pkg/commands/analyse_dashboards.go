// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse_dashboards.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/grafana-tools/sdk"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/cmd/cortextool/pkg/analyse"
)

type DashboardAnalyseCommand struct {
	DashFilesList []string
	outputFile    string
}

func (cmd *DashboardAnalyseCommand) run(k *kingpin.ParseContext) error {
	output := &analyse.MetricsInGrafana{}
	output.OverallMetrics = make(map[string]struct{})

	for _, file := range cmd.DashFilesList {
		var board sdk.Board
		buf, err := loadFile(file)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(buf, &board); err != nil {
			fmt.Fprintf(os.Stderr, "%s for %s\n", err, file)
			continue
		}
		analyse.ParseMetricsInBoard(output, board)
	}

	err := writeOut(output, cmd.outputFile)
	if err != nil {
		return err
	}
	return nil
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
