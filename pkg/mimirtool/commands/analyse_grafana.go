// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse_grafana.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/grafana-tools/sdk"
	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/mimirtool/analyze"
	"github.com/grafana/mimir/pkg/mimirtool/minisdk"
)

type GrafanaAnalyzeCommand struct {
	address     string
	apiKey      string
	readTimeout time.Duration
	folders     folderTitles

	outputFile string
}

type folderTitles []string

func (f *folderTitles) Set(value string) error {
	*f = append(*f, value)
	return nil
}

func (f folderTitles) String() string {
	return strings.Join(f, ",")
}

func (f folderTitles) IsCumulative() bool {
	return true
}

func (cmd *GrafanaAnalyzeCommand) run(_ *kingpin.ParseContext) error {
	c, err := sdk.NewClient(cmd.address, cmd.apiKey, sdk.DefaultHTTPClient)
	if err != nil {
		return err
	}

	output, err := AnalyzeGrafana(context.Background(), c, cmd.folders, cmd.readTimeout)
	if err != nil {
		return err
	}
	err = writeOut(output, cmd.outputFile)
	if err != nil {
		return err
	}

	return nil
}

// AnalyzeGrafana analyze grafana's dashboards and return the list metrics used in them.
func AnalyzeGrafana(ctx context.Context, c *sdk.Client, folders []string, readTimeout time.Duration) (*analyze.MetricsInGrafana, error) {

	output := &analyze.MetricsInGrafana{}
	output.OverallMetrics = make(map[string]struct{})

	boardLinks, err := getAllDashboards(ctx, c)
	if err != nil {
		return nil, err
	}

	filterOnFolders := len(folders) > 0

	for _, link := range boardLinks {
		if filterOnFolders && !slices.Contains(folders, link.FolderTitle) {
			continue
		}

		err := processDashboard(ctx, c, link, output, readTimeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s for %s %s\n", err, link.UID, link.Title)
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

// processDashboard fetches and processes a single Grafana dashboard.
func processDashboard(ctx context.Context, c *sdk.Client, link sdk.FoundBoard, output *analyze.MetricsInGrafana, readTimeout time.Duration) error {
	fetchCtx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()

	data, _, err := c.GetRawDashboardByUID(fetchCtx, link.UID)
	if err != nil {
		return err
	}

	board, err := unmarshalDashboard(data, link)
	if err != nil {
		return err
	}

	analyze.ParseMetricsInBoard(output, board)
	return nil
}

func getAllDashboards(ctx context.Context, c *sdk.Client) ([]sdk.FoundBoard, error) {
	var currentPage uint = 1
	var results []sdk.FoundBoard
	for {
		nextPageResults, err := c.Search(ctx, sdk.SearchType(sdk.SearchTypeDashboard), sdk.SearchPage(currentPage))
		if err != nil {
			return nil, err
		}
		// no more pages, we got everything
		if len(nextPageResults) == 0 {
			return results, nil
		}
		// we found more results, let's keep going
		results = append(results, nextPageResults...)
		currentPage++
	}
}

func unmarshalDashboard(data []byte, link sdk.FoundBoard) (minisdk.Board, error) {
	var board minisdk.Board
	if err := json.Unmarshal(data, &board); err != nil {
		return minisdk.Board{}, fmt.Errorf("can't unmarshal dashboard %s (%s): %w", link.UID, link.Title, err)
	}

	return board, nil
}

func writeOut(mig *analyze.MetricsInGrafana, outputFile string) error {
	out, err := json.MarshalIndent(mig, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(outputFile, out, os.FileMode(int(0o666))); err != nil {
		return err
	}

	return nil
}
