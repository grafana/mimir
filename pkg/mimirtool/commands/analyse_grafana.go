// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse_grafana.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/mimirtool/analyze"
	"github.com/grafana/mimir/pkg/mimirtool/client"
	"github.com/grafana/mimir/pkg/mimirtool/minisdk"
	"github.com/grafana/mimir/pkg/mimirtool/util"
)

type GrafanaAnalyzeCommand struct {
	address                     string
	apiKey                      string
	readTimeout                 time.Duration
	folders                     folderTitles
	outputFile                  string
	enableExperimentalFunctions bool

	logger log.Logger
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
	c, err := minisdk.NewClient(cmd.address, cmd.apiKey, http.DefaultClient, client.UserAgent())
	if err != nil {
		return err
	}

	output, err := AnalyzeGrafana(context.Background(), c, cmd.folders, cmd.readTimeout, util.CreatePromQLParser(cmd.enableExperimentalFunctions), cmd.logger)
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
func AnalyzeGrafana(ctx context.Context, c *minisdk.Client, folders []string, readTimeout time.Duration, promqlParser parser.Parser, logger log.Logger) (*analyze.MetricsInGrafana, error) {

	output := &analyze.MetricsInGrafana{}
	output.OverallMetrics = make(map[string]struct{})

	boardLinks, err := getAllDashboards(ctx, c)
	if err != nil {
		return nil, err
	}

	filterOnFolders := len(folders) > 0
	libraryPanels := libraryPanelCache{}

	for _, link := range boardLinks {
		if filterOnFolders && !slices.Contains(folders, link.FolderTitle) {
			continue
		}

		err := processDashboard(ctx, c, link, output, readTimeout, libraryPanels, promqlParser, logger)
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

type libraryPanelCache map[string]minisdk.Panel

// processDashboard fetches and processes a single Grafana dashboard.
func processDashboard(ctx context.Context, c *minisdk.Client, link minisdk.FoundBoard, output *analyze.MetricsInGrafana, readTimeout time.Duration, libraryPanels libraryPanelCache, promqlParser parser.Parser, logger log.Logger) error {
	fetchCtx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()

	data, err := c.GetRawDashboardByUID(fetchCtx, link.UID)
	if err != nil {
		return err
	}

	board, err := unmarshalDashboard(data, link)
	if err != nil {
		return err
	}

	parseErrors := resolveLibraryPanels(ctx, c, &board, libraryPanels, readTimeout)
	analyze.ParseMetricsInBoardWithParseErrors(output, board, parseErrors, promqlParser, logger)
	return nil
}

func resolveLibraryPanels(ctx context.Context, c *minisdk.Client, board *minisdk.Board, libraryPanels libraryPanelCache, readTimeout time.Duration) []error {
	var parseErrors []error
	for _, panel := range board.Panels {
		if panel == nil {
			continue
		}
		parseErrors = append(parseErrors, resolveLibraryPanel(ctx, c, panel, libraryPanels, readTimeout)...)
	}

	for _, row := range board.Rows {
		if row == nil {
			continue
		}
		for i := range row.Panels {
			parseErrors = append(parseErrors, resolveLibraryPanel(ctx, c, &row.Panels[i], libraryPanels, readTimeout)...)
		}
	}

	return parseErrors
}

func resolveLibraryPanel(ctx context.Context, c *minisdk.Client, panel *minisdk.Panel, libraryPanels libraryPanelCache, readTimeout time.Duration) []error {
	var parseErrors []error
	if panel.LibraryPanel != nil {
		libraryPanel, err := getLibraryPanel(ctx, c, *panel.LibraryPanel, libraryPanels, readTimeout)
		if err != nil {
			parseErrors = append(parseErrors, err)
		} else {
			*panel = libraryPanel
		}
	}

	for i := range panel.SubPanels {
		parseErrors = append(parseErrors, resolveLibraryPanel(ctx, c, &panel.SubPanels[i], libraryPanels, readTimeout)...)
	}

	return parseErrors
}

func getLibraryPanel(ctx context.Context, c *minisdk.Client, ref minisdk.LibraryPanelRef, libraryPanels libraryPanelCache, readTimeout time.Duration) (minisdk.Panel, error) {
	if ref.UID == "" {
		return minisdk.Panel{}, fmt.Errorf("library panel %q has no uid", ref.Name)
	}

	if panel, ok := libraryPanels[ref.UID]; ok {
		return panel, nil
	}

	fetchCtx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()

	panel, err := c.GetLibraryElementByUID(fetchCtx, ref.UID)
	if err != nil {
		return minisdk.Panel{}, fmt.Errorf("library panel %q (%s): %w", ref.Name, ref.UID, err)
	}
	libraryPanels[ref.UID] = panel
	return panel, nil
}

func getAllDashboards(ctx context.Context, c *minisdk.Client) ([]minisdk.FoundBoard, error) {
	var currentPage uint = 1
	var results []minisdk.FoundBoard
	for {
		nextPageResults, err := c.Search(ctx, minisdk.SearchType(minisdk.SearchTypeDashboard), minisdk.SearchPage(currentPage))
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

func unmarshalDashboard(data []byte, link minisdk.FoundBoard) (minisdk.Board, error) {
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
