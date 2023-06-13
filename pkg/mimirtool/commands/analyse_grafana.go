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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"golang.org/x/exp/slices"

	"github.com/grafana-tools/sdk"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/pkg/mimirtool/analyze"
	"github.com/grafana/mimir/pkg/mimirtool/minisdk"
)

type GrafanaAnalyzeCommand struct {
	address     string
	apiKey      string
	readTimeout time.Duration
	folders     folderTitles
	folderIDs   folderIDs

	outputFile    string
	datasourceUID string
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

type folderIDs []string

func (f *folderIDs) Set(value string) error {
	*f = append(*f, value)
	return nil
}

func (f folderIDs) String() string {
	return strings.Join(f, ",")
}

func (f folderIDs) IsCumulative() bool {
	return true
}

func (cmd *GrafanaAnalyzeCommand) run(_ *kingpin.ParseContext) error {
	output := &analyze.MetricsInGrafana{}
	output.OverallMetrics = make(map[string]struct{})

	ctx, cancel := context.WithTimeout(context.Background(), cmd.readTimeout)
	defer cancel()

	c, err := sdk.NewClient(cmd.address, cmd.apiKey, sdk.DefaultHTTPClient)
	if err != nil {
		return err
	}

	var params []sdk.SearchParam
	if len(cmd.folderIDs) > 0 {
		for _, folderIdStr := range cmd.folderIDs {
			if folderIdInt, err := strconv.Atoi(folderIdStr); err == nil {
				params = append(params, sdk.SearchFolderID(folderIdInt))
			}
		}
	}
	params = append(params,
		sdk.SearchType(sdk.SearchTypeDashboard),
		sdk.SearchQuery(""),
		sdk.SearchStarred(false),
	)

	boardLinks, err := c.Search(ctx, params...)
	if err != nil {
		return err
	}

	filterOnFolders := len(cmd.folders) > 0

	for _, link := range boardLinks {
		if filterOnFolders && !slices.Contains(cmd.folders, link.FolderTitle) {
			continue
		}
		data, _, err := c.GetRawDashboardByUID(ctx, link.UID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s for %s %s\n", err, link.UID, link.Title)
			continue
		}
		board, err := unmarshalDashboard(data, link)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s for %s %s\n", err, link.UID, link.Title)
			continue
		}
		analyze.ParseMetricsInBoard(output, board, cmd.datasourceUID)
	}

	err = writeOut(output, cmd.outputFile)
	if err != nil {
		return err
	}

	return nil
}

func unmarshalDashboard(data []byte, link sdk.FoundBoard) (minisdk.Board, error) {
	var board minisdk.Board
	if err := json.Unmarshal(data, &board); err != nil {
		return minisdk.Board{}, fmt.Errorf("can't unmarshal dashboard %s (%s): %w", link.UID, link.Title, err)
	}

	return board, nil
}

func writeOut(mig *analyze.MetricsInGrafana, outputFile string) error {
	var metricsUsed model.LabelValues
	for metric := range mig.OverallMetrics {
		metricsUsed = append(metricsUsed, model.LabelValue(metric))
	}
	sort.Sort(metricsUsed)

	mig.MetricsUsed = metricsUsed
	out, err := json.MarshalIndent(mig, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(outputFile, out, os.FileMode(int(0o666))); err != nil {
		return err
	}

	return nil
}
