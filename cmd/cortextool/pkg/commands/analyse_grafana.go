package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/grafana-tools/sdk"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql/parser"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/cortex-tools/pkg/analyse"
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
		parseMetricsInBoard(output, board)
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

func parseMetricsInBoard(mig *analyse.MetricsInGrafana, board sdk.Board) {
	var parseErrors []error
	metrics := make(map[string]struct{})

	// Iterate through all the panels and collect metrics
	for _, panel := range board.Panels {
		parseErrors = append(parseErrors, metricsFromPanel(*panel, metrics)...)
		if panel.RowPanel != nil {
			for _, subPanel := range panel.RowPanel.Panels {
				parseErrors = append(parseErrors, metricsFromPanel(subPanel, metrics)...)
			}
		}
	}

	// Iterate through all the rows and collect metrics
	for _, row := range board.Rows {
		for _, panel := range row.Panels {
			parseErrors = append(parseErrors, metricsFromPanel(panel, metrics)...)
		}
	}

	// Process metrics in templating
	parseErrors = append(parseErrors, metricsFromTemplating(board.Templating, metrics)...)

	var parseErrs []string
	for _, err := range parseErrors {
		parseErrs = append(parseErrs, err.Error())
	}

	var metricsInBoard []string
	for metric := range metrics {
		if metric == "" {
			continue
		}

		metricsInBoard = append(metricsInBoard, metric)
		mig.OverallMetrics[metric] = struct{}{}
	}
	sort.Strings(metricsInBoard)

	mig.Dashboards = append(mig.Dashboards, analyse.DashboardMetrics{
		Slug:        board.Slug,
		UID:         board.UID,
		Title:       board.Title,
		Metrics:     metricsInBoard,
		ParseErrors: parseErrs,
	})

}

func metricsFromTemplating(templating sdk.Templating, metrics map[string]struct{}) []error {
	parseErrors := []error{}
	for _, templateVar := range templating.List {
		if templateVar.Type != "query" {
			continue
		}
		if query, ok := templateVar.Query.(string); ok {
			// label_values
			if strings.Contains(query, "label_values") {
				re := regexp.MustCompile(`label_values\(([a-zA-Z0-9_]+)`)
				query = re.FindStringSubmatch(query)[1]
			}
			// query_result
			if strings.Contains(query, "query_result") {
				re := regexp.MustCompile(`query_result\((.+)\)`)
				query = re.FindStringSubmatch(query)[1]
			}
			err := parseQuery(query, metrics)
			if err != nil {
				parseErrors = append(parseErrors, errors.Wrapf(err, "query=%v", query))
				log.Debugln("msg", "promql parse error", "err", err, "query", query)
				continue
			}
		} else {
			err := fmt.Errorf("templating type error: name=%v", templateVar.Name)
			parseErrors = append(parseErrors, err)
			log.Debugln("msg", "templating parse error", "err", err)
			continue
		}
	}
	return parseErrors
}

func metricsFromPanel(panel sdk.Panel, metrics map[string]struct{}) []error {
	var parseErrors []error

	if panel.GetTargets() == nil {
		return parseErrors
	}

	for _, target := range *panel.GetTargets() {
		// Prometheus has this set.
		if target.Expr == "" {
			continue
		}
		query := target.Expr
		err := parseQuery(query, metrics)
		if err != nil {
			parseErrors = append(parseErrors, errors.Wrapf(err, "query=%v", query))
			log.Debugln("msg", "promql parse error", "err", err, "query", query)
			continue
		}
	}

	return parseErrors
}

func parseQuery(query string, metrics map[string]struct{}) error {
	query = strings.ReplaceAll(query, `$__interval`, "5m")
	query = strings.ReplaceAll(query, `$interval`, "5m")
	query = strings.ReplaceAll(query, `$resolution`, "5s")
	query = strings.ReplaceAll(query, "$__rate_interval", "15s")
	query = strings.ReplaceAll(query, "$__range", "1d")
	query = strings.ReplaceAll(query, "${__range_s:glob}", "30")
	query = strings.ReplaceAll(query, "${__range_s}", "30")
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return err
	}

	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		if n, ok := node.(*parser.VectorSelector); ok {
			metrics[n.Name] = struct{}{}
		}

		return nil
	})

	return nil
}
