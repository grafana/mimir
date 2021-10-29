package analyse

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/grafana-tools/sdk"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql/parser"
)

type MetricsInGrafana struct {
	MetricsUsed    []string            `json:"metricsUsed"`
	OverallMetrics map[string]struct{} `json:"-"`
	Dashboards     []DashboardMetrics  `json:"dashboards"`
}

type DashboardMetrics struct {
	Slug        string   `json:"slug"`
	UID         string   `json:"uid,omitempty"`
	Title       string   `json:"title"`
	Metrics     []string `json:"metrics"`
	ParseErrors []string `json:"parse_errors"`
}

func ParseMetricsInBoard(mig *MetricsInGrafana, board sdk.Board) {
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

	mig.Dashboards = append(mig.Dashboards, DashboardMetrics{
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
				sm := re.FindStringSubmatch(query)
				// In case of really gross queries, like - https://github.com/grafana/jsonnet-libs/blob/e97ab17f67ab40d5fe3af7e59151dd43be03f631/hass-mixin/dashboard.libsonnet#L93
				if len(sm) > 0 {
					query = sm[1]
				}
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
