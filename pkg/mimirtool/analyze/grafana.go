// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/analyse/grafana.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package analyze

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/grafana/regexp"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/pkg/mimirtool/minisdk"
)

var (
	lvRegexp                     = regexp.MustCompile(`(?s)label_values\((.+),.+\)`)
	lvNoQueryRegexp              = regexp.MustCompile(`(?s)label_values\((.+)\)`)
	qrRegexp                     = regexp.MustCompile(`(?s)query_result\((.+)\)`)
	validMetricName              = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	variableRangeQueryRangeRegex = regexp.MustCompile(`\[\$?\w+?]`)
	variableSubqueryRangeRegex   = regexp.MustCompile(`\[\$?\w+:\$?\w+?]`)
	variableReplacer             = strings.NewReplacer(
		"$__interval", "5m",
		"$interval", "5m",
		"$resolution", "5s",
		"$__rate_interval", "15s",
		"$rate_interval", "15s",
		"$__range", "1d",
		"${__range_s:glob}", "30",
		"${__range_s}", "30",
	)
)

type MetricsInGrafana struct {
	MetricsUsed    model.LabelValues   `json:"metricsUsed"`
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

func ParseMetricsInBoard(mig *MetricsInGrafana, board minisdk.Board) {
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

	parseErrs := make([]string, 0, len(parseErrors))
	for _, err := range parseErrors {
		parseErrs = append(parseErrs, err.Error())
	}

	metricsInBoard := make([]string, 0, len(metrics))
	for metric := range metrics {
		if metric == "" {
			continue
		}

		metricsInBoard = append(metricsInBoard, metric)
		mig.OverallMetrics[metric] = struct{}{}
	}
	slices.Sort(metricsInBoard)

	mig.Dashboards = append(mig.Dashboards, DashboardMetrics{
		Slug:        board.Slug,
		UID:         board.UID,
		Title:       board.Title,
		Metrics:     metricsInBoard,
		ParseErrors: parseErrs,
	})
}

func getQueryFromTemplating(name string, q interface{}) (string, error) {
	if query, ok := q.(string); ok {
		return query, nil
	}
	// Fix for variables created within the grafana UI, they come in as JSON objects
	// with a refId field, i.e. { "query": "label_values(my_metric, cluster)", "refId": "AutomatedName"}
	if queryObj, ok := q.(map[string]interface{}); ok {
		if query, ok := queryObj["query"].(string); ok {
			return query, nil
		}
	}
	return "", fmt.Errorf("templating type error: name=%v", name)
}

func metricsFromTemplating(templating minisdk.Templating, metrics map[string]struct{}) []error {
	parseErrors := []error{}
	for _, templateVar := range templating.List {
		if templateVar.Type != "query" {
			continue
		}

		query, err := getQueryFromTemplating(templateVar.Name, templateVar.Query)
		if err != nil {
			parseErrors = append(parseErrors, err)
			log.Debugln("msg", "templating parse error", "err", err)
			continue
		}

		// label_values(query, label)
		if lvRegexp.MatchString(query) {
			sm := lvRegexp.FindStringSubmatch(query)
			// In case of really gross queries, like - https://github.com/grafana/jsonnet-libs/blob/e97ab17f67ab40d5fe3af7e59151dd43be03f631/hass-mixin/dashboard.libsonnet#L93
			if len(sm) > 0 {
				query = sm[1]
			} else {
				continue
			}
		} else if lvNoQueryRegexp.MatchString(query) {
			// No query so no metric.
			continue
		} else if qrRegexp.MatchString(query) {
			// query_result(query)
			query = qrRegexp.FindStringSubmatch(query)[1]
		}
		err = parseQuery(query, metrics)
		if err != nil {
			parseErrors = append(parseErrors, errors.Wrapf(err, "query=%v", query))
			log.Debugln("msg", "promql parse error", "err", err, "query", query)
			continue
		}
	}
	return parseErrors
}

// Workaround to support Grafana "timeseries" panel. This should
// be implemented in grafana/tools-sdk, and removed from here.
func getCustomPanelTargets(panel minisdk.Panel) *[]minisdk.Target {
	if panel.CommonPanel.Type != "timeseries" {
		return nil
	}

	// Heavy handed approach to re-marshal the panel and parse it again
	// so that we can extract the 'targets' field in the right format.

	bytes, err := json.Marshal(panel.CustomPanel)
	if err != nil {
		log.Debugln("msg", "panel re-marshalling error", "err", err)
		return nil
	}

	type panelType struct {
		Targets []minisdk.Target `json:"targets,omitempty"`
	}

	var parsedPanel panelType
	err = json.Unmarshal(bytes, &parsedPanel)
	if err != nil {
		log.Debugln("msg", "panel parsing error", "err", err)
		return nil
	}

	return &parsedPanel.Targets
}

func metricsFromPanel(panel minisdk.Panel, metrics map[string]struct{}) []error {
	var parseErrors []error

	targets := panel.GetTargets()
	if targets == nil {
		targets = getCustomPanelTargets(panel)
		if targets == nil {
			parseErrors = append(parseErrors, fmt.Errorf("unsupported panel type: %q", panel.CommonPanel.Type))
			return parseErrors
		}
	}

	for _, target := range *targets {
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

func replaceVariables(query string) string {
	query = variableReplacer.Replace(query)
	query = variableRangeQueryRangeRegex.ReplaceAllLiteralString(query, `[5m]`)
	query = variableSubqueryRangeRegex.ReplaceAllLiteralString(query, `[5m:1m]`)
	return query
}

func parseQuery(query string, metrics map[string]struct{}) error {
	expr, err := parser.ParseExpr(replaceVariables(query))
	if err != nil {
		return err
	}

	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if n, ok := node.(*parser.VectorSelector); ok {
			// VectorSelector has .Name when it's explicitly set as `name{...}`.
			// Otherwise we need to look into the matchers.
			if n.Name != "" {
				metrics[n.Name] = struct{}{}
				return nil
			}
			for _, m := range n.LabelMatchers {
				if m.Name == labels.MetricName && validMetricName.MatchString(m.Value) {
					metrics[m.Value] = struct{}{}
					return nil
				}
			}
		}

		return nil
	})

	return nil
}
