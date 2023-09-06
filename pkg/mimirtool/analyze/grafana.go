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
	lvRegexp         = regexp.MustCompile(`(?s)label_values\((.+),.+\)`)
	lvNoQueryRegexp  = regexp.MustCompile(`(?s)label_values\((.+)\)`)
	qrRegexp         = regexp.MustCompile(`(?s)query_result\((.+)\)`)
	validMetricName  = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	durationRegexp   = regexp.MustCompile(`\[\$.+\]`)
	var1LabelRegexp  = regexp.MustCompile(`=\${[a-zA-Z0-9_]+(:[a-zA-Z0-9]+)?}`)
	var2LabelRegexp  = regexp.MustCompile(`=\$[a-zA-Z0-9_]+`)
	var1MetricRegexp = regexp.MustCompile(`\${[a-zA-Z0-9_]+(:[a-zA-Z0-9]+)?}`)
	var2MetricRegexp = regexp.MustCompile(`\$[a-zA-Z0-9_]+`)
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
	Queries     []string `json:"queries"`
}

func ParseMetricsInBoard(mig *MetricsInGrafana, board minisdk.Board, datasourceUID string) {
	var parseErrors []error
	metrics := make(map[string]struct{})
	queries := make(map[string]struct{})

	// Iterate through all the panels and collect metrics
	for _, panel := range board.Panels {
		parseErrors = append(parseErrors, metricsFromPanel(*panel, metrics, queries, datasourceUID)...)
		if panel.RowPanel != nil {
			for _, subPanel := range panel.RowPanel.Panels {
				parseErrors = append(parseErrors, metricsFromPanel(subPanel, metrics, queries, datasourceUID)...)
			}
		}
	}

	// Iterate through all the rows and collect metrics
	for _, row := range board.Rows {
		for _, panel := range row.Panels {
			parseErrors = append(parseErrors, metricsFromPanel(panel, metrics, queries, datasourceUID)...)
		}
	}

	// Process metrics in templating
	parseErrors = append(parseErrors, metricsFromTemplating(board.Templating, metrics, datasourceUID)...)

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

	queriesInBoard := make([]string, 0, len(queries))
	for query := range queries {
		if query == "" {
			continue
		}
		queriesInBoard = append(queriesInBoard, query)
	}
	slices.Sort(queriesInBoard)

	mig.Dashboards = append(mig.Dashboards, DashboardMetrics{
		Slug:        board.Slug,
		UID:         board.UID,
		Title:       board.Title,
		Metrics:     metricsInBoard,
		ParseErrors: parseErrs,
		Queries:     queriesInBoard,
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

func metricsFromTemplating(templating minisdk.Templating, metrics map[string]struct{}, datasourceUID string) []error {
	parseErrors := []error{}
	for _, templateVar := range templating.List {
		if templateVar.Type != "query" {
			continue
		}

		if datasourceUID != "" && templateVar.Datasource != nil {
			// legacy DS
			if templateVar.Datasource.LegacyName != "" && templateVar.Datasource.LegacyName != datasourceUID {
				log.Debugln("metricsFromTemplating", "Legacy Datasource", templateVar.Datasource.LegacyName, "not matching target ds", datasourceUID)
				continue
			} else {
				if templateVar.Datasource.UID != "" && templateVar.Datasource.UID != datasourceUID {
					log.Debugln("metricsFromTemplating", "Datasource UID", templateVar.Datasource.UID, "not matching target ds", datasourceUID)
					continue
				}
			}
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
func getCustomPanelTargets(panel minisdk.Panel, datasourceUID string) *[]minisdk.Target {
	if panel.CommonPanel.Type != "timeseries" {
		return nil
	}
	if datasourceUID != "" {
		if panel.Datasource != nil {
			// legacy datasource ("datasource":"xxxxx")
			if panel.Datasource.LegacyName != "" && panel.Datasource.LegacyName != datasourceUID {
				log.Debugln("getCustomPanelTargets", "Legacy datasource", panel.Datasource.LegacyName, "not matching target ds", datasourceUID)
				return nil
			} else {
				// normal datasource (with type and uid)
				// we'll filter mixed targets later
				if panel.Datasource.Type != "datasource" && panel.Datasource.UID != "" && panel.Datasource.UID != datasourceUID {
					log.Debugln("getCustomPanelTargets", "Datasource UID", panel.Datasource.UID, "not matching target ds", datasourceUID)
					return nil
				}
			}
		} else {
			// if datasourceUID is defined we'll filter out null datasource too
			return nil
		}
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

func metricsFromPanel(panel minisdk.Panel, metrics map[string]struct{}, queries map[string]struct{}, datasourceUID string) []error {
	var parseErrors []error

	targets := panel.GetTargets(datasourceUID)
	if targets == nil {
		targets = getCustomPanelTargets(panel, datasourceUID)
		if targets == nil {
			if datasourceUID != "" {
				parseErrors = append(parseErrors, fmt.Errorf("unsupported panel type: %q or all metrics filtered out by datasourceUID: %s", panel.CommonPanel.Type, datasourceUID))
			} else {
				parseErrors = append(parseErrors, fmt.Errorf("unsupported panel type: %q", panel.CommonPanel.Type))
			}
			return parseErrors
		}
	}

	for _, target := range *targets {
		// Prometheus has this set.
		if target.Expr == "" {
			continue
		}
		// filter datasource in mixed targets
		if datasourceUID != "" && target.Datasource != nil {
			// legacy datasource ("datasource":"xxxxx")
			if target.Datasource.LegacyName != "" && target.Datasource.LegacyName != datasourceUID {
				log.Debugln("metricsFromPanel", "Legacy datasource", target.Datasource.LegacyName, "not matching target ds", datasourceUID)
				continue
			} else {
				// mixed target in normal datasource
				if target.Datasource.Type == "datasource" && target.Datasource.UID != "" && target.Datasource.UID != datasourceUID {
					log.Debugln("metricsFromPanel", "Datasource UID", target.Datasource.UID, "not matching target ds", datasourceUID)
					continue
				}
			}
		}
		query := target.Expr
		err := parseQuery(query, metrics)
		if err != nil {
			parseErrors = append(parseErrors, errors.Wrapf(err, "query=%v", query))
			log.Debugln("msg", "promql parse error", "err", err, "query", query)
			continue
		} else {
			queries[query] = struct{}{}
		}
	}

	return parseErrors
}

func parseQuery(query string, metrics map[string]struct{}) error {
	// replace standard Grafana Prometheus macros
	query = strings.ReplaceAll(query, `$__interval`, "5m")
	query = strings.ReplaceAll(query, `$interval`, "5m")
	query = strings.ReplaceAll(query, `$resolution`, "5s")
	query = strings.ReplaceAll(query, "$__rate_interval", "15s")
	query = strings.ReplaceAll(query, "$__range", "1d")
	query = strings.ReplaceAll(query, "${__range_s:glob}", "30")
	query = strings.ReplaceAll(query, "${__range_s}", "30")
	// replace duration variable, e.g. [$agregation_window]
	query = durationRegexp.ReplaceAllString(query, "[5m]")
	// replace label variable, e.g. metric{label=${value}} or metric{label=${value:format}}
	query = var1LabelRegexp.ReplaceAllString(query, `="variable"`)
	// replace label variable, e.g. metric{label=$value}
	query = var2LabelRegexp.ReplaceAllString(query, `="variable"`)
	// replace metric variable, e.g. ${metric}{} or ${metric:format}{}
	query = var1MetricRegexp.ReplaceAllString(query, `variable`)
	// replace metric variable, e.g. $metric{}
	query = var2MetricRegexp.ReplaceAllString(query, `variable`)

	expr, err := parser.ParseExpr(query)
	if err != nil {
		return err
	}

	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
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
