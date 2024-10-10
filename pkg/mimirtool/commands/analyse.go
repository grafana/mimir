// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"runtime"
	"strconv"

	"github.com/alecthomas/kingpin/v2"
)

type AnalyzeCommand struct{}

func (cmd *AnalyzeCommand) Register(app *kingpin.Application, envVars EnvVarNames) {
	analyzeCmd := app.Command("analyze", "Run analysis against your Prometheus, Grafana, and Grafana Mimir to see which metrics are being used and exported.")

	paCmd := &PrometheusAnalyzeCommand{}
	prometheusAnalyzeCmd := analyzeCmd.Command("prometheus", "Take the metrics being used in Grafana and get the cardinality from a Prometheus.").Action(paCmd.run)
	prometheusAnalyzeCmd.Flag("address", "Address of the Prometheus or Grafana Mimir instance; alternatively, set "+envVars.Address+".").
		Envar(envVars.Address).
		Required().
		StringVar(&paCmd.address)
	prometheusAnalyzeCmd.Flag("prometheus-http-prefix", "HTTP URL path under which the Prometheus api will be served.").
		Default("").
		StringVar(&paCmd.prometheusHTTPPrefix)
	prometheusAnalyzeCmd.Flag("auth-token", "Authentication token bearer authentication; alternatively, set "+envVars.AuthToken+".").
		Default("").
		Envar(envVars.AuthToken).
		StringVar(&paCmd.authToken)
	prometheusAnalyzeCmd.Flag("id", "Basic auth username to use when contacting Prometheus or Grafana Mimir, also set as tenant ID; alternatively, set "+envVars.TenantID+".").
		Envar(envVars.TenantID).
		Default("").
		StringVar(&paCmd.username)
	prometheusAnalyzeCmd.Flag("key", "Basic auth password to use when contacting Prometheus or Grafana Mimir; alternatively, set "+envVars.APIKey+"").
		Envar(envVars.APIKey).
		Default("").
		StringVar(&paCmd.password)
	prometheusAnalyzeCmd.Flag("read-timeout", "timeout for read requests").
		Default("30s").
		DurationVar(&paCmd.readTimeout)
	prometheusAnalyzeCmd.Flag("grafana-metrics-file", "The path for the input file containing the metrics from grafana-analyze command").
		Default("metrics-in-grafana.json").
		StringVar(&paCmd.grafanaMetricsFile)
	prometheusAnalyzeCmd.Flag("ruler-metrics-file", "The path for the input file containing the metrics from ruler-analyze command").
		Default("metrics-in-ruler.json").
		StringVar(&paCmd.rulerMetricsFile)
	prometheusAnalyzeCmd.Flag("concurrency", "Concurrency (Default: runtime.NumCPU())").
		Default(strconv.Itoa(runtime.NumCPU())).
		IntVar(&paCmd.concurrency)
	prometheusAnalyzeCmd.Flag("output", "The path for the output file").
		Default("prometheus-metrics.json").
		StringVar(&paCmd.outputFile)

	gaCmd := &GrafanaAnalyzeCommand{}
	grafanaAnalyzeCmd := analyzeCmd.Command("grafana", "Analyze and output the metrics used in Grafana Dashboards.").Action(gaCmd.run)

	grafanaAnalyzeCmd.Flag("address", "Address of the Grafana instance, alternatively set $GRAFANA_ADDRESS.").
		Envar("GRAFANA_ADDRESS").
		Required().
		StringVar(&gaCmd.address)
	grafanaAnalyzeCmd.Flag("key", "API key to use when contacting Grafana, alternatively set $GRAFANA_API_KEY. To use basic auth set to \"username:password\". To use a Bearer token provide a value without a colon.").
		Envar("GRAFANA_API_KEY").
		Default("").
		StringVar(&gaCmd.apiKey)
	grafanaAnalyzeCmd.Flag("read-timeout", "timeout for read requests").
		Default("300s").
		DurationVar(&gaCmd.readTimeout)
	grafanaAnalyzeCmd.Flag("output", "The path for the output file").
		Default("metrics-in-grafana.json").
		StringVar(&gaCmd.outputFile)
	grafanaAnalyzeCmd.Flag("folder-title", "Limit dashboards analysis for unused metrics based on their exact folder title. When repeated any of the matching folders will be analyzed.").
		SetValue(&gaCmd.folders)

	raCmd := &RulerAnalyzeCommand{}
	rulerAnalyzeCmd := analyzeCmd.Command("ruler", "Analyze and extract the metrics that are used in Grafana Mimir rules").
		Action(raCmd.run)
	rulerAnalyzeCmd.Flag("address", "Address of the Prometheus or Grafana Mimir instance; alternatively, set "+envVars.Address+".").
		Envar(envVars.Address).
		Required().
		StringVar(&raCmd.ClientConfig.Address)
	rulerAnalyzeCmd.Flag("id", "Basic auth username and X-Scope-OrgID value to use when contacting Prometheus or Grafana Mimir; alternatively, set "+envVars.TenantID+".").
		Envar(envVars.TenantID).
		Default("").
		StringVar(&raCmd.ClientConfig.ID)
	rulerAnalyzeCmd.Flag("key", "Basic auth password to use when contacting Prometheus or Grafana Mimir; alternatively, set "+envVars.APIKey+".").
		Envar(envVars.APIKey).
		Default("").
		StringVar(&raCmd.ClientConfig.Key)
	rulerAnalyzeCmd.Flag("output", "The path for the output file").
		Default("metrics-in-ruler.json").
		StringVar(&raCmd.outputFile)
	rulerAnalyzeCmd.Flag("auth-token", "Authentication token bearer authentication; alternatively, set "+envVars.AuthToken+".").
		Default("").
		Envar(envVars.AuthToken).
		StringVar(&raCmd.ClientConfig.AuthToken)

	daCmd := &DashboardAnalyzeCommand{}
	dashboardAnalyzeCmd := analyzeCmd.Command("dashboard", "Analyze and output the metrics used in Grafana dashboard files").Action(daCmd.run)
	dashboardAnalyzeCmd.Arg("files", "Dashboard files").
		Required().
		ExistingFilesVar(&daCmd.DashFilesList)
	dashboardAnalyzeCmd.Flag("output", "The path for the output file").
		Default("metrics-in-grafana.json").
		StringVar(&daCmd.outputFile)

	rfCmd := &RuleFileAnalyzeCommand{}
	ruleFileAnalyzeCmd := analyzeCmd.Command("rule-file", "Analyze and output the metrics used in Prometheus rules files").Action(rfCmd.run)
	ruleFileAnalyzeCmd.Arg("files", "Rules files").
		Required().
		ExistingFilesVar(&rfCmd.RuleFilesList)
	ruleFileAnalyzeCmd.Flag("output", "The path for the output file").
		Default("metrics-in-ruler.json").
		StringVar(&rfCmd.outputFile)
}
