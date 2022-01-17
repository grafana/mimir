package commands

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

type AnalyseCommand struct {
}

func (cmd *AnalyseCommand) Register(app *kingpin.Application) {
	analyseCmd := app.Command("analyse", "Run analysis against your Prometheus, Grafana and Cortex to see which metrics being used and exported.")

	paCmd := &PrometheusAnalyseCommand{}
	prometheusAnalyseCmd := analyseCmd.Command("prometheus", "Take the metrics being used in Grafana and get the cardinality from a Prometheus.").Action(paCmd.run)
	prometheusAnalyseCmd.Flag("address", "Address of the Prometheus/Cortex instance, alternatively set $CORTEX_ADDRESS.").
		Envar("CORTEX_ADDRESS").
		Required().
		StringVar(&paCmd.address)
	prometheusAnalyseCmd.Flag("id", "Username to use when contacting Prometheus/Cortex, alternatively set $CORTEX_TENANT_ID.").
		Envar("CORTEX_TENANT_ID").
		Default("").
		StringVar(&paCmd.username)
	prometheusAnalyseCmd.Flag("key", "Password to use when contacting Prometheus/Cortex, alternatively set $CORTEX_API_KEY.").
		Envar("CORTEX_API_KEY").
		Default("").
		StringVar(&paCmd.password)
	prometheusAnalyseCmd.Flag("read-timeout", "timeout for read requests").
		Default("30s").
		DurationVar(&paCmd.readTimeout)
	prometheusAnalyseCmd.Flag("grafana-metrics-file", "The path for the input file containing the metrics from grafana-analyse command").
		Default("metrics-in-grafana.json").
		StringVar(&paCmd.grafanaMetricsFile)
	prometheusAnalyseCmd.Flag("ruler-metrics-file", "The path for the input file containing the metrics from ruler-analyse command").
		Default("metrics-in-ruler.json").
		StringVar(&paCmd.rulerMetricsFile)
	prometheusAnalyseCmd.Flag("output", "The path for the output file").
		Default("prometheus-metrics.json").
		StringVar(&paCmd.outputFile)

	gaCmd := &GrafanaAnalyseCommand{}
	grafanaAnalyseCmd := analyseCmd.Command("grafana", "Analyse and output the metrics used in Grafana Dashboards.").Action(gaCmd.run)

	grafanaAnalyseCmd.Flag("address", "Address of the Grafana instance, alternatively set $GRAFANA_ADDRESS.").
		Envar("GRAFANA_ADDRESS").
		Required().
		StringVar(&gaCmd.address)
	grafanaAnalyseCmd.Flag("key", "Api key to use when contacting Grafana, alternatively set $GRAFANA_API_KEY.").
		Envar("GRAFANA_API_KEY").
		Default("").
		StringVar(&gaCmd.apiKey)
	grafanaAnalyseCmd.Flag("read-timeout", "timeout for read requests").
		Default("300s").
		DurationVar(&gaCmd.readTimeout)
	grafanaAnalyseCmd.Flag("output", "The path for the output file").
		Default("metrics-in-grafana.json").
		StringVar(&gaCmd.outputFile)

	raCmd := &RulerAnalyseCommand{}
	rulerAnalyseCmd := analyseCmd.Command("ruler", "Analyse and extract the metrics used in Cortex rules").
		Action(raCmd.run)
	rulerAnalyseCmd.Flag("address", "Address of the Prometheus/Cortex instance, alternatively set $CORTEX_ADDRESS.").
		Envar("CORTEX_ADDRESS").
		Required().
		StringVar(&raCmd.ClientConfig.Address)
	rulerAnalyseCmd.Flag("id", "Username to use when contacting Prometheus/Cortex, alternatively set $CORTEX_TENANT_ID.").
		Envar("CORTEX_TENANT_ID").
		Default("").
		StringVar(&raCmd.ClientConfig.ID)
	rulerAnalyseCmd.Flag("key", "Password to use when contacting Prometheus/Cortex, alternatively set $CORTEX_API_KEY.").
		Envar("CORTEX_API_KEY").
		Default("").
		StringVar(&raCmd.ClientConfig.Key)
	rulerAnalyseCmd.Flag("output", "The path for the output file").
		Default("metrics-in-ruler.json").
		StringVar(&raCmd.outputFile)

	daCmd := &DashboardAnalyseCommand{}
	dashboardAnalyseCmd := analyseCmd.Command("dashboard", "Analyse and output the metrics used in Grafana dashboard files").Action(daCmd.run)
	dashboardAnalyseCmd.Arg("files", "Dashboard files").
		Required().
		ExistingFilesVar(&daCmd.DashFilesList)
	dashboardAnalyseCmd.Flag("output", "The path for the output file").
		Default("metrics-in-grafana.json").
		StringVar(&daCmd.outputFile)

	rfCmd := &RuleFileAnalyseCommand{}
	ruleFileAnalyseCmd := analyseCmd.Command("rule-file", "Analyse and output the metrics used in Prometheus rules files").Action(rfCmd.run)
	ruleFileAnalyseCmd.Arg("files", "Rules files").
		Required().
		ExistingFilesVar(&rfCmd.RuleFilesList)
	ruleFileAnalyseCmd.Flag("output", "The path for the output file").
		Default("metrics-in-ruler.json").
		StringVar(&rfCmd.outputFile)
}
