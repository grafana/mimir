// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/alerts.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/pkg/mimirtool/client"
	"github.com/grafana/mimir/pkg/mimirtool/printer"
)

var (
	nonDuplicateAlerts = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "mimirtool_alerts_single_source",
			Help: "Alerts found by the alerts verify command that are coming from a single source rather than multiple sources..",
		},
	)
)

// AlertmanagerCommand configures and executes rule related mimir api operations
type AlertmanagerCommand struct {
	ClientConfig           client.Config
	AlertmanagerURL        url.URL
	AlertmanagerConfigFile string
	TemplateFiles          []string
	DisableColor           bool

	cli *client.MimirClient
}

// AlertCommand configures and executes rule related PromQL queries for alerts comparison.
type AlertCommand struct {
	CortexURL      string
	IgnoreString   string
	IgnoreAlerts   map[string]interface{}
	SourceLabel    string
	NumSources     int
	GracePeriod    int
	CheckFrequency int
	ClientConfig   client.Config
	cli            *client.MimirClient
}

// Register rule related commands and flags with the kingpin application
func (a *AlertmanagerCommand) Register(app *kingpin.Application, envVars EnvVarNames) {
	alertCmd := app.Command("alertmanager", "View and edit Alertmanager configurations that are stored in Grafana Mimir.").PreAction(a.setup)
	alertCmd.Flag("address", "Address of the Grafana Mimir cluster; alternatively, set "+envVars.Address+".").Envar(envVars.Address).Required().StringVar(&a.ClientConfig.Address)
	alertCmd.Flag("id", "Grafana Mimir tenant ID; alternatively, set "+envVars.TenantID+".").Envar(envVars.TenantID).Required().StringVar(&a.ClientConfig.ID)
	alertCmd.Flag("user", fmt.Sprintf("API user to use when contacting Grafana Mimir; alternatively, set %s. If empty, %s is used instead.", envVars.APIUser, envVars.TenantID)).Default("").Envar(envVars.APIUser).StringVar(&a.ClientConfig.User)
	alertCmd.Flag("key", "API key to use when contacting Grafana Mimir; alternatively, set "+envVars.APIKey+".").Default("").Envar(envVars.APIKey).StringVar(&a.ClientConfig.Key)
	alertCmd.Flag("tls-ca-path", "TLS CA certificate to verify Grafana Mimir API as part of mTLS; alternatively, set "+envVars.TLSCAPath+".").Default("").Envar(envVars.TLSCAPath).StringVar(&a.ClientConfig.TLS.CAPath)
	alertCmd.Flag("tls-cert-path", "TLS client certificate to authenticate with the Grafana Mimir API as part of mTLS; alternatively, set "+envVars.TLSCertPath+".").Default("").Envar(envVars.TLSCertPath).StringVar(&a.ClientConfig.TLS.CertPath)
	alertCmd.Flag("tls-key-path", "TLS client certificate private key to authenticate with the Grafana Mimir API as part of mTLS; alternatively, set "+envVars.TLSKeyPath+".").Default("").Envar(envVars.TLSKeyPath).StringVar(&a.ClientConfig.TLS.KeyPath)

	// Get Alertmanager Configs Command
	getAlertsCmd := alertCmd.Command("get", "Get the Alertmanager configuration that is currently in the Grafana Mimir Alertmanager.").Action(a.getConfig)
	getAlertsCmd.Flag("disable-color", "disable colored output").BoolVar(&a.DisableColor)

	alertCmd.Command("delete", "Delete the Alertmanager configuration that is currently in the Grafana Mimir Alertmanager.").Action(a.deleteConfig)

	loadalertCmd := alertCmd.Command("load", "Load a set of rules to a designated Grafana Mimir endpoint").Action(a.loadConfig)
	loadalertCmd.Arg("config", "alertmanager configuration to load").Required().StringVar(&a.AlertmanagerConfigFile)
	loadalertCmd.Arg("template-files", "The template files to load").ExistingFilesVar(&a.TemplateFiles)
}

func (a *AlertmanagerCommand) setup(k *kingpin.ParseContext) error {
	cli, err := client.New(a.ClientConfig)
	if err != nil {
		return err
	}
	a.cli = cli

	return nil
}

func (a *AlertmanagerCommand) getConfig(k *kingpin.ParseContext) error {
	cfg, templates, err := a.cli.GetAlertmanagerConfig(context.Background())
	if err != nil {
		if err == client.ErrResourceNotFound {
			log.Infof("no Alertmanager config currently exists for this user")
			return nil
		}
		return err
	}

	p := printer.New(a.DisableColor)

	return p.PrintAlertmanagerConfig(cfg, templates)
}

func (a *AlertmanagerCommand) loadConfig(k *kingpin.ParseContext) error {
	content, err := os.ReadFile(a.AlertmanagerConfigFile)
	if err != nil {
		return errors.Wrap(err, "unable to load config file: "+a.AlertmanagerConfigFile)
	}

	cfg := string(content)
	_, err = config.Load(cfg)
	if err != nil {
		return err
	}

	templates := map[string]string{}
	for _, f := range a.TemplateFiles {
		tmpl, err := os.ReadFile(f)
		if err != nil {
			return errors.Wrap(err, "unable to load template file: "+f)
		}
		templates[f] = string(tmpl)
	}

	return a.cli.CreateAlertmanagerConfig(context.Background(), cfg, templates)
}

func (a *AlertmanagerCommand) deleteConfig(k *kingpin.ParseContext) error {
	err := a.cli.DeleteAlermanagerConfig(context.Background())
	if err != nil && err != client.ErrResourceNotFound {
		return err
	}
	return nil
}

func (a *AlertCommand) Register(app *kingpin.Application, envVars EnvVarNames) {
	alertCmd := app.Command("alerts", "View active alerts in alertmanager.").PreAction(a.setup)
	alertCmd.Flag("address", "Address of the Grafana Mimir cluster, alternatively set "+envVars.Address+".").Envar(envVars.Address).Required().StringVar(&a.ClientConfig.Address)
	alertCmd.Flag("id", "Mimir tenant id, alternatively set "+envVars.TenantID+".").Envar(envVars.TenantID).Required().StringVar(&a.ClientConfig.ID)
	alertCmd.Flag("user", fmt.Sprintf("API user to use when contacting Grafana Mimir, alternatively set %s. If empty, %s will be used instead.", envVars.APIUser, envVars.TenantID)).Default("").Envar(envVars.APIUser).StringVar(&a.ClientConfig.User)
	alertCmd.Flag("key", "API key to use when contacting Grafana Mimir; alternatively, set "+envVars.APIKey+".").Default("").Envar(envVars.APIKey).StringVar(&a.ClientConfig.Key)

	verifyAlertsCmd := alertCmd.Command("verify", "Verifies whether or not alerts in an Alertmanager cluster are deduplicated; useful for verifying correct configuration when transferring from Prometheus to Grafana Mimir alert evaluation.").Action(a.verifyConfig)
	verifyAlertsCmd.Flag("ignore-alerts", "A comma separated list of Alert names to ignore in deduplication checks.").StringVar(&a.IgnoreString)
	verifyAlertsCmd.Flag("source-label", "Label to look for when deciding if two alerts are duplicates of each other from separate sources.").Default("prometheus").StringVar(&a.SourceLabel)
	verifyAlertsCmd.Flag("grace-period", "Grace period, don't consider alert groups with the incorrect amount of alert replicas erroneous unless the alerts have existed for more than this amount of time, in minutes.").Default("2").IntVar(&a.GracePeriod)
	verifyAlertsCmd.Flag("frequency", "Setting this value will turn mimirtool into a long-running process, running the alerts verify check every # of minutes specified").IntVar(&a.CheckFrequency)
}

func (a *AlertCommand) setup(k *kingpin.ParseContext) error {
	cli, err := client.New(a.ClientConfig)
	if err != nil {
		return err
	}
	a.cli = cli

	return nil
}

type queryResult struct {
	Status string    `json:"status"`
	Data   queryData `json:"data"`
}

type queryData struct {
	ResultType string   `json:"resultType"`
	Result     []metric `json:"result"`
}

type metric struct {
	Metric map[string]string `json:"metric"`
}

func (a *AlertCommand) verifyConfig(k *kingpin.ParseContext) error {
	var empty interface{}
	if a.IgnoreString != "" {
		a.IgnoreAlerts = make(map[string]interface{})
		chunks := strings.Split(a.IgnoreString, ",")

		for _, name := range chunks {
			a.IgnoreAlerts[name] = empty
			log.Info("Ignoring alerts with name: ", name)
		}
	}
	lhs := fmt.Sprintf("ALERTS{source!=\"%s\", alertstate=\"firing\"} offset %dm unless ignoring(source) ALERTS{source=\"%s\", alertstate=\"firing\"}",
		a.SourceLabel,
		a.GracePeriod,
		a.SourceLabel)
	rhs := fmt.Sprintf("ALERTS{source=\"%s\", alertstate=\"firing\"} offset %dm unless ignoring(source) ALERTS{source!=\"%s\", alertstate=\"firing\"}",
		a.SourceLabel,
		a.GracePeriod,
		a.SourceLabel)

	query := fmt.Sprintf("%s or %s", lhs, rhs)
	if a.CheckFrequency <= 0 {
		_, err := a.runVerifyQuery(context.Background(), query)
		return err
	}

	// Use a different registerer than default so we don't get all the Mimir metrics, but include Go runtime metrics.
	goStats := collectors.NewGoCollector()
	reg := prometheus.NewRegistry()
	reg.MustRegister(nonDuplicateAlerts)
	reg.MustRegister(goStats)

	http.Handle("/metrics", promhttp.HandlerFor(
		reg,
		promhttp.HandlerOpts{},
	))

	go func() {
		log.Fatal(http.ListenAndServe(":9090", nil))
	}()

	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
		cancel()
	}()
	var lastErr error
	var n int

	go func() {
		ticker := time.NewTicker(time.Duration(a.CheckFrequency) * time.Minute)
		for {
			n, lastErr = a.runVerifyQuery(ctx, query)
			nonDuplicateAlerts.Set(float64(n))
			select {
			case <-c:
				cancel()
				return
			case <-ticker.C:
				continue
			}
		}

	}()
	<-ctx.Done()
	return lastErr
}

func (a *AlertCommand) runVerifyQuery(ctx context.Context, query string) (int, error) {
	res, err := a.cli.Query(ctx, query)

	if err != nil {
		return 0, err
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()

	var data queryResult
	err = json.Unmarshal(body, &data)
	if err != nil {
		return 0, err
	}

	for _, m := range data.Data.Result {
		if _, ok := a.IgnoreAlerts[m.Metric["alertname"]]; !ok {
			log.WithFields(log.Fields{
				"alertname": m.Metric["alertname"],
				"state":     m.Metric,
			}).Infof("alert found that was not in both sources")
		}
	}
	log.WithFields(log.Fields{"count": len(data.Data.Result)}).Infof("found mismatching alerts")
	return len(data.Data.Result), nil
}
