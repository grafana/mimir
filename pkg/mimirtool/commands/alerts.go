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
	"path/filepath"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	gokitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/featurecontrol"
	"github.com/prometheus/alertmanager/matchers/compat"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"golang.org/x/term"

	"github.com/grafana/mimir/pkg/mimirtool/client"
	"github.com/grafana/mimir/pkg/mimirtool/printer"
)

// AlertmanagerCommand configures and executes rule related mimir api operations
type AlertmanagerCommand struct {
	ClientConfig           client.Config
	AlertmanagerURL        url.URL
	AlertmanagerConfigFile string
	TemplateFiles          []string
	DisableColor           bool
	ForceColor             bool
	ValidateOnly           bool
	OutputDir              string
	UTF8StrictMode         bool

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

	// Metrics.
	nonDuplicateAlerts prometheus.Gauge
}

// Register rule related commands and flags with the kingpin application
func (a *AlertmanagerCommand) Register(app *kingpin.Application, envVars EnvVarNames) {
	alertCmd := app.Command("alertmanager", "View and edit Alertmanager configurations that are stored in Grafana Mimir.").PreAction(a.setup)
	alertCmd.Flag("user", fmt.Sprintf("Basic auth API user to use when contacting Grafana Mimir; alternatively, set %s. If empty, %s is used instead.", envVars.APIUser, envVars.TenantID)).Default("").Envar(envVars.APIUser).StringVar(&a.ClientConfig.User)
	alertCmd.Flag("key", "Basic auth API key to use when contacting Grafana Mimir; alternatively, set "+envVars.APIKey+".").Default("").Envar(envVars.APIKey).StringVar(&a.ClientConfig.Key)
	alertCmd.Flag("tls-ca-path", "TLS CA certificate to verify Grafana Mimir API as part of mTLS; alternatively, set "+envVars.TLSCAPath+".").Default("").Envar(envVars.TLSCAPath).StringVar(&a.ClientConfig.TLS.CAPath)
	alertCmd.Flag("tls-cert-path", "TLS client certificate to authenticate with the Grafana Mimir API as part of mTLS; alternatively, set "+envVars.TLSCertPath+".").Default("").Envar(envVars.TLSCertPath).StringVar(&a.ClientConfig.TLS.CertPath)
	alertCmd.Flag("tls-key-path", "TLS client certificate private key to authenticate with the Grafana Mimir API as part of mTLS; alternatively, set "+envVars.TLSKeyPath+".").Default("").Envar(envVars.TLSKeyPath).StringVar(&a.ClientConfig.TLS.KeyPath)
	alertCmd.Flag("tls-insecure-skip-verify", "Skip TLS certificate verification; alternatively, set "+envVars.TLSInsecureSkipVerify+".").Default("false").Envar(envVars.TLSInsecureSkipVerify).BoolVar(&a.ClientConfig.TLS.InsecureSkipVerify)
	alertCmd.Flag("auth-token", "Authentication token bearer authentication; alternatively, set "+envVars.AuthToken+".").Default("").Envar(envVars.AuthToken).StringVar(&a.ClientConfig.AuthToken)
	alertCmd.Flag("utf8-strict-mode", "Enable UTF-8 strict mode. Allows UTF-8 characters in the matchers for routes and inhibition rules, in silences, and in the labels for alerts.").Default("false").BoolVar(&a.UTF8StrictMode)

	// Get Alertmanager Configs Command
	getAlertsCmd := alertCmd.Command("get", "Get the Alertmanager configuration that is currently in the Grafana Mimir Alertmanager.").Action(a.getConfig)
	getAlertsCmd.Flag("disable-color", "disable colored output").BoolVar(&a.DisableColor)
	getAlertsCmd.Flag("force-color", "force colored output").BoolVar(&a.ForceColor)
	getAlertsCmd.Flag("output-dir", "The directory where the config and templates will be written to and disables printing to console.").ExistingDirVar(&a.OutputDir)

	deleteCmd := alertCmd.Command("delete", "Delete the Alertmanager configuration that is currently in the Grafana Mimir Alertmanager.").Action(a.deleteConfig)

	loadalertCmd := alertCmd.Command("load", "Load Alertmanager tenant configuration and template files into Grafana Mimir.").Action(a.loadConfig)
	loadalertCmd.Arg("config", "Alertmanager configuration file to load").Required().StringVar(&a.AlertmanagerConfigFile)
	loadalertCmd.Arg("template-files", "The template files to load").ExistingFilesVar(&a.TemplateFiles)

	for _, cmd := range []*kingpin.CmdClause{getAlertsCmd, deleteCmd, loadalertCmd} {
		cmd.Flag("address", "Address of the Grafana Mimir cluster; alternatively, set "+envVars.Address+".").Envar(envVars.Address).Required().StringVar(&a.ClientConfig.Address)
		cmd.Flag("id", "Grafana Mimir tenant ID; alternatively, set "+envVars.TenantID+". Used for X-Scope-OrgID HTTP header. Also used for basic auth if --user is not provided.").Envar(envVars.TenantID).Required().StringVar(&a.ClientConfig.ID)
		a.ClientConfig.ExtraHeaders = map[string]string{}
		cmd.Flag("extra-headers", "Extra headers to add to the requests in header=value format, alternatively set newline separated "+envVars.ExtraHeaders+".").Envar(envVars.ExtraHeaders).StringMapVar(&a.ClientConfig.ExtraHeaders)
	}

	migrateCmd := alertCmd.Command("migrate-utf8", "Migrate the Alertmanager tenant configuration for UTF-8.").Action(a.migrateConfig)
	migrateCmd.Arg("config", "Alertmanager configuration file to load").Required().StringVar(&a.AlertmanagerConfigFile)
	migrateCmd.Arg("template-files", "The template files to load").ExistingFilesVar(&a.TemplateFiles)
	migrateCmd.Flag("disable-color", "disable colored output").BoolVar(&a.DisableColor)
	migrateCmd.Flag("force-color", "force colored output").BoolVar(&a.ForceColor)
	migrateCmd.Flag("output-dir", "The directory where the migrated configuration and templates will be written to and disables printing to console.").ExistingDirVar(&a.OutputDir)

	verifyalertCmd := alertCmd.Command("verify", "Verify Alertmanager tenant configuration and template files.").Action(a.verifyAlertmanagerConfig)
	verifyalertCmd.Arg("config", "Alertmanager configuration to verify").Required().StringVar(&a.AlertmanagerConfigFile)
	verifyalertCmd.Arg("template-files", "The template files to verify").ExistingFilesVar(&a.TemplateFiles)

	trCmd := &TemplateRenderCmd{}
	renderCmd := alertCmd.Command("render", "Render a given definition in a template file to standard output.").Action(trCmd.render)
	renderCmd.Flag("template.glob", "Glob of paths that will be expanded and used for rendering.").Required().StringsVar(&trCmd.TemplateFilesGlobs)
	renderCmd.Flag("template.text", "The template that will be rendered.").Required().StringVar(&trCmd.TemplateText)
	renderCmd.Flag("template.type", "The type of the template. Can be either text (default) or html.").EnumVar(&trCmd.TemplateType, "html", "text")
	renderCmd.Flag("template.data", "Full path to a file which contains the data of the alert(-s) with which the --template-text will be rendered. Must be in JSON. File must be formatted according to the following layout: https://pkg.go.dev/github.com/prometheus/alertmanager/template#Data. If none has been specified then a predefined, simple alert will be used for rendering.").FileVar(&trCmd.TemplateData)
	renderCmd.Flag("id", "Basic auth username to use when rendering template used by the function `tenantID`, also set as tenant ID; alternatively, set "+envVars.TenantID+".").
		Envar(envVars.TenantID).
		Default("").
		StringVar(&trCmd.TenantID)
}

func (a *AlertmanagerCommand) setup(_ *kingpin.ParseContext) error {
	// The default mode for mimirtool is to first use the new parser for UTF-8 matchers,
	// and if it fails, fallback to the classic parser. If this happens, it also logs a
	// warning to stdout. It is possible to disable to fallback, and use just the UTF-8
	// parser, with the -utf8-strict-mode flag. This will help operators ensure their
	// configurations are compatible with the new parser going forward.
	l := level.NewFilter(gokitlog.NewLogfmtLogger(os.Stdout), level.AllowInfo())
	features := ""
	if a.UTF8StrictMode {
		features = featurecontrol.FeatureUTF8StrictMode
	}
	flags, err := featurecontrol.NewFlags(l, features)
	if err != nil {
		return err
	}
	compat.InitFromFlags(l, flags)

	cli, err := client.New(a.ClientConfig)
	if err != nil {
		return err
	}
	a.cli = cli

	return nil
}

func (a *AlertmanagerCommand) getConfig(_ *kingpin.ParseContext) error {
	cfg, templates, err := a.cli.GetAlertmanagerConfig(context.Background())
	if err != nil {
		if errors.Is(err, client.ErrResourceNotFound) {
			log.Infof("no Alertmanager config currently exists for this user")
			return nil
		}
		return err
	}

	if a.OutputDir == "" {
		p := printer.New(a.DisableColor, a.ForceColor, term.IsTerminal(int(os.Stdout.Fd())))
		return p.PrintAlertmanagerConfig(cfg, templates)
	}
	return a.outputAlertManagerConfigTemplates(cfg, templates)
}

func (a *AlertmanagerCommand) outputAlertManagerConfigTemplates(config string, templates map[string]string) error {
	var baseDir string
	var fileOutputLocation string
	baseDir, err := filepath.Abs(a.OutputDir)
	if err != nil {
		return err
	}
	fileOutputLocation = filepath.Join(baseDir, "config.yaml")
	log.Debugf("writing the config file to %s", fileOutputLocation)
	err = os.WriteFile(fileOutputLocation, []byte(config), os.FileMode(0o600))
	if err != nil {
		return err
	}

	for fn, template := range templates {
		fileOutputLocation = filepath.Join(baseDir, fn)
		log.Debugf("writing the template file to %s", fileOutputLocation)
		err = os.WriteFile(fileOutputLocation, []byte(template), os.FileMode(0o600))
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *AlertmanagerCommand) readAlertManagerConfig() (string, map[string]string, error) {
	content, err := os.ReadFile(a.AlertmanagerConfigFile)
	if err != nil {
		return "", nil, errors.Wrap(err, "unable to load config file: "+a.AlertmanagerConfigFile)
	}

	cfg := string(content)
	_, err = config.Load(cfg)
	if err != nil {
		return "", nil, err
	}

	templates, err := a.readAlertManagerConfigTemplates()
	if err != nil {
		return "", nil, err
	}

	return cfg, templates, nil
}

func (a *AlertmanagerCommand) readAlertManagerConfigTemplates() (map[string]string, error) {
	templates := map[string]string{}
	originalPaths := map[string]string{}
	for _, f := range a.TemplateFiles {
		tmpl, err := os.ReadFile(f)
		if err != nil {
			return nil, errors.Wrap(err, "unable to load template file: "+f)
		}
		name := filepath.Base(f)
		if _, ok := templates[name]; ok {
			return nil, errors.Errorf("cannot have multiple templates with same file names but different paths: %s collides with %s", f, originalPaths[name])
		}
		templates[name] = string(tmpl)
		originalPaths[name] = f
	}
	return templates, nil
}

func (a *AlertmanagerCommand) verifyAlertmanagerConfig(_ *kingpin.ParseContext) error {
	_, _, err := a.readAlertManagerConfig()
	return err
}

func (a *AlertmanagerCommand) loadConfig(_ *kingpin.ParseContext) error {
	cfg, templates, err := a.readAlertManagerConfig()
	if err != nil {
		return err
	}
	return a.cli.CreateAlertmanagerConfig(context.Background(), cfg, templates)
}

func (a *AlertmanagerCommand) deleteConfig(_ *kingpin.ParseContext) error {
	err := a.cli.DeleteAlermanagerConfig(context.Background())
	if err != nil && !errors.Is(err, client.ErrResourceNotFound) {
		return err
	}
	return nil
}

func (a *AlertmanagerCommand) migrateConfig(_ *kingpin.ParseContext) error {
	cfg, templates, err := a.readAlertManagerConfig()
	if err != nil {
		return err
	}
	cfg, err = migrateCfg(cfg)
	if err != nil {
		return fmt.Errorf("failed to migrate cfg: %w", err)
	}
	if a.OutputDir == "" {
		p := printer.New(a.DisableColor, a.ForceColor, term.IsTerminal(int(os.Stdout.Fd())))
		return p.PrintAlertmanagerConfig(cfg, templates)
	}
	return a.outputAlertManagerConfigTemplates(cfg, templates)
}

func (a *AlertCommand) Register(app *kingpin.Application, envVars EnvVarNames, reg prometheus.Registerer) {
	alertCmd := app.Command("alerts", "View active alerts in alertmanager.").PreAction(func(k *kingpin.ParseContext) error { return a.setup(k, reg) })
	alertCmd.Flag("address", "Address of the Grafana Mimir cluster, alternatively set "+envVars.Address+".").Envar(envVars.Address).Required().StringVar(&a.ClientConfig.Address)
	alertCmd.Flag("id", "Mimir tenant id, alternatively set "+envVars.TenantID+". Used for X-Scope-OrgID HTTP header. Also used for basic auth if --user is not provided..").Envar(envVars.TenantID).Required().StringVar(&a.ClientConfig.ID)
	alertCmd.Flag("user", fmt.Sprintf("Basic auth username to use when contacting Grafana Mimir, alternatively set %s. If empty, %s will be used instead. ", envVars.APIUser, envVars.TenantID)).Default("").Envar(envVars.APIUser).StringVar(&a.ClientConfig.User)
	alertCmd.Flag("key", "Basic auth password to use when contacting Grafana Mimir; alternatively, set "+envVars.APIKey+".").Default("").Envar(envVars.APIKey).StringVar(&a.ClientConfig.Key)
	alertCmd.Flag("auth-token", "Authentication token for bearer token or JWT auth, alternatively set "+envVars.AuthToken+".").Default("").Envar(envVars.AuthToken).StringVar(&a.ClientConfig.AuthToken)
	a.ClientConfig.ExtraHeaders = map[string]string{}
	alertCmd.Flag("extra-headers", "Extra headers to add to the requests in header=value format, alternatively set newline separated "+envVars.ExtraHeaders+".").Envar(envVars.ExtraHeaders).StringMapVar(&a.ClientConfig.ExtraHeaders)

	verifyAlertsCmd := alertCmd.Command("verify", "Verifies whether or not alerts in an Alertmanager cluster are deduplicated; useful for verifying correct configuration when transferring from Prometheus to Grafana Mimir alert evaluation.").Action(a.verifyConfig)
	verifyAlertsCmd.Flag("ignore-alerts", "A comma separated list of Alert names to ignore in deduplication checks.").StringVar(&a.IgnoreString)
	verifyAlertsCmd.Flag("source-label", "Label to look for when deciding if two alerts are duplicates of each other from separate sources.").Default("prometheus").StringVar(&a.SourceLabel)
	verifyAlertsCmd.Flag("grace-period", "Grace period, don't consider alert groups with the incorrect amount of alert replicas erroneous unless the alerts have existed for more than this amount of time, in minutes.").Default("2").IntVar(&a.GracePeriod)
	verifyAlertsCmd.Flag("frequency", "Setting this value will turn mimirtool into a long-running process, running the alerts verify check every # of minutes specified").IntVar(&a.CheckFrequency)
}

func (a *AlertCommand) setup(_ *kingpin.ParseContext, reg prometheus.Registerer) error {
	a.nonDuplicateAlerts = promauto.With(reg).NewGauge(
		prometheus.GaugeOpts{
			Name: "mimirtool_alerts_single_source",
			Help: "Alerts found by the alerts verify command that are coming from a single source rather than multiple sources..",
		},
	)

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

func (a *AlertCommand) verifyConfig(_ *kingpin.ParseContext) error {
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
	reg.MustRegister(a.nonDuplicateAlerts)
	reg.MustRegister(goStats)

	http.Handle("/metrics", promhttp.HandlerFor(
		reg,
		promhttp.HandlerOpts{},
	))

	go func() {
		server := http.Server{
			Addr:         ":9090",
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
		}
		log.Fatal(server.ListenAndServe())
	}()

	ctx := context.Background()

	ctx, cancel := context.WithCancelCause(ctx)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	defer func() {
		signal.Stop(c)
		cancel(cancellation.NewErrorf("application stopped"))
	}()
	var lastErr error
	var n int

	go func() {
		ticker := time.NewTicker(time.Duration(a.CheckFrequency) * time.Minute)
		for {
			n, lastErr = a.runVerifyQuery(ctx, query)
			a.nonDuplicateAlerts.Set(float64(n))
			select {
			case <-c:
				cancel(cancellation.NewErrorf("application received shutdown signal"))
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
