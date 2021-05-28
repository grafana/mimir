package alerting

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/grafana/cortex-tools/pkg/client"
	"github.com/grafana/cortex-tools/pkg/rules/rwrulefmt"
	"github.com/prometheus/client_golang/prometheus"
	yaml "gopkg.in/yaml.v3"
)

// Case represents a metric that can be used for exporting, then verified against an Alertmanager webhook
type Case interface {
	prometheus.Collector

	Name() string
}

// CortexClient represents a client for syncing ruler and alertmanager configuration with Cortex
type CortexClient interface {
	CreateAlertmanagerConfig(ctx context.Context, cfg string, templates map[string]string) error
	CreateRuleGroup(ctx context.Context, namespace string, rg rwrulefmt.RuleGroup) error
}

// GaugeCase represents a case in the form of a gauge
type GaugeCase struct {
	prometheus.GaugeFunc
	name string
}

// Name represents the name of the case
func (d *GaugeCase) Name() string {
	return d.name
}

type RunnerConfig struct {
	AlertmanagerURL string
	AlertmanagerID  string
	RulerURL        string
	RulerID         string
	User            string
	Key             string

	RulesConfigFile        string
	AlertmanagerConfigFile string
	ConfigSyncInterval     time.Duration
}

func (cfg *RunnerConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.AlertmanagerConfigFile, "configs.alertmanager-file", "", "Filepath of the Alertmanager configuration file")
	f.StringVar(&cfg.AlertmanagerURL, "configs.alertmanager-url", "", "The URL under the Alertmanager is reachable")
	f.StringVar(&cfg.AlertmanagerID, "configs.alertmanager-id", "", "The user ID of the Alertmanager tenant")

	f.StringVar(&cfg.RulesConfigFile, "configs.rulegroup-file", "", "Filepath of the alert and recording rules configuration file")
	f.StringVar(&cfg.RulerURL, "configs.ruler-url", "", "The URL under the Ruler is reachable")
	f.StringVar(&cfg.RulerID, "configs.ruler-id", "", "The user ID of the Ruler tenant")

	f.StringVar(&cfg.User, "configs.user", "", "The API user to use for syncing configuration. The same user is used for both the alertmanager and ruler. If empty, configs.ruler-id is used instead.")
	f.StringVar(&cfg.Key, "configs.key", "", "The API key to use for syncing configuration. The same key is used for both the alertmanager and ruler.")
	f.DurationVar(&cfg.ConfigSyncInterval, "configs.sync-interval", 30*time.Minute, "How often should we sync the configuration with the ruler and alertmanager")
}

// Runner runs a set of cases for evaluation
type Runner struct {
	logger log.Logger
	cfg    RunnerConfig

	mtx   sync.RWMutex
	cases []Case

	quit        chan struct{}
	wg          sync.WaitGroup
	amClient    CortexClient
	amConfig    []byte
	rulerClient CortexClient
	rulerConfig rwrulefmt.RuleGroup
}

// NewRunner returns a runner that holds cases for collection and evaluation.
func NewRunner(cfg RunnerConfig, logger log.Logger) (*Runner, error) {
	// Create the client meant to communicate with the Alertmanager
	amClient, err := client.New(client.Config{
		Address: cfg.AlertmanagerURL,
		ID:      cfg.AlertmanagerID,
		User:    cfg.User,
		Key:     cfg.Key,
	})
	if err != nil {
		return nil, err
	}

	// Create the client meant to communicate with the Ruler
	rulerClient, err := client.New(client.Config{
		Address: cfg.RulerURL,
		ID:      cfg.RulerID,
		User:    cfg.User,
		Key:     cfg.Key,
	})
	if err != nil {
		return nil, err
	}

	var amConfig []byte
	if cfg.AlertmanagerConfigFile != "" {
		amConfig, err = ioutil.ReadFile(cfg.AlertmanagerConfigFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read Alertmanager configuration file %q: %s", cfg.AlertmanagerConfigFile, err)
		}
		level.Info(logger).Log("msg", "alertmanager configuration loaded")
	}

	var rulerConfig rwrulefmt.RuleGroup
	if cfg.RulesConfigFile != "" {
		b, err := ioutil.ReadFile(cfg.RulesConfigFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read Rules configuration file %q: %s", cfg.RulesConfigFile, err)
		}

		decoder := yaml.NewDecoder(bytes.NewReader(b))
		decoder.KnownFields(true)
		if err := decoder.Decode(&rulerConfig); err != nil {
			return nil, fmt.Errorf("unable to load the Rules configuration file %q: $%s", cfg.RulesConfigFile, err)
		}
		level.Info(logger).Log("msg", "ruler configuration loaded")
	}

	tc := &Runner{
		cfg:    cfg,
		logger: logger,

		amClient:    amClient,
		amConfig:    amConfig,
		rulerClient: rulerClient,
		rulerConfig: rulerConfig,

		quit: make(chan struct{}),
	}

	tc.wg.Add(1)
	go tc.pushRulerAndAMConfig()
	return tc, nil
}

// Stop the checking goroutine.
func (r *Runner) Stop() {
	close(r.quit)
	r.wg.Wait()
}

// Add a new TestCase.
func (r *Runner) Add(tc Case) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.cases = append(r.cases, tc)
}

// Describe implements prometheus.Collector.
func (r *Runner) Describe(c chan<- *prometheus.Desc) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	for _, t := range r.cases {
		t.Describe(c)
	}
}

// Collect implements prometheus.Collector.
func (r *Runner) Collect(c chan<- prometheus.Metric) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	for _, t := range r.cases {
		t.Collect(c)
	}
}

func (r *Runner) pushRulerAndAMConfig() {
	defer r.wg.Add(-1)

	if r.amConfig == nil && len(r.rulerConfig.Rules) == 0 {
		level.Info(r.logger).Log("msg", "no ruler or Alertmanager configuration - skipping sync")
		return
	}

	level.Info(r.logger).Log("msg", "starting sync with Alertmanager and ruler")
	r.syncRuler()
	r.syncAlertmanager()

	ticker := time.NewTicker(time.Duration(r.cfg.ConfigSyncInterval))
	defer ticker.Stop()

	for {
		select {
		case <-r.quit:
			return
		case <-ticker.C:
			r.syncRuler()
			r.syncAlertmanager()
		}
	}
}

func (r *Runner) syncAlertmanager() {
	err := r.amClient.CreateAlertmanagerConfig(context.Background(), string(r.amConfig), map[string]string{})
	if err != nil {
		level.Error(r.logger).Log("msg", "failed to sync configuration with Alertmanager", "err", err)
		return
	}

	level.Info(r.logger).Log("msg", "sync with Alertmanager complete")
}

func (r *Runner) syncRuler() {
	err := r.rulerClient.CreateRuleGroup(context.Background(), "e2ealerting", r.rulerConfig)
	if err != nil {
		level.Error(r.logger).Log("msg", "failed to sync configuration with Ruler", "err", err)
		return
	}

	level.Info(r.logger).Log("msg", "sync with ruler complete")
}

// NewGaugeCase creates a gauge metric that exposes the current time when collected.
func NewGaugeCase(name string) Case {
	return &GaugeCase{
		name: name,
		GaugeFunc: prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      name,
				Help:      "Exposes the time of the scrape as its value to help measure end to end latency upon receiving an alert on it.",
			},
			func() float64 {
				return float64(time.Now().Unix())
			},
		),
	}
}
