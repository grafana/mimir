// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/alertmanager.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/alerting/definition"
	"github.com/grafana/alerting/images"
	alertingNotify "github.com/grafana/alerting/notify"
	"github.com/grafana/alerting/notify/nfstatus"
	alertingReceivers "github.com/grafana/alerting/receivers"
	alertingTemplates "github.com/grafana/alerting/templates"
	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/api"
	"github.com/prometheus/alertmanager/cluster"
	"github.com/prometheus/alertmanager/cluster/clusterpb"
	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/dispatch"
	"github.com/prometheus/alertmanager/featurecontrol"
	"github.com/prometheus/alertmanager/inhibit"
	"github.com/prometheus/alertmanager/nflog"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/notify/discord"
	"github.com/prometheus/alertmanager/notify/email"
	"github.com/prometheus/alertmanager/notify/msteams"
	"github.com/prometheus/alertmanager/notify/opsgenie"
	"github.com/prometheus/alertmanager/notify/pagerduty"
	"github.com/prometheus/alertmanager/notify/pushover"
	"github.com/prometheus/alertmanager/notify/slack"
	"github.com/prometheus/alertmanager/notify/sns"
	"github.com/prometheus/alertmanager/notify/telegram"
	"github.com/prometheus/alertmanager/notify/victorops"
	"github.com/prometheus/alertmanager/notify/webex"
	"github.com/prometheus/alertmanager/notify/webhook"
	"github.com/prometheus/alertmanager/notify/wechat"
	"github.com/prometheus/alertmanager/provider/mem"
	"github.com/prometheus/alertmanager/silence"
	"github.com/prometheus/alertmanager/template"
	"github.com/prometheus/alertmanager/timeinterval"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/alertmanager/ui"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	commoncfg "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"

	"github.com/grafana/mimir/pkg/alertmanager/alertstore"
	util_net "github.com/grafana/mimir/pkg/util/net"
	"github.com/grafana/mimir/pkg/util/version"
)

const (
	// MaintenancePeriod is used for periodic storing of silences and notifications to local file.
	maintenancePeriod = 15 * time.Minute

	// Filenames used within tenant-directory
	notificationLogSnapshot = "notifications"
	silencesSnapshot        = "silences"
	templatesDir            = "templates"

	nflogStateKeyPrefix    = "nfl:"
	silencesStateKeyPrefix = "sil:"
)

// Config configures an Alertmanager.
type Config struct {
	UserID                            string
	Logger                            log.Logger
	PeerTimeout                       time.Duration
	Retention                         time.Duration
	MaxConcurrentGetRequestsPerTenant int
	ExternalURL                       *url.URL
	Limits                            Limits
	Features                          featurecontrol.Flagger

	// Tenant-specific local directory where AM can store its state (notifications, silences, templates). When AM is stopped, entire dir is removed.
	TenantDataDir string

	ShardingEnabled   bool
	ReplicationFactor int
	Replicator        Replicator
	Store             alertstore.AlertStore
	PersisterConfig   PersisterConfig

	GrafanaAlertmanagerCompatibility bool
	GrafanaAlertmanagerTenantSuffix  string
}

// An Alertmanager manages the alerts for one user.
type Alertmanager struct {
	cfg             *Config
	api             *api.API
	logger          log.Logger
	state           *state
	persister       *statePersister
	nflog           *nflog.Log
	silences        *silence.Silences
	marker          types.Marker
	alerts          *mem.Alerts
	dispatcher      *dispatch.Dispatcher
	inhibitor       *inhibit.Inhibitor
	pipelineBuilder *notify.PipelineBuilder
	maintenanceStop chan struct{}
	wg              sync.WaitGroup
	mux             *http.ServeMux
	registry        *prometheus.Registry
	receiversMtx    sync.Mutex
	receivers       []*nfstatus.Receiver
	templatesMtx    sync.RWMutex
	templates       []alertingTemplates.TemplateDefinition
	tmplExternalURL *url.URL
	emailCfgMtx     sync.RWMutex
	emailCfg        alertingReceivers.EmailSenderConfig

	// usingGrafanaState indicates if the Grafana Alertmanager state is being used.
	usingGrafanaState atomic.Bool

	// Pipeline created during last ApplyConfig call. Used for testing only.
	lastPipeline notify.Stage

	// The Dispatcher is the only component we need to recreate when we call ApplyConfig.
	// Given its metrics don't have any variable labels we need to re-use the same metrics.
	dispatcherMetrics *dispatch.DispatcherMetrics
	// This needs to be set to the hash of the config. All the hashes need to be same
	// for deduping of alerts to work, hence we need this metric. See https://github.com/prometheus/alertmanager/issues/596
	// Further, in upstream AM, this metric is handled using the config coordinator which we don't use
	// hence we need to generate the metric ourselves.
	configHashMetric prometheus.Gauge

	rateLimitedNotifications *prometheus.CounterVec
}

var (
	webReload = make(chan chan error)
)

func init() {
	go func() {
		// Since this is not a "normal" Alertmanager which reads its config
		// from disk, we just accept and ignore web-based reload signals. Config
		// updates are only applied externally via ApplyConfig().
		// nolint:revive // We want to drain the channel, we don't need to do anything inside the loop body.
		for range webReload {
		}
	}()
}

// State helps with replication and synchronization of notifications and silences across several alertmanager replicas.
type State interface {
	AddState(string, cluster.State, prometheus.Registerer) cluster.ClusterChannel
	Position() int
	WaitReady(context.Context) error
}

var (
	// Returned when a user is not known to all replicas.
	errAllReplicasUserNotFound = errors.New("all replicas returned user not found")
)

// Replicator is used to exchange state with peers via the ring when sharding is enabled.
type Replicator interface {
	// ReplicateStateForUser writes the given partial state to the necessary replicas.
	ReplicateStateForUser(ctx context.Context, userID string, part *clusterpb.Part) error
	// The alertmanager replication protocol relies on a position related to other replicas.
	// This position is then used to identify who should notify about the alert first.
	GetPositionForUser(userID string) int
	// ReadFullStateForUser obtains the full state from other replicas in the cluster.
	// If all the replicas were successfully contacted, but the user was not found in
	// all the replicas, then errAllReplicasUserNotFound is returned.
	ReadFullStateForUser(context.Context, string) ([]*clusterpb.FullState, error)
}

// New creates a new Alertmanager.
func New(cfg *Config, reg *prometheus.Registry) (*Alertmanager, error) {
	if cfg.TenantDataDir == "" {
		return nil, fmt.Errorf("directory for tenant-specific AlertManager is not configured")
	}

	am := &Alertmanager{
		cfg:             cfg,
		logger:          log.With(cfg.Logger, "user", cfg.UserID),
		maintenanceStop: make(chan struct{}),
		configHashMetric: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "alertmanager_config_hash",
			Help: "Hash of the currently loaded alertmanager configuration. Note that this is not a cryptographically strong hash.",
		}),

		rateLimitedNotifications: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "alertmanager_notification_rate_limited_total",
			Help: "Number of rate-limited notifications per integration.",
		}, []string{"integration"}), // "integration" is consistent with other alertmanager metrics.
	}

	am.registry = reg
	am.state = newReplicatedStates(cfg.UserID, cfg.ReplicationFactor, cfg.Replicator, cfg.Store, am.logger, am.registry)
	am.persister = newStatePersister(cfg.PersisterConfig, cfg.UserID, am.state, cfg.Store, am.logger, am.registry)

	var err error
	snapshotFile := filepath.Join(cfg.TenantDataDir, notificationLogSnapshot)
	am.nflog, err = nflog.New(nflog.Options{
		SnapshotFile: snapshotFile,
		Retention:    cfg.Retention,
		Logger:       log.With(am.logger, "component", "nflog"),
		Metrics:      am.registry,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create notification log: %v", err)
	}

	// Run the nflog maintenance in a dedicated goroutine.
	am.wg.Add(1)
	go func() {
		am.nflog.Maintenance(maintenancePeriod, snapshotFile, am.maintenanceStop, nil)
		am.wg.Done()
	}()

	c := am.state.AddState(nflogStateKeyPrefix+cfg.UserID, am.nflog, am.registry)
	am.nflog.SetBroadcast(c.Broadcast)

	am.marker = types.NewMarker(am.registry)

	silencesFile := filepath.Join(cfg.TenantDataDir, silencesSnapshot)
	am.silences, err = silence.New(silence.Options{
		SnapshotFile: silencesFile,
		Retention:    cfg.Retention,
		Limits: silence.Limits{
			MaxSilences:         func() int { return cfg.Limits.AlertmanagerMaxSilencesCount(cfg.UserID) },
			MaxSilenceSizeBytes: func() int { return cfg.Limits.AlertmanagerMaxSilenceSizeBytes(cfg.UserID) },
		},
		Logger:  log.With(am.logger, "component", "silences"),
		Metrics: am.registry,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create silences: %v", err)
	}

	c = am.state.AddState(silencesStateKeyPrefix+cfg.UserID, am.silences, am.registry)
	am.silences.SetBroadcast(c.Broadcast)

	// State replication needs to be started after the state keys are defined.
	if err := am.state.StartAsync(context.Background()); err != nil {
		return nil, errors.Wrap(err, "failed to start ring-based replication service")
	}

	if err := am.persister.StartAsync(context.Background()); err != nil {
		return nil, errors.Wrap(err, "failed to start state persister service")
	}

	am.pipelineBuilder = notify.NewPipelineBuilder(am.registry, cfg.Features)

	// Run the silences maintenance in a dedicated goroutine.
	am.wg.Add(1)
	go func() {
		am.silences.Maintenance(maintenancePeriod, silencesFile, am.maintenanceStop, nil)
		am.wg.Done()
	}()

	var callback mem.AlertStoreCallback
	if am.cfg.Limits != nil {
		callback = newAlertsLimiter(am.cfg.UserID, am.cfg.Limits, reg)
	}

	am.alerts, err = mem.NewAlerts(context.Background(), am.marker, 30*time.Minute, callback, am.logger, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to create alerts: %v", err)
	}

	am.api, err = api.New(api.Options{
		Alerts:      am.alerts,
		Silences:    am.silences,
		StatusFunc:  am.marker.Status,
		Concurrency: cfg.MaxConcurrentGetRequestsPerTenant,
		// Mimir should not expose cluster information back to its tenants.
		Peer:     &NilPeer{},
		Registry: am.registry,
		Logger:   log.With(am.logger, "component", "api"),
		GroupFunc: func(f1 func(*dispatch.Route) bool, f2 func(*types.Alert, time.Time) bool) (dispatch.AlertGroups, map[model.Fingerprint][]string) {
			return am.dispatcher.Groups(f1, f2)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create api: %v", err)
	}

	router := route.New().WithPrefix(am.cfg.ExternalURL.Path)

	ui.Register(router, webReload, log.With(am.logger, "component", "ui"))
	am.mux = am.api.Register(router, am.cfg.ExternalURL.Path)

	// Override some extra paths registered in the router (eg. /metrics which by default exposes prometheus.DefaultRegisterer).
	// Entire router is registered in Mux to "/" path, so there is no conflict with overwriting specific paths.
	for _, p := range []string{"/metrics", "/-/reload", "/debug/"} {
		a := path.Join(am.cfg.ExternalURL.Path, p)
		// Preserve end slash, as for Mux it means entire subtree.
		if strings.HasSuffix(p, "/") {
			a = a + "/"
		}
		am.mux.Handle(a, http.NotFoundHandler())
	}

	// This route is an experimental Mimir extension to the receivers API, so we put
	// it under an additional prefix to avoid any confusion with upstream Alertmanager.
	if cfg.GrafanaAlertmanagerCompatibility {
		am.mux.Handle("/api/v1/grafana/receivers", http.HandlerFunc(am.GetReceiversHandler))
		am.mux.Handle("/api/v1/grafana/templates/test", http.HandlerFunc(am.TestTemplatesHandler))
		am.mux.Handle("/api/v1/grafana/receivers/test", http.HandlerFunc(am.TestReceiversHandler))
	}

	am.dispatcherMetrics = dispatch.NewDispatcherMetrics(true, am.registry)

	//TODO: From this point onward, the alertmanager _might_ receive requests - we need to make sure we've settled and are ready.
	return am, nil
}

func (am *Alertmanager) GetReceiversHandler(w http.ResponseWriter, _ *http.Request) {
	am.receiversMtx.Lock()
	receivers := am.receivers
	am.receiversMtx.Unlock()

	response := alertingNotify.GetReceivers(receivers)

	d, err := json.Marshal(response)
	if err != nil {
		http.Error(w,
			fmt.Sprintf("error marshalling receivers: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(d); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (am *Alertmanager) TestTemplatesHandler(w http.ResponseWriter, r *http.Request) {
	c := alertingNotify.TestTemplatesConfigBodyParams{}
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		http.Error(w,
			fmt.Sprintf("error unmarshalling test templates config JSON: %s", err.Error()),
			http.StatusBadRequest)
		return
	}

	am.templatesMtx.RLock()
	tmpls := make([]alertingTemplates.TemplateDefinition, len(am.templates))
	copy(tmpls, am.templates)
	am.templatesMtx.RUnlock()

	response, err := alertingNotify.TestTemplate(r.Context(), c, tmpls, am.cfg.ExternalURL.String(), am.logger)
	if err != nil {
		http.Error(w,
			fmt.Sprintf("error testing templates: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (am *Alertmanager) TestReceiversHandler(w http.ResponseWriter, r *http.Request) {
	c := alertingNotify.TestReceiversConfigBodyParams{}
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		http.Error(w,
			fmt.Sprintf("error unmarshalling test receivers config JSON: %s", err.Error()),
			http.StatusBadRequest)
	}

	am.templatesMtx.RLock()
	tmpls := make([]string, 0, len(am.templates))
	for _, tmpl := range am.templates {
		tmpls = append(tmpls, tmpl.Template)
	}
	am.templatesMtx.RUnlock()

	response, status, err := alertingNotify.TestReceivers(r.Context(), c, tmpls, am.buildGrafanaReceiverIntegrations, am.tmplExternalURL.String())
	if err != nil {
		http.Error(w,
			fmt.Sprintf("error testing receivers: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(status)
}

func (am *Alertmanager) WaitInitialStateSync(ctx context.Context) error {
	if err := am.state.AwaitRunning(ctx); err != nil {
		return errors.Wrap(err, "failed to wait for ring-based replication service")
	}
	return nil
}

// clusterWait returns a function that inspects the current peer state and returns
// a duration of one base timeout for each peer with a higher ID than ourselves.
func clusterWait(position func() int, timeout time.Duration) func() time.Duration {
	return func() time.Duration {
		return time.Duration(position()) * timeout
	}
}

// ApplyConfig applies a new configuration to an Alertmanager.
func (am *Alertmanager) ApplyConfig(conf *definition.PostableApiAlertingConfig, tmpls []alertingTemplates.TemplateDefinition, rawCfg string, tmplExternalURL *url.URL, staticHeaders map[string]string) error {
	templates := make([]string, 0, len(tmpls))
	for _, tmpl := range tmpls {
		templates = append(templates, tmpl.Template)
	}

	tmpl, err := loadTemplates(templates, WithCustomFunctions(am.cfg.UserID))
	if err != nil {
		return err
	}
	tmpl.ExternalURL = tmplExternalURL

	cfg := definition.GrafanaToUpstreamConfig(conf)
	am.api.Update(&cfg, func(_ model.LabelSet) {})

	// Ensure inhibitor is set before being called
	if am.inhibitor != nil {
		am.inhibitor.Stop()
	}

	// Ensure dispatcher is set before being called
	if am.dispatcher != nil {
		am.dispatcher.Stop()
	}

	am.inhibitor = inhibit.NewInhibitor(am.alerts, conf.InhibitRules, am.marker, log.With(am.logger, "component", "inhibitor"))

	waitFunc := clusterWait(am.state.Position, am.cfg.PeerTimeout)

	timeoutFunc := func(d time.Duration) time.Duration {
		if d < notify.MinTimeout {
			d = notify.MinTimeout
		}
		return d + waitFunc()
	}

	integrationsMap, err := am.buildIntegrationsMap(cfg.Global, conf.Receivers, tmpl, templates, staticHeaders)
	if err != nil {
		return err
	}

	am.emailCfgMtx.Lock()
	am.emailCfg = alertingReceivers.EmailSenderConfig{
		AuthPassword:  string(cfg.Global.SMTPAuthPassword),
		AuthUser:      cfg.Global.SMTPAuthUsername,
		CertFile:      cfg.Global.HTTPConfig.TLSConfig.CertFile,
		ContentTypes:  []string{"text/html"},
		EhloIdentity:  cfg.Global.SMTPHello,
		ExternalURL:   tmpl.ExternalURL.String(),
		FromAddress:   cfg.Global.SMTPFrom,
		FromName:      "Grafana",
		Host:          cfg.Global.SMTPSmarthost.String(),
		KeyFile:       cfg.Global.HTTPConfig.TLSConfig.KeyFile,
		SkipVerify:    !cfg.Global.SMTPRequireTLS,
		StaticHeaders: staticHeaders,
		SentBy:        fmt.Sprintf("Mimir v%s", version.Version),
	}
	am.emailCfgMtx.Unlock()

	timeIntervals := make(map[string][]timeinterval.TimeInterval, len(conf.MuteTimeIntervals)+len(conf.TimeIntervals))
	for _, ti := range conf.MuteTimeIntervals {
		timeIntervals[ti.Name] = ti.TimeIntervals
	}

	for _, ti := range conf.TimeIntervals {
		timeIntervals[ti.Name] = ti.TimeIntervals
	}
	intervener := timeinterval.NewIntervener(timeIntervals)

	route := dispatch.NewRoute(cfg.Route, nil)

	receivers := make([]*nfstatus.Receiver, 0, len(integrationsMap))
	activeReceivers := alertingNotify.GetActiveReceiversMap(route)

	baseIntegrationsMap := make(map[string][]*notify.Integration)
	for name, v := range integrationsMap {
		_, isActive := activeReceivers[name]
		receivers = append(receivers, nfstatus.NewReceiver(name, isActive, v))
		baseIntegrationsMap[name] = nfstatus.GetIntegrations(v)
	}
	am.receiversMtx.Lock()
	am.receivers = receivers
	am.receiversMtx.Unlock()

	am.templatesMtx.Lock()
	am.templates = tmpls
	am.tmplExternalURL = tmplExternalURL
	am.templatesMtx.Unlock()

	pipeline := am.pipelineBuilder.New(
		baseIntegrationsMap,
		waitFunc,
		am.inhibitor,
		silence.NewSilencer(am.silences, am.marker, am.logger),
		intervener,
		am.nflog,
		am.state,
	)
	am.lastPipeline = pipeline
	am.dispatcher = dispatch.NewDispatcher(
		am.alerts,
		route,
		pipeline,
		am.marker,
		timeoutFunc,
		&dispatcherLimits{tenant: am.cfg.UserID, limits: am.cfg.Limits},
		log.With(am.logger, "component", "dispatcher", "insight", "true"),
		am.dispatcherMetrics,
	)

	go am.dispatcher.Run()
	go am.inhibitor.Run()

	am.configHashMetric.Set(md5HashAsMetricValue([]byte(rawCfg)))
	return nil
}

// Stop stops the Alertmanager.
func (am *Alertmanager) Stop() {
	if am.inhibitor != nil {
		am.inhibitor.Stop()
	}

	if am.dispatcher != nil {
		am.dispatcher.Stop()
	}

	am.persister.StopAsync()
	am.state.StopAsync()

	am.alerts.Close()
	close(am.maintenanceStop)
}

func (am *Alertmanager) StopAndWait() {
	am.Stop()

	if err := am.persister.AwaitTerminated(context.Background()); err != nil {
		level.Warn(am.logger).Log("msg", "error while stopping state persister service", "err", err)
	}

	if err := am.state.AwaitTerminated(context.Background()); err != nil {
		level.Warn(am.logger).Log("msg", "error while stopping ring-based replication service", "err", err)
	}

	am.wg.Wait()
}

func (am *Alertmanager) mergePartialExternalState(part *clusterpb.Part) error {
	return am.state.MergePartialState(part)
}

func (am *Alertmanager) mergeFullExternalState(fs *clusterpb.FullState) error {
	return am.state.MergeFullStates([]*clusterpb.FullState{fs})
}

func (am *Alertmanager) getFullState() (*clusterpb.FullState, error) {
	return am.state.GetFullState()
}

// buildIntegrationsMap builds a map of name to the list of integration notifiers off of a list of receiver config.
func (am *Alertmanager) buildIntegrationsMap(gCfg *config.GlobalConfig, nc []*definition.PostableApiReceiver, tmpl *template.Template, tmpls []string, staticHeaders map[string]string) (map[string][]*nfstatus.Integration, error) {
	// Create a firewall binded to the per-tenant config.
	firewallDialer := util_net.NewFirewallDialer(newFirewallDialerConfigProvider(am.cfg.UserID, am.cfg.Limits))

	// Create a function that wraps a notifier with rate limiting.
	nw := func(integrationName string, notifier notify.Notifier) notify.Notifier {
		if am.cfg.Limits != nil {
			rl := &tenantRateLimits{
				tenant:      am.cfg.UserID,
				limits:      am.cfg.Limits,
				integration: integrationName,
			}

			return newRateLimitedNotifier(notifier, rl, 10*time.Second, am.rateLimitedNotifications.WithLabelValues(integrationName))
		}
		return notifier
	}

	emailCfg := alertingReceivers.EmailSenderConfig{
		AuthPassword:  string(gCfg.SMTPAuthPassword),
		AuthUser:      gCfg.SMTPAuthUsername,
		CertFile:      gCfg.HTTPConfig.TLSConfig.CertFile,
		ContentTypes:  []string{"text/html"},
		EhloIdentity:  gCfg.SMTPHello,
		ExternalURL:   tmpl.ExternalURL.String(),
		FromAddress:   gCfg.SMTPFrom,
		FromName:      "Grafana",
		Host:          gCfg.SMTPSmarthost.String(),
		KeyFile:       gCfg.HTTPConfig.TLSConfig.KeyFile,
		SkipVerify:    !gCfg.SMTPRequireTLS,
		StaticHeaders: staticHeaders,
		SentBy:        fmt.Sprintf("Mimir v%s", version.Version),
	}

	var gTmpl *template.Template
	integrationsMap := make(map[string][]*nfstatus.Integration, len(nc))
	for _, rcv := range nc {
		var integrations []*nfstatus.Integration
		var err error
		if rcv.Type() == definition.GrafanaReceiverType {
			// Create the Grafana template struct if it has not already been created.
			if gTmpl == nil {
				gTmpl, err = loadTemplates(tmpls, WithCustomFunctions(am.cfg.UserID))
				if err != nil {
					return nil, err
				}
				if err := gTmpl.Parse(strings.NewReader(alertingTemplates.DefaultTemplateString)); err != nil {
					return nil, err
				}
				gTmpl.ExternalURL = tmpl.ExternalURL
			}
			integrations, err = buildGrafanaReceiverIntegrations(emailCfg, alertingNotify.PostableAPIReceiverToAPIReceiver(rcv), gTmpl, am.logger)
		} else {
			integrations, err = buildReceiverIntegrations(rcv.Receiver, tmpl, firewallDialer, am.logger, nw)
		}
		if err != nil {
			return nil, err
		}

		integrationsMap[rcv.Name] = integrations
	}

	return integrationsMap, nil
}

func (am *Alertmanager) buildGrafanaReceiverIntegrations(rcv *alertingNotify.APIReceiver, tmpl *template.Template) ([]*nfstatus.Integration, error) {
	am.emailCfgMtx.RLock()
	emailCfg := am.emailCfg
	am.emailCfgMtx.RUnlock()

	return buildGrafanaReceiverIntegrations(emailCfg, rcv, tmpl, am.logger)
}

func buildGrafanaReceiverIntegrations(emailCfg alertingReceivers.EmailSenderConfig, rcv *alertingNotify.APIReceiver, tmpl *template.Template, logger log.Logger) ([]*nfstatus.Integration, error) {
	// The decrypt functions and the context are used to decrypt the configuration.
	// We don't need to decrypt anything, so we can pass a no-op decrypt func and a context.Background().
	rCfg, err := alertingNotify.BuildReceiverConfiguration(context.Background(), rcv, alertingNotify.NoopDecode, alertingNotify.NoopDecrypt)
	if err != nil {
		return nil, err
	}

	whFn := func(alertingReceivers.Metadata) (alertingReceivers.WebhookSender, error) {
		return NewSender(logger), nil
	}

	integrations, err := alertingNotify.BuildReceiverIntegrations(
		rCfg,
		tmpl,
		&images.UnavailableProvider{}, // TODO: include images in notifications
		newLoggerFactory(logger),
		whFn,
		alertingReceivers.NewEmailSenderFactory(emailCfg),
		1, // orgID is always 1.
		version.Version,
	)
	if err != nil {
		return nil, err
	}

	return integrations, nil
}

// buildReceiverIntegrations builds a list of integration notifiers off of a
// receiver config.
// Taken from https://github.com/prometheus/alertmanager/blob/94d875f1227b29abece661db1a68c001122d1da5/cmd/alertmanager/main.go#L112-L159.
func buildReceiverIntegrations(nc config.Receiver, tmpl *template.Template, firewallDialer *util_net.FirewallDialer, logger log.Logger, wrapper func(string, notify.Notifier) notify.Notifier) ([]*nfstatus.Integration, error) {
	var (
		errs         types.MultiError
		integrations []*nfstatus.Integration
		add          = func(name string, i int, rs notify.ResolvedSender, f func(l log.Logger) (notify.Notifier, error)) {
			n, err := f(log.With(logger, "integration", name))
			if err != nil {
				errs.Add(err)
				return
			}
			n = wrapper(name, n)
			integrations = append(integrations, nfstatus.NewIntegration(n, rs, name, i, nc.Name))
		}
	)

	// Inject the firewall to any receiver integration supporting it.
	httpOps := []commoncfg.HTTPClientOption{
		commoncfg.WithDialContextFunc(firewallDialer.DialContext),
	}

	for i, c := range nc.WebhookConfigs {
		add("webhook", i, c, func(l log.Logger) (notify.Notifier, error) { return webhook.New(c, tmpl, l, httpOps...) })
	}
	for i, c := range nc.EmailConfigs {
		add("email", i, c, func(l log.Logger) (notify.Notifier, error) { return email.New(c, tmpl, l), nil })
	}
	for i, c := range nc.PagerdutyConfigs {
		add("pagerduty", i, c, func(l log.Logger) (notify.Notifier, error) { return pagerduty.New(c, tmpl, l, httpOps...) })
	}
	for i, c := range nc.OpsGenieConfigs {
		add("opsgenie", i, c, func(l log.Logger) (notify.Notifier, error) { return opsgenie.New(c, tmpl, l, httpOps...) })
	}
	for i, c := range nc.WechatConfigs {
		add("wechat", i, c, func(l log.Logger) (notify.Notifier, error) { return wechat.New(c, tmpl, l, httpOps...) })
	}
	for i, c := range nc.SlackConfigs {
		add("slack", i, c, func(l log.Logger) (notify.Notifier, error) { return slack.New(c, tmpl, l, httpOps...) })
	}
	for i, c := range nc.VictorOpsConfigs {
		add("victorops", i, c, func(l log.Logger) (notify.Notifier, error) { return victorops.New(c, tmpl, l, httpOps...) })
	}
	for i, c := range nc.PushoverConfigs {
		add("pushover", i, c, func(l log.Logger) (notify.Notifier, error) { return pushover.New(c, tmpl, l, httpOps...) })
	}
	for i, c := range nc.SNSConfigs {
		add("sns", i, c, func(l log.Logger) (notify.Notifier, error) { return sns.New(c, tmpl, l, httpOps...) })
	}
	for i, c := range nc.TelegramConfigs {
		add("telegram", i, c, func(l log.Logger) (notify.Notifier, error) { return telegram.New(c, tmpl, l, httpOps...) })
	}
	for i, c := range nc.DiscordConfigs {
		add("discord", i, c, func(l log.Logger) (notify.Notifier, error) { return discord.New(c, tmpl, l, httpOps...) })
	}
	for i, c := range nc.WebexConfigs {
		add("webex", i, c, func(l log.Logger) (notify.Notifier, error) { return webex.New(c, tmpl, l, httpOps...) })
	}
	for i, c := range nc.MSTeamsConfigs {
		add("msteams", i, c, func(l log.Logger) (notify.Notifier, error) { return msteams.New(c, tmpl, l, httpOps...) })
	}
	// If we add support for more integrations, we need to add them to validation as well. See validation.allowedIntegrationNames field.
	if errs.Len() > 0 {
		return nil, &errs
	}
	return integrations, nil
}

func md5HashAsMetricValue(data []byte) float64 {
	sum := md5.Sum(data) //nolint:gosec
	// We only want 48 bits as a float64 only has a 53 bit mantissa.
	smallSum := sum[0:6]
	var bytes = make([]byte, 8)
	copy(bytes, smallSum)
	return float64(binary.LittleEndian.Uint64(bytes))
}

// NilPeer implements the Alertmanager cluster.ClusterPeer interface used by the API to expose cluster information.
// In a multi-tenant environment, we choose not to expose these to tenants and thus are not implemented.
type NilPeer struct{}

func (p *NilPeer) Name() string                   { return "" }
func (p *NilPeer) Status() string                 { return "ready" }
func (p *NilPeer) Peers() []cluster.ClusterMember { return nil }

type firewallDialerConfigProvider struct {
	userID string
	limits Limits
}

func newFirewallDialerConfigProvider(userID string, limits Limits) firewallDialerConfigProvider {
	return firewallDialerConfigProvider{
		userID: userID,
		limits: limits,
	}
}

func (p firewallDialerConfigProvider) BlockCIDRNetworks() []flagext.CIDR {
	return p.limits.AlertmanagerReceiversBlockCIDRNetworks(p.userID)
}

func (p firewallDialerConfigProvider) BlockPrivateAddresses() bool {
	return p.limits.AlertmanagerReceiversBlockPrivateAddresses(p.userID)
}

type tenantRateLimits struct {
	tenant      string
	integration string
	limits      Limits
}

func (t *tenantRateLimits) RateLimit() rate.Limit {
	return t.limits.NotificationRateLimit(t.tenant, t.integration)
}

func (t *tenantRateLimits) Burst() int {
	return t.limits.NotificationBurstSize(t.tenant, t.integration)
}

type dispatcherLimits struct {
	tenant string
	limits Limits
}

func (g *dispatcherLimits) MaxNumberOfAggregationGroups() int {
	return g.limits.AlertmanagerMaxDispatcherAggregationGroups(g.tenant)
}

var (
	errTooManyAlerts = "too many alerts, limit: %d"
	errAlertsTooBig  = "alerts too big, total size limit: %d bytes"
)

// alertsLimiter limits the number and size of alerts being received by the Alertmanager.
// We consider an alert unique based on its fingerprint (a hash of its labels) and
// its size it's determined by the sum of bytes of its labels, annotations, and generator URL.
type alertsLimiter struct {
	tenant string
	limits Limits

	failureCounter prometheus.Counter

	mx        sync.Mutex
	sizes     map[model.Fingerprint]int
	count     int
	totalSize int
}

func newAlertsLimiter(tenant string, limits Limits, reg prometheus.Registerer) *alertsLimiter {
	limiter := &alertsLimiter{
		tenant: tenant,
		limits: limits,
		sizes:  map[model.Fingerprint]int{},
		failureCounter: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "alertmanager_alerts_insert_limited_total",
			Help: "Number of failures to insert new alerts to in-memory alert store.",
		}),
	}

	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "alertmanager_alerts_limiter_current_alerts",
		Help: "Number of alerts tracked by alerts limiter.",
	}, func() float64 {
		c, _ := limiter.currentStats()
		return float64(c)
	})

	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "alertmanager_alerts_limiter_current_alerts_size_bytes",
		Help: "Total size of alerts tracked by alerts limiter.",
	}, func() float64 {
		_, s := limiter.currentStats()
		return float64(s)
	})

	return limiter
}

func (a *alertsLimiter) PreStore(alert *types.Alert, existing bool) error {
	if alert == nil {
		return nil
	}

	fp := alert.Fingerprint()

	countLimit := a.limits.AlertmanagerMaxAlertsCount(a.tenant)
	sizeLimit := a.limits.AlertmanagerMaxAlertsSizeBytes(a.tenant)

	sizeDiff := alertSize(alert.Alert)

	a.mx.Lock()
	defer a.mx.Unlock()

	if !existing && countLimit > 0 && (a.count+1) > countLimit {
		a.failureCounter.Inc()
		return fmt.Errorf(errTooManyAlerts, countLimit)
	}

	if existing {
		sizeDiff -= a.sizes[fp]
	}

	if sizeLimit > 0 && (a.totalSize+sizeDiff) > sizeLimit {
		a.failureCounter.Inc()
		return fmt.Errorf(errAlertsTooBig, sizeLimit)
	}

	return nil
}

func (a *alertsLimiter) PostStore(alert *types.Alert, existing bool) {
	if alert == nil {
		return
	}

	newSize := alertSize(alert.Alert)
	fp := alert.Fingerprint()

	a.mx.Lock()
	defer a.mx.Unlock()

	if existing {
		a.totalSize -= a.sizes[fp]
	} else {
		a.count++
	}
	a.sizes[fp] = newSize
	a.totalSize += newSize
}

func (a *alertsLimiter) PostDelete(alert *types.Alert) {
	if alert == nil {
		return
	}

	fp := alert.Fingerprint()

	a.mx.Lock()
	defer a.mx.Unlock()

	a.totalSize -= a.sizes[fp]
	delete(a.sizes, fp)
	a.count--
}

func (a *alertsLimiter) currentStats() (count, totalSize int) {
	a.mx.Lock()
	defer a.mx.Unlock()

	return a.count, a.totalSize
}

func alertSize(alert model.Alert) int {
	size := 0
	for l, v := range alert.Labels {
		size += len(l)
		size += len(v)
	}
	for l, v := range alert.Annotations {
		size += len(l)
		size += len(v)
	}
	size += len(alert.GeneratorURL)
	return size
}
