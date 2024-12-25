// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/multitenant.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/alerting/definition"
	alertingTemplates "github.com/grafana/alerting/templates"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/httpgrpc/server"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/cluster/clusterpb"
	amconfig "github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/featurecontrol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/alertmanager/alertmanagerpb"
	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore"
	"github.com/grafana/mimir/pkg/util"
)

const (
	// Reasons for (re)syncing alertmanager configurations from object storage.
	reasonPeriodic   = "periodic"
	reasonInitial    = "initial"
	reasonRingChange = "ring-change"

	// ringAutoForgetUnhealthyPeriods is how many consecutive timeout periods an unhealthy instance
	// in the ring will be automatically removed.
	ringAutoForgetUnhealthyPeriods = 5
)

var (
	errEmptyExternalURL                    = errors.New("-alertmanager.web.external-url cannot be empty")
	errInvalidExternalURLEndingSlash       = errors.New("the configured external URL is invalid: should not end with /")
	errInvalidExternalURLMissingScheme     = errors.New("the configured external URL is invalid because it's missing the scheme (e.g. https://)")
	errInvalidExternalURLMissingHostname   = errors.New("the configured external URL is invalid because it's missing the hostname")
	errZoneAwarenessEnabledWithoutZoneInfo = errors.New("the configured alertmanager has zone awareness enabled but zone is not set")
	errNotUploadingFallback                = errors.New("not uploading fallback configuration")
)

// MultitenantAlertmanagerConfig is the configuration for a multitenant Alertmanager.
type MultitenantAlertmanagerConfig struct {
	DataDir        string           `yaml:"data_dir"`
	Retention      time.Duration    `yaml:"retention" category:"advanced"`
	ExternalURL    flagext.URLValue `yaml:"external_url"`
	PollInterval   time.Duration    `yaml:"poll_interval" category:"advanced"`
	MaxRecvMsgSize int64            `yaml:"max_recv_msg_size" category:"advanced"`

	// Sharding confiuration for the Alertmanager
	ShardingRing RingConfig `yaml:"sharding_ring"`

	FallbackConfigFile string `yaml:"fallback_config_file"`

	PeerTimeout time.Duration `yaml:"peer_timeout" category:"advanced"`

	EnableAPI                               bool `yaml:"enable_api" category:"advanced"`
	GrafanaAlertmanagerCompatibilityEnabled bool `yaml:"grafana_alertmanager_compatibility_enabled" category:"experimental"`

	MaxConcurrentGetRequestsPerTenant int `yaml:"max_concurrent_get_requests_per_tenant" category:"advanced"`

	// For distributor.
	AlertmanagerClient ClientConfig `yaml:"alertmanager_client"`

	// For the state persister.
	Persister PersisterConfig `yaml:",inline"`

	// Allow disabling of full_state object cleanup.
	EnableStateCleanup bool `yaml:"enable_state_cleanup" category:"advanced"`

	// Enable UTF-8 strict mode. When enabled, Alertmanager uses the new UTF-8 parser
	// when parsing label matchers in tenant configurations and HTTP requests, instead
	// of the old regular expression parser, referred to as classic mode.
	// Enable this mode once confident that all tenant configurations are forwards
	// compatible.
	UTF8StrictMode bool `yaml:"utf8_strict_mode" category:"experimental"`
	// Enables logging when parsing label matchers. If UTF-8 strict mode is enabled,
	// then the UTF-8 parser will be logged. If it is disabled, then the old regular
	// expression parser will be logged.
	LogParsingLabelMatchers bool `yaml:"log_parsing_label_matchers" category:"experimental"`
	UTF8MigrationLogging    bool `yaml:"utf8_migration_logging" category:"experimental"`
}

const (
	defaultPeerTimeout = 15 * time.Second
)

// RegisterFlags adds the features required to config this to the given FlagSet.
func (cfg *MultitenantAlertmanagerConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.StringVar(&cfg.DataDir, "alertmanager.storage.path", "./data-alertmanager/", "Directory to store Alertmanager state and temporarily configuration files. The content of this directory is not required to be persisted between restarts unless Alertmanager replication has been disabled.")
	f.DurationVar(&cfg.Retention, "alertmanager.storage.retention", 5*24*time.Hour, "How long should we store stateful data (notification logs and silences). For notification log entries, refers to how long should we keep entries before they expire and are deleted. For silences, refers to how long should tenants view silences after they expire and are deleted.")
	f.Int64Var(&cfg.MaxRecvMsgSize, "alertmanager.max-recv-msg-size", 100<<20, "Maximum size (bytes) of an accepted HTTP request body.")

	_ = cfg.ExternalURL.Set("http://localhost:8080/alertmanager") // set the default
	f.Var(&cfg.ExternalURL, "alertmanager.web.external-url", "The URL under which Alertmanager is externally reachable (eg. could be different than -http.alertmanager-http-prefix in case Alertmanager is served via a reverse proxy). This setting is used both to configure the internal requests router and to generate links in alert templates. If the external URL has a path portion, it will be used to prefix all HTTP endpoints served by Alertmanager, both the UI and API.")

	f.StringVar(&cfg.FallbackConfigFile, "alertmanager.configs.fallback", "", "Filename of fallback config to use if none specified for instance.")
	f.DurationVar(&cfg.PollInterval, "alertmanager.configs.poll-interval", 15*time.Second, "How frequently to poll Alertmanager configs.")

	f.BoolVar(&cfg.EnableAPI, "alertmanager.enable-api", true, "Enable the alertmanager config API.")
	f.BoolVar(&cfg.GrafanaAlertmanagerCompatibilityEnabled, "alertmanager.grafana-alertmanager-compatibility-enabled", false, "Enable routes to support the migration and operation of the Grafana Alertmanager.")
	f.IntVar(&cfg.MaxConcurrentGetRequestsPerTenant, "alertmanager.max-concurrent-get-requests-per-tenant", 0, "Maximum number of concurrent GET requests allowed per tenant. The zero value (and negative values) result in a limit of GOMAXPROCS or 8, whichever is larger. Status code 503 is served for GET requests that would exceed the concurrency limit.")

	f.BoolVar(&cfg.EnableStateCleanup, "alertmanager.enable-state-cleanup", true, "Enables periodic cleanup of alertmanager stateful data (notification logs and silences) from object storage. When enabled, data is removed for any tenant that does not have a configuration.")

	cfg.AlertmanagerClient.RegisterFlagsWithPrefix("alertmanager.alertmanager-client", f)
	cfg.Persister.RegisterFlagsWithPrefix("alertmanager", f)
	cfg.ShardingRing.RegisterFlags(f, logger)

	f.DurationVar(&cfg.PeerTimeout, "alertmanager.peer-timeout", defaultPeerTimeout, "Time to wait between peers to send notifications.")

	f.BoolVar(&cfg.UTF8StrictMode, "alertmanager.utf8-strict-mode-enabled", false, "Enable UTF-8 strict mode. Allows UTF-8 characters in the matchers for routes and inhibition rules, in silences, and in the labels for alerts. It is recommended that all tenants run the `migrate-utf8` command in mimirtool before enabling this mode. Otherwise, some tenant configurations might fail to load. For more information, refer to [Enable UTF-8](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/components/alertmanager/#enable-utf-8). Enabling and then disabling UTF-8 strict mode can break existing Alertmanager configurations if tenants added UTF-8 characters to their Alertmanager configuration while it was enabled.")
	f.BoolVar(&cfg.LogParsingLabelMatchers, "alertmanager.log-parsing-label-matchers", false, "Enable logging when parsing label matchers. This flag is intended to be used with -alertmanager.utf8-strict-mode-enabled to validate UTF-8 strict mode is working as intended.")
	f.BoolVar(&cfg.UTF8MigrationLogging, "alertmanager.utf8-migration-logging-enabled", false, "Enable logging of tenant configurations that are incompatible with UTF-8 strict mode.")
}

// Validate config and returns error on failure
func (cfg *MultitenantAlertmanagerConfig) Validate() error {
	if cfg.ExternalURL.String() == "" {
		return errEmptyExternalURL
	}

	// Either the configured URL is clearly a path (starting with /) or it must be a full URL.
	if !strings.HasPrefix(cfg.ExternalURL.String(), "/") {
		if cfg.ExternalURL.Scheme == "" {
			return errInvalidExternalURLMissingScheme
		}

		if cfg.ExternalURL.Host == "" {
			return errInvalidExternalURLMissingHostname
		}
	}

	if strings.HasSuffix(cfg.ExternalURL.Path, "/") {
		return errInvalidExternalURLEndingSlash
	}

	if err := cfg.Persister.Validate(); err != nil {
		return err
	}

	if cfg.ShardingRing.ZoneAwarenessEnabled && cfg.ShardingRing.InstanceZone == "" {
		return errZoneAwarenessEnabledWithoutZoneInfo
	}

	return nil
}

func (cfg *MultitenantAlertmanagerConfig) CheckExternalURL(alertmanagerHTTPPrefix string, logger log.Logger) {
	if cfg.ExternalURL.Path != alertmanagerHTTPPrefix {
		level.Warn(logger).Log("msg", fmt.Sprintf(""+
			"The configured Alertmanager HTTP prefix '%s' is different than the path specified in the external URL '%s': "+
			"the Alertmanager UI and API may not work as expected unless you have a reverse proxy exposing the Alertmanager endpoints under '%s' prefix",
			alertmanagerHTTPPrefix, cfg.ExternalURL.String(), alertmanagerHTTPPrefix))
	}
}

type multitenantAlertmanagerMetrics struct {
	grafanaStateSize              *prometheus.GaugeVec
	lastReloadSuccessful          *prometheus.GaugeVec
	lastReloadSuccessfulTimestamp *prometheus.GaugeVec
}

func newMultitenantAlertmanagerMetrics(reg prometheus.Registerer) *multitenantAlertmanagerMetrics {
	m := &multitenantAlertmanagerMetrics{}

	m.grafanaStateSize = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "alertmanager_grafana_state_size_bytes",
		Help:      "Size of the grafana alertmanager state.",
	}, []string{"user"})

	m.lastReloadSuccessful = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "alertmanager_config_last_reload_successful",
		Help:      "Boolean set to 1 whenever the last configuration reload attempt was successful.",
	}, []string{"user"})

	m.lastReloadSuccessfulTimestamp = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "alertmanager_config_last_reload_successful_seconds",
		Help:      "Timestamp of the last successful configuration reload.",
	}, []string{"user"})

	return m
}

// Limits defines limits used by Alertmanager.
type Limits interface {
	// AlertmanagerReceiversBlockCIDRNetworks returns the list of network CIDRs that should be blocked
	// in the Alertmanager receivers for the given user.
	AlertmanagerReceiversBlockCIDRNetworks(user string) []flagext.CIDR

	// AlertmanagerReceiversBlockPrivateAddresses returns true if private addresses should be blocked
	// in the Alertmanager receivers for the given user.
	AlertmanagerReceiversBlockPrivateAddresses(user string) bool

	// NotificationRateLimit methods return limit used by rate-limiter for given integration.
	// If set to 0, no notifications are allowed.
	// rate.Inf = all notifications are allowed.
	//
	// Note that when negative or zero values specified by user are translated to rate.Limit by Overrides,
	// and may have different meaning there.
	NotificationRateLimit(tenant string, integration string) rate.Limit

	// NotificationBurstSize returns burst-size for rate limiter for given integration type. If 0, no notifications are allowed except
	// when limit == rate.Inf.
	NotificationBurstSize(tenant string, integration string) int

	// AlertmanagerMaxGrafanaConfigSize returns max size of the grafana configuration file that user is allowed to upload. If 0, there is no limit.
	AlertmanagerMaxGrafanaConfigSize(tenant string) int

	// AlertmanagerMaxConfigSize returns max size of configuration file that user is allowed to upload. If 0, there is no limit.
	AlertmanagerMaxConfigSize(tenant string) int

	// AlertmanagerMaxGrafanaStateSize returns the max size of the grafana state in bytes. If 0, there is no limit.
	AlertmanagerMaxGrafanaStateSize(tenant string) int

	// AlertmanagerMaxSilencesCount returns the max number of silences, including expired silences. If negative or 0, there is no limit.
	AlertmanagerMaxSilencesCount(tenant string) int

	// AlertmanagerMaxSilenceSizeBytes returns the max silence size in bytes. If negative or 0, there is no limit.
	AlertmanagerMaxSilenceSizeBytes(tenant string) int

	// AlertmanagerMaxTemplatesCount returns max number of templates that tenant can use in the configuration. 0 = no limit.
	AlertmanagerMaxTemplatesCount(tenant string) int

	// AlertmanagerMaxTemplateSize returns max size of individual template. 0 = no limit.
	AlertmanagerMaxTemplateSize(tenant string) int

	// AlertmanagerMaxDispatcherAggregationGroups returns maximum number of aggregation groups in Alertmanager's dispatcher that a tenant can have.
	// Each aggregation group consumes single goroutine. 0 = unlimited.
	AlertmanagerMaxDispatcherAggregationGroups(t string) int

	// AlertmanagerMaxAlertsCount returns max number of alerts that tenant can have active at the same time. 0 = no limit.
	AlertmanagerMaxAlertsCount(tenant string) int

	// AlertmanagerMaxAlertsSizeBytes returns total max size of alerts that tenant can have active at the same time. 0 = no limit.
	// Size of the alert is computed from alert labels, annotations and generator URL.
	AlertmanagerMaxAlertsSizeBytes(tenant string) int
}

// A MultitenantAlertmanager manages Alertmanager instances for multiple
// organizations.
type MultitenantAlertmanager struct {
	services.Service

	cfg *MultitenantAlertmanagerConfig

	// Ring used for sharding alertmanager instances.
	// When sharding is disabled, the flow is:
	//   ServeHTTP() -> serveRequest()
	// When sharding is enabled:
	//   ServeHTTP() -> distributor.DistributeRequest() -> (sends to other AM or even the current)
	//     -> HandleRequest() (gRPC call) -> grpcServer() -> handlerForGRPCServer.ServeHTTP() -> serveRequest().
	ringLifecycler *ring.BasicLifecycler
	ring           *ring.Ring
	distributor    *Distributor
	grpcServer     *server.Server

	// Last ring state. This variable is not protected with a mutex because it's always
	// accessed by a single goroutine at a time.
	ringLastState ring.ReplicationSet

	// Subservices manager (ring, lifecycler)
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	store alertstore.AlertStore

	// The fallback config is stored as a string and parsed every time it's needed
	// because we mutate the parsed results and don't want those changes to take
	// effect here.
	fallbackConfig string

	alertmanagersMtx sync.Mutex
	alertmanagers    map[string]*Alertmanager
	// Stores the current set of configurations we're running in each tenant's Alertmanager.
	// Used for comparing configurations as we synchronize them.
	cfgs map[string]alertspb.AlertConfigDesc

	logger              log.Logger
	alertmanagerMetrics *alertmanagerMetrics
	multitenantMetrics  *multitenantAlertmanagerMetrics

	alertmanagerClientsPool ClientsPool

	limits   Limits
	features featurecontrol.Flagger

	registry          prometheus.Registerer
	ringCheckErrors   prometheus.Counter
	tenantsOwned      prometheus.Gauge
	tenantsDiscovered prometheus.Gauge
	syncTotal         *prometheus.CounterVec
	syncFailures      *prometheus.CounterVec
}

// NewMultitenantAlertmanager creates a new MultitenantAlertmanager.
func NewMultitenantAlertmanager(cfg *MultitenantAlertmanagerConfig, store alertstore.AlertStore, limits Limits, features featurecontrol.Flagger, logger log.Logger, registerer prometheus.Registerer) (*MultitenantAlertmanager, error) {
	err := os.MkdirAll(cfg.DataDir, 0777)
	if err != nil {
		return nil, fmt.Errorf("unable to create Alertmanager data directory %q: %s", cfg.DataDir, err)
	}

	fallbackConfig, err := ComputeFallbackConfig(cfg.FallbackConfigFile)
	if err != nil {
		return nil, err
	}

	ringStore, err := kv.NewClient(
		cfg.ShardingRing.Common.KVStore,
		ring.GetCodec(),
		kv.RegistererWithKVName(prometheus.WrapRegistererWithPrefix("cortex_", registerer), "alertmanager"),
		logger,
	)
	if err != nil {
		return nil, errors.Wrap(err, "create KV store client")
	}

	return createMultitenantAlertmanager(cfg, fallbackConfig, store, ringStore, limits, features, logger, registerer)
}

// ComputeFallbackConfig will load, validate and return the provided fallbackConfigFile
// or return an valid empty default configuration if none is provided.
func ComputeFallbackConfig(fallbackConfigFile string) ([]byte, error) {
	if fallbackConfigFile != "" {
		fallbackConfig, err := os.ReadFile(fallbackConfigFile)
		if err != nil {
			return nil, fmt.Errorf("unable to read fallback config %q: %s", fallbackConfigFile, err)
		}
		_, err = amconfig.LoadFile(fallbackConfigFile)
		if err != nil {
			return nil, fmt.Errorf("unable to load fallback config %q: %s", fallbackConfigFile, err)
		}
		return fallbackConfig, nil
	}
	globalConfig := amconfig.DefaultGlobalConfig()
	defaultConfig := amconfig.Config{
		Global: &globalConfig,
		Route: &amconfig.Route{
			Receiver: "empty-receiver",
		},
		Receivers: []amconfig.Receiver{
			{
				Name: "empty-receiver",
			},
		},
	}
	fallbackConfig, err := yaml.Marshal(defaultConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal default fallback config: %s", err)
	}
	return fallbackConfig, nil
}

func createMultitenantAlertmanager(cfg *MultitenantAlertmanagerConfig, fallbackConfig []byte, store alertstore.AlertStore, ringStore kv.Client, limits Limits, features featurecontrol.Flagger, logger log.Logger, registerer prometheus.Registerer) (*MultitenantAlertmanager, error) {
	am := &MultitenantAlertmanager{
		cfg:                 cfg,
		fallbackConfig:      string(fallbackConfig),
		cfgs:                map[string]alertspb.AlertConfigDesc{},
		alertmanagers:       map[string]*Alertmanager{},
		alertmanagerMetrics: newAlertmanagerMetrics(logger),
		multitenantMetrics:  newMultitenantAlertmanagerMetrics(registerer),
		store:               store,
		logger:              log.With(logger, "component", "MultiTenantAlertmanager"),
		registry:            registerer,
		limits:              limits,
		features:            features,
		ringCheckErrors: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_alertmanager_ring_check_errors_total",
			Help: "Number of errors that have occurred when checking the ring for ownership.",
		}),
		syncTotal: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_alertmanager_sync_configs_total",
			Help: "Total number of times the alertmanager sync operation triggered.",
		}, []string{"reason"}),
		syncFailures: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_alertmanager_sync_configs_failed_total",
			Help: "Total number of times the alertmanager sync operation failed.",
		}, []string{"reason"}),
		tenantsDiscovered: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_alertmanager_tenants_discovered",
			Help: "Number of tenants with an Alertmanager configuration discovered.",
		}),
		tenantsOwned: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_alertmanager_tenants_owned",
			Help: "Current number of tenants owned by the Alertmanager instance.",
		}),
	}

	// Initialize the top-level metrics.
	for _, r := range []string{reasonInitial, reasonPeriodic, reasonRingChange} {
		am.syncTotal.WithLabelValues(r)
		am.syncFailures.WithLabelValues(r)
	}

	lifecyclerCfg, err := am.cfg.ShardingRing.ToLifecyclerConfig(am.logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize Alertmanager's lifecycler config")
	}

	// Define lifecycler delegates in reverse order (last to be called defined first because they're
	// chained via "next delegate").
	delegate := ring.BasicLifecyclerDelegate(ring.NewInstanceRegisterDelegate(ring.JOINING, RingNumTokens))
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, am.logger)
	delegate = ring.NewAutoForgetDelegate(am.cfg.ShardingRing.Common.HeartbeatTimeout*ringAutoForgetUnhealthyPeriods, delegate, am.logger)

	am.ringLifecycler, err = ring.NewBasicLifecycler(lifecyclerCfg, RingNameForServer, RingKey, ringStore, delegate, am.logger, prometheus.WrapRegistererWithPrefix("cortex_", am.registry))
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize Alertmanager's lifecycler")
	}

	am.ring, err = ring.NewWithStoreClientAndStrategy(am.cfg.ShardingRing.toRingConfig(), RingNameForServer, RingKey, ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), prometheus.WrapRegistererWithPrefix("cortex_", am.registry), am.logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize Alertmanager's ring")
	}

	am.grpcServer = server.NewServer(&handlerForGRPCServer{am: am}, server.WithReturn4XXErrors)

	am.alertmanagerClientsPool = newAlertmanagerClientsPool(client.NewRingServiceDiscovery(am.ring), cfg.AlertmanagerClient, logger, am.registry)
	am.distributor, err = NewDistributor(cfg.AlertmanagerClient, cfg.MaxRecvMsgSize, am.ring, am.alertmanagerClientsPool, log.With(logger, "component", "AlertmanagerDistributor"), am.registry)
	if err != nil {
		return nil, errors.Wrap(err, "create distributor")
	}

	if registerer != nil {
		registerer.MustRegister(am.alertmanagerMetrics)
	}

	am.Service = services.NewBasicService(am.starting, am.run, am.stopping)

	return am, nil
}

// handlerForGRPCServer acts as a handler for gRPC server to serve
// the serveRequest() via the standard ServeHTTP.
type handlerForGRPCServer struct {
	am *MultitenantAlertmanager
}

func (h *handlerForGRPCServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.am.serveRequest(w, req)
}

func (am *MultitenantAlertmanager) starting(ctx context.Context) (err error) {
	defer func() {
		if err == nil || am.subservices == nil {
			return
		}

		if stopErr := services.StopManagerAndAwaitStopped(context.Background(), am.subservices); stopErr != nil {
			level.Error(am.logger).Log("msg", "failed to gracefully stop alertmanager dependencies", "err", stopErr)
		}
	}()

	if am.subservices, err = services.NewManager(am.ringLifecycler, am.ring, am.distributor); err != nil {
		return errors.Wrap(err, "failed to start alertmanager's subservices")
	}

	if err = services.StartManagerAndAwaitHealthy(ctx, am.subservices); err != nil {
		return errors.Wrap(err, "failed to start alertmanager's subservices")
	}

	am.subservicesWatcher = services.NewFailureWatcher()
	am.subservicesWatcher.WatchManager(am.subservices)

	// We wait until the instance is in the JOINING state, once it does we know that tokens are assigned to this instance and we'll be ready to perform an initial sync of configs.
	level.Info(am.logger).Log("msg", "waiting until alertmanager is JOINING in the ring")
	if err = ring.WaitInstanceState(ctx, am.ring, am.ringLifecycler.GetInstanceID(), ring.JOINING); err != nil {
		return err
	}
	level.Info(am.logger).Log("msg", "alertmanager is JOINING in the ring")

	// At this point, if sharding is enabled, the instance is registered with some tokens
	// and we can run the initial iteration to sync configs.
	if err := am.loadAndSyncConfigs(ctx, reasonInitial); err != nil {
		return err
	}

	// Store the ring state after the initial Alertmanager configs sync has been done and before we do change
	// our state in the ring.
	am.ringLastState, _ = am.ring.GetAllHealthy(RingOp)

	// Make sure that all the alertmanagers we were initially configured with have
	// fetched state from the replicas, before advertising as ACTIVE. This will
	// reduce the possibility that we lose state when new instances join/leave.
	level.Info(am.logger).Log("msg", "waiting until initial state sync is complete for all users")
	if err := am.waitInitialStateSync(ctx); err != nil {
		return errors.Wrap(err, "failed to wait for initial state sync")
	}
	level.Info(am.logger).Log("msg", "initial state sync is complete")

	// With the initial sync now completed, we should have loaded all assigned alertmanager configurations to this instance. We can switch it to ACTIVE and start serving requests.
	if err := am.ringLifecycler.ChangeState(ctx, ring.ACTIVE); err != nil {
		return errors.Wrapf(err, "switch instance to %s in the ring", ring.ACTIVE)
	}

	// Wait until the ring client detected this instance in the ACTIVE state.
	level.Info(am.logger).Log("msg", "waiting until alertmanager is ACTIVE in the ring")
	if err := ring.WaitInstanceState(ctx, am.ring, am.ringLifecycler.GetInstanceID(), ring.ACTIVE); err != nil {
		return err
	}
	level.Info(am.logger).Log("msg", "alertmanager is ACTIVE in the ring")

	return nil
}

func (am *MultitenantAlertmanager) run(ctx context.Context) error {
	tick := time.NewTicker(am.cfg.PollInterval)
	defer tick.Stop()

	ringTicker := time.NewTicker(util.DurationWithJitter(am.cfg.ShardingRing.RingCheckPeriod, 0.2))
	defer ringTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-am.subservicesWatcher.Chan():
			return errors.Wrap(err, "alertmanager subservices failed")
		case <-tick.C:
			// We don't want to halt execution here but instead just log what happened.
			if err := am.loadAndSyncConfigs(ctx, reasonPeriodic); err != nil {
				level.Warn(am.logger).Log("msg", "error while synchronizing alertmanager configs", "err", err)
			}
		case <-ringTicker.C:
			// We ignore the error because in case of error it will return an empty
			// replication set which we use to compare with the previous state.
			currRingState, _ := am.ring.GetAllHealthy(RingOp)

			if ring.HasReplicationSetChanged(am.ringLastState, currRingState) {
				am.ringLastState = currRingState
				if err := am.loadAndSyncConfigs(ctx, reasonRingChange); err != nil {
					level.Warn(am.logger).Log("msg", "error while synchronizing alertmanager configs", "err", err)
				}
			}
		}
	}
}

func (am *MultitenantAlertmanager) loadAndSyncConfigs(ctx context.Context, syncReason string) error {
	level.Info(am.logger).Log("msg", "synchronizing alertmanager configs for users")
	am.syncTotal.WithLabelValues(syncReason).Inc()

	allUsers, cfgs, err := am.loadAlertmanagerConfigs(ctx)
	if err != nil {
		am.syncFailures.WithLabelValues(syncReason).Inc()
		return err
	}

	am.syncConfigs(ctx, cfgs)
	am.deleteUnusedLocalUserState()

	// Note when cleaning up remote state, remember that the user may not necessarily be configured
	// in this instance. Therefore, pass the list of _all_ configured users to filter by.
	if am.cfg.EnableStateCleanup {
		am.deleteUnusedRemoteUserState(ctx, allUsers)
	}

	return nil
}

func (am *MultitenantAlertmanager) waitInitialStateSync(ctx context.Context) error {
	am.alertmanagersMtx.Lock()
	ams := make([]*Alertmanager, 0, len(am.alertmanagers))
	for _, userAM := range am.alertmanagers {
		ams = append(ams, userAM)
	}
	am.alertmanagersMtx.Unlock()

	for _, userAM := range ams {
		if err := userAM.WaitInitialStateSync(ctx); err != nil {
			return err
		}
	}

	return nil
}

// stopping runs when MultitenantAlertmanager transitions to Stopping state.
func (am *MultitenantAlertmanager) stopping(_ error) error {
	am.alertmanagersMtx.Lock()
	for _, am := range am.alertmanagers {
		am.StopAndWait()
	}
	am.alertmanagersMtx.Unlock()

	if am.subservices != nil {
		// subservices manages ring and lifecycler.
		_ = services.StopManagerAndAwaitStopped(context.Background(), am.subservices)
	}
	return nil
}

// loadAlertmanagerConfigs Loads (and filters) the alertmanagers configuration from object storage, taking into consideration the sharding strategy. Returns:
// - The list of discovered users (all users with a configuration in storage)
// - The configurations of users owned by this instance.
func (am *MultitenantAlertmanager) loadAlertmanagerConfigs(ctx context.Context) ([]string, map[string]alertspb.AlertConfigDescs, error) {
	// Find all users with an alertmanager config.
	allUserIDs, err := am.store.ListAllUsers(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to list users with alertmanager configuration")
	}
	numUsersDiscovered := len(allUserIDs)
	ownedUserIDs := make([]string, 0, len(allUserIDs))

	// Filter out users not owned by this shard.
	for _, userID := range allUserIDs {
		if am.isUserOwned(userID) {
			ownedUserIDs = append(ownedUserIDs, userID)
		}
	}
	numUsersOwned := len(ownedUserIDs)

	// Load the configs for the owned users.
	configs, err := am.store.GetAlertConfigs(ctx, ownedUserIDs)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to load alertmanager configurations for owned users")
	}

	am.tenantsDiscovered.Set(float64(numUsersDiscovered))
	am.tenantsOwned.Set(float64(numUsersOwned))
	return allUserIDs, configs, nil
}

func (am *MultitenantAlertmanager) isUserOwned(userID string) bool {
	alertmanagers, err := am.ring.Get(shardByUser(userID), SyncRingOp, nil, nil, nil)
	if err != nil {
		am.ringCheckErrors.Inc()
		level.Error(am.logger).Log("msg", "failed to load alertmanager configuration", "user", userID, "err", err)
		return false
	}

	return alertmanagers.Includes(am.ringLifecycler.GetInstanceAddr())
}

func (am *MultitenantAlertmanager) syncConfigs(ctx context.Context, cfgMap map[string]alertspb.AlertConfigDescs) {
	level.Debug(am.logger).Log("msg", "adding configurations", "num_configs", len(cfgMap))
	for user, cfgs := range cfgMap {
		cfg, err := am.computeConfig(cfgs)
		if err != nil {
			am.multitenantMetrics.lastReloadSuccessful.WithLabelValues(user).Set(float64(0))
			level.Warn(am.logger).Log("msg", "error computing config", "err", err)
			continue
		}

		if err := am.syncStates(ctx, cfg); err != nil {
			level.Error(am.logger).Log("msg", "error syncing states", "err", err, "user", user)
		}

		if err := am.setConfig(cfg); err != nil {
			am.multitenantMetrics.lastReloadSuccessful.WithLabelValues(user).Set(float64(0))
			level.Warn(am.logger).Log("msg", "error applying config", "err", err)
			continue
		}

		am.multitenantMetrics.lastReloadSuccessful.WithLabelValues(user).Set(float64(1))
		am.multitenantMetrics.lastReloadSuccessfulTimestamp.WithLabelValues(user).SetToCurrentTime()
	}

	userAlertmanagersToStop := map[string]*Alertmanager{}

	am.alertmanagersMtx.Lock()
	for userID, userAM := range am.alertmanagers {
		if _, exists := cfgMap[userID]; !exists {
			userAlertmanagersToStop[userID] = userAM
			delete(am.alertmanagers, userID)
			delete(am.cfgs, userID)
			am.multitenantMetrics.lastReloadSuccessful.DeleteLabelValues(userID)
			am.multitenantMetrics.lastReloadSuccessfulTimestamp.DeleteLabelValues(userID)
			am.alertmanagerMetrics.removeUserRegistry(userID)
		}
	}
	am.alertmanagersMtx.Unlock()

	// Now stop alertmanagers and wait until they are really stopped, without holding lock.
	for userID, userAM := range userAlertmanagersToStop {
		level.Info(am.logger).Log("msg", "deactivating per-tenant alertmanager", "user", userID)
		userAM.StopAndWait()
		level.Info(am.logger).Log("msg", "deactivated per-tenant alertmanager", "user", userID)
	}
}

// computeConfig takes an AlertConfigDescs struct containing Mimir and Grafana configurations.
// It returns the final configuration and external URL the Alertmanager will use.
func (am *MultitenantAlertmanager) computeConfig(cfgs alertspb.AlertConfigDescs) (amConfig, error) {
	cfg := amConfig{
		AlertConfigDesc: cfgs.Mimir,
		tmplExternalURL: am.cfg.ExternalURL.URL,
	}

	switch {
	// Mimir configuration.
	case !cfgs.Grafana.Promoted:
		level.Debug(am.logger).Log("msg", "grafana configuration not promoted, using mimir config", "user", cfgs.Mimir.User)
		return cfg, nil

	case cfgs.Grafana.Default:
		level.Debug(am.logger).Log("msg", "grafana configuration is default, using mimir config", "user", cfgs.Mimir.User)
		return cfg, nil

	case cfgs.Grafana.RawConfig == "":
		level.Debug(am.logger).Log("msg", "grafana configuration is empty, using mimir config", "user", cfgs.Mimir.User)
		return cfg, nil

	// Grafana configuration.
	case cfgs.Mimir.RawConfig == am.fallbackConfig:
		level.Debug(am.logger).Log("msg", "mimir configuration is default, using grafana config with the default globals", "user", cfgs.Mimir.User)
		return createUsableGrafanaConfig(cfgs.Grafana, cfgs.Mimir.RawConfig)

	case cfgs.Mimir.RawConfig == "":
		level.Debug(am.logger).Log("msg", "mimir configuration is empty, using grafana config with the default globals", "user", cfgs.Grafana.User)
		return createUsableGrafanaConfig(cfgs.Grafana, am.fallbackConfig)

	// Both configurations.
	// TODO: merge configurations.
	default:
		level.Warn(am.logger).Log("msg", "merging configurations not implemented, using mimir config", "user", cfgs.Mimir.User)
		return cfg, nil
	}
}

// syncStates promotes/unpromotes the Grafana state and updates the 'promoted' flag if needed.
func (am *MultitenantAlertmanager) syncStates(ctx context.Context, cfg amConfig) error {
	// fetching grafana state first so we can register its size independently of it being promoted or not
	s, err := am.store.GetFullGrafanaState(ctx, cfg.User)
	if err != nil {
		if errors.Is(err, alertspb.ErrNotFound) {
			// This is expected if the state was already promoted.
			level.Debug(am.logger).Log("msg", "grafana state not found, skipping promotion", "user", cfg.User)
			am.multitenantMetrics.grafanaStateSize.DeleteLabelValues(cfg.User)
			return nil
		}
		return err
	}
	am.multitenantMetrics.grafanaStateSize.WithLabelValues(cfg.User).Set(float64(s.State.Size()))

	am.alertmanagersMtx.Lock()
	userAM, ok := am.alertmanagers[cfg.User]
	am.alertmanagersMtx.Unlock()

	// If we're not using Grafana configuration, we shouldn't use Grafana state.
	// Update the flag accordingly.
	if !cfg.usingGrafanaConfig {
		if ok && userAM.usingGrafanaState.CompareAndSwap(true, false) {
			level.Debug(am.logger).Log("msg", "Grafana state unpromoted", "user", cfg.User)
		}
		return nil
	}

	// If the Alertmanager is already using Grafana state, do nothing.
	if ok && userAM.usingGrafanaState.Load() {
		return nil
	}

	// Promote the Grafana Alertmanager state and update the usingGrafanaState flag.
	level.Debug(am.logger).Log("msg", "promoting Grafana state", "user", cfg.User)
	// Translate Grafana state keys to Mimir state keys.
	for i, p := range s.State.Parts {
		switch p.Key {
		case "silences":
			s.State.Parts[i].Key = silencesStateKeyPrefix + cfg.User
		case "notifications":
			s.State.Parts[i].Key = nflogStateKeyPrefix + cfg.User
		default:
			return fmt.Errorf("unknown part key %q", p.Key)
		}
	}

	if !ok {
		level.Debug(am.logger).Log("msg", "no Alertmanager found, creating new one before applying Grafana state", "user", cfg.User)
		if err := am.setConfig(cfg); err != nil {
			return fmt.Errorf("error creating new Alertmanager for user %s: %w", cfg.User, err)
		}
		am.alertmanagersMtx.Lock()
		userAM, ok = am.alertmanagers[cfg.User]
		am.alertmanagersMtx.Unlock()
		if !ok {
			return fmt.Errorf("Alertmanager for user %s not found after creation", cfg.User)
		}
	}

	if err := userAM.mergeFullExternalState(s.State); err != nil {
		return err
	}
	userAM.usingGrafanaState.Store(true)

	// Delete state.
	if err := am.store.DeleteFullGrafanaState(ctx, cfg.User); err != nil {
		return fmt.Errorf("error deleting grafana state for user %s: %w", cfg.User, err)
	}
	level.Debug(am.logger).Log("msg", "Grafana state promoted", "user", cfg.User)
	return nil
}

type amConfig struct {
	alertspb.AlertConfigDesc
	tmplExternalURL    *url.URL
	staticHeaders      map[string]string
	usingGrafanaConfig bool
}

// setConfig applies the given configuration to the alertmanager for `userID`,
// creating an alertmanager if it doesn't already exist.
func (am *MultitenantAlertmanager) setConfig(cfg amConfig) error {
	if am.cfg.UTF8MigrationLogging {
		// Instead of using "config" as the origin, as in Prometheus Alertmanager, we use "tenant".
		// The reason for this that the config.Load function uses the origin "config",
		// which is correct, but Mimir uses config.Load to validate both API requests and tenant
		// configurations. This means metrics from API requests are confused with metrics from
		// tenant configurations. To avoid this confusion, we use a different origin.
		validateMatchersInConfigDesc(am.logger, "tenant", cfg.AlertConfigDesc)
	}

	level.Debug(am.logger).Log("msg", "setting config", "user", cfg.User)

	am.alertmanagersMtx.Lock()
	defer am.alertmanagersMtx.Unlock()

	existing, hasExisting := am.alertmanagers[cfg.User]

	rawCfg := cfg.RawConfig
	var userAmConfig *definition.PostableApiAlertingConfig
	var err error
	if cfg.RawConfig == "" {
		if am.fallbackConfig == "" {
			return fmt.Errorf("blank Alertmanager configuration for %v", cfg.User)
		}
		level.Debug(am.logger).Log("msg", "blank Alertmanager configuration; using fallback", "user", cfg.User)
		userAmConfig, err = definition.LoadCompat([]byte(am.fallbackConfig))
		if err != nil {
			return fmt.Errorf("unable to load fallback configuration for %v: %v", cfg.User, err)
		}
		rawCfg = am.fallbackConfig
	} else {
		userAmConfig, err = definition.LoadCompat([]byte(cfg.RawConfig))
		if err != nil && hasExisting {
			// This means that if a user has a working config and
			// they submit a broken one, the Manager will keep running the last known
			// working configuration.
			return fmt.Errorf("invalid Alertmanager configuration for %v: %v", cfg.User, err)
		}
	}

	// We can have an empty configuration here if:
	// 1) the user had a previous alertmanager
	// 2) then, submitted a non-working configuration (and we kept running the prev working config)
	// 3) finally, the cortex AM instance is restarted and the running version is no longer present
	if userAmConfig == nil {
		return fmt.Errorf("no usable Alertmanager configuration for %v", cfg.User)
	}

	templates := make([]alertingTemplates.TemplateDefinition, 0, len(cfg.Templates))
	for _, tmpl := range cfg.Templates {
		templates = append(templates, alertingTemplates.TemplateDefinition{
			Name:     tmpl.Filename,
			Template: tmpl.Body,
		})
	}

	// If no Alertmanager instance exists for this user yet, start one.
	if !hasExisting {
		level.Debug(am.logger).Log("msg", "initializing new per-tenant alertmanager", "user", cfg.User)
		newAM, err := am.newAlertmanager(cfg.User, userAmConfig, templates, rawCfg, cfg.tmplExternalURL, cfg.staticHeaders)
		if err != nil {
			return err
		}
		am.alertmanagers[cfg.User] = newAM
	} else if configChanged(am.cfgs[cfg.User], cfg.AlertConfigDesc) {
		level.Info(am.logger).Log("msg", "updating new per-tenant alertmanager", "user", cfg.User)
		// If the config changed, apply the new one.
		err := existing.ApplyConfig(userAmConfig, templates, rawCfg, cfg.tmplExternalURL, cfg.staticHeaders)
		if err != nil {
			return fmt.Errorf("unable to apply Alertmanager config for user %v: %v", cfg.User, err)
		}
	}

	am.cfgs[cfg.User] = cfg.AlertConfigDesc
	return nil
}

func (am *MultitenantAlertmanager) getTenantDirectory(userID string) string {
	return filepath.Join(am.cfg.DataDir, userID)
}

func (am *MultitenantAlertmanager) newAlertmanager(userID string, amConfig *definition.PostableApiAlertingConfig, templates []alertingTemplates.TemplateDefinition, rawCfg string, tmplExternalURL *url.URL, staticHeaders map[string]string) (*Alertmanager, error) {
	reg := prometheus.NewRegistry()

	tenantDir := am.getTenantDirectory(userID)
	err := os.MkdirAll(tenantDir, 0777)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create per-tenant directory %v", tenantDir)
	}

	newAM, err := New(&Config{
		UserID:                            userID,
		TenantDataDir:                     tenantDir,
		Logger:                            am.logger,
		PeerTimeout:                       am.cfg.PeerTimeout,
		Retention:                         am.cfg.Retention,
		MaxConcurrentGetRequestsPerTenant: am.cfg.MaxConcurrentGetRequestsPerTenant,
		ExternalURL:                       am.cfg.ExternalURL.URL,
		Replicator:                        am,
		ReplicationFactor:                 am.cfg.ShardingRing.ReplicationFactor,
		Store:                             am.store,
		PersisterConfig:                   am.cfg.Persister,
		Limits:                            am.limits,
		Features:                          am.features,
		GrafanaAlertmanagerCompatibility:  am.cfg.GrafanaAlertmanagerCompatibilityEnabled,
	}, reg)
	if err != nil {
		return nil, fmt.Errorf("unable to start Alertmanager for user %v: %v", userID, err)
	}

	if err := newAM.ApplyConfig(amConfig, templates, rawCfg, tmplExternalURL, staticHeaders); err != nil {
		newAM.Stop()
		return nil, fmt.Errorf("unable to apply initial config for user %v: %v", userID, err)
	}

	am.alertmanagerMetrics.addUserRegistry(userID, reg)
	return newAM, nil
}

// GetPositionForUser returns the position this Alertmanager instance holds in the ring related to its other replicas for an specific user.
func (am *MultitenantAlertmanager) GetPositionForUser(userID string) int {
	// If we have a replication factor of 1 or less we don't need to do any work and can immediately return.
	if am.ring == nil || am.ring.ReplicationFactor() <= 1 {
		return 0
	}

	set, err := am.ring.Get(shardByUser(userID), RingOp, nil, nil, nil)
	if err != nil {
		level.Error(am.logger).Log("msg", "unable to read the ring while trying to determine the alertmanager position", "err", err)
		// If we're  unable to determine the position, we don't want a tenant to miss out on the notification - instead,
		// just assume we're the first in line and run the risk of a double notification.
		return 0
	}

	var position int
	for i, instance := range set.Instances {
		if instance.Addr == am.ringLifecycler.GetInstanceAddr() {
			position = i
			break
		}
	}

	return position
}

// ServeHTTP serves the Alertmanager's web UI and API.
func (am *MultitenantAlertmanager) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if am.State() != services.Running {
		http.Error(w, "Alertmanager not ready", http.StatusServiceUnavailable)
		return
	}

	am.distributor.DistributeRequest(w, req)
}

// HandleRequest implements gRPC Alertmanager service, which receives request from AlertManager-Distributor.
func (am *MultitenantAlertmanager) HandleRequest(ctx context.Context, in *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
	return am.grpcServer.Handle(ctx, in)
}

// serveRequest serves the Alertmanager's web UI and API.
func (am *MultitenantAlertmanager) serveRequest(w http.ResponseWriter, req *http.Request) {
	userID, err := tenant.TenantID(req.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	am.alertmanagersMtx.Lock()
	userAM, ok := am.alertmanagers[userID]
	am.alertmanagersMtx.Unlock()

	if ok {
		userAM.mux.ServeHTTP(w, req)
		return
	}

	if am.fallbackConfig != "" {
		userAM, err = am.alertmanagerFromFallbackConfig(req.Context(), userID)
		if errors.Is(err, errNotUploadingFallback) {
			level.Warn(am.logger).Log("msg", "not initializing Alertmanager", "user", userID, "err", err)
			http.Error(w, "Not initializing the Alertmanager", http.StatusNotAcceptable)
			return
		} else if err != nil {
			level.Error(am.logger).Log("msg", "unable to initialize the Alertmanager with a fallback configuration", "user", userID, "err", err)
			http.Error(w, "Failed to initialize the Alertmanager", http.StatusInternalServerError)
			return
		}

		userAM.mux.ServeHTTP(w, req)
		return
	}

	level.Info(am.logger).Log("msg", "the Alertmanager has no configuration and no fallback specified", "user", userID)
	http.Error(w, "the Alertmanager is not configured", http.StatusPreconditionFailed)
}

func (am *MultitenantAlertmanager) alertmanagerFromFallbackConfig(ctx context.Context, userID string) (*Alertmanager, error) {
	// Make sure we never create fallback instances for a user not owned by this instance.
	// This check is not strictly necessary as the configuration polling loop will deactivate
	// any tenants which are not meant to be in an instance, but it is confusing and potentially
	// wasteful to have them start-up when they are not needed.
	if !am.isUserOwned(userID) {
		return nil, errors.Wrap(errNotUploadingFallback, "user not owned by this instance")
	}

	// We should be careful never to replace an existing configuration with the fallback.
	// There is a small window of time between the check and upload where a user could
	// have uploaded a configuration, but this only applies to the first ever request made.
	_, err := am.store.GetAlertConfig(ctx, userID)
	if err == nil {
		// If there is a configuration, then the polling cycle should pick it up.
		return nil, errors.Wrap(errNotUploadingFallback, "user has a configuration")
	}
	if !errors.Is(err, alertspb.ErrNotFound) {
		return nil, errors.Wrap(err, "failed to check for existing configuration")
	}

	level.Warn(am.logger).Log("msg", "no configuration exists for user; uploading fallback configuration", "user", userID)

	// Upload an empty config so that the Alertmanager is no de-activated in the next poll
	cfgDesc := alertspb.ToProto("", nil, userID)
	err = am.store.SetAlertConfig(ctx, cfgDesc)
	if err != nil {
		return nil, err
	}

	// Calling setConfig with an empty configuration will use the fallback config.
	amConfig := amConfig{
		AlertConfigDesc: cfgDesc,
		tmplExternalURL: am.cfg.ExternalURL.URL,
	}
	err = am.setConfig(amConfig)
	if err != nil {
		return nil, err
	}

	am.alertmanagersMtx.Lock()
	defer am.alertmanagersMtx.Unlock()
	return am.alertmanagers[userID], nil
}

// ReplicateStateForUser attempts to replicate a partial state sent by an alertmanager to its other replicas through the ring.
func (am *MultitenantAlertmanager) ReplicateStateForUser(ctx context.Context, userID string, part *clusterpb.Part) error {
	level.Debug(am.logger).Log("msg", "message received for replication", "user", userID, "key", part.Key)

	selfAddress := am.ringLifecycler.GetInstanceAddr()
	err := ring.DoBatchWithOptions(ctx, RingOp, am.ring, []uint32{shardByUser(userID)}, func(desc ring.InstanceDesc, _ []int) error {
		if desc.GetAddr() == selfAddress {
			return nil
		}

		c, err := am.alertmanagerClientsPool.GetClientFor(desc.GetAddr())
		if err != nil {
			return err
		}

		resp, err := c.UpdateState(user.InjectOrgID(ctx, userID), part)
		if err != nil {
			return err
		}

		switch resp.Status {
		case alertmanagerpb.UpdateStateStatus_MERGE_ERROR:
			level.Error(am.logger).Log("msg", "state replication failed", "user", userID, "key", part.Key, "err", resp.Error)
		case alertmanagerpb.UpdateStateStatus_USER_NOT_FOUND:
			level.Debug(am.logger).Log("msg", "user not found while trying to replicate state", "user", userID, "key", part.Key)
		}
		return nil
	}, ring.DoBatchOptions{})

	return err
}

// ReadFullStateForUser attempts to read the full state from each replica for user. Note that it will try to obtain and return
// state from all replicas, but will consider it a success if state is obtained from at least one replica.
func (am *MultitenantAlertmanager) ReadFullStateForUser(ctx context.Context, userID string) ([]*clusterpb.FullState, error) {
	// Only get the set of replicas which contain the specified user.
	key := shardByUser(userID)
	replicationSet, err := am.ring.Get(key, RingOp, nil, nil, nil)
	if err != nil {
		return nil, err
	}

	// We should only query state from other replicas, and not our own state.
	addrs := replicationSet.GetAddressesWithout(am.ringLifecycler.GetInstanceAddr())

	var (
		resultsMtx sync.Mutex
		results    []*clusterpb.FullState
		notFound   int
	)

	// Note that the jobs swallow the errors - this is because we want to give each replica a chance to respond.
	err = concurrency.ForEachJob(ctx, len(addrs), len(addrs), func(ctx context.Context, idx int) error {
		addr := addrs[idx]
		level.Debug(am.logger).Log("msg", "contacting replica for full state", "user", userID, "addr", addr)

		c, err := am.alertmanagerClientsPool.GetClientFor(addr)
		if err != nil {
			level.Error(am.logger).Log("msg", "failed to get rpc client", "err", err)
			return nil
		}

		resp, err := c.ReadState(user.InjectOrgID(ctx, userID), &alertmanagerpb.ReadStateRequest{})
		if err != nil {
			level.Error(am.logger).Log("msg", "rpc reading state from replica failed", "addr", addr, "user", userID, "err", err)
			return nil
		}

		switch resp.Status {
		case alertmanagerpb.ReadStateStatus_READ_OK:
			resultsMtx.Lock()
			results = append(results, resp.State)
			resultsMtx.Unlock()
		case alertmanagerpb.ReadStateStatus_READ_ERROR:
			level.Error(am.logger).Log("msg", "error trying to read state", "addr", addr, "user", userID, "err", resp.Error)
		case alertmanagerpb.ReadStateStatus_READ_USER_NOT_FOUND:
			level.Debug(am.logger).Log("msg", "user not found while trying to read state", "addr", addr, "user", userID)
			resultsMtx.Lock()
			notFound++
			resultsMtx.Unlock()
		default:
			level.Error(am.logger).Log("msg", "unknown response trying to read state", "addr", addr, "user", userID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// If all replicas do not know the user, propagate that outcome for the client to decide what to do.
	if notFound == len(addrs) {
		return nil, errAllReplicasUserNotFound
	}

	// We only require the state from a single replica, though we return as many as we were able to obtain.
	if len(results) == 0 {
		return nil, fmt.Errorf("failed to read state from any replica")
	}

	return results, nil
}

// UpdateState implements the Alertmanager service.
func (am *MultitenantAlertmanager) UpdateState(ctx context.Context, part *clusterpb.Part) (*alertmanagerpb.UpdateStateResponse, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	am.alertmanagersMtx.Lock()
	userAM, ok := am.alertmanagers[userID]
	am.alertmanagersMtx.Unlock()

	if !ok {
		// We can end up trying to replicate state to an alertmanager that is no longer available due to e.g. a ring topology change.
		level.Debug(am.logger).Log("msg", "user does not have an alertmanager in this instance", "user", userID)
		return &alertmanagerpb.UpdateStateResponse{
			Status: alertmanagerpb.UpdateStateStatus_USER_NOT_FOUND,
			Error:  "alertmanager for this user does not exists",
		}, nil
	}

	if err = userAM.mergePartialExternalState(part); err != nil {
		return &alertmanagerpb.UpdateStateResponse{
			Status: alertmanagerpb.UpdateStateStatus_MERGE_ERROR,
			Error:  err.Error(),
		}, nil
	}

	return &alertmanagerpb.UpdateStateResponse{Status: alertmanagerpb.UpdateStateStatus_OK}, nil
}

// deleteUnusedRemoteUserState deletes state objects in remote storage for users that are no longer configured.
func (am *MultitenantAlertmanager) deleteUnusedRemoteUserState(ctx context.Context, allUsers []string) {
	users := make(map[string]struct{}, len(allUsers))
	for _, userID := range allUsers {
		users[userID] = struct{}{}
	}

	usersWithState, err := am.store.ListUsersWithFullState(ctx)
	if err != nil {
		level.Warn(am.logger).Log("msg", "failed to list users with state", "err", err)
		return
	}

	for _, userID := range usersWithState {
		if _, ok := users[userID]; ok {
			continue
		}

		err := am.store.DeleteFullState(ctx, userID)
		if err != nil {
			level.Warn(am.logger).Log("msg", "failed to delete remote state for user", "user", userID, "err", err)
		} else {
			level.Info(am.logger).Log("msg", "deleted remote state for user", "user", userID)
		}
	}
}

// deleteUnusedLocalUserState deletes local files for users that we no longer need.
func (am *MultitenantAlertmanager) deleteUnusedLocalUserState() {
	userDirs := am.getPerUserDirectories()

	// And delete remaining files.
	for userID, dir := range userDirs {
		am.alertmanagersMtx.Lock()
		userAM := am.alertmanagers[userID]
		am.alertmanagersMtx.Unlock()

		// Don't delete directory if AM for user still exists.
		if userAM != nil {
			continue
		}

		err := os.RemoveAll(dir)
		if err != nil {
			level.Warn(am.logger).Log("msg", "failed to delete directory for user", "dir", dir, "user", userID, "err", err)
		} else {
			level.Info(am.logger).Log("msg", "deleted local directory for user", "dir", dir, "user", userID)
		}
	}
}

// getPerUserDirectories returns map of users to their directories (full path). Only users with local
// directory are returned.
func (am *MultitenantAlertmanager) getPerUserDirectories() map[string]string {
	files, err := os.ReadDir(am.cfg.DataDir)
	if err != nil {
		level.Warn(am.logger).Log("msg", "failed to list local dir", "dir", am.cfg.DataDir, "err", err)
		return nil
	}

	result := map[string]string{}

	for _, f := range files {
		fullPath := filepath.Join(am.cfg.DataDir, f.Name())

		if !f.IsDir() {
			level.Warn(am.logger).Log("msg", "ignoring unexpected file while scanning local alertmanager configs", "file", fullPath)
			continue
		}

		result[f.Name()] = fullPath
	}
	return result
}

// ReadState implements the Alertmanager service.
func (am *MultitenantAlertmanager) ReadState(ctx context.Context, _ *alertmanagerpb.ReadStateRequest) (*alertmanagerpb.ReadStateResponse, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	am.alertmanagersMtx.Lock()
	userAM, ok := am.alertmanagers[userID]
	am.alertmanagersMtx.Unlock()

	if !ok {
		level.Debug(am.logger).Log("msg", "user does not have an alertmanager in this instance", "user", userID)
		return &alertmanagerpb.ReadStateResponse{
			Status: alertmanagerpb.ReadStateStatus_READ_USER_NOT_FOUND,
			Error:  "alertmanager for this user does not exists",
		}, nil
	}

	state, err := userAM.getFullState()
	if err != nil {
		return &alertmanagerpb.ReadStateResponse{
			Status: alertmanagerpb.ReadStateStatus_READ_ERROR,
			Error:  err.Error(),
		}, nil
	}

	return &alertmanagerpb.ReadStateResponse{
		Status: alertmanagerpb.ReadStateStatus_READ_OK,
		State:  state,
	}, nil
}

// validateTemplateFilename validated the template filename and returns error if it's not valid.
// The validation done in this function is a first fence to avoid having a tenant submitting
// a config which may escape the per-tenant data directory on disk.
func validateTemplateFilename(filename string) error {
	if filepath.Base(filename) != filename {
		return fmt.Errorf("invalid template name %q: the template name cannot contain any path", filename)
	}

	// Further enforce no path in the template name.
	if filepath.Dir(filepath.Clean(filename)) != "." {
		return fmt.Errorf("invalid template name %q: the template name cannot contain any path", filename)
	}

	return nil
}

// safeTemplateFilepath builds and return the template filepath within the provided dir.
// This function also performs a security check to make sure the provided templateName
// doesn't contain a relative path escaping the provided dir.
func safeTemplateFilepath(dir, templateName string) (string, error) {
	// We expect all template files to be stored and referenced within the provided directory.
	containerDir, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}

	// Build the actual path of the template.
	actualPath, err := filepath.Abs(filepath.Join(containerDir, templateName))
	if err != nil {
		return "", err
	}

	// If actualPath is same as containerDir, it's likely that actualPath was empty, or just ".".
	if containerDir == actualPath {
		return "", fmt.Errorf("invalid template name %q", templateName)
	}

	if !strings.HasSuffix(containerDir, string(os.PathSeparator)) {
		containerDir = containerDir + string(os.PathSeparator)
	}

	// Ensure the actual path of the template is within the expected directory.
	// This check is a counter-measure to make sure the tenant is not trying to
	// escape its own directory on disk.
	if !strings.HasPrefix(actualPath, containerDir) {
		return "", fmt.Errorf("invalid template name %q: the template filepath is escaping the per-tenant local directory", templateName)
	}

	return actualPath, nil
}

// storeTemplateFile stores template file at the given templateFilepath.
// Returns true, if file content has changed (new or updated file), false if file with the same name
// and content was already stored locally.
func storeTemplateFile(templateFilepath, content string) (bool, error) {
	// Make sure the directory exists.
	dir := filepath.Dir(templateFilepath)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return false, fmt.Errorf("unable to create Alertmanager templates directory %q: %s", dir, err)
	}

	// Check if the template file already exists and if it has changed
	if tmpl, err := os.ReadFile(templateFilepath); err == nil && string(tmpl) == content {
		return false, nil
	} else if err != nil && !os.IsNotExist(err) {
		return false, err
	}

	if err := os.WriteFile(templateFilepath, []byte(content), 0644); err != nil {
		return false, fmt.Errorf("unable to create Alertmanager template file %q: %s", templateFilepath, err)
	}

	return true, nil
}

func configChanged(left, right alertspb.AlertConfigDesc) bool {
	if left.User != right.User {
		return true
	}
	if left.RawConfig != right.RawConfig {
		return true
	}

	existing := make(map[string]string)
	for _, tm := range left.Templates {
		existing[tm.Filename] = tm.Body
	}

	for _, tm := range right.Templates {
		corresponding, ok := existing[tm.Filename]
		if !ok {
			return true // Right has a template that left does not.
		}
		if corresponding != tm.Body {
			return true // The template content is different.
		}
		delete(existing, tm.Filename)
	}

	return len(existing) != 0 // Left has a template that right does not.
}
