// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/multitenant.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/alerting/definition"
	alertingNotify "github.com/grafana/alerting/notify"
	alertingReceivers "github.com/grafana/alerting/receivers"
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
	"github.com/prometheus/alertmanager/cluster/clusterpb"
	amconfig "github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/featurecontrol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"go.yaml.in/yaml/v3"
	"golang.org/x/time/rate"

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
	errConfigNotFound                      = errors.New("configuration not found")
)

// MultitenantAlertmanagerConfig is the configuration for a multitenant Alertmanager.
type MultitenantAlertmanagerConfig struct {
	DataDir          string           `yaml:"data_dir"`
	Retention        time.Duration    `yaml:"retention" category:"advanced"`
	ExternalURL      flagext.URLValue `yaml:"external_url"`
	PollInterval     time.Duration    `yaml:"poll_interval" category:"advanced"`
	MaxRecvMsgSize   int64            `yaml:"max_recv_msg_size" category:"advanced"`
	StateReadTimeout time.Duration    `yaml:"state_read_timeout" category:"experimental"`

	// Sharding configuration for the Alertmanager.
	ShardingRing RingConfig `yaml:"sharding_ring"`

	FallbackConfigFile string `yaml:"fallback_config_file"`

	PeerTimeout time.Duration `yaml:"peer_timeout" category:"advanced"`

	EnableAPI bool `yaml:"enable_api" category:"advanced"`

	GrafanaAlertmanagerCompatibilityEnabled bool          `yaml:"grafana_alertmanager_compatibility_enabled" category:"experimental"`
	GrafanaAlertmanagerIdleGracePeriod      time.Duration `yaml:"grafana_alertmanager_idle_grace_period" category:"experimental"`

	MaxConcurrentGetRequestsPerTenant int `yaml:"max_concurrent_get_requests_per_tenant" category:"advanced"`

	// For distributor.
	AlertmanagerClient ClientConfig `yaml:"alertmanager_client"`

	// For the state persister.
	Persister PersisterConfig `yaml:",inline"`

	// Allow disabling of full_state object cleanup.
	EnableStateCleanup bool `yaml:"enable_state_cleanup" category:"advanced"`

	// StrictInitializationEnabled is an experimental feature that allows the multi-tenant Alertmanager
	// to skip starting Alertmanagers for tenants without a non-default, non-empty configuration.
	// For Grafana Alertmanager tenants, configurations must also be marked as "promoted".
	StrictInitializationEnabled bool `yaml:"strict_initialization" category:"experimental"`

	// Enable UTF-8 strict mode. When enabled, Alertmanager uses the new UTF-8 parser
	// when parsing label matchers in tenant configurations and HTTP requests, instead
	// of the old regular expression parser, referred to as classic mode.
	// Enable this mode once confident that all tenant configurations are forwards
	// compatible.
	UTF8StrictMode bool `yaml:"utf8_strict_mode" category:"advanced"`
	// Enables logging when parsing label matchers. If UTF-8 strict mode is enabled,
	// then the UTF-8 parser will be logged. If it is disabled, then the old regular
	// expression parser will be logged.
	LogParsingLabelMatchers bool `yaml:"log_parsing_label_matchers" category:"experimental"`
	UTF8MigrationLogging    bool `yaml:"utf8_migration_logging" category:"experimental"`
}

const (
	defaultGrafanaAlertmanagerGracePeriod = 5 * time.Minute
	defaultPeerTimeout                    = 15 * time.Second
)

// RegisterFlags adds the features required to config this to the given FlagSet.
func (cfg *MultitenantAlertmanagerConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.StringVar(&cfg.DataDir, "alertmanager.storage.path", "./data-alertmanager/", "Directory to store Alertmanager state and temporarily configuration files. The content of this directory is not required to be persisted between restarts unless Alertmanager replication has been disabled.")
	f.DurationVar(&cfg.Retention, "alertmanager.storage.retention", 5*24*time.Hour, "How long should we store stateful data (notification logs and silences). For notification log entries, refers to how long should we keep entries before they expire and are deleted. For silences, refers to how long should tenants view silences after they expire and are deleted.")
	f.DurationVar(&cfg.StateReadTimeout, "alertmanager.storage.state-read-timeout", 15*time.Second, "Timeout for reading the state from object storage during the initial sync. Set to `0` for no timeout.")
	f.Int64Var(&cfg.MaxRecvMsgSize, "alertmanager.max-recv-msg-size", 100<<20, "Maximum size (bytes) of an accepted HTTP request body.")

	_ = cfg.ExternalURL.Set("http://localhost:8080/alertmanager") // set the default
	f.Var(&cfg.ExternalURL, "alertmanager.web.external-url", "The URL under which Alertmanager is externally reachable (eg. could be different than -http.alertmanager-http-prefix in case Alertmanager is served via a reverse proxy). This setting is used both to configure the internal requests router and to generate links in alert templates. If the external URL has a path portion, it will be used to prefix all HTTP endpoints served by Alertmanager, both the UI and API.")

	f.StringVar(&cfg.FallbackConfigFile, "alertmanager.configs.fallback", "", "Filename of fallback config to use if none specified for instance.")
	f.DurationVar(&cfg.PollInterval, "alertmanager.configs.poll-interval", 15*time.Second, "How frequently to poll Alertmanager configs.")

	f.BoolVar(&cfg.EnableAPI, "alertmanager.enable-api", true, "Enable the alertmanager config API.")
	f.BoolVar(&cfg.GrafanaAlertmanagerCompatibilityEnabled, "alertmanager.grafana-alertmanager-compatibility-enabled", false, "Enable routes to support the migration and operation of the Grafana Alertmanager.")
	f.DurationVar(&cfg.GrafanaAlertmanagerIdleGracePeriod, "alertmanager.grafana-alertmanager-grace-period", defaultGrafanaAlertmanagerGracePeriod, "Duration to wait before shutting down an idle Alertmanager using an unpromoted or default configuration when strict initialization is enabled.")
	f.IntVar(&cfg.MaxConcurrentGetRequestsPerTenant, "alertmanager.max-concurrent-get-requests-per-tenant", 0, "Maximum number of concurrent GET requests allowed per tenant. The zero value (and negative values) result in a limit of GOMAXPROCS or 8, whichever is larger. Status code 503 is served for GET requests that would exceed the concurrency limit.")

	f.BoolVar(&cfg.EnableStateCleanup, "alertmanager.enable-state-cleanup", true, "Enables periodic cleanup of alertmanager stateful data (notification logs and silences) from object storage. When enabled, data is removed for any tenant that does not have a configuration.")

	cfg.AlertmanagerClient.RegisterFlagsWithPrefix("alertmanager.alertmanager-client", f)
	cfg.Persister.RegisterFlagsWithPrefix("alertmanager", f)
	cfg.ShardingRing.RegisterFlags(f, logger)

	f.DurationVar(&cfg.PeerTimeout, "alertmanager.peer-timeout", defaultPeerTimeout, "Time to wait between peers to send notifications.")

	f.BoolVar(&cfg.StrictInitializationEnabled, "alertmanager.strict-initialization-enabled", false, "Skip initializing Alertmanagers for tenants without a non-default, non-empty configuration. For Grafana Alertmanager tenants, configurations not marked as 'promoted' will also be skipped.")

	f.BoolVar(&cfg.UTF8StrictMode, "alertmanager.utf8-strict-mode-enabled", false, "Enable UTF-8 strict mode. Allows UTF-8 characters in the matchers for routes and inhibition rules, in silences, and in the labels for alerts. It is recommended that all tenants run the `migrate-utf8` command in mimirtool before enabling this mode. Otherwise, some tenant configurations might fail to load. For more information, refer to [Enable UTF-8](https://grafana.com/docs/mimir/<MIMIR_VERSION>/references/architecture/components/alertmanager/#enable-utf-8).")
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
	initializationsOnRequestTotal *prometheus.CounterVec
	tenantsSkipped                prometheus.Gauge
}

func newMultitenantAlertmanagerMetrics(reg prometheus.Registerer) *multitenantAlertmanagerMetrics {
	m := &multitenantAlertmanagerMetrics{}

	m.grafanaStateSize = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_alertmanager_grafana_state_size_bytes",
		Help: "Size of the grafana alertmanager state.",
	}, []string{"user"})

	m.lastReloadSuccessful = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_alertmanager_config_last_reload_successful",
		Help: "Boolean set to 1 whenever the last configuration reload attempt was successful.",
	}, []string{"user"})

	m.lastReloadSuccessfulTimestamp = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_alertmanager_config_last_reload_successful_seconds",
		Help: "Timestamp of the last successful configuration reload.",
	}, []string{"user"})

	m.initializationsOnRequestTotal = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "alertmanager_initializations_on_request_total",
		Help:      "Total number of on-request initializations for Alertmanagers that were previously skipped.",
	}, []string{"user"})

	m.tenantsSkipped = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace: "cortex",
		Name:      "alertmanager_tenants_skipped",
		Help:      "Number of per-tenant alertmanagers that were skipped during the last configuration sync.",
	})

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
	// Stores the current set of configuration hashes we're running in each tenant's Alertmanager.
	// Used for comparing configurations as we synchronize them.
	cfgs map[string]model.Fingerprint

	logger              log.Logger
	alertmanagerMetrics *alertmanagerMetrics
	multitenantMetrics  *multitenantAlertmanagerMetrics

	alertmanagerClientsPool ClientsPool

	limits   Limits
	features featurecontrol.Flagger

	// lastRequestTime tracks request timestamps for conditionally-started Grafana Alertmanagers.
	// A zero-value timestamp for a tenant means that their Alertmanager was skipped during the last config sync.
	// This map is used alongside the configured grace period to determine when to shut down idle Alertmanagers.
	lastRequestTime sync.Map

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
		return nil, fmt.Errorf("create KV store client: %w", err)
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
		cfgs:                map[string]model.Fingerprint{},
		alertmanagers:       map[string]*Alertmanager{},
		lastRequestTime:     sync.Map{},
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
		return nil, fmt.Errorf("failed to initialize Alertmanager's lifecycler config: %w", err)
	}

	// Define lifecycler delegates in reverse order (last to be called defined first because they're
	// chained via "next delegate").
	delegate := ring.BasicLifecyclerDelegate(ring.NewInstanceRegisterDelegate(ring.JOINING, RingNumTokens))
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, am.logger)
	delegate = ring.NewAutoForgetDelegate(am.cfg.ShardingRing.Common.HeartbeatTimeout*ringAutoForgetUnhealthyPeriods, delegate, am.logger)

	am.ringLifecycler, err = ring.NewBasicLifecycler(lifecyclerCfg, RingNameForServer, RingKey, ringStore, delegate, am.logger, prometheus.WrapRegistererWithPrefix("cortex_", am.registry))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Alertmanager's lifecycler: %w", err)
	}

	am.ring, err = ring.NewWithStoreClientAndStrategy(am.cfg.ShardingRing.toRingConfig(), RingNameForServer, RingKey, ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), prometheus.WrapRegistererWithPrefix("cortex_", am.registry), am.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Alertmanager's ring: %w", err)
	}

	am.grpcServer = server.NewServer(&handlerForGRPCServer{am: am}, server.WithReturn4XXErrors)

	am.alertmanagerClientsPool = newAlertmanagerClientsPool(client.NewRingServiceDiscovery(am.ring), cfg.AlertmanagerClient, logger, am.registry)
	am.distributor, err = NewDistributor(cfg.AlertmanagerClient, cfg.MaxRecvMsgSize, am.ring, am.alertmanagerClientsPool, log.With(logger, "component", "AlertmanagerDistributor"), am.registry)
	if err != nil {
		return nil, fmt.Errorf("create distributor: %w", err)
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
		return fmt.Errorf("failed to start alertmanager's subservices: %w", err)
	}

	if err = services.StartManagerAndAwaitHealthy(ctx, am.subservices); err != nil {
		return fmt.Errorf("failed to start alertmanager's subservices: %w", err)
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
		return fmt.Errorf("failed to wait for initial state sync: %w", err)
	}
	level.Info(am.logger).Log("msg", "initial state sync is complete")

	// With the initial sync now completed, we should have loaded all assigned alertmanager configurations to this instance. We can switch it to ACTIVE and start serving requests.
	if err := am.ringLifecycler.ChangeState(ctx, ring.ACTIVE); err != nil {
		return fmt.Errorf("switch instance to %s in the ring: %w", ring.ACTIVE, err)
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
			return fmt.Errorf("alertmanager subservices failed: %w", err)
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
		return nil, nil, fmt.Errorf("failed to list users with alertmanager configuration: %w", err)
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
		return nil, nil, fmt.Errorf("failed to load alertmanager configurations for owned users: %w", err)
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
	amInitSkipped := map[string]struct{}{}
	for user, cfgs := range cfgMap {
		cfg, startAM, err := am.computeConfig(cfgs)
		if err != nil {
			am.multitenantMetrics.lastReloadSuccessful.WithLabelValues(user).Set(float64(0))
			level.Warn(am.logger).Log("msg", "error computing config", "err", err, "user", user)
			continue
		}

		if !startAM {
			level.Debug(am.logger).Log("msg", "not initializing alertmanager for grafana tenant without a promoted, non-default configuration", "user", user)
			amInitSkipped[user] = struct{}{}
			continue
		}

		if am.cfg.GrafanaAlertmanagerCompatibilityEnabled {
			if err := am.syncStates(ctx, cfg); err != nil {
				level.Error(am.logger).Log("msg", "error syncing states", "err", err, "user", user)
			}
		}

		if err := am.setConfig(cfg); err != nil {
			am.multitenantMetrics.lastReloadSuccessful.WithLabelValues(user).Set(float64(0))
			level.Warn(am.logger).Log("msg", "error applying config", "err", err, "user", user)
			continue
		}

		am.multitenantMetrics.lastReloadSuccessful.WithLabelValues(user).Set(float64(1))
		am.multitenantMetrics.lastReloadSuccessfulTimestamp.WithLabelValues(user).SetToCurrentTime()
	}
	am.multitenantMetrics.tenantsSkipped.Set(float64(len(amInitSkipped)))

	userAlertmanagersToStop := map[string]*Alertmanager{}
	am.alertmanagersMtx.Lock()
	for userID, userAM := range am.alertmanagers {
		_, exists := cfgMap[userID]
		_, initSkipped := amInitSkipped[userID]
		if !exists || initSkipped {
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

// computeConfig takes Mimir and Grafana configurations and returns a config we can use to start an Alertmanager.
// A bool is returned, indicating whether the Alertmanager should be started.
//
// Order of precedence:
// 1. Custom Mimir configurations
// 2. Custom, promoted Grafana configurations
// 3. Default Grafana configurations
// 4. Default Mimir configurations (lowest precedence, created by default for all tenants)
func (am *MultitenantAlertmanager) computeConfig(cfgs alertspb.AlertConfigDescs) (amConfig, bool, error) {
	// Custom Mimir configurations have the highest precedence.
	if cfgs.Mimir.RawConfig != am.fallbackConfig && cfgs.Mimir.RawConfig != "" {
		if cfgs.Grafana.Promoted {
			level.Warn(am.logger).Log("msg", "merging configurations not implemented, using mimir config", "user", cfgs.Mimir.User)
		}
		am.removeFromSkippedList(cfgs.Mimir.User)
		return amConfigFromMimirConfig(cfgs.Mimir, am.cfg.ExternalURL.URL), true, nil
	}

	// Unpromoted/empty Grafana configurations are always ignored.
	if !cfgs.Grafana.Promoted || cfgs.Grafana.RawConfig == "" {
		// Only return the default Mimir config (lowest precedence) if the tenant is receiving requests.
		if am.isTenantActive(cfgs.Mimir.User) {
			return amConfigFromMimirConfig(cfgs.Mimir, am.cfg.ExternalURL.URL), true, nil
		}
		return amConfig{}, false, nil
	}

	// Custom Grafana configurations have the second highest precedence.
	if !cfgs.Grafana.Default {
		am.removeFromSkippedList(cfgs.Mimir.User)
		cfg, err := am.amConfigFromGrafanaConfig(cfgs.Grafana)
		return cfg, true, err
	}

	// We have no custom configs. Check the last activity time to determine whether to start the AM.
	if !am.isTenantActive(cfgs.Mimir.User) {
		return amConfig{}, false, nil
	}

	// Default Grafana configurations have the third highest precedence.
	cfg, err := am.amConfigFromGrafanaConfig(cfgs.Grafana)
	return cfg, true, err
}

// removeFromSkippedList remove a tenant from the 'skipped' tenants list.
func (am *MultitenantAlertmanager) removeFromSkippedList(userID string) {
	if !am.cfg.StrictInitializationEnabled {
		return
	}

	if _, ok := am.lastRequestTime.LoadAndDelete(userID); ok {
		level.Debug(am.logger).Log("msg", "user now has a usable config, removing it from skipped list", "user", userID)
	}
}

// isTenantActive checks the last request time for a tenant, returning 'true' if the grace period has not yet expired.
// If strict initialization is not enabled, it always returns true, as we don't keep track of request times.
func (am *MultitenantAlertmanager) isTenantActive(userID string) bool {
	if !am.cfg.StrictInitializationEnabled {
		return true
	}

	lastRequestTime, loaded := am.lastRequestTime.LoadOrStore(userID, time.Time{}.Unix())
	if !loaded || time.Unix(lastRequestTime.(int64), 0).IsZero() {
		return false
	}

	// Use the zero value to signal that the tenant was skipped.
	// If the value stored is not what we have in memory, the tenant received a request since the last read.
	gracePeriodExpired := time.Since(time.Unix(lastRequestTime.(int64), 0)) >= am.cfg.GrafanaAlertmanagerIdleGracePeriod
	if gracePeriodExpired && am.lastRequestTime.CompareAndSwap(userID, lastRequestTime, time.Time{}.Unix()) {
		return false
	}

	level.Debug(am.logger).Log("msg", "user has no usable config but is receiving requests, keeping Alertmanager active", "user", userID)
	return true
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
	if !cfg.UsingGrafanaConfig {
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

	if err := userAM.mergeFullGrafanaState(s.State); err != nil {
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
	User               string
	RawConfig          string
	Templates          []definition.PostableApiTemplate
	TmplExternalURL    *url.URL
	UsingGrafanaConfig bool
	EmailConfig        alertingReceivers.EmailSenderConfig
}

func (f amConfig) fingerprint() model.Fingerprint {
	sum := fnv.New64a()

	writeBytes := func(b []byte) {
		_, _ = sum.Write(b)
		_, _ = sum.Write([]byte{255}) // add separator between fields. It is impossible in strings so, it reduces collisions in strings
	}
	writeString := func(s string) {
		if len(s) == 0 {
			writeBytes(nil)
			return
		}
		// #nosec G103 -- nosemgrep: use-of-unsafe-block
		// avoid allocation when converting string to byte slice
		writeBytes(unsafe.Slice(unsafe.StringData(s), len(s)))
	}
	writeBool := func(b bool) {
		if b {
			writeBytes([]byte{1})
		} else {
			writeBytes([]byte{0})
		}
	}

	writeString(f.User)
	writeString(f.RawConfig)
	writeBool(f.UsingGrafanaConfig)
	if f.TmplExternalURL != nil {
		writeString(f.TmplExternalURL.String())
	} else {
		writeBytes(nil)
	}
	writeString(f.EmailConfig.AuthPassword)
	writeString(f.EmailConfig.AuthUser)
	writeString(f.EmailConfig.CertFile)
	writeString(f.EmailConfig.EhloIdentity)
	writeString(f.EmailConfig.ExternalURL)
	writeString(f.EmailConfig.FromName)
	writeString(f.EmailConfig.FromAddress)
	writeString(f.EmailConfig.Host)
	writeString(f.EmailConfig.KeyFile)
	writeBool(f.EmailConfig.SkipVerify)
	writeString(f.EmailConfig.StartTLSPolicy)
	writeString(f.EmailConfig.SentBy)
	result := sum.Sum64()

	writeBytes(nil)
	// Calculate hash for each key-value pair independently and combine it using XOR
	// so we do not need to care about the random order of the pairs in the map.
	var mapFp uint64
	for k, v := range f.EmailConfig.StaticHeaders {
		sum.Reset()
		writeString(k)
		writeString(v)
		mapFp ^= sum.Sum64()
	}

	// Ignore order in templates because they're usually built from the map
	var templatesFp uint64
	for _, template := range f.Templates {
		sum.Reset()
		writeString(template.Name)
		writeString(template.Content)
		writeString(string(template.Kind))
		templatesFp ^= sum.Sum64()
	}

	// Ignore order in content types because it does not matter
	var contentTypesFp uint64
	for _, ct := range f.EmailConfig.ContentTypes {
		sum.Reset()
		writeString(ct)
		contentTypesFp ^= sum.Sum64()
	}

	// combine all hashes, including ones for empty slices\map
	sum.Reset()
	tmp := make([]byte, 8)
	binary.LittleEndian.PutUint64(tmp, result)
	writeBytes(tmp)
	binary.LittleEndian.PutUint64(tmp, mapFp)
	writeBytes(tmp)
	binary.LittleEndian.PutUint64(tmp, templatesFp)
	writeBytes(tmp)
	binary.LittleEndian.PutUint64(tmp, contentTypesFp)
	writeBytes(tmp)
	return model.Fingerprint(sum.Sum64())
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
		validateMatchersInConfigDesc(am.logger, "tenant", alertspb.AlertConfigDesc{
			User:      cfg.User,
			RawConfig: cfg.RawConfig,
		})
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

	cfgFp := cfg.fingerprint()
	// If no Alertmanager instance exists for this user yet, start one.
	if !hasExisting {
		level.Debug(am.logger).Log("msg", "initializing new per-tenant alertmanager", "user", cfg.User)
		newAM, err := am.newAlertmanager(cfg.User, userAmConfig, cfg.Templates, rawCfg, cfg.TmplExternalURL, cfg.EmailConfig, cfg.UsingGrafanaConfig)
		if err != nil {
			return err
		}
		am.alertmanagers[cfg.User] = newAM
	} else {
		curFp := am.cfgs[cfg.User]
		if curFp != cfgFp {
			level.Info(am.logger).Log("msg", "updating per-tenant alertmanager", "user", cfg.User, "old_fingerprint", curFp, "new_fingerprint", cfgFp)
			am.cfgs[cfg.User] = cfgFp
			// If the config changed, apply the new one.
			err := existing.ApplyConfig(userAmConfig, alertingNotify.PostableAPITemplatesToTemplateDefinitions(cfg.Templates), rawCfg, cfg.TmplExternalURL, cfg.EmailConfig, cfg.UsingGrafanaConfig)
			if err != nil {
				return fmt.Errorf("unable to apply Alertmanager config for user %v: %v", cfg.User, err)
			}
		}
	}

	am.cfgs[cfg.User] = cfgFp
	return nil
}

func (am *MultitenantAlertmanager) getTenantDirectory(userID string) string {
	return filepath.Join(am.cfg.DataDir, userID)
}

func (am *MultitenantAlertmanager) newAlertmanager(userID string, amConfig *definition.PostableApiAlertingConfig, templates []definition.PostableApiTemplate, rawCfg string, tmplExternalURL *url.URL, emailCfg alertingReceivers.EmailSenderConfig, usingGrafanaConfig bool) (*Alertmanager, error) {
	reg := prometheus.NewRegistry()

	tenantDir := am.getTenantDirectory(userID)
	err := os.MkdirAll(tenantDir, 0777)
	if err != nil {
		return nil, fmt.Errorf("failed to create per-tenant directory %v: %w", tenantDir, err)
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
		StateReadTimeout:                  am.cfg.StateReadTimeout,
	}, reg)
	if err != nil {
		return nil, fmt.Errorf("unable to start Alertmanager for user %v: %v", userID, err)
	}

	if err := newAM.ApplyConfig(amConfig, alertingNotify.PostableAPITemplatesToTemplateDefinitions(templates), rawCfg, tmplExternalURL, emailCfg, usingGrafanaConfig); err != nil {
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

		// If needed, update the last time the Alertmanager received requests.
		if _, ok := am.lastRequestTime.Load(userID); ok {
			level.Debug(am.logger).Log("msg", "updating last request reception time", "user", userID)
			am.lastRequestTime.Store(userID, time.Now().Unix())
		}
		return
	}

	// If the Alertmanager initialization was skipped, start the Alertmanager.
	if ok := am.lastRequestTime.CompareAndSwap(userID, time.Time{}.Unix(), time.Now().Unix()); ok {
		userAM, err = am.startAlertmanager(req.Context(), userID)
		if err != nil {
			if errors.Is(err, errNotUploadingFallback) || errors.Is(err, errConfigNotFound) {
				level.Warn(am.logger).Log("msg", "not initializing Alertmanager", "user", userID, "err", err)
				http.Error(w, "Not initializing the Alertmanager", http.StatusNotAcceptable)
				return
			}
			level.Error(am.logger).Log("msg", "unable to initialize the Alertmanager", "user", userID, "err", err)
			http.Error(w, "Failed to initialize the Alertmanager", http.StatusInternalServerError)
			return
		}

		am.lastRequestTime.Store(userID, time.Now().Unix())
		am.multitenantMetrics.initializationsOnRequestTotal.WithLabelValues(userID).Inc()
		level.Debug(am.logger).Log("msg", "Alertmanager initialized after receiving request", "user", userID)
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

// startAlertmanager will start the Alertmanager for a tenant, using the fallback configuration if no config is found.
func (am *MultitenantAlertmanager) startAlertmanager(ctx context.Context, userID string) (*Alertmanager, error) {
	// Avoid starting the Alertmanager for tenants not owned by this instance.
	if !am.isUserOwned(userID) {
		am.lastRequestTime.Delete(userID)
		return nil, fmt.Errorf("user not owned by this instance: %w", errNotUploadingFallback)
	}

	cfgMap, err := am.store.GetAlertConfigs(ctx, []string{userID})
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing configuration: %w", err)
	}

	cfg, ok := cfgMap[userID]
	if !ok {
		return nil, errConfigNotFound
	}

	amConfig, _, err := am.computeConfig(cfg) // The second value indicates whether we should start the AM or not. Ignore it.
	if err != nil {
		return nil, fmt.Errorf("failed to compute config: %w", err)
	}

	if err := am.setConfig(amConfig); err != nil {
		return nil, err
	}
	am.alertmanagersMtx.Lock()
	defer am.alertmanagersMtx.Unlock()
	return am.alertmanagers[userID], nil
}

func (am *MultitenantAlertmanager) alertmanagerFromFallbackConfig(ctx context.Context, userID string) (*Alertmanager, error) {
	// Make sure we never create fallback instances for a user not owned by this instance.
	// This check is not strictly necessary as the configuration polling loop will deactivate
	// any tenants which are not meant to be in an instance, but it is confusing and potentially
	// wasteful to have them start-up when they are not needed.
	if !am.isUserOwned(userID) {
		return nil, fmt.Errorf("user not owned by this instance: %w", errNotUploadingFallback)
	}

	// We should be careful never to replace an existing configuration with the fallback.
	// There is a small window of time between the check and upload where a user could
	// have uploaded a configuration, but this only applies to the first ever request made.
	_, err := am.store.GetAlertConfig(ctx, userID)
	if err == nil {
		// If there is a configuration, then the polling cycle should pick it up.
		return nil, fmt.Errorf("user has a configuration: %w", errNotUploadingFallback)
	}
	if !errors.Is(err, alertspb.ErrNotFound) {
		return nil, fmt.Errorf("failed to check for existing configuration: %w", err)
	}

	level.Warn(am.logger).Log("msg", "no configuration exists for user; uploading fallback configuration", "user", userID)

	// Upload an empty config so that the Alertmanager is no de-activated in the next poll
	cfgDesc := alertspb.ToProto("", nil, userID)
	err = am.store.SetAlertConfig(ctx, cfgDesc)
	if err != nil {
		return nil, err
	}

	// Calling setConfig with an empty configuration will use the fallback config.
	amConfig := amConfigFromMimirConfig(cfgDesc, am.cfg.ExternalURL.URL)
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
		case alertmanagerpb.MERGE_ERROR:
			level.Error(am.logger).Log("msg", "state replication failed", "user", userID, "key", part.Key, "err", resp.Error)
		case alertmanagerpb.USER_NOT_FOUND:
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
		case alertmanagerpb.READ_OK:
			resultsMtx.Lock()
			results = append(results, resp.State)
			resultsMtx.Unlock()
		case alertmanagerpb.READ_ERROR:
			level.Error(am.logger).Log("msg", "error trying to read state", "addr", addr, "user", userID, "err", resp.Error)
		case alertmanagerpb.READ_USER_NOT_FOUND:
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
			Status: alertmanagerpb.USER_NOT_FOUND,
			Error:  "alertmanager for this user does not exists",
		}, nil
	}

	if err = userAM.mergePartialExternalState(part); err != nil {
		return &alertmanagerpb.UpdateStateResponse{
			Status: alertmanagerpb.MERGE_ERROR,
			Error:  err.Error(),
		}, nil
	}

	return &alertmanagerpb.UpdateStateResponse{Status: alertmanagerpb.OK}, nil
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
			Status: alertmanagerpb.READ_USER_NOT_FOUND,
			Error:  "alertmanager for this user does not exists",
		}, nil
	}

	state, err := userAM.getFullState()
	if err != nil {
		return &alertmanagerpb.ReadStateResponse{
			Status: alertmanagerpb.READ_ERROR,
			Error:  err.Error(),
		}, nil
	}

	return &alertmanagerpb.ReadStateResponse{
		Status: alertmanagerpb.READ_OK,
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
