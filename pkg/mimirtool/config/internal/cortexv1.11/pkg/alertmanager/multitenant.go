// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"flag"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/cluster"
	"golang.org/x/time/rate"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/alertmanager/alertstore"
)

const (
	// If a config sets the webhook URL to this, it will be rewritten to
	// a URL derived from Config.AutoWebhookRoot
	autoWebhookURL = "http://internal.monitor"

	// Reasons for (re)syncing alertmanager configurations from object storage.
	reasonPeriodic   = "periodic"
	reasonInitial    = "initial"
	reasonRingChange = "ring-change"

	// ringAutoForgetUnhealthyPeriods is how many consecutive timeout periods an unhealthy instance
	// in the ring will be automatically removed.
	ringAutoForgetUnhealthyPeriods = 5
)

var (
	errInvalidExternalURL                  = errors.New("the configured external URL is invalid: should not end with /")
	errShardingLegacyStorage               = errors.New("deprecated -alertmanager.storage.* not supported with -alertmanager.sharding-enabled, use -alertmanager-storage.*")
	errShardingUnsupportedStorage          = errors.New("the configured alertmanager storage backend is not supported when sharding is enabled")
	errZoneAwarenessEnabledWithoutZoneInfo = errors.New("the configured alertmanager has zone awareness enabled but zone is not set")
)

// MultitenantAlertmanagerConfig is the configuration for a multitenant Alertmanager.
type MultitenantAlertmanagerConfig struct {
	DataDir        string           `yaml:"data_dir"`
	Retention      time.Duration    `yaml:"retention"`
	ExternalURL    flagext.URLValue `yaml:"external_url"`
	PollInterval   time.Duration    `yaml:"poll_interval"`
	MaxRecvMsgSize int64            `yaml:"max_recv_msg_size"`

	// Enable sharding for the Alertmanager
	ShardingEnabled bool       `yaml:"sharding_enabled"`
	ShardingRing    RingConfig `yaml:"sharding_ring"`

	FallbackConfigFile string `yaml:"fallback_config_file"`
	AutoWebhookRoot    string `yaml:"auto_webhook_root"`

	Store   alertstore.LegacyConfig `yaml:"storage" doc:"description=Deprecated. Use -alertmanager-storage.* CLI flags and their respective YAML config options instead."`
	Cluster ClusterConfig           `yaml:"cluster"`

	EnableAPI bool `yaml:"enable_api"`

	// For distributor.
	AlertmanagerClient ClientConfig `yaml:"alertmanager_client"`

	// For the state persister.
	Persister PersisterConfig `yaml:",inline"`
}

type ClusterConfig struct {
	ListenAddr       string                 `yaml:"listen_address"`
	AdvertiseAddr    string                 `yaml:"advertise_address"`
	Peers            flagext.StringSliceCSV `yaml:"peers"`
	PeerTimeout      time.Duration          `yaml:"peer_timeout"`
	GossipInterval   time.Duration          `yaml:"gossip_interval"`
	PushPullInterval time.Duration          `yaml:"push_pull_interval"`
}

const (
	defaultClusterAddr = "0.0.0.0:9094"
	defaultPeerTimeout = 15 * time.Second
)

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

	// AlertmanagerMaxConfigSize returns max size of configuration file that user is allowed to upload. If 0, there is no limit.
	AlertmanagerMaxConfigSize(tenant string) int

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

func (cfg *MultitenantAlertmanagerConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.DataDir, "alertmanager.storage.path", "data/", "Base path for data storage.")
	f.DurationVar(&cfg.Retention, "alertmanager.storage.retention", 5*24*time.Hour, "How long to keep data for.")
	f.Int64Var(&cfg.MaxRecvMsgSize, "alertmanager.max-recv-msg-size", 16<<20, "Maximum size (bytes) of an accepted HTTP request body.")

	f.Var(&cfg.ExternalURL, "alertmanager.web.external-url", "The URL under which Alertmanager is externally reachable (for example, if Alertmanager is served via a reverse proxy). Used for generating relative and absolute links back to Alertmanager itself. If the URL has a path portion, it will be used to prefix all HTTP endpoints served by Alertmanager. If omitted, relevant URL components will be derived automatically.")

	f.StringVar(&cfg.FallbackConfigFile, "alertmanager.configs.fallback", "", "Filename of fallback config to use if none specified for instance.")
	f.StringVar(&cfg.AutoWebhookRoot, "alertmanager.configs.auto-webhook-root", "", "Root of URL to generate if config is "+autoWebhookURL)
	f.DurationVar(&cfg.PollInterval, "alertmanager.configs.poll-interval", 15*time.Second, "How frequently to poll Cortex configs")

	f.BoolVar(&cfg.EnableAPI, "experimental.alertmanager.enable-api", false, "Enable the experimental alertmanager config api.")

	f.BoolVar(&cfg.ShardingEnabled, "alertmanager.sharding-enabled", false, "Shard tenants across multiple alertmanager instances.")

	cfg.AlertmanagerClient.RegisterFlagsWithPrefix("alertmanager.alertmanager-client", f)
	cfg.Persister.RegisterFlagsWithPrefix("alertmanager", f)
	cfg.ShardingRing.RegisterFlags(f)
	cfg.Store.RegisterFlags(f)
	cfg.Cluster.RegisterFlags(f)
}
func (cfg *ClusterConfig) RegisterFlags(f *flag.FlagSet) {
	prefix := "alertmanager.cluster."
	f.StringVar(&cfg.ListenAddr, prefix+"listen-address", defaultClusterAddr, "Listen address and port for the cluster. Not specifying this flag disables high-availability mode.")
	f.StringVar(&cfg.AdvertiseAddr, prefix+"advertise-address", "", "Explicit address or hostname to advertise in cluster.")
	f.Var(&cfg.Peers, prefix+"peers", "Comma-separated list of initial peers.")
	f.DurationVar(&cfg.PeerTimeout, prefix+"peer-timeout", defaultPeerTimeout, "Time to wait between peers to send notifications.")
	f.DurationVar(&cfg.GossipInterval, prefix+"gossip-interval", cluster.DefaultGossipInterval, "The interval between sending gossip messages. By lowering this value (more frequent) gossip messages are propagated across cluster more quickly at the expense of increased bandwidth usage.")
	f.DurationVar(&cfg.PushPullInterval, prefix+"push-pull-interval", cluster.DefaultPushPullInterval, "The interval between gossip state syncs. Setting this interval lower (more frequent) will increase convergence speeds across larger clusters at the expense of increased bandwidth usage.")
}
