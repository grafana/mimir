// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"flag"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util/grpcclient"
	util_log "github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util/log"
)

type Config struct {
	// This is used for template expansion in alerts; must be a valid URL.
	ExternalURL flagext.URLValue `yaml:"external_url"`
	// GRPC Client configuration.
	ClientTLSConfig grpcclient.Config `yaml:"ruler_client"`
	// How frequently to evaluate rules by default.
	EvaluationInterval time.Duration `yaml:"evaluation_interval"`
	// How frequently to poll for updated rules.
	PollInterval time.Duration `yaml:"poll_interval"`
	// Rule Storage and Polling configuration.
	StoreConfig RuleStoreConfig `yaml:"storage" doc:"description=Deprecated. Use -ruler-storage.* CLI flags and their respective YAML config options instead."`
	// Path to store rule files for prom manager.
	RulePath string `yaml:"rule_path"`

	// URL of the Alertmanager to send notifications to.
	AlertmanagerURL string `yaml:"alertmanager_url"`
	// Whether to use DNS SRV records to discover Alertmanager.
	AlertmanagerDiscovery bool `yaml:"enable_alertmanager_discovery"`
	// How long to wait between refreshing the list of Alertmanager based on DNS service discovery.
	AlertmanagerRefreshInterval time.Duration `yaml:"alertmanager_refresh_interval"`
	// Enables the ruler notifier to use the Alertmananger V2 API.
	AlertmanangerEnableV2API bool `yaml:"enable_alertmanager_v2"`
	// Capacity of the queue for notifications to be sent to the Alertmanager.
	NotificationQueueCapacity int `yaml:"notification_queue_capacity"`
	// HTTP timeout duration when sending notifications to the Alertmanager.
	NotificationTimeout time.Duration `yaml:"notification_timeout"`
	// Client configs for interacting with the Alertmanager
	Notifier NotifierConfig `yaml:"alertmanager_client"`

	// Max time to tolerate outage for restoring "for" state of alert.
	OutageTolerance time.Duration `yaml:"for_outage_tolerance"`
	// Minimum duration between alert and restored "for" state. This is maintained only for alerts with configured "for" time greater than grace period.
	ForGracePeriod time.Duration `yaml:"for_grace_period"`
	// Minimum amount of time to wait before resending an alert to Alertmanager.
	ResendDelay time.Duration `yaml:"resend_delay"`

	// Enable sharding rule groups.
	EnableSharding   bool          `yaml:"enable_sharding"`
	ShardingStrategy string        `yaml:"sharding_strategy"`
	SearchPendingFor time.Duration `yaml:"search_pending_for"`
	Ring             RingConfig    `yaml:"ring"`
	FlushCheckPeriod time.Duration `yaml:"flush_period"`

	EnableAPI bool `yaml:"enable_api"`

	EnabledTenants  flagext.StringSliceCSV `yaml:"enabled_tenants"`
	DisabledTenants flagext.StringSliceCSV `yaml:"disabled_tenants"`

	RingCheckPeriod time.Duration `yaml:"-"`

	EnableQueryStats bool `yaml:"query_stats_enabled"`
}

var (
	supportedShardingStrategies = []string{util.ShardingStrategyDefault, util.ShardingStrategyShuffle}

	// Validation errors.
	errInvalidShardingStrategy = errors.New("invalid sharding strategy")
	errInvalidTenantShardSize  = errors.New("invalid tenant shard size, the value must be greater than 0")
)

const (
	// Number of concurrent group list and group loads operations.
	loadRulesConcurrency  = 10
	fetchRulesConcurrency = 16

	rulerSyncReasonInitial    = "initial"
	rulerSyncReasonPeriodic   = "periodic"
	rulerSyncReasonRingChange = "ring-change"

	// Limit errors
	errMaxRuleGroupsPerUserLimitExceeded        = "per-user rule groups limit (limit: %d actual: %d) exceeded"
	errMaxRulesPerRuleGroupPerUserLimitExceeded = "per-user rules per rule group limit (limit: %d actual: %d) exceeded"

	// errors
	errListAllUser = "unable to list the ruler users"
)

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.ClientTLSConfig.RegisterFlagsWithPrefix("ruler.client", f)
	cfg.StoreConfig.RegisterFlags(f)
	cfg.Ring.RegisterFlags(f)
	cfg.Notifier.RegisterFlags(f)

	// Deprecated Flags that will be maintained to avoid user disruption

	//lint:ignore faillint Need to pass the global logger like this for warning on deprecated methods
	flagext.DeprecatedFlag(f, "ruler.client-timeout", "This flag has been renamed to ruler.configs.client-timeout", util_log.Logger)
	//lint:ignore faillint Need to pass the global logger like this for warning on deprecated methods
	flagext.DeprecatedFlag(f, "ruler.group-timeout", "This flag is no longer functional.", util_log.Logger)
	//lint:ignore faillint Need to pass the global logger like this for warning on deprecated methods
	flagext.DeprecatedFlag(f, "ruler.num-workers", "This flag is no longer functional. For increased concurrency horizontal sharding is recommended", util_log.Logger)

	cfg.ExternalURL.URL, _ = url.Parse("") // Must be non-nil
	f.Var(&cfg.ExternalURL, "ruler.external.url", "URL of alerts return path.")
	f.DurationVar(&cfg.EvaluationInterval, "ruler.evaluation-interval", 1*time.Minute, "How frequently to evaluate rules")
	f.DurationVar(&cfg.PollInterval, "ruler.poll-interval", 1*time.Minute, "How frequently to poll for rule changes")

	f.StringVar(&cfg.AlertmanagerURL, "ruler.alertmanager-url", "", "Comma-separated list of URL(s) of the Alertmanager(s) to send notifications to. Each Alertmanager URL is treated as a separate group in the configuration. Multiple Alertmanagers in HA per group can be supported by using DNS resolution via -ruler.alertmanager-discovery.")
	f.BoolVar(&cfg.AlertmanagerDiscovery, "ruler.alertmanager-discovery", false, "Use DNS SRV records to discover Alertmanager hosts.")
	f.DurationVar(&cfg.AlertmanagerRefreshInterval, "ruler.alertmanager-refresh-interval", 1*time.Minute, "How long to wait between refreshing DNS resolutions of Alertmanager hosts.")
	f.BoolVar(&cfg.AlertmanangerEnableV2API, "ruler.alertmanager-use-v2", false, "If enabled requests to Alertmanager will utilize the V2 API.")
	f.IntVar(&cfg.NotificationQueueCapacity, "ruler.notification-queue-capacity", 10000, "Capacity of the queue for notifications to be sent to the Alertmanager.")
	f.DurationVar(&cfg.NotificationTimeout, "ruler.notification-timeout", 10*time.Second, "HTTP timeout duration when sending notifications to the Alertmanager.")

	f.DurationVar(&cfg.SearchPendingFor, "ruler.search-pending-for", 5*time.Minute, "Time to spend searching for a pending ruler when shutting down.")
	f.BoolVar(&cfg.EnableSharding, "ruler.enable-sharding", false, "Distribute rule evaluation using ring backend")
	f.StringVar(&cfg.ShardingStrategy, "ruler.sharding-strategy", util.ShardingStrategyDefault, fmt.Sprintf("The sharding strategy to use. Supported values are: %s.", strings.Join(supportedShardingStrategies, ", ")))
	f.DurationVar(&cfg.FlushCheckPeriod, "ruler.flush-period", 1*time.Minute, "Period with which to attempt to flush rule groups.")
	f.StringVar(&cfg.RulePath, "ruler.rule-path", "/rules", "file path to store temporary rule files for the prometheus rule managers")
	f.BoolVar(&cfg.EnableAPI, "experimental.ruler.enable-api", false, "Enable the ruler api")
	f.DurationVar(&cfg.OutageTolerance, "ruler.for-outage-tolerance", time.Hour, `Max time to tolerate outage for restoring "for" state of alert.`)
	f.DurationVar(&cfg.ForGracePeriod, "ruler.for-grace-period", 10*time.Minute, `Minimum duration between alert and restored "for" state. This is maintained only for alerts with configured "for" time greater than grace period.`)
	f.DurationVar(&cfg.ResendDelay, "ruler.resend-delay", time.Minute, `Minimum amount of time to wait before resending an alert to Alertmanager.`)

	f.Var(&cfg.EnabledTenants, "ruler.enabled-tenants", "Comma separated list of tenants whose rules this ruler can evaluate. If specified, only these tenants will be handled by ruler, otherwise this ruler can process rules from all tenants. Subject to sharding.")
	f.Var(&cfg.DisabledTenants, "ruler.disabled-tenants", "Comma separated list of tenants whose rules this ruler cannot evaluate. If specified, a ruler that would normally pick the specified tenant(s) for processing will ignore them instead. Subject to sharding.")

	f.BoolVar(&cfg.EnableQueryStats, "ruler.query-stats-enabled", false, "Report the wall time for ruler queries to complete as a per user metric and as an info level log message.")

	cfg.RingCheckPeriod = 5 * time.Second
}
