// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/ruler.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/rulefmt"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/weaveworks/common/user"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/ruler/rulestore"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

var (
	errInvalidTenantShardSize = errors.New("invalid tenant shard size, the value must be greater or equal to 0")
)

const (
	// RulerRingKey is the key under which we store the rulers ring in the KVStore.
	RulerRingKey = "ring"
)

type rulesSyncReason string

const (
	// Number of concurrent group list and group loads operations.
	loadRulesConcurrency  = 10
	fetchRulesConcurrency = 16

	rulerSyncReasonInitial    rulesSyncReason = "initial"
	rulerSyncReasonPeriodic   rulesSyncReason = "periodic"
	rulerSyncReasonRingChange rulesSyncReason = "ring-change"

	// Limit errors
	errMaxRuleGroupsPerUserLimitExceeded        = "per-user rule groups limit (limit: %d actual: %d) exceeded"
	errMaxRulesPerRuleGroupPerUserLimitExceeded = "per-user rules per rule group limit (limit: %d actual: %d) exceeded"

	// errors
	errListAllUser = "unable to list the ruler users"
)

// Config is the configuration for the recording rules server.
type Config struct {
	// This is used for template expansion in alerts; must be a valid URL.
	ExternalURL flagext.URLValue `yaml:"external_url"`
	// GRPC Client configuration.
	ClientTLSConfig grpcclient.Config `yaml:"ruler_client" doc:"description=Configures the gRPC client used to communicate between ruler instances."`
	// How frequently to evaluate rules by default.
	EvaluationInterval time.Duration `yaml:"evaluation_interval" category:"advanced"`
	// How frequently to poll for updated rules.
	PollInterval time.Duration `yaml:"poll_interval" category:"advanced"`
	// Path to store rule files for prom manager.
	RulePath string `yaml:"rule_path"`

	// URL of the Alertmanager to send notifications to.
	AlertmanagerURL string `yaml:"alertmanager_url"`
	// How long to wait between refreshing the list of Alertmanager based on DNS service discovery.
	AlertmanagerRefreshInterval time.Duration `yaml:"alertmanager_refresh_interval" category:"advanced"`
	// Capacity of the queue for notifications to be sent to the Alertmanager.
	NotificationQueueCapacity int `yaml:"notification_queue_capacity" category:"advanced"`
	// HTTP timeout duration when sending notifications to the Alertmanager.
	NotificationTimeout time.Duration `yaml:"notification_timeout" category:"advanced"`
	// Client configs for interacting with the Alertmanager
	Notifier NotifierConfig `yaml:"alertmanager_client"`

	// Max time to tolerate outage for restoring "for" state of alert.
	OutageTolerance time.Duration `yaml:"for_outage_tolerance" category:"advanced"`
	// Minimum duration between alert and restored "for" state. This is maintained only for alerts with configured "for" time greater than grace period.
	ForGracePeriod time.Duration `yaml:"for_grace_period" category:"advanced"`
	// Minimum amount of time to wait before resending an alert to Alertmanager.
	ResendDelay time.Duration `yaml:"resend_delay" category:"advanced"`

	// Enable sharding rule groups.
	Ring RingConfig `yaml:"ring"`

	EnableAPI bool `yaml:"enable_api"`

	EnabledTenants  flagext.StringSliceCSV `yaml:"enabled_tenants" category:"advanced"`
	DisabledTenants flagext.StringSliceCSV `yaml:"disabled_tenants" category:"advanced"`

	RingCheckPeriod time.Duration `yaml:"-"`

	EnableQueryStats bool `yaml:"query_stats_enabled" category:"advanced"`

	QueryFrontend QueryFrontendConfig `yaml:"query_frontend"`

	TenantFederation TenantFederationConfig `yaml:"tenant_federation"`
}

// Validate config and returns error on failure
func (cfg *Config) Validate(limits validation.Limits, log log.Logger) error {
	if limits.RulerTenantShardSize < 0 {
		return errInvalidTenantShardSize
	}

	if err := cfg.ClientTLSConfig.Validate(log); err != nil {
		return errors.Wrap(err, "invalid ruler gRPC client config")
	}

	if err := cfg.QueryFrontend.Validate(); err != nil {
		return errors.Wrap(err, "invalid ruler query-frontend config")
	}

	return nil
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.ClientTLSConfig.RegisterFlagsWithPrefix("ruler.client", f)
	cfg.Ring.RegisterFlags(f, logger)
	cfg.Notifier.RegisterFlags(f)
	cfg.TenantFederation.RegisterFlags(f)
	cfg.QueryFrontend.RegisterFlags(f)

	cfg.ExternalURL.URL, _ = url.Parse("") // Must be non-nil
	f.Var(&cfg.ExternalURL, "ruler.external.url", "URL of alerts return path.")
	f.DurationVar(&cfg.EvaluationInterval, "ruler.evaluation-interval", 1*time.Minute, "How frequently to evaluate rules")
	f.DurationVar(&cfg.PollInterval, "ruler.poll-interval", 1*time.Minute, "How frequently to poll for rule changes")

	f.StringVar(&cfg.AlertmanagerURL, "ruler.alertmanager-url", "", "Comma-separated list of URL(s) of the Alertmanager(s) to send notifications to. Each URL is treated as a separate group. Multiple Alertmanagers in HA per group can be supported by using DNS service discovery format, comprehensive of the scheme. Basic auth is supported as part of the URL.")
	f.DurationVar(&cfg.AlertmanagerRefreshInterval, "ruler.alertmanager-refresh-interval", 1*time.Minute, "How long to wait between refreshing DNS resolutions of Alertmanager hosts.")
	f.IntVar(&cfg.NotificationQueueCapacity, "ruler.notification-queue-capacity", 10000, "Capacity of the queue for notifications to be sent to the Alertmanager.")
	f.DurationVar(&cfg.NotificationTimeout, "ruler.notification-timeout", 10*time.Second, "HTTP timeout duration when sending notifications to the Alertmanager.")

	f.StringVar(&cfg.RulePath, "ruler.rule-path", "./data-ruler/", "Directory to store temporary rule files loaded by the Prometheus rule managers. This directory is not required to be persisted between restarts.")
	f.BoolVar(&cfg.EnableAPI, "ruler.enable-api", true, "Enable the ruler config API.")
	f.DurationVar(&cfg.OutageTolerance, "ruler.for-outage-tolerance", time.Hour, `Max time to tolerate outage for restoring "for" state of alert.`)
	f.DurationVar(&cfg.ForGracePeriod, "ruler.for-grace-period", 2*time.Minute, `This grace period controls which alerts the ruler restores after a restart. `+
		`Alerts with "for" duration lower than this grace period are not restored after a ruler restart. `+
		`This means that if the alerts have been firing before the ruler restarted, they will now go to pending state and then to firing again after their "for" duration expires. `+
		`Alerts with "for" duration greater than or equal to this grace period that have been pending before the ruler restart will remain in pending state for at least this grace period. `+
		`Alerts with "for" duration greater than or equal to this grace period that have been firing before the ruler restart will continue to be firing after the restart.`)
	f.DurationVar(&cfg.ResendDelay, "ruler.resend-delay", time.Minute, `Minimum amount of time to wait before resending an alert to Alertmanager.`)

	f.Var(&cfg.EnabledTenants, "ruler.enabled-tenants", "Comma separated list of tenants whose rules this ruler can evaluate. If specified, only these tenants will be handled by ruler, otherwise this ruler can process rules from all tenants. Subject to sharding.")
	f.Var(&cfg.DisabledTenants, "ruler.disabled-tenants", "Comma separated list of tenants whose rules this ruler cannot evaluate. If specified, a ruler that would normally pick the specified tenant(s) for processing will ignore them instead. Subject to sharding.")

	f.BoolVar(&cfg.EnableQueryStats, "ruler.query-stats-enabled", false, "Report the wall time for ruler queries to complete as a per-tenant metric and as an info level log message.")

	cfg.RingCheckPeriod = 5 * time.Second
}

type rulerMetrics struct {
	listRules       prometheus.Histogram
	loadRuleGroups  prometheus.Histogram
	ringCheckErrors prometheus.Counter
	rulerSync       *prometheus.CounterVec
}

func newRulerMetrics(reg prometheus.Registerer) *rulerMetrics {
	return &rulerMetrics{
		listRules: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ruler_list_rules_seconds",
			Help:    "Time spent listing rules.",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 15, 30},
		}),
		loadRuleGroups: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ruler_load_rule_groups_seconds",
			Help:    "Time spent loading all rules for the rule groups in this ruler.",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 15, 30},
		}),
		ringCheckErrors: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ruler_ring_check_errors_total",
			Help: "Number of errors that have occurred when checking the ring for ownership",
		}),
		rulerSync: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ruler_sync_rules_total",
			Help: "Total number of times the ruler sync operation triggered.",
		}, []string{"reason"}),
	}
}

// MultiTenantManager is the interface of interaction with a Manager that is tenant aware.
type MultiTenantManager interface {
	// SyncRuleGroups is used to sync the Manager with rules from the RuleStore.
	// If existing user is missing in the ruleGroups map, its ruler manager will be stopped.
	SyncRuleGroups(ctx context.Context, ruleGroups map[string]rulespb.RuleGroupList)
	// GetRules fetches rules for a particular tenant (userID).
	GetRules(userID string) []*promRules.Group
	// Stop stops all Manager components.
	Stop()
	// ValidateRuleGroup validates a rulegroup
	ValidateRuleGroup(rulefmt.RuleGroup) []error
	// Start evaluating rules.
	Start()
}

// Ruler evaluates rules.
//
//	+---------------------------------------------------------------+
//	|                                                               |
//	|                   Query       +-------------+                 |
//	|            +------------------>             |                 |
//	|            |                  |    Store    |                 |
//	|            | +----------------+             |                 |
//	|            | |     Rules      +-------------+                 |
//	|            | |                                                |
//	|            | |                                                |
//	|            | |                                                |
//	|       +----+-v----+   Filter  +------------+                  |
//	|       |           +----------->            |                  |
//	|       |   Ruler   |           |    Ring    |                  |
//	|       |           <-----------+            |                  |
//	|       +-------+---+   Rules   +------------+                  |
//	|               |                                               |
//	|               |                                               |
//	|               |                                               |
//	|               |    Load      +-----------------+              |
//	|               +-------------->                 |              |
//	|                              |     Manager     |              |
//	|                              |                 |              |
//	|                              +-----------------+              |
//	|                                                               |
//	+---------------------------------------------------------------+
type Ruler struct {
	services.Service

	cfg        Config
	lifecycler *ring.BasicLifecycler
	ring       *ring.Ring
	store      rulestore.RuleStore
	manager    MultiTenantManager
	limits     RulesLimits

	metrics *rulerMetrics

	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	// Pool of clients used to connect to other ruler replicas.
	clientsPool ClientsPool

	allowedTenants *util.AllowedTenants

	registry prometheus.Registerer
	logger   log.Logger
}

// NewRuler creates a new ruler from a distributor and chunk store.
func NewRuler(cfg Config, manager MultiTenantManager, reg prometheus.Registerer, logger log.Logger, ruleStore rulestore.RuleStore, limits RulesLimits) (*Ruler, error) {
	return newRuler(cfg, manager, reg, logger, ruleStore, limits, newRulerClientPool(cfg.ClientTLSConfig, logger, reg))
}

func newRuler(cfg Config, manager MultiTenantManager, reg prometheus.Registerer, logger log.Logger, ruleStore rulestore.RuleStore, limits RulesLimits, clientPool ClientsPool) (*Ruler, error) {
	ruler := &Ruler{
		cfg:            cfg,
		store:          ruleStore,
		manager:        manager,
		registry:       reg,
		logger:         logger,
		limits:         limits,
		clientsPool:    clientPool,
		allowedTenants: util.NewAllowedTenants(cfg.EnabledTenants, cfg.DisabledTenants),
		metrics:        newRulerMetrics(reg),
	}

	if len(cfg.EnabledTenants) > 0 {
		level.Info(ruler.logger).Log("msg", "ruler using enabled users", "enabled", strings.Join(cfg.EnabledTenants, ", "))
	}
	if len(cfg.DisabledTenants) > 0 {
		level.Info(ruler.logger).Log("msg", "ruler using disabled users", "disabled", strings.Join(cfg.DisabledTenants, ", "))
	}

	ringStore, err := kv.NewClient(
		cfg.Ring.Common.KVStore,
		ring.GetCodec(),
		kv.RegistererWithKVName(prometheus.WrapRegistererWithPrefix("cortex_", reg), "ruler"),
		logger,
	)
	if err != nil {
		return nil, errors.Wrap(err, "create KV store client")
	}

	if err := enableSharding(ruler, ringStore); err != nil {
		return nil, errors.Wrap(err, "setup ruler sharding ring")
	}

	ruler.Service = services.NewBasicService(ruler.starting, ruler.run, ruler.stopping)
	return ruler, nil
}

func enableSharding(r *Ruler, ringStore kv.Client) error {
	lifecyclerCfg, err := r.cfg.Ring.ToLifecyclerConfig(r.logger)
	if err != nil {
		return errors.Wrap(err, "failed to initialize ruler's lifecycler config")
	}

	// Define lifecycler delegates in reverse order (last to be called defined first because they're
	// chained via "next delegate").
	delegate := ring.BasicLifecyclerDelegate(ring.NewInstanceRegisterDelegate(ring.JOINING, r.cfg.Ring.NumTokens))
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, r.logger)
	delegate = ring.NewAutoForgetDelegate(r.cfg.Ring.Common.HeartbeatTimeout*ringAutoForgetUnhealthyPeriods, delegate, r.logger)

	rulerRingName := "ruler"
	r.lifecycler, err = ring.NewBasicLifecycler(lifecyclerCfg, rulerRingName, RulerRingKey, ringStore, delegate, r.logger, prometheus.WrapRegistererWithPrefix("cortex_", r.registry))
	if err != nil {
		return errors.Wrap(err, "failed to initialize ruler's lifecycler")
	}

	r.ring, err = ring.NewWithStoreClientAndStrategy(r.cfg.Ring.toRingConfig(), rulerRingName, RulerRingKey, ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), prometheus.WrapRegistererWithPrefix("cortex_", r.registry), r.logger)
	if err != nil {
		return errors.Wrap(err, "failed to initialize ruler's ring")
	}

	return nil
}

func (r *Ruler) starting(ctx context.Context) error {
	var err error

	if r.subservices, err = services.NewManager(r.lifecycler, r.ring, r.clientsPool); err != nil {
		return errors.Wrap(err, "unable to start ruler subservices")
	}

	r.subservicesWatcher = services.NewFailureWatcher()
	r.subservicesWatcher.WatchManager(r.subservices)

	if err = services.StartManagerAndAwaitHealthy(ctx, r.subservices); err != nil {
		return errors.Wrap(err, "unable to start ruler subservices")
	}

	// Sync the rule when the ruler is JOINING the ring.
	// Activate the rule evaluation after the ruler is ACTIVE in the ring.
	// This is to make sure that the ruler is ready to evaluate alert rules once it is ACTIVE in the ring.
	level.Info(r.logger).Log("msg", "waiting until ruler is JOINING in the ring")
	if err := ring.WaitInstanceState(ctx, r.ring, r.lifecycler.GetInstanceID(), ring.JOINING); err != nil {
		return err
	}
	level.Info(r.logger).Log("msg", "ruler is JOINING in the ring")

	// here during joining, we can start to download rules from object storage and sync them to the local rule manager
	r.syncRules(ctx, rulerSyncReasonInitial)

	if err = r.lifecycler.ChangeState(ctx, ring.ACTIVE); err != nil {
		return errors.Wrapf(err, "switch instance to %s in the ring", ring.ACTIVE)
	}

	level.Info(r.logger).Log("msg", "waiting until ruler is ACTIVE in the ring")
	if err := ring.WaitInstanceState(ctx, r.ring, r.lifecycler.GetInstanceID(), ring.ACTIVE); err != nil {
		return err
	}
	level.Info(r.logger).Log("msg", "ruler is ACTIVE in the ring")

	r.manager.Start()
	level.Info(r.logger).Log("msg", "ruler is only now starting to evaluate rules")

	// TODO: ideally, ruler would wait until its queryable is finished starting.
	return nil
}

// Stop stops the Ruler.
// Each function of the ruler is terminated before leaving the ring
func (r *Ruler) stopping(_ error) error {
	r.manager.Stop()

	if r.subservices != nil {
		_ = services.StopManagerAndAwaitStopped(context.Background(), r.subservices)
	}
	return nil
}

var sep = []byte("/")

func tokenForGroup(g *rulespb.RuleGroupDesc) uint32 {
	ringHasher := fnv.New32a()

	// Hasher never returns err.
	_, _ = ringHasher.Write([]byte(g.User))
	_, _ = ringHasher.Write(sep)
	_, _ = ringHasher.Write([]byte(g.Namespace))
	_, _ = ringHasher.Write(sep)
	_, _ = ringHasher.Write([]byte(g.Name))

	return ringHasher.Sum32()
}

func instanceOwnsRuleGroup(r ring.ReadRing, g *rulespb.RuleGroupDesc, instanceAddr string, reason rulesSyncReason) (bool, error) {
	hash := tokenForGroup(g)
	var rlrs ring.ReplicationSet
	var err error
	if reason == rulerSyncReasonInitial {
		rlrs, err = r.Get(hash, RuleSyncRingOp, nil, nil, nil)
	} else {
		rlrs, err = r.Get(hash, RuleEvalRingOp, nil, nil, nil)
	}
	if err != nil {
		return false, errors.Wrap(err, "error reading ring to verify rule group ownership")
	}

	return rlrs.Instances[0].Addr == instanceAddr, nil
}

func (r *Ruler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.ring.ServeHTTP(w, req)
}

func (r *Ruler) run(ctx context.Context) error {
	level.Info(r.logger).Log("msg", "ruler up and running")

	tick := time.NewTicker(r.cfg.PollInterval)
	defer tick.Stop()

	ringLastState, _ := r.ring.GetAllHealthy(RuleEvalRingOp)
	ringTicker := time.NewTicker(util.DurationWithJitter(r.cfg.RingCheckPeriod, 0.2))
	defer ringTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			r.syncRules(ctx, rulerSyncReasonPeriodic)
		case <-ringTicker.C:
			// We ignore the error because in case of error it will return an empty
			// replication set which we use to compare with the previous state.
			currRingState, _ := r.ring.GetAllHealthy(RuleEvalRingOp)

			if ring.HasReplicationSetChanged(ringLastState, currRingState) {
				ringLastState = currRingState
				r.syncRules(ctx, rulerSyncReasonRingChange)
			}
		case err := <-r.subservicesWatcher.Chan():
			return errors.Wrap(err, "ruler subservice failed")
		}
	}
}

// It's not safe to call this function concurrently.
// We expect this function is only called from Ruler.run().
func (r *Ruler) syncRules(ctx context.Context, reason rulesSyncReason) {
	level.Debug(r.logger).Log("msg", "syncing rules", "reason", reason)
	r.metrics.rulerSync.WithLabelValues(string(reason)).Inc()

	configs, err := r.listRules(ctx, reason)
	if err != nil {
		level.Error(r.logger).Log("msg", "unable to list rules", "err", err)
		return
	}

	err = r.loadRuleGroups(ctx, configs)
	if err != nil {
		level.Error(r.logger).Log("msg", "unable to load rules owned by this ruler", "err", err)
		return
	}

	// Filter out all rules for which their evaluation has been disabled for the given tenant.
	configs = filterRuleGroupsByEnabled(configs, r.limits, r.logger)

	// This will also delete local group files for users that are no longer in 'configs' map.
	r.manager.SyncRuleGroups(ctx, configs)
}

func (r *Ruler) loadRuleGroups(ctx context.Context, configs map[string]rulespb.RuleGroupList) error {
	start := time.Now()
	defer func() {
		r.metrics.loadRuleGroups.Observe(time.Since(start).Seconds())
	}()
	return r.store.LoadRuleGroups(ctx, configs)
}

func (r *Ruler) listRules(ctx context.Context, reason rulesSyncReason) (result map[string]rulespb.RuleGroupList, err error) {
	start := time.Now()
	defer func() {
		r.metrics.listRules.Observe(time.Since(start).Seconds())
	}()

	result, err = r.listRulesSharded(ctx, reason)
	if err != nil {
		return
	}

	for userID := range result {
		if !r.allowedTenants.IsAllowed(userID) {
			level.Debug(r.logger).Log("msg", "ignoring rule groups for user, not allowed", "user", userID)
			delete(result, userID)
		}
	}
	return
}

func (r *Ruler) listRulesSharded(ctx context.Context, reason rulesSyncReason) (map[string]rulespb.RuleGroupList, error) {
	users, err := r.store.ListAllUsers(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "unable to list users of ruler")
	}

	// Only users in userRings will be used in the to load the rules.
	userRings := map[string]ring.ReadRing{}
	for _, u := range users {
		if shardSize := r.limits.RulerTenantShardSize(u); shardSize > 0 {
			subRing := r.ring.ShuffleShard(u, shardSize)

			// Include the user only if it belongs to this ruler shard.
			if subRing.HasInstance(r.lifecycler.GetInstanceID()) {
				userRings[u] = subRing
			}
		} else {
			// A shard size of 0 means shuffle sharding is disabled for this specific user.
			// In that case we use the full ring so that rule groups will be sharded across all rulers.
			userRings[u] = r.ring
		}
	}

	if len(userRings) == 0 {
		return nil, nil
	}

	userCh := make(chan string, len(userRings))
	for u := range userRings {
		userCh <- u
	}
	close(userCh)

	mu := sync.Mutex{}
	result := map[string]rulespb.RuleGroupList{}

	concurrency := loadRulesConcurrency
	if len(userRings) < concurrency {
		concurrency = len(userRings)
	}

	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			for userID := range userCh {
				groups, err := r.store.ListRuleGroupsForUserAndNamespace(gctx, userID, "")
				if err != nil {
					return errors.Wrapf(err, "failed to fetch rule groups for user %s", userID)
				}

				filtered := filterRuleGroupsByOwnership(userID, groups, userRings[userID], r.lifecycler.GetInstanceAddr(), r.logger, r.metrics.ringCheckErrors, reason)
				if len(filtered) == 0 {
					continue
				}

				mu.Lock()
				result[userID] = filtered
				mu.Unlock()
			}
			return nil
		})
	}

	err = g.Wait()
	return result, err
}

// filterRuleGroupsByOwnership returns map of rule groups that given instance "owns" based on supplied ring.
// This function only uses User, Namespace, and Name fields of individual RuleGroups.
//
// Reason why this function is not a method on Ruler is to make sure we don't accidentally use r.ring,
// but only ring passed as parameter.
func filterRuleGroupsByOwnership(userID string, ruleGroups []*rulespb.RuleGroupDesc, ring ring.ReadRing, instanceAddr string, log log.Logger, ringCheckErrors prometheus.Counter, reason rulesSyncReason) []*rulespb.RuleGroupDesc {
	// Prune the rule group to only contain rules that this ruler is responsible for, based on ring.
	var result []*rulespb.RuleGroupDesc
	for _, g := range ruleGroups {
		owned, err := instanceOwnsRuleGroup(ring, g, instanceAddr, reason)
		if err != nil {
			ringCheckErrors.Inc()
			level.Error(log).Log("msg", "failed to check if the ruler replica owns the rule group", "user", userID, "namespace", g.Namespace, "group", g.Name, "err", err)
			continue
		}

		if owned {
			level.Debug(log).Log("msg", "rule group owned", "user", g.User, "namespace", g.Namespace, "name", g.Name)
			result = append(result, g)
		} else {
			level.Debug(log).Log("msg", "rule group not owned, ignoring", "user", g.User, "namespace", g.Namespace, "name", g.Name)
		}
	}

	return result
}

// filterRuleGroupsByEnabled filters out from the input configs all the recording and/or alerting rules whose evaluation
// has been disabled for the given tenant.
//
// This function doesn't modify the input configs in place (even if it could) in order to reduce the likelihood of introducing
// future bugs, in case the rule groups will be cached in memory.
func filterRuleGroupsByEnabled(configs map[string]rulespb.RuleGroupList, limits RulesLimits, logger log.Logger) (filtered map[string]rulespb.RuleGroupList) {
	// Quick case: nothing if do if no user has rules disabled.
	shouldFilter := false
	for userID := range configs {
		recordingEnabled := limits.RulerRecordingRulesEvaluationEnabled(userID)
		alertingEnabled := limits.RulerAlertingRulesEvaluationEnabled(userID)

		if !recordingEnabled || !alertingEnabled {
			shouldFilter = true
			break
		}
	}

	if !shouldFilter {
		return configs
	}

	// Filter out disabled rules.
	filtered = make(map[string]rulespb.RuleGroupList, len(configs))

	for userID, groups := range configs {
		recordingEnabled := limits.RulerRecordingRulesEvaluationEnabled(userID)
		alertingEnabled := limits.RulerAlertingRulesEvaluationEnabled(userID)

		// Quick case: nothing to do if all rules are enabled.
		if recordingEnabled && alertingEnabled {
			filtered[userID] = groups
			continue
		}

		// Quick case: remove the user at all if all rules are disabled.
		if !recordingEnabled && !alertingEnabled {
			// We don't expect rules evaluation to be disabled for the normal use case. For this reason,
			// when it's disabled we prefer to log it with "info" instead of "debug" to make it more visible.
			level.Info(logger).Log(
				"msg", "filtered out all rules because evaluation is disabled for the tenant",
				"user", userID,
				"recording_rules_enabled", recordingEnabled,
				"alerting_rules_enabled", alertingEnabled)
			continue
		}

		filtered[userID] = make(rulespb.RuleGroupList, 0, len(groups))
		removedRulesTotal := 0

		for _, group := range groups {
			filteredGroup, removedRules := filterRuleGroupByEnabled(group, recordingEnabled, alertingEnabled)
			if filteredGroup != nil {
				filtered[userID] = append(filtered[userID], filteredGroup)
			}

			removedRulesTotal += removedRules
		}

		if removedRulesTotal > 0 {
			// We don't expect rules evaluation to be disabled for the normal use case. For this reason,
			// when it's disabled we prefer to log it with "info" instead of "debug" to make it more visible.
			level.Info(logger).Log(
				"msg", "filtered out rules because evaluation is disabled for the tenant",
				"user", userID,
				"removed_rules", removedRulesTotal,
				"recording_rules_enabled", recordingEnabled,
				"alerting_rules_enabled", alertingEnabled)
		}
	}

	return filtered
}

func filterRuleGroupByEnabled(group *rulespb.RuleGroupDesc, recordingEnabled, alertingEnabled bool) (filtered *rulespb.RuleGroupDesc, removedRules int) {
	// Check if there are actually rules to be removed.
	for _, rule := range group.Rules {
		if rule.Record != "" && !recordingEnabled {
			removedRules++
		} else if rule.Alert != "" && !alertingEnabled {
			removedRules++
		}
	}

	// Quick case: no rules to remove.
	if removedRules == 0 {
		return group, 0
	}

	// Quick case: all rules to remove.
	if removedRules == len(group.Rules) {
		return nil, removedRules
	}

	// Create a copy of the group and remove some rules.
	filtered = &rulespb.RuleGroupDesc{
		Name:          group.Name,
		Namespace:     group.Namespace,
		Interval:      group.Interval,
		Rules:         make([]*rulespb.RuleDesc, 0, len(group.Rules)-removedRules),
		User:          group.User,
		Options:       group.Options,
		SourceTenants: group.SourceTenants,
	}

	for _, rule := range group.Rules {
		if rule.Record != "" && !recordingEnabled {
			continue
		}
		if rule.Alert != "" && !alertingEnabled {
			continue
		}

		filtered.Rules = append(filtered.Rules, rule)
	}

	return filtered, removedRules
}

// GetRules retrieves the running rules from this ruler and all running rulers in the ring.
func (r *Ruler) GetRules(ctx context.Context) ([]*GroupStateDesc, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, fmt.Errorf("no user id found in context")
	}

	ring := ring.ReadRing(r.ring)

	if shardSize := r.limits.RulerTenantShardSize(userID); shardSize > 0 {
		ring = r.ring.ShuffleShard(userID, shardSize)
	}

	rulers, err := ring.GetReplicationSetForOperation(RuleEvalRingOp)
	if err != nil {
		return nil, err
	}

	ctx, err = user.InjectIntoGRPCRequest(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to inject user ID into grpc request, %v", err)
	}

	var (
		mergedMx sync.Mutex
		merged   []*GroupStateDesc
	)

	// Concurrently fetch rules from all rulers. Since rules are not replicated,
	// we need all requests to succeed.
	addrs := rulers.GetAddresses()
	err = concurrency.ForEachJob(ctx, len(addrs), len(addrs), func(ctx context.Context, idx int) error {
		addr := addrs[idx]

		rulerClient, err := r.clientsPool.GetClientFor(addr)
		if err != nil {
			return errors.Wrapf(err, "unable to get client for ruler %s", addr)
		}

		newGrps, err := rulerClient.Rules(ctx, &RulesRequest{})
		if err != nil {
			return errors.Wrapf(err, "unable to retrieve rules from ruler %s", addr)
		}

		mergedMx.Lock()
		merged = append(merged, newGrps.Groups...)
		mergedMx.Unlock()

		return nil
	})

	return merged, err
}

// Rules implements the rules service
func (r *Ruler) Rules(ctx context.Context, in *RulesRequest) (*RulesResponse, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, fmt.Errorf("no user id found in context")
	}

	groupDescs, err := r.getLocalRules(userID)
	if err != nil {
		return nil, err
	}

	return &RulesResponse{Groups: groupDescs}, nil
}

func (r *Ruler) getLocalRules(userID string) ([]*GroupStateDesc, error) {
	groups := r.manager.GetRules(userID)

	groupDescs := make([]*GroupStateDesc, 0, len(groups))
	prefix := filepath.Join(r.cfg.RulePath, userID) + "/"

	for _, group := range groups {
		interval := group.Interval()

		// The mapped filename is url path escaped encoded to make handling `/` characters easier
		decodedNamespace, err := url.PathUnescape(strings.TrimPrefix(group.File(), prefix))
		if err != nil {
			return nil, errors.Wrap(err, "unable to decode rule filename")
		}

		groupDesc := &GroupStateDesc{
			Group: &rulespb.RuleGroupDesc{
				Name:          group.Name(),
				Namespace:     decodedNamespace,
				Interval:      interval,
				User:          userID,
				SourceTenants: group.SourceTenants(),
			},

			EvaluationTimestamp: group.GetLastEvaluation(),
			EvaluationDuration:  group.GetEvaluationTime(),
		}
		for _, r := range group.Rules() {
			lastError := ""
			if r.LastError() != nil {
				lastError = r.LastError().Error()
			}

			var ruleDesc *RuleStateDesc
			switch rule := r.(type) {
			case *promRules.AlertingRule:
				rule.ActiveAlerts()
				alerts := []*AlertStateDesc{}
				for _, a := range rule.ActiveAlerts() {
					alerts = append(alerts, &AlertStateDesc{
						State:           a.State.String(),
						Labels:          mimirpb.FromLabelsToLabelAdapters(a.Labels),
						Annotations:     mimirpb.FromLabelsToLabelAdapters(a.Annotations),
						Value:           a.Value,
						ActiveAt:        a.ActiveAt,
						FiredAt:         a.FiredAt,
						ResolvedAt:      a.ResolvedAt,
						LastSentAt:      a.LastSentAt,
						ValidUntil:      a.ValidUntil,
						KeepFiringSince: a.KeepFiringSince,
					})
				}
				ruleDesc = &RuleStateDesc{
					Rule: &rulespb.RuleDesc{
						Expr:          rule.Query().String(),
						Alert:         rule.Name(),
						For:           rule.HoldDuration(),
						KeepFiringFor: rule.KeepFiringFor(),
						Labels:        mimirpb.FromLabelsToLabelAdapters(rule.Labels()),
						Annotations:   mimirpb.FromLabelsToLabelAdapters(rule.Annotations()),
					},
					State:               rule.State().String(),
					Health:              string(rule.Health()),
					LastError:           lastError,
					Alerts:              alerts,
					EvaluationTimestamp: rule.GetEvaluationTimestamp(),
					EvaluationDuration:  rule.GetEvaluationDuration(),
				}
			case *promRules.RecordingRule:
				ruleDesc = &RuleStateDesc{
					Rule: &rulespb.RuleDesc{
						Record: rule.Name(),
						Expr:   rule.Query().String(),
						Labels: mimirpb.FromLabelsToLabelAdapters(rule.Labels()),
					},
					Health:              string(rule.Health()),
					LastError:           lastError,
					EvaluationTimestamp: rule.GetEvaluationTimestamp(),
					EvaluationDuration:  rule.GetEvaluationDuration(),
				}
			default:
				return nil, errors.Errorf("failed to assert type of rule '%v'", rule.Name())
			}
			groupDesc.ActiveRules = append(groupDesc.ActiveRules, ruleDesc)
		}
		groupDescs = append(groupDescs, groupDesc)
	}
	return groupDescs, nil
}

// AssertMaxRuleGroups limit has not been reached compared to the current
// number of total rule groups in input and returns an error if so.
func (r *Ruler) AssertMaxRuleGroups(userID string, rg int) error {
	limit := r.limits.RulerMaxRuleGroupsPerTenant(userID)

	if limit <= 0 {
		return nil
	}

	if rg <= limit {
		return nil
	}

	return fmt.Errorf(errMaxRuleGroupsPerUserLimitExceeded, limit, rg)
}

// AssertMaxRulesPerRuleGroup limit has not been reached compared to the current
// number of rules in a rule group in input and returns an error if so.
func (r *Ruler) AssertMaxRulesPerRuleGroup(userID string, rules int) error {
	limit := r.limits.RulerMaxRulesPerRuleGroup(userID)

	if limit <= 0 {
		return nil
	}

	if rules <= limit {
		return nil
	}
	return fmt.Errorf(errMaxRulesPerRuleGroupPerUserLimitExceeded, limit, rules)
}

func (r *Ruler) DeleteTenantConfiguration(w http.ResponseWriter, req *http.Request) {
	logger := util_log.WithContext(req.Context(), r.logger)

	userID, err := tenant.TenantID(req.Context())
	if err != nil {
		// When Mimir is running, it uses Auth Middleware for checking X-Scope-OrgID and injecting tenant into context.
		// Auth Middleware sends http.StatusUnauthorized if X-Scope-OrgID is missing, so we do too here, for consistency.
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	err = r.store.DeleteNamespace(req.Context(), userID, "") // Empty namespace = delete all rule groups.
	if err != nil && !errors.Is(err, rulestore.ErrGroupNamespaceNotFound) {
		respondError(logger, w, err.Error())
		return
	}

	level.Info(logger).Log("msg", "deleted all tenant rule groups", "user", userID)
	w.WriteHeader(http.StatusOK)
}

func (r *Ruler) ListAllRules(w http.ResponseWriter, req *http.Request) {
	logger := util_log.WithContext(req.Context(), r.logger)

	userIDs, err := r.store.ListAllUsers(req.Context())
	if err != nil {
		level.Error(logger).Log("msg", errListAllUser, "err", err)
		http.Error(w, fmt.Sprintf("%s: %s", errListAllUser, err.Error()), http.StatusInternalServerError)
		return
	}

	done := make(chan struct{})
	iter := make(chan interface{})

	go func() {
		util.StreamWriteYAMLResponse(w, iter, logger)
		close(done)
	}()

	err = concurrency.ForEachUser(req.Context(), userIDs, fetchRulesConcurrency, func(ctx context.Context, userID string) error {
		rg, err := r.store.ListRuleGroupsForUserAndNamespace(ctx, userID, "")
		if err != nil {
			return errors.Wrapf(err, "failed to fetch ruler config for user %s", userID)
		}
		userRules := map[string]rulespb.RuleGroupList{userID: rg}
		if err := r.store.LoadRuleGroups(ctx, userRules); err != nil {
			return errors.Wrapf(err, "failed to load ruler config for user %s", userID)
		}
		data := map[string]map[string][]rulefmt.RuleGroup{userID: userRules[userID].Formatted()}

		select {
		case iter <- data:
		case <-done: // stop early, if sending response has already finished
		}

		return nil
	})
	if err != nil {
		level.Error(logger).Log("msg", "failed to list all ruler configs", "err", err)
	}
	close(iter)
	<-done
}
