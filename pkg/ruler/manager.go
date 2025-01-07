// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/manager.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/user"
	ot "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/notifier"
	promRules "github.com/prometheus/prometheus/rules"
	"go.uber.org/atomic"
	"golang.org/x/net/context/ctxhttp"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
)

type DefaultMultiTenantManager struct {
	cfg            Config
	notifierCfg    *config.Config
	managerFactory ManagerFactory

	mapper *mapper

	// Struct for holding per-user Prometheus rules Managers.
	userManagerMtx sync.RWMutex
	userManagers   map[string]RulesManager

	// Prometheus rules managers metrics.
	userManagerMetrics *ManagerMetrics

	// Per-user notifiers with separate queues.
	notifiersMtx sync.Mutex
	notifiers    map[string]*rulerNotifier

	managersTotal                 prometheus.Gauge
	lastReloadSuccessful          *prometheus.GaugeVec
	lastReloadSuccessfulTimestamp *prometheus.GaugeVec
	configUpdatesTotal            *prometheus.CounterVec
	registry                      prometheus.Registerer
	logger                        log.Logger

	rulerIsRunning atomic.Bool
}

func NewDefaultMultiTenantManager(cfg Config, managerFactory ManagerFactory, reg prometheus.Registerer, logger log.Logger, dnsResolver cache.AddressProvider) (*DefaultMultiTenantManager, error) {
	refreshMetrics := discovery.NewRefreshMetrics(reg)
	ncfg, err := buildNotifierConfig(&cfg, dnsResolver, refreshMetrics)
	if err != nil {
		return nil, err
	}

	userManagerMetrics := NewManagerMetrics(logger)
	if reg != nil {
		reg.MustRegister(userManagerMetrics)
	}

	return &DefaultMultiTenantManager{
		cfg:                cfg,
		notifierCfg:        ncfg,
		managerFactory:     managerFactory,
		notifiers:          map[string]*rulerNotifier{},
		mapper:             newMapper(cfg.RulePath, logger),
		userManagers:       map[string]RulesManager{},
		userManagerMetrics: userManagerMetrics,
		managersTotal: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "ruler_managers_total",
			Help:      "Total number of managers registered and running in the ruler",
		}),
		lastReloadSuccessful: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "ruler_config_last_reload_successful",
			Help:      "Boolean set to 1 whenever the last configuration reload attempt was successful.",
		}, []string{"user"}),
		lastReloadSuccessfulTimestamp: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "ruler_config_last_reload_successful_seconds",
			Help:      "Timestamp of the last successful configuration reload.",
		}, []string{"user"}),
		configUpdatesTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "ruler_config_updates_total",
			Help:      "Total number of config updates triggered by a user",
		}, []string{"user"}),
		registry: reg,
		logger:   logger,
	}, nil
}

// SyncFullRuleGroups implements MultiTenantManager.
// It's not safe to call this function concurrently with SyncFullRuleGroups() or SyncPartialRuleGroups().
func (r *DefaultMultiTenantManager) SyncFullRuleGroups(ctx context.Context, ruleGroupsByUser map[string]rulespb.RuleGroupList) {
	if !r.cfg.TenantFederation.Enabled {
		removeFederatedRuleGroups(ruleGroupsByUser, r.logger)
	}

	if err := r.syncRulesToManagerConcurrently(ctx, ruleGroupsByUser); err != nil {
		// We don't log it because the only error we could get here is a context canceled.
		return
	}

	// Check for deleted users and remove them.
	r.removeUsersIf(func(userID string) bool {
		_, exists := ruleGroupsByUser[userID]
		return !exists
	})
}

// SyncPartialRuleGroups implements MultiTenantManager.
// It's not safe to call this function concurrently with SyncFullRuleGroups() or SyncPartialRuleGroups().
func (r *DefaultMultiTenantManager) SyncPartialRuleGroups(ctx context.Context, ruleGroupsByUser map[string]rulespb.RuleGroupList) {
	if !r.cfg.TenantFederation.Enabled {
		removeFederatedRuleGroups(ruleGroupsByUser, r.logger)
	}

	// Filter out tenants with no rule groups.
	ruleGroupsByUser, removedUsers := filterRuleGroupsByNotEmptyUsers(ruleGroupsByUser)

	if err := r.syncRulesToManagerConcurrently(ctx, ruleGroupsByUser); err != nil {
		// We don't log it because the only error we could get here is a context canceled.
		return
	}

	// Check for deleted users and remove them.
	r.removeUsersIf(func(userID string) bool {
		_, removed := removedUsers[userID]
		return removed
	})
}

func (r *DefaultMultiTenantManager) Start() {
	r.userManagerMtx.Lock()
	defer r.userManagerMtx.Unlock()

	// Skip starting the user managers if the ruler is already running.
	if r.rulerIsRunning.Load() {
		return
	}

	for _, mngr := range r.userManagers {
		go mngr.Run()
	}
	// set rulerIsRunning to true once user managers are started.
	r.rulerIsRunning.Store(true)
}

// syncRulesToManagerConcurrently calls syncRulesToManager() concurrently for each user in the input
// ruleGroups. The max concurrency is limited. This function is expected to return an error only if
// the input ctx is canceled.
func (r *DefaultMultiTenantManager) syncRulesToManagerConcurrently(ctx context.Context, ruleGroups map[string]rulespb.RuleGroupList) error {
	// Sync the rules to disk and then update the user's Prometheus Rules Manager.
	// Since users are different, we can sync rules in parallel.
	users := make([]string, 0, len(ruleGroups))
	for userID := range ruleGroups {
		users = append(users, userID)
	}

	err := concurrency.ForEachJob(ctx, len(users), 10, func(_ context.Context, idx int) error {
		userID := users[idx]
		r.syncRulesToManager(userID, ruleGroups[userID])
		return nil
	})

	// Update the metric even in case of error.
	r.userManagerMtx.RLock()
	r.managersTotal.Set(float64(len(r.userManagers)))
	r.userManagerMtx.RUnlock()

	return err
}

// syncRulesToManager maps the rule files to disk, detects any changes and will create/update
// the user's Prometheus Rules Manager. Since this method writes to disk it is not safe to call
// concurrently for the same user.
func (r *DefaultMultiTenantManager) syncRulesToManager(user string, groups rulespb.RuleGroupList) {
	// Map the files to disk and return the file names to be passed to the users manager if they
	// have been updated
	update, files, err := r.mapper.MapRules(user, groups.Formatted())
	if err != nil {
		r.lastReloadSuccessful.WithLabelValues(user).Set(0)
		level.Error(r.logger).Log("msg", "unable to map rule files", "user", user, "err", err)
		return
	}

	manager, created, err := r.getOrCreateManager(user)
	if err != nil {
		r.lastReloadSuccessful.WithLabelValues(user).Set(0)
		level.Error(r.logger).Log("msg", "unable to create rule manager", "user", user, "err", err)
		return
	}

	// We need to update the manager only if it was just created or rules on disk have changed.
	if !(created || update) {
		level.Debug(r.logger).Log("msg", "rules have not changed, skipping rule manager update", "user", user)
		return
	}

	level.Debug(r.logger).Log("msg", "updating rules", "user", user)
	r.configUpdatesTotal.WithLabelValues(user).Inc()

	err = manager.Update(r.cfg.EvaluationInterval, files, labels.EmptyLabels(), r.cfg.ExternalURL.String(), nil)
	if err != nil {
		r.lastReloadSuccessful.WithLabelValues(user).Set(0)
		level.Error(r.logger).Log("msg", "unable to update rule manager", "user", user, "err", err)
		return
	}

	r.lastReloadSuccessful.WithLabelValues(user).Set(1)
	r.lastReloadSuccessfulTimestamp.WithLabelValues(user).SetToCurrentTime()
}

// getOrCreateManager retrieves the user manager. If it doesn't exist, it will create and start it first.
func (r *DefaultMultiTenantManager) getOrCreateManager(user string) (RulesManager, bool, error) {
	// Check if it already exists. Since rules are synched frequently, we expect to already exist
	// most of the times.
	r.userManagerMtx.RLock()
	manager, exists := r.userManagers[user]
	r.userManagerMtx.RUnlock()

	if exists {
		return manager, false, nil
	}

	// The manager doesn't exist. We take an exclusive lock to create it.
	r.userManagerMtx.Lock()
	defer r.userManagerMtx.Unlock()

	// Ensure it hasn't been created in the meanwhile.
	manager, exists = r.userManagers[user]
	if exists {
		return manager, false, nil
	}

	level.Debug(r.logger).Log("msg", "creating rule manager for user", "user", user)
	manager, err := r.newManager(user)
	if err != nil {
		return nil, false, err
	}

	// manager.Run() starts running the manager and blocks until Stop() is called.
	// Hence run it as another goroutine.
	// We only start the rule manager if the ruler is in running state.
	if r.rulerIsRunning.Load() {
		go manager.Run()
	}

	r.userManagers[user] = manager
	return manager, true, nil
}

// newManager creates a prometheus rule manager wrapped with a user id
// configured storage, appendable, notifier, and instrumentation
func (r *DefaultMultiTenantManager) newManager(userID string) (RulesManager, error) {
	notifier, err := r.getOrCreateNotifier(userID)
	if err != nil {
		return nil, err
	}

	// Create a new Prometheus registry and register it within
	// our metrics struct for the provided user.
	reg := prometheus.NewRegistry()
	r.userManagerMetrics.AddUserRegistry(userID, reg)

	// We pass context.Background() to the managerFactory because the manager is shut down via Stop()
	// instead of context cancellations. Cancelling the context might cause inflight evaluations to be immediately
	// aborted. We want a graceful shutdown of evaluations.
	return r.managerFactory(context.Background(), userID, notifier, r.logger, reg), nil
}

func (r *DefaultMultiTenantManager) getOrCreateNotifier(userID string) (*notifier.Manager, error) {
	r.notifiersMtx.Lock()
	defer r.notifiersMtx.Unlock()

	n, ok := r.notifiers[userID]
	if ok {
		return n.notifier, nil
	}

	reg := prometheus.WrapRegistererWith(prometheus.Labels{"user": userID}, r.registry)
	reg = prometheus.WrapRegistererWithPrefix("cortex_", reg)
	var err error
	if n, err = newRulerNotifier(&notifier.Options{
		QueueCapacity:   r.cfg.NotificationQueueCapacity,
		DrainOnShutdown: true,
		Registerer:      reg,
		Do: func(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
			// Note: The passed-in context comes from the Prometheus notifier
			// and does *not* contain the userID. So it needs to be added to the context
			// here before using the context to inject the userID into the HTTP request.
			ctx = user.InjectOrgID(ctx, userID)
			if err := user.InjectOrgIDIntoHTTPRequest(ctx, req); err != nil {
				return nil, err
			}
			// Jaeger complains the passed-in context has an invalid span ID, so start a new root span
			sp := ot.GlobalTracer().StartSpan("notify", ot.Tag{Key: "organization", Value: userID})
			defer sp.Finish()
			ctx = ot.ContextWithSpan(ctx, sp)
			_ = ot.GlobalTracer().Inject(sp.Context(), ot.HTTPHeaders, ot.HTTPHeadersCarrier(req.Header))
			return ctxhttp.Do(ctx, client, req)
		},
	}, log.With(r.logger, "user", userID)); err != nil {
		return nil, err
	}

	n.run()

	// This should never fail, unless there's a programming mistake.
	if err := n.applyConfig(r.notifierCfg); err != nil {
		return nil, err
	}

	r.notifiers[userID] = n
	return n.notifier, nil
}

// removeUsersIf stops the manager and cleanup the resources for each user for which
// the input shouldRemove() function returns true.
func (r *DefaultMultiTenantManager) removeUsersIf(shouldRemove func(userID string) bool) {
	r.userManagerMtx.Lock()
	defer r.userManagerMtx.Unlock()

	// Check for deleted users and remove them
	for userID, mngr := range r.userManagers {
		if !shouldRemove(userID) {
			continue
		}

		// Stop manager in the background, so we don't block further resharding operations.
		// The manager won't terminate until any inflight evaluations are complete.
		go mngr.Stop()
		delete(r.userManagers, userID)

		r.mapper.cleanupUser(userID)
		r.lastReloadSuccessful.DeleteLabelValues(userID)
		r.lastReloadSuccessfulTimestamp.DeleteLabelValues(userID)
		r.configUpdatesTotal.DeleteLabelValues(userID)
		r.userManagerMetrics.RemoveUserRegistry(userID)
		level.Info(r.logger).Log("msg", "deleted rule manager and local rule files", "user", userID)
	}

	r.managersTotal.Set(float64(len(r.userManagers)))

	// Note that we don't remove any notifiers here:
	// - stopping a notifier can take quite some time, as it needs to drain the notification queue (if enabled), and we don't want to block further resharding operations
	// - we can safely reuse the notifier if the tenant is resharded back to this ruler in the future
}

func (r *DefaultMultiTenantManager) GetRules(userID string) []*promRules.Group {
	r.userManagerMtx.RLock()
	mngr, exists := r.userManagers[userID]
	r.userManagerMtx.RUnlock()

	if exists {
		return mngr.RuleGroups()
	}
	return nil
}

func (r *DefaultMultiTenantManager) Stop() {
	level.Info(r.logger).Log("msg", "stopping user managers")
	wg := sync.WaitGroup{}
	r.userManagerMtx.Lock()
	for userID, manager := range r.userManagers {
		level.Debug(r.logger).Log("msg", "shutting down user manager", "user", userID)
		wg.Add(1)
		go func(manager RulesManager, user string) {
			manager.Stop()
			wg.Done()
			level.Debug(r.logger).Log("msg", "user manager shut down", "user", user)
		}(manager, userID)
		delete(r.userManagers, userID)
	}
	wg.Wait()
	r.userManagerMtx.Unlock()
	level.Info(r.logger).Log("msg", "all user managers stopped")

	// Stop notifiers after all rule evaluations have finished, so that we have
	// a chance to send any notifications generated while shutting down.
	// rulerNotifier.stop() may take some time to complete if notifications need to be drained from the queue.
	level.Info(r.logger).Log("msg", "stopping user notifiers")
	wg = sync.WaitGroup{}
	r.notifiersMtx.Lock()
	for _, n := range r.notifiers {
		wg.Add(1)
		go func(n *rulerNotifier) {
			defer wg.Done()
			n.stop()
		}(n)
	}
	wg.Wait()
	r.notifiersMtx.Unlock()
	level.Info(r.logger).Log("msg", "all user notifiers stopped")

	// cleanup user rules directories
	r.mapper.cleanup()
}

func (r *DefaultMultiTenantManager) ValidateRuleGroup(g rulefmt.RuleGroup) []error {
	var errs []error

	if g.Name == "" {
		errs = append(errs, errors.New("invalid rules configuration: rule group name must not be empty"))
		return errs
	}

	if len(g.Rules) == 0 {
		errs = append(errs, fmt.Errorf("invalid rules configuration: rule group '%s' has no rules", g.Name))
		return errs
	}

	if !r.cfg.TenantFederation.Enabled && len(g.SourceTenants) > 0 {
		errs = append(errs, fmt.Errorf("invalid rules configuration: rule group '%s' is a federated rule group, "+
			"but rules federation is disabled; please contact your service administrator to have it enabled", g.Name))
	}

	//nolint:staticcheck // We want to intentionally access a deprecated field
	if g.EvaluationDelay != nil && g.QueryOffset != nil && *g.EvaluationDelay != *g.QueryOffset {
		errs = append(errs, fmt.Errorf("invalid rules configuration: rule group '%s' has both query_offset and (deprecated) evaluation_delay set, but to different values; please remove the deprecated evaluation_delay and use query_offset instead", g.Name))
	}

	for i, r := range g.Rules {
		for _, err := range r.Validate() {
			var ruleName string
			if r.Alert.Value != "" {
				ruleName = r.Alert.Value
			} else {
				ruleName = r.Record.Value
			}
			errs = append(errs, &rulefmt.Error{
				Group:    g.Name,
				Rule:     i,
				RuleName: ruleName,
				Err:      err,
			})
		}
	}

	return errs
}

// filterRuleGroupsByNotEmptyUsers filters out all the tenants that have no rule groups.
// The returned removed map may be nil if no user was removed from the input configs.
//
// This function doesn't modify the input configs in place (even if it could) in order to reduce the likelihood of introducing
// future bugs, in case the rule groups will be cached in memory.
func filterRuleGroupsByNotEmptyUsers(configs map[string]rulespb.RuleGroupList) (filtered map[string]rulespb.RuleGroupList, removed map[string]struct{}) {
	// Find tenants to remove.
	for userID, ruleGroups := range configs {
		if len(ruleGroups) > 0 {
			continue
		}

		// Ensure the map is initialised.
		if removed == nil {
			removed = make(map[string]struct{})
		}

		removed[userID] = struct{}{}
	}

	// Nothing to do if there are no users to remove.
	if len(removed) == 0 {
		return configs, removed
	}

	// Filter out tenants to remove.
	filtered = make(map[string]rulespb.RuleGroupList, len(configs)-len(removed))
	for userID, ruleGroups := range configs {
		if _, isRemoved := removed[userID]; !isRemoved {
			filtered[userID] = ruleGroups
		}
	}

	return filtered, removed
}
