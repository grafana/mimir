// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"errors"
	"math"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/dns"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/ruler/wal"
)

var (
	delimiter             = "/"
	flushDeadline         = 10 * time.Second
	remoteTimeout, _      = model.ParseDuration("10s")
	remoteWriteMetricName = "queue_highest_sent_timestamp_seconds"
)

type RemoteWriteConfig struct {
	WALDir  string `yaml:"wal_dir"`
	Enabled bool   `yaml:"enabled"`

	WALTruncateFrequency time.Duration `yaml:"wal_truncate_frequency"`
	MinWALTime           time.Duration `yaml:"min_wal_time"`
	MaxWALTime           time.Duration `yaml:"max_wal_time"`
}

// RemoteWriteAppendable holds appenders that can perform remote write
type RemoteWriteAppendable struct {
	pusher Pusher
	// key of this map will be userID + delimiter + groupName
	// this can be used to uniquely identify appenders
	appenders map[string]*remoteWriteAppender
	// this is a map indexing remote write urls to the groups that are using them
	// since we want to have a common WAL for a given remote write endpoint, this map
	// will help us keep track of how many groups are using the same WAL
	rwURLToGroups map[string][]string
	appendersMtx  sync.RWMutex
	rulesLimits   RulesLimits

	totalWrites  prometheus.Counter
	failedWrites prometheus.Counter
}

func NewRemoteWriteAppendable(p Pusher, rulesLimits RulesLimits, totalWrites, failedWrites prometheus.Counter) *RemoteWriteAppendable {
	return &RemoteWriteAppendable{
		pusher:        p,
		appenders:     make(map[string]*remoteWriteAppender),
		rwURLToGroups: make(map[string][]string),
		rulesLimits:   rulesLimits,
		totalWrites:   totalWrites,
		failedWrites:  failedWrites,
	}
}

func (t *RemoteWriteAppendable) Appender(ctx context.Context) storage.Appender {
	userID, err := ExtractTenantIDs(ctx)
	var l log.Logger
	if err != nil {
		level.Error(l).Log("msg", "error getting userID from context", "err", err)
		// this should never happen, ctx always has a userID, but just to be doubly sure
		return nil // TODO figure out non nil but not nop appender
	}
	if q := ctx.Value(promql.QueryOrigin{}); q != nil {
		if ruleGroup, ok := q.(map[string]interface{})["ruleGroup"]; ok {
			if groupName, ok := ruleGroup.(map[string]string)["name"]; ok {
				level.Debug(l).Log("msg",
					"fetching remote write appender",
					"userGroup",
					userID+delimiter+groupName)
				t.appendersMtx.RLock()
				defer t.appendersMtx.RUnlock()
				groupKey := userID + delimiter + groupName
				if appender, exists := t.appenders[groupKey]; exists {
					return appender.walStore.Appender(groupKey)
				}
			}
		}
	}
	level.Debug(l).Log("msg",
		"rulegroup does not have remote write config, sending metrics to distributor")

	app := NewPusherAppendable(t.pusher, userID, t.rulesLimits, t.totalWrites, t.failedWrites)
	return app.Appender(ctx)
}

// Implements the storage.ReadyScrapeManager interface to be compatible with metadata remote-write.
// Currently, no metadata should be sent.
type noopManagerGetter struct{}

func (n *noopManagerGetter) Get() (*scrape.Manager, error) {
	return nil, nil
}

// remoteWriteAppender holds all of the info for a remote write client.
// This includes the WAL, as well as the RemoteWriteConfig
type remoteWriteAppender struct {
	// walStorage implements storage.Appender
	walStore *wal.Storage
	// actual storage that will take a WAL watcher and push to remote endpoint
	remoteWriteStorage *remote.WriteStorage
	// remoteWriteConfig
	remoteWriteConfig *config.RemoteWriteConfig
	// truncateLoopCanceller can stop a context
	truncateLoopCanceller context.CancelFunc

	logger log.Logger

	vc *metricValueCollector
}

func newRemoteWriteAppender(path string,
	flushDeadline time.Duration,
	rwConfig *config.RemoteWriteConfig,
	reg prometheus.Registerer,
	logger log.Logger,
	cfg RemoteWriteConfig) (*remoteWriteAppender, error) {
	walStore, err := wal.NewStorage(logger, prometheus.WrapRegistererWith(prometheus.Labels{"wal": path}, reg), path)
	if err != nil {
		return nil, err
	}
	rwStorage := remote.NewWriteStorage(logger, reg, path, flushDeadline, &noopManagerGetter{})
	conf := &config.Config{
		GlobalConfig:       config.GlobalConfig{},
		AlertingConfig:     config.AlertingConfig{},
		RuleFiles:          nil,
		ScrapeConfigs:      nil,
		RemoteWriteConfigs: []*config.RemoteWriteConfig{rwConfig},
		RemoteReadConfigs:  nil,
	}
	err = rwStorage.ApplyConfig(conf)
	if err != nil {
		return nil, err
	}

	vc := newMetricValueCollector(prometheus.DefaultGatherer, remoteWriteMetricName)
	appender := &remoteWriteAppender{
		walStore:           walStore,
		remoteWriteStorage: rwStorage,
		remoteWriteConfig:  rwConfig,
		logger:             logger,
		vc:                 vc,
	}
	appender.addTruncater(cfg)

	return appender, nil
}

func (t *remoteWriteAppender) Stop() error {
	// Stop the truncateLoop
	t.truncateLoopCanceller()

	if err := t.walStore.Close(); err != nil {
		return err
	}
	return t.remoteWriteStorage.Close()
}

func (t *remoteWriteAppender) addTruncater(cfg RemoteWriteConfig) {
	ctx, cancel := context.WithCancel(context.Background())
	go func(ctx context.Context, walStorage *wal.Storage) {
		var lastTs int64 = math.MinInt64

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(cfg.WALTruncateFrequency):
				// The timestamp ts is used to determine which series are not receiving
				// samples and may be deleted from the WAL. Their most recent append
				// timestamp is compared to ts, and if that timestamp is older than ts,
				// they are considered inactive and may be deleted.
				//
				// Subtracting a duration from ts will delay when it will be considered
				// inactive and scheduled for deletion.
				ts := t.getOldestRemoteWriteTimestamp() - cfg.MinWALTime.Milliseconds()
				if ts < 0 {
					ts = 0
				}

				// Network issues can prevent the result of getRemoteWriteTimestamp from
				// changing. We don't want data in the WAL to grow forever, so we set a cap
				// on the maximum age data can be. If our ts is older than this cutoff point,
				// we'll shift it forward to start deleting very stale data.
				if minTS := timestamp.FromTime(time.Now().Add(-cfg.MaxWALTime)); ts < minTS {
					ts = minTS
				}

				if ts == lastTs {
					level.Debug(t.logger).Log("msg", "not truncating the WAL, remote_write timestamp is unchanged", "ts", ts)
					continue
				}

				level.Debug(t.logger).Log("msg", "truncating the WAL", "ts", ts)
				err := t.walStore.Truncate(ts)
				if err != nil {
					// The only issue here is larger disk usage and a greater replay time,
					// so we'll only log this as a warning.
					level.Warn(t.logger).Log("msg", "could not truncate WAL", "err", err)
					continue
				}
				lastTs = ts
			}
		}
	}(ctx, t.walStore)

	t.truncateLoopCanceller = cancel
}

func (t *remoteWriteAppender) getOldestRemoteWriteTimestamp() int64 {
	lbls := []string{t.remoteWriteConfig.Name}

	vals, err := t.vc.GetValues("remote_name", lbls...)
	if err != nil {
		level.Error(t.logger).Log("msg", "could not get remote write timestamps", "err", err)
		return 0
	}
	if len(vals) == 0 {
		return 0
	}

	// We use the lowest value from the metric since we don't want to delete any
	// segments from the WAL until they've been written by all of the remote_write
	// configurations.
	ts := int64(math.MaxInt64)
	for _, val := range vals {
		ival := int64(val)
		if ival < ts {
			ts = ival
		}
	}

	// Convert to the millisecond precision which is used by the WAL
	return ts * 1000
}

// metricValueCollector wraps around a Gatherer and provides utilities for
// pulling metric values from a given metric name and label matchers.
//
// This is used by the agent instances to find the most recent timestamp
// successfully remote_written to for purposes of safely truncating the WAL.
//
// metricValueCollector is only intended for use with Gauges and Counters.
type metricValueCollector struct {
	g     prometheus.Gatherer
	match string
}

// newMetricValueCollector creates a new MetricValueCollector.
func newMetricValueCollector(g prometheus.Gatherer, match string) *metricValueCollector {
	return &metricValueCollector{
		g:     g,
		match: match,
	}
}

// GetValues looks through all the tracked metrics and returns all values
// for metrics that match some key value pair.
func (vc *metricValueCollector) GetValues(label string, labelValues ...string) ([]float64, error) {
	vals := []float64{}

	families, err := vc.g.Gather()
	if err != nil {
		return nil, err
	}

	for _, family := range families {
		if !strings.Contains(family.GetName(), vc.match) {
			continue
		}

		for _, m := range family.GetMetric() {
			matches := false
			for _, l := range m.GetLabel() {
				if l.GetName() != label {
					continue
				}

				v := l.GetValue()
				for _, match := range labelValues {
					if match == v {
						matches = true
						break
					}
				}
				break
			}
			if !matches {
				continue
			}

			var value float64
			if m.Gauge != nil {
				value = m.Gauge.GetValue()
			} else {
				return nil, errors.New("tracking unexpected metric type")
			}

			vals = append(vals, value)
		}
	}

	return vals, nil
}

// TODO this doesn't need to be a wrapper it just needs to extend
// RWMultiTenantManager is a wrapper around DefaultMultiTenantManager
// It creates/deletes remote write appenders based on ruleGroups
type RWMultiTenantManager struct {
	*DefaultMultiTenantManager

	remoteWriteConfig RemoteWriteConfig
	// need access to rwAppendable from RWMultiTenantManager in order to
	// delete users and groups that no longer exist
	rwAppendable *RemoteWriteAppendable

	reg    prometheus.Registerer
	logger log.Logger
}

func NewRWMultiTenantManager(
	cfg Config,
	managerFactory ManagerFactory,
	p *RemoteWriteAppendable,
	reg prometheus.Registerer,
	logger log.Logger,
) (*RWMultiTenantManager, error) {
	// We need to prefix and add a label to the metrics for the DNS resolver because, unlike other mimir components,
	// it doesn't already have the `cortex_` prefix and the `component` label to the metrics it emits
	dnsProviderReg := prometheus.WrapRegistererWithPrefix(
		"cortex_",
		prometheus.WrapRegistererWith(
			prometheus.Labels{"component": "ruler"},
			reg,
		),
	)
	dnsResolver := dns.NewProvider(logger, dnsProviderReg, dns.GolangResolverType)
	manager, err := NewDefaultMultiTenantManager(cfg, managerFactory, reg, logger, dnsResolver)
	if err != nil {
		return nil, err
	}
	return &RWMultiTenantManager{
		DefaultMultiTenantManager: manager,
		remoteWriteConfig:         cfg.RWConfig,
		rwAppendable:              p,
		logger:                    logger,
		reg:                       reg,
	}, nil
}

// Stop all remote write appenders before stopping managers and notifiers
func (t *RWMultiTenantManager) Stop() {
	if t.remoteWriteConfig.Enabled {
		level.Info(t.logger).Log("msg", "stopping all remote write appenders")
		t.rwAppendable.appendersMtx.Lock()
		for groupKey, appender := range t.rwAppendable.appenders {
			err := appender.Stop()
			if err != nil {
				level.Error(t.logger).Log("msg", "error stopping remote write appender", "group", groupKey, "err", err)
			}
		}
		t.rwAppendable.appendersMtx.Unlock()
	}
	t.DefaultMultiTenantManager.Stop()
}

// SyncRuleGroups implements MultiTenantManager
func (t *RWMultiTenantManager) SyncRuleGroups(ctx context.Context, ruleGroups map[string]rulespb.RuleGroupList) {
	// If remote-write rule groups are enabled, sync the provided rule groups
	// with the embedded appenders.
	if t.remoteWriteConfig.Enabled {
		t.loadAppenders(ruleGroups)
		defer t.cleanAppenders(ruleGroups)
	}

	// DefaultMultiTenantManager.SyncRuleGroups is placed between creation of new rule groups and deletion of old
	// rule groups for a reason:
	// For any new rule groups, the respective remoteWriteAppender should be created before the prometheus manager runs
	// so that the appender is ready before rule group evaluation.
	// For any rule groups that have been deleted, the prometheus manager (and hence rule group evaluations) should be
	// stopped before the remoteWriteAppender.
	t.DefaultMultiTenantManager.SyncRuleGroups(ctx, ruleGroups)
}

// loadAppenders creates/syncs remoteWriteAppenders depending on the provided rule groups.
// It iterates through new rule groups and checks the following:
// - if a group exists
//   - do nothing, continue
//
// - if group does not exist
//   - if remoteWriteAppender for the endpoint exists already
//   - reuse
//   - else create new remoteWriteAppender for the endpoint

// only going to use this appender if enabled do the check outside of here
func (t *RWMultiTenantManager) loadAppenders(ruleGroups map[string]rulespb.RuleGroupList) {
	t.rwAppendable.appendersMtx.Lock()
	defer t.rwAppendable.appendersMtx.Unlock()
	for userID, userGroups := range ruleGroups {
		for _, userGroup := range userGroups {
			groupKey := userID + delimiter + userGroup.Name
			// marshal into a remote write config to access URL
			// rg := rulespb.FromProto(userGroup) // this is a prom rule group not mimir
			// TODO rg maybe seems like redundancies of wrapping leftover from GEM; maybe refactor loadAppenders for less confusion
			// - if a group exists
			if _, exists := t.rwAppendable.appenders[groupKey]; exists {
				//   - do nothing, continue
				continue
			}

			// within RuleGroupDesc there is a RWUrl use this per group

			//   - if remoteWriteAppender for the endpoint exists already
			// t.rwAppendable.rwURLToGroups[rg] this whole thing may be excessive what do we actually want?
			if groupsUsingAppender, ok := t.rwAppendable.rwURLToGroups[userGroup.RemoteWriteUrl]; ok {
				if len(groupsUsingAppender) > 0 {
					//     - reuse
					t.rwAppendable.appenders[groupKey] = t.rwAppendable.appenders[groupsUsingAppender[0]]
					t.rwAppendable.rwURLToGroups[userGroup.RemoteWriteUrl] = append(groupsUsingAppender, groupKey)
					continue
				}
			}

			//   - else create new remoteWriteAppender for the endpoint
			rwURL, err := url.Parse(userGroup.RemoteWriteUrl)
			if err != nil {
				level.Error(t.logger).Log("msg", "Error parsing remote write config!", "rwURL", userGroup.RemoteWriteUrl, "err", err)
				continue
			}

			// groupKey is already of the form $userID/$groupName, so we just append to the WAL dir here
			walPath := path.Join(t.remoteWriteConfig.WALDir, groupKey)
			level.Info(t.logger).Log("msg", "creating remoteWriteAppender", "userGroup", groupKey, "wal", walPath, "endpoint", rwURL)

			t.rwAppendable.appenders[groupKey], err = newRemoteWriteAppender(
				walPath,
				flushDeadline,
				&config.RemoteWriteConfig{ // lets use sensible defaults here
					URL: &config_util.URL{
						URL: rwURL,
					},
					RemoteTimeout:       remoteTimeout,
					WriteRelabelConfigs: nil,
					Name:                "ruler_remote_write_" + groupKey,
					HTTPClientConfig:    config_util.HTTPClientConfig{},
					QueueConfig:         config.DefaultQueueConfig,
				},
				t.reg,
				t.logger,
				t.remoteWriteConfig,
			)
			if err != nil {
				level.Error(t.logger).Log("msg", "error creating RemoteWriteAppender", "err", err)
			}

			// add this group as a user of the remote write appender
			t.rwAppendable.rwURLToGroups[userGroup.RemoteWriteUrl] = []string{groupKey}
		}
	}
}

// cleanAppenders deletes remoteWriteAppenders depending on provided rule groups by
// checking for deleted rule groups for a tenant and and closing their appenders.
// - if group exists in new rulegroups
//   - continue
//
// - if group does not exist
//   - if no other userGroup is using it
//   - delete appender
//   - else
//   - mark the series for this userGroup stale

func (t *RWMultiTenantManager) cleanAppenders(ruleGroups map[string]rulespb.RuleGroupList) {
	t.rwAppendable.appendersMtx.Lock()
	defer t.rwAppendable.appendersMtx.Unlock()

	for groupKey, appender := range t.rwAppendable.appenders {
		keySplit := strings.SplitN(groupKey, delimiter, 2)
		userID := keySplit[0]
		groupName := keySplit[1]

		// - if group exists in new rulegroups
		userGroupExists := false
		if userGroups, exists := ruleGroups[userID]; exists {
			for _, rg := range userGroups {
				if rg.Name == groupName {
					userGroupExists = true
				}
			}
		}

		if userGroupExists {
			//   - continue
			continue
		}

		// - if group does not exist
		groupsUsingAppender := t.rwAppendable.rwURLToGroups[appender.remoteWriteConfig.URL.String()]
		for pos, groupUsingAppender := range groupsUsingAppender {
			if groupUsingAppender == groupKey {
				// trim this group from list of groups using the appender
				groupsUsingAppender = append(groupsUsingAppender[0:pos], groupsUsingAppender[pos+1:]...)
				t.rwAppendable.rwURLToGroups[appender.remoteWriteConfig.URL.String()] = groupsUsingAppender
				break
			}
		}
		if len(t.rwAppendable.rwURLToGroups[appender.remoteWriteConfig.URL.String()]) == 0 {
			//  - if no other userGroup is using it
			//    - delete appender
			level.Info(t.logger).Log("msg", "stopping remote write appender", "groupKey", groupKey)
			err := appender.Stop()
			if err != nil {
				level.Error(t.logger).Log("msg",
					"error stopping remote write appender",
					"groupKey",
					groupKey,
					"err",
					err)
			}
			delete(t.rwAppendable.rwURLToGroups, appender.remoteWriteConfig.URL.String())
		} else {
			//  - else
			//   -  mark the series for this userGroup stale
			err := t.rwAppendable.appenders[groupKey].walStore.MarkSeriesStale(groupKey)
			if err != nil {
				level.Error(t.logger).Log("msg", "error marking series stale", "groupKey", groupKey, "err", err)
			}
		}
		delete(t.rwAppendable.appenders, groupKey)
		level.Info(t.logger).Log("msg", "deleting remote write appender", "groupKey", groupKey)
	}
}
