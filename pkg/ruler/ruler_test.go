// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/ruler_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/ruler/rulestore"
	"github.com/grafana/mimir/pkg/ruler/rulestore/bucketclient"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/util"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestMain(m *testing.M) {
	util_test.VerifyNoLeakTestMain(m)
}

func defaultRulerConfig(t testing.TB) Config {
	t.Helper()

	// Create a new temporary directory for the rules, so that
	// each test will run in isolation.
	rulesDir := t.TempDir()

	codec := ring.GetCodec()
	consul, closer := consul.NewInMemoryClient(codec, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.RulePath = rulesDir
	cfg.Ring.Common.KVStore.Mock = consul
	cfg.Ring.NumTokens = 1
	cfg.Ring.Common.ListenPort = 0
	cfg.Ring.Common.InstanceAddr = "localhost"
	cfg.Ring.Common.InstanceID = "localhost"
	cfg.EnableQueryStats = false

	return cfg
}

type mockRulerClient struct {
	ruler           *Ruler
	rulesCallsCount *atomic.Int32
}

func (c *mockRulerClient) Rules(ctx context.Context, in *RulesRequest, _ ...grpc.CallOption) (*RulesResponse, error) {
	c.rulesCallsCount.Inc()
	return c.ruler.Rules(ctx, in)
}

func (c *mockRulerClient) SyncRules(ctx context.Context, in *SyncRulesRequest, _ ...grpc.CallOption) (*SyncRulesResponse, error) {
	return c.ruler.SyncRules(ctx, in)
}

type mockRulerClientsPool struct {
	ClientsPool
	cfg           Config
	rulerAddrMap  map[string]*Ruler
	numberOfCalls atomic.Int32
}

func (p *mockRulerClientsPool) GetClientForInstance(inst ring.InstanceDesc) (RulerClient, error) {
	for _, r := range p.rulerAddrMap {
		if r.lifecycler.GetInstanceAddr() == inst.Addr {
			return &mockRulerClient{
				ruler:           r,
				rulesCallsCount: &p.numberOfCalls,
			}, nil
		}
	}

	return nil, fmt.Errorf("unable to find ruler for addr %s %s", inst.Id, inst.Addr)
}

func newMockClientsPool(cfg Config, logger log.Logger, reg prometheus.Registerer, rulerAddrMap map[string]*Ruler) *mockRulerClientsPool {
	return &mockRulerClientsPool{
		ClientsPool:  newRulerClientPool(cfg.ClientTLSConfig, logger, reg),
		cfg:          cfg,
		rulerAddrMap: rulerAddrMap,
	}
}

type prepareOption func(opts *prepareOptions)

type prepareOptions struct {
	limits           RulesLimits
	logger           log.Logger
	registerer       prometheus.Registerer
	rulerAddrMap     map[string]*Ruler
	rulerAddrAutoMap bool
	start            bool
	managerQueryFunc rules.QueryFunc
}

func applyPrepareOptions(t *testing.T, instanceID string, opts ...prepareOption) prepareOptions {
	defaultLogger := testutil.NewLogger(t)
	defaultLogger = log.With(defaultLogger, "instance", instanceID)
	defaultLogger = level.NewFilter(defaultLogger, level.AllowInfo())

	applied := prepareOptions{
		// Default limits in the ruler tests.
		limits: validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
			defaults.RulerEvaluationDelay = 0
			defaults.RulerMaxRuleGroupsPerTenant = 20
			defaults.RulerMaxRulesPerRuleGroup = 15
		}),
		rulerAddrMap: map[string]*Ruler{},
		logger:       defaultLogger,
		registerer:   prometheus.NewPedanticRegistry(),
	}

	for _, opt := range opts {
		opt(&applied)
	}

	return applied
}

// withStart is a prepareOption that automatically starts the ruler.
func withStart() prepareOption {
	return func(opts *prepareOptions) {
		opts.start = true
	}
}

// withLimits is a prepareOption that overrides the limits used in the test.
func withLimits(limits RulesLimits) prepareOption {
	return func(opts *prepareOptions) {
		opts.limits = limits
	}
}

// withRulerAddrMap is a prepareOption that configures the mapping between rulers and their network addresses.
func withRulerAddrMap(addrs map[string]*Ruler) prepareOption {
	return func(opts *prepareOptions) {
		opts.rulerAddrMap = addrs
	}
}

// withRulerAddrAutomaticMapping is a prepareOption that automatically configures the mapping between rulers and their network addresses.
func withRulerAddrAutomaticMapping() prepareOption {
	return func(opts *prepareOptions) {
		opts.rulerAddrAutoMap = true
	}
}

// withPrometheusRegisterer is a prepareOption that configures the Prometheus registerer to pass to the ruler.
func withPrometheusRegisterer(reg prometheus.Registerer) prepareOption {
	return func(opts *prepareOptions) {
		opts.registerer = reg
	}
}

// withManagerQueryFunc is a prepareOption that configures the query function to pass to the ruler manager.
func withManagerQueryFunc(queryFunc rules.QueryFunc) prepareOption {
	return func(opts *prepareOptions) {
		opts.managerQueryFunc = queryFunc
	}
}

func prepareRuler(t *testing.T, cfg Config, storage rulestore.RuleStore, opts ...prepareOption) *Ruler {
	options := applyPrepareOptions(t, cfg.Ring.Common.InstanceID, opts...)
	manager := prepareRulerManager(t, cfg, opts...)

	ruler, err := newRuler(cfg, manager, options.registerer, options.logger, storage, storage, options.limits, newMockClientsPool(cfg, options.logger, options.registerer, options.rulerAddrMap))
	require.NoError(t, err)

	if options.rulerAddrAutoMap {
		options.rulerAddrMap[cfg.Ring.Common.InstanceAddr] = ruler
	}

	// Start the ruler if requested to do so.
	if options.start {
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ruler))

		// Ensure the service is stopped at the end of the test.
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ruler))
		})
	}

	return ruler
}

func prepareRulerManager(t *testing.T, cfg Config, opts ...prepareOption) *DefaultMultiTenantManager {
	options := applyPrepareOptions(t, cfg.Ring.Common.InstanceID, opts...)

	noopQueryable := storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
		return storage.NoopQuerier(), nil
	})

	var queryFunc rules.QueryFunc
	if options.managerQueryFunc != nil {
		queryFunc = options.managerQueryFunc
	} else {
		queryFunc = func(context.Context, string, time.Time) (promql.Vector, error) {
			return nil, nil
		}
	}

	// Mock the pusher
	pusher := newPusherMock()
	pusher.MockPush(&mimirpb.WriteResponse{}, nil)

	managerFactory := DefaultTenantManagerFactory(cfg, pusher, noopQueryable, queryFunc, &NoopMultiTenantConcurrencyController{}, options.limits, options.registerer)
	manager, err := NewDefaultMultiTenantManager(cfg, managerFactory, prometheus.NewRegistry(), options.logger, nil)
	require.NoError(t, err)

	return manager
}

var _ MultiTenantManager = &DefaultMultiTenantManager{}

func TestNotifierSendsUserIDHeader(t *testing.T) {
	var wg sync.WaitGroup

	// We do expect 1 API call for the user create with the getOrCreateNotifier()
	wg.Add(1)
	ts := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		userID, _, err := tenant.ExtractTenantIDFromHTTPRequest(r)
		require.NoError(t, err)
		assert.Equal(t, "1", userID)
		wg.Done()
	}))
	defer ts.Close()

	// We create an empty rule store so that the ruler will not load any rule from it.
	cfg := defaultRulerConfig(t)
	cfg.AlertmanagerURL = ts.URL

	manager := prepareRulerManager(t, cfg)
	defer manager.Stop()

	n, err := manager.getOrCreateNotifier("1")
	require.NoError(t, err)

	// Loop until notifier discovery syncs up
	for len(n.Alertmanagers()) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	n.Send(&notifier.Alert{
		Labels: labels.FromStrings("alertname", "testalert"),
	})

	wg.Wait()

	// Ensure we have metrics in the notifier.
	assert.NoError(t, prom_testutil.GatherAndCompare(manager.registry.(*prometheus.Registry), strings.NewReader(`
		# HELP cortex_prometheus_notifications_dropped_total Total number of alerts dropped due to errors when sending to Alertmanager.
		# TYPE cortex_prometheus_notifications_dropped_total counter
		cortex_prometheus_notifications_dropped_total{user="1"} 0
	`), "cortex_prometheus_notifications_dropped_total"))
}

func TestRuler_Rules(t *testing.T) {
	testCases := map[string]struct {
		mockRules map[string]rulespb.RuleGroupList
		userID    string
	}{
		"rules - user1": {
			userID:    "user1",
			mockRules: mockRules,
		},
		"rules - user2": {
			userID:    "user2",
			mockRules: mockRules,
		},
		"federated rule group": {
			userID: "user1",
			mockRules: map[string]rulespb.RuleGroupList{
				"user1": {
					&rulespb.RuleGroupDesc{
						Name:          "group1",
						Namespace:     "namespace1",
						User:          "user1",
						SourceTenants: []string{"tenant-1"},
						Rules:         []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up"), createAlertingRule("UP_ALERT", "up < 1")},
						Interval:      interval,
					},
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			cfg := defaultRulerConfig(t)
			cfg.TenantFederation.Enabled = true

			r := prepareRuler(t, cfg, newMockRuleStore(tc.mockRules), withStart())

			// Rules will be synchronized asynchronously, so we wait until the expected number of rule groups
			// has been synched.
			ctx := user.InjectOrgID(context.Background(), tc.userID)
			test.Poll(t, 5*time.Second, len(mockRules[tc.userID]), func() interface{} {
				rls, _ := r.Rules(ctx, &RulesRequest{})
				return len(rls.Groups)
			})

			rls, err := r.Rules(ctx, &RulesRequest{})
			require.NoError(t, err)
			require.Len(t, rls.Groups, len(mockRules[tc.userID]))

			for i, rg := range rls.Groups {
				expectedRg := tc.mockRules[tc.userID][i]
				compareRuleGroupDescToStateDesc(t, expectedRg, rg)
			}
		})
	}
}

func TestRuler_ExcludeAlerts(t *testing.T) {
	alertingMockRules := map[string]rulespb.RuleGroupList{
		"user1": {
			&rulespb.RuleGroupDesc{
				Name:          "group1",
				Namespace:     "namespace1",
				User:          "user1",
				SourceTenants: []string{"tenant-1"},
				Rules:         []*rulespb.RuleDesc{createAlertingRule("testAlert", "up")},
				Interval:      time.Duration(0 * time.Second),
			},
		},
	}

	testCases := map[string]struct {
		mockRules           map[string]rulespb.RuleGroupList
		userID              string
		excludeAlerts       bool
		expectedAlertsCount int
	}{
		"rules - user1 - exclude_alerts=true does not return alerts": {
			userID:              "user1",
			mockRules:           alertingMockRules,
			excludeAlerts:       true,
			expectedAlertsCount: 0,
		},
		"rules - user1 - exclude_alerts=false returns alerts": {
			userID:              "user1",
			mockRules:           alertingMockRules,
			excludeAlerts:       false,
			expectedAlertsCount: 1,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			cfg := defaultRulerConfig(t)
			cfg.EvaluationInterval = time.Second
			cfg.TenantFederation.Enabled = true

			// Mock the query function to return a constant vector
			constantQueryFunc := func(context.Context, string, time.Time) (promql.Vector, error) {
				return promql.Vector{
					{T: 12345, F: 1.0},
				}, nil
			}

			r := prepareRuler(t, cfg, newMockRuleStore(tc.mockRules), withStart(), withManagerQueryFunc(constantQueryFunc))

			// Rules will be synchronized asynchronously, so we wait until the expected number of rule groups
			// has been synched.
			ctx := user.InjectOrgID(context.Background(), tc.userID)
			test.Poll(t, 5*time.Second, len(mockRules[tc.userID]), func() interface{} {
				rls, _ := r.Rules(ctx, &RulesRequest{ExcludeAlerts: tc.excludeAlerts})
				return len(rls.Groups)
			})

			// Rules will be evaluated after some time
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				rls, err := r.Rules(ctx, &RulesRequest{ExcludeAlerts: tc.excludeAlerts})
				assert.NoError(c, err)
				assert.Len(c, rls.Groups, len(mockRules[tc.userID]))

				for _, ruleGroup := range rls.Groups {
					for _, activeRule := range ruleGroup.ActiveRules {
						assert.Len(c, activeRule.Alerts, tc.expectedAlertsCount)
					}
				}
			}, time.Second*5, 1*time.Second)
		})
	}
}

func compareRuleGroupDescToStateDesc(t *testing.T, expected *rulespb.RuleGroupDesc, got *GroupStateDesc) {
	t.Helper()

	require.Equal(t, expected.Name, got.Group.Name)
	require.Equal(t, expected.Namespace, got.Group.Namespace)
	require.Len(t, expected.Rules, len(got.ActiveRules))
	require.ElementsMatch(t, expected.SourceTenants, got.Group.SourceTenants)
	for i := range got.ActiveRules {
		require.Equal(t, expected.Rules[i].Record, got.ActiveRules[i].Rule.Record)
		require.Equal(t, expected.Rules[i].Alert, got.ActiveRules[i].Rule.Alert)
	}
}

func TestGetRules(t *testing.T) {
	// ruler ID -> (user ID -> list of groups).
	type expectedRulesMap map[string]map[string]rulespb.RuleGroupList

	type testCase struct {
		shuffleShardSize int
	}

	expectedRulesByRuler := expectedRulesMap{
		"ruler1": map[string]rulespb.RuleGroupList{
			"user1": {
				&rulespb.RuleGroupDesc{User: "user1", Namespace: "namespace", Name: "first", Rules: []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up")}, Interval: 10 * time.Second},
				&rulespb.RuleGroupDesc{User: "user1", Namespace: "namespace", Name: "second", Rules: []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up")}, Interval: 10 * time.Second},
			},
			"user2": {
				&rulespb.RuleGroupDesc{User: "user2", Namespace: "namespace", Name: "third", Rules: []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up")}, Interval: 10 * time.Second},
			},
		},
		"ruler2": map[string]rulespb.RuleGroupList{
			"user1": {
				&rulespb.RuleGroupDesc{User: "user1", Namespace: "namespace", Name: "third", Rules: []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up")}, Interval: 10 * time.Second},
			},
			"user2": {
				&rulespb.RuleGroupDesc{User: "user2", Namespace: "namespace", Name: "first", Rules: []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up")}, Interval: 10 * time.Second},
				&rulespb.RuleGroupDesc{User: "user2", Namespace: "namespace", Name: "second", Rules: []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up")}, Interval: 10 * time.Second},
			},
		},
		"ruler3": map[string]rulespb.RuleGroupList{
			"user3": {
				&rulespb.RuleGroupDesc{User: "user3", Namespace: "namespace", Name: "third", Rules: []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up")}, Interval: 10 * time.Second},
			},
			"user2": {
				&rulespb.RuleGroupDesc{User: "user2", Namespace: "namespace", Name: "forth", Rules: []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up")}, Interval: 10 * time.Second},
				&rulespb.RuleGroupDesc{User: "user2", Namespace: "namespace", Name: "fifty", Rules: []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up")}, Interval: 10 * time.Second},
			},
		},
	}

	testCases := map[string]testCase{
		"Shuffle Shard Size 0": {
			shuffleShardSize: 0,
		},
		"Shuffle Shard Size 2": {
			shuffleShardSize: 2,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var (
				allRulesByUser   = map[string]rulespb.RuleGroupList{}
				allRulesByRuler  = map[string]rulespb.RuleGroupList{}
				allTokensByRuler = map[string][]uint32{}
				registryByRuler  = map[string]*prometheus.Registry{}
				rulerAddrMap     = map[string]*Ruler{}
				storage          = newMockRuleStore(allRulesByUser)
				ctx              = context.Background()
			)

			kvStore, cleanUp := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, cleanUp.Close()) })

			createAndStartRuler := func(id string) *Ruler {
				cfg := defaultRulerConfig(t)
				cfg.Ring.Common.InstanceID = id
				cfg.Ring.Common.InstanceAddr = id
				cfg.Ring.Common.KVStore = kv.Config{Mock: kvStore}
				cfg.Ring.NumTokens = 0          // Join the ring with no tokens because they will be injected later.
				cfg.PollInterval = time.Hour    // No periodic syncing because we want to trigger it.
				cfg.RingCheckPeriod = time.Hour // No syncing on ring change because we want to trigger it.

				reg := prometheus.NewPedanticRegistry()
				registryByRuler[id] = reg

				return prepareRuler(t, cfg, storage, withStart(), withRulerAddrMap(rulerAddrMap), withRulerAddrAutomaticMapping(), withPrometheusRegisterer(reg), withLimits(validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
					defaults.RulerEvaluationDelay = 0
					defaults.RulerTenantShardSize = tc.shuffleShardSize
				})))
			}

			for rID, r := range expectedRulesByRuler {
				createAndStartRuler(rID)
				for user, rules := range r {
					allRulesByUser[user] = append(allRulesByUser[user], rules...)
					allRulesByRuler[rID] = append(allRulesByRuler[rID], rules...)
					allTokensByRuler[rID] = generateTokenForGroups(rules, 1)
				}
			}

			// Pre-condition check: we expect rulers have done the initial sync (but they have no tokens in the ring at this point).
			for _, reg := range registryByRuler {
				verifySyncRulesMetric(t, reg, 1, 0)
			}

			// Inject the tokens for each ruler.
			require.NoError(t, kvStore.CAS(ctx, RulerRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
				d, _ := in.(*ring.Desc)
				if d == nil {
					d = ring.NewDesc()
				}
				for rID, tokens := range allTokensByRuler {
					d.AddIngester(rID, rulerAddrMap[rID].lifecycler.GetInstanceAddr(), "", tokens, ring.ACTIVE, time.Now(), false, time.Time{})
				}
				return d, true, nil
			}))

			// Wait a bit to make sure ruler's ring is updated.
			time.Sleep(100 * time.Millisecond)

			// Sync rules on each ruler.
			for _, r := range rulerAddrMap {
				r.syncRules(ctx, nil, rulerSyncReasonInitial, true)
			}

			// Call GetRules() on each ruler.
			for u := range allRulesByUser {
				ctx := user.InjectOrgID(ctx, u)

				for _, r := range rulerAddrMap {
					rules, err := r.GetRules(ctx, RulesRequest{Filter: AnyRule})
					require.NoError(t, err)
					require.Equal(t, len(allRulesByUser[u]), len(rules))

					mockPoolClient := r.clientsPool.(*mockRulerClientsPool)
					if tc.shuffleShardSize > 0 {
						require.Equal(t, int32(tc.shuffleShardSize), mockPoolClient.numberOfCalls.Load())
					} else {
						require.Equal(t, int32(len(rulerAddrMap)), mockPoolClient.numberOfCalls.Load())
					}
					mockPoolClient.numberOfCalls.Store(0)
				}
			}

			// Ensure rule groups have been sharded among rulers.
			totalLoadedRules := 0
			totalConfiguredRules := 0

			for rID, r := range rulerAddrMap {
				localRules, err := r.listRuleGroupsToSyncForAllUsers(ctx, rulerSyncReasonPeriodic, true)
				require.NoError(t, err)
				for _, rules := range localRules {
					totalLoadedRules += len(rules)
				}
				totalConfiguredRules += len(allRulesByRuler[rID])
			}

			require.Equal(t, totalConfiguredRules, totalLoadedRules)
		})
	}
}

func TestSharding(t *testing.T) {
	const (
		user1 = "user1"
		user2 = "user2"
		user3 = "user3"
	)

	user1Group1 := &rulespb.RuleGroupDesc{User: user1, Namespace: "namespace", Name: "first"}
	user1Group2 := &rulespb.RuleGroupDesc{User: user1, Namespace: "namespace", Name: "second"}
	user2Group1 := &rulespb.RuleGroupDesc{User: user2, Namespace: "namespace", Name: "first"}
	user3Group1 := &rulespb.RuleGroupDesc{User: user3, Namespace: "namespace", Name: "first"}

	// Must be distinct for test to work.
	user1Group1Token := tokenForGroup(user1Group1)
	user1Group2Token := tokenForGroup(user1Group2)
	user2Group1Token := tokenForGroup(user2Group1)
	user3Group1Token := tokenForGroup(user3Group1)

	noRules := map[string]rulespb.RuleGroupList{}
	allRules := map[string]rulespb.RuleGroupList{
		user1: {user1Group1, user1Group2},
		user2: {user2Group1},
		user3: {user3Group1},
	}

	// ruler ID -> (user ID -> list of groups).
	type expectedRulesMap map[string]map[string]rulespb.RuleGroupList

	type testCase struct {
		shuffleShardSize int
		setupRing        func(*ring.Desc)
		enabledUsers     []string
		disabledUsers    []string

		expectedRules expectedRulesMap
	}

	const (
		ruler1     = "ruler-1"
		ruler1Host = "1.1.1.1"
		ruler1Port = 9999
		ruler1Addr = "1.1.1.1:9999"

		ruler2     = "ruler-2"
		ruler2Host = "2.2.2.2"
		ruler2Port = 9999
		ruler2Addr = "2.2.2.2:9999"

		ruler3     = "ruler-3"
		ruler3Host = "3.3.3.3"
		ruler3Port = 9999
		ruler3Addr = "3.3.3.3:9999"
	)

	testCases := map[string]testCase{
		"single ruler, with ring setup": {
			shuffleShardSize: 0,
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", []uint32{0}, ring.ACTIVE, time.Now(), false, time.Time{})
			},
			expectedRules: expectedRulesMap{ruler1: allRules},
		},

		"single ruler, with ring setup, single user enabled": {
			shuffleShardSize: 0,
			enabledUsers:     []string{user1},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", []uint32{0}, ring.ACTIVE, time.Now(), false, time.Time{})
			},
			expectedRules: expectedRulesMap{ruler1: map[string]rulespb.RuleGroupList{
				user1: {user1Group1, user1Group2},
			}},
		},

		"single ruler with ring setup, single user disabled": {
			shuffleShardSize: 0,
			disabledUsers:    []string{user1},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", []uint32{0}, ring.ACTIVE, time.Now(), false, time.Time{})
			},
			expectedRules: expectedRulesMap{ruler1: map[string]rulespb.RuleGroupList{
				user2: {user2Group1},
				user3: {user3Group1},
			}},
		},

		"shard size 0, multiple ACTIVE rulers": {
			shuffleShardSize: 0,
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
					user2: {user2Group1},
				},

				ruler2: map[string]rulespb.RuleGroupList{
					user1: {user1Group2},
					user3: {user3Group1},
				},
			},
		},

		"shard size 0, multiple ACTIVE rulers, single enabled user": {
			shuffleShardSize: 0,
			enabledUsers:     []string{user1},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
				},

				ruler2: map[string]rulespb.RuleGroupList{
					user1: {user1Group2},
				},
			},
		},

		"shard size 0, multiple ACTIVE rulers, single disabled user": {
			shuffleShardSize: 0,
			disabledUsers:    []string{user1},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
				},

				ruler2: map[string]rulespb.RuleGroupList{
					user3: {user3Group1},
				},
			},
		},

		"shard size 0, unhealthy ACTIVE ruler": {
			shuffleShardSize: 0,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
				desc.Ingesters[ruler2] = ring.InstanceDesc{
					Addr:      ruler2Addr,
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}),
				}
			},

			expectedRules: expectedRulesMap{
				// This ruler doesn't get rules from unhealthy ruler (RF=1).
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
					user2: {user2Group1},
				},
				ruler2: noRules,
			},
		},

		"shard size 0, LEAVING ruler": {
			shuffleShardSize: 0,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.LEAVING, time.Now(), false, time.Time{})
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
			},

			expectedRules: expectedRulesMap{
				// LEAVING ruler doesn't get any rules.
				ruler1: noRules,
				ruler2: allRules,
			},
		},

		"shard size 0, JOINING ruler": {
			shuffleShardSize: 0,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.JOINING, time.Now(), false, time.Time{})
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
			},

			expectedRules: expectedRulesMap{
				// JOINING ruler has no rules yet.
				ruler1: noRules,
				ruler2: allRules,
			},
		},

		"shard size 0, single ruler": {
			shuffleShardSize: 0,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{0}), ring.ACTIVE, time.Now(), false, time.Time{})
			},

			expectedRules: expectedRulesMap{
				ruler1: allRules,
			},
		},

		"shard size 1, multiple rulers": {
			shuffleShardSize: 1,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, userToken(user2, 0) + 1, userToken(user3, 0) + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group1Token + 1, user1Group2Token + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
			},

			expectedRules: expectedRulesMap{
				ruler1: allRules,
				ruler2: noRules,
			},
		},

		// Same test as previous one, but with shard size=2. Second ruler gets all the rules.
		"shard size 2, two rulers": {
			shuffleShardSize: 2,

			setupRing: func(desc *ring.Desc) {
				// Exact same tokens setup as previous test.
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, userToken(user2, 0) + 1, userToken(user3, 0) + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group1Token + 1, user1Group2Token + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
			},

			expectedRules: expectedRulesMap{
				ruler1: noRules,
				ruler2: allRules,
			},
		},

		"shard size 1, two rulers, distributed users": {
			shuffleShardSize: 1,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group1, user1Group2},
				},
				ruler2: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
					user3: {user3Group1},
				},
			},
		},
		"shard size 2, three rulers": {
			shuffleShardSize: 2,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, user1Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, user1Group2Token + 1, userToken(user2, 1) + 1, userToken(user3, 1) + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
				},
				ruler2: map[string]rulespb.RuleGroupList{
					user1: {user1Group2},
				},
				ruler3: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
					user3: {user3Group1},
				},
			},
		},
		"shard size 2, three rulers, ruler2 has no users": {
			shuffleShardSize: 2,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, userToken(user2, 1) + 1, user1Group1Token + 1, user1Group2Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, userToken(user3, 1) + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group1, user1Group2},
				},
				ruler2: noRules, // Ruler2 owns token for user2group1, but user-2 will only be handled by ruler-1 and 3.
				ruler3: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
					user3: {user3Group1},
				},
			},
		},

		"shard size 2, three rulers, single enabled user": {
			shuffleShardSize: 2,
			enabledUsers:     []string{user1},

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, user1Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, user1Group2Token + 1, userToken(user2, 1) + 1, userToken(user3, 1) + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group1},
				},
				ruler2: map[string]rulespb.RuleGroupList{
					user1: {user1Group2},
				},
				ruler3: map[string]rulespb.RuleGroupList{},
			},
		},

		"shard size 2, three rulers, single disabled user": {
			shuffleShardSize: 2,
			disabledUsers:    []string{user1},

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, user1Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, user1Group2Token + 1, userToken(user2, 1) + 1, userToken(user3, 1) + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{},
				ruler2: map[string]rulespb.RuleGroupList{},
				ruler3: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
					user3: {user3Group1},
				},
			},
		},
		"shard size 2, 3 rulers, ruler2 is in joining stat": {
			shuffleShardSize: 2,

			setupRing: func(desc *ring.Desc) {
				// user1, group2 should have been owned by ruler2, but ruler2 is in JOINING state. So, it would be owned by ruler1.
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, user1Group2Token + 1, userToken(user2, 1) + 1, userToken(user3, 0) + 1}), ring.JOINING, time.Now(), false, time.Time{})
				// user2, user3 should have been owned by ruler2 or ruler3, but ruler2 is in JOINING state. So, it would be owned by ruler3.
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 1) + 1}), ring.ACTIVE, time.Now(), false, time.Time{})
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group1, user1Group2},
				},
				ruler2: map[string]rulespb.RuleGroupList{},
				ruler3: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
					user3: {user3Group1},
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			setupRuler := func(id string, host string, port int, forceRing *ring.Ring) *Ruler {
				cfg := Config{
					Ring: RingConfig{
						Common: util.CommonRingConfig{
							InstanceID:   id,
							InstanceAddr: host,
							InstancePort: port,
							KVStore: kv.Config{
								Mock: kvStore,
							},
							HeartbeatTimeout: 1 * time.Minute,
						},
					},
					EnabledTenants:  tc.enabledUsers,
					DisabledTenants: tc.disabledUsers,
				}

				r := prepareRuler(t, cfg, newMockRuleStore(allRules), withLimits(validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
					defaults.RulerEvaluationDelay = 0
					defaults.RulerTenantShardSize = tc.shuffleShardSize
				})))

				// Ensure the manager is stopped before leaving the test.
				t.Cleanup(r.manager.Stop)

				if forceRing != nil {
					r.ring = forceRing
				}
				return r
			}

			r1 := setupRuler(ruler1, ruler1Host, ruler1Port, nil)

			rulerRing := r1.ring

			// We start ruler's ring, but nothing else (not even lifecycler).
			if rulerRing != nil {
				require.NoError(t, services.StartAndAwaitRunning(context.Background(), rulerRing))
				t.Cleanup(func() {
					require.NoError(t, services.StopAndAwaitTerminated(context.Background(), rulerRing))
				})
			}

			var r2, r3 *Ruler
			if rulerRing != nil {
				// Reuse ring from r1.
				r2 = setupRuler(ruler2, ruler2Host, ruler2Port, rulerRing)
				r3 = setupRuler(ruler3, ruler3Host, ruler3Port, rulerRing)
			}

			if tc.setupRing != nil {
				err := kvStore.CAS(context.Background(), RulerRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
					d, _ := in.(*ring.Desc)
					if d == nil {
						d = ring.NewDesc()
					}

					tc.setupRing(d)

					return d, true, nil
				})
				require.NoError(t, err)
				// Wait a bit to make sure ruler's ring is updated.
				time.Sleep(100 * time.Millisecond)
			}

			// Always add ruler1 to expected rulers, even if there is no ring (no sharding).
			loadedRules1, err := r1.listRuleGroupsToSyncForAllUsers(context.Background(), rulerSyncReasonPeriodic, true)
			require.NoError(t, err)

			expected := expectedRulesMap{
				ruler1: loadedRules1,
			}

			addToExpected := func(id string, r *Ruler) {
				// Only expect rules from other rulers when using ring, and they are present in the ring.
				if r != nil && rulerRing != nil && rulerRing.HasInstance(id) {
					loaded, err := r.listRuleGroupsToSyncForAllUsers(context.Background(), rulerSyncReasonPeriodic, true)
					require.NoError(t, err)
					// Normalize nil map to empty one.
					if loaded == nil {
						loaded = map[string]rulespb.RuleGroupList{}
					}
					expected[id] = loaded
				}
			}

			addToExpected(ruler2, r2)
			addToExpected(ruler3, r3)

			require.Equal(t, tc.expectedRules, expected)
		})
	}
}

func TestRuler_NotifySyncRulesAsync_ShouldTriggerRulesSyncingOnAllRulersWhenEnabled(t *testing.T) {
	const (
		numRulers     = 2
		numRuleGroups = 10
		userID        = "user-1"
		namespace     = "test"
	)

	for _, rulerShardSize := range []int{0, 1} {
		t.Run(fmt.Sprintf("Ruler shard size = %d", rulerShardSize), func(t *testing.T) {
			var (
				ctx          = context.Background()
				logger       = log.NewNopLogger()
				rulerAddrMap = map[string]*Ruler{}
			)

			// Create a filesystem backed storage.
			bucketCfg := bucket.Config{StorageBackendConfig: bucket.StorageBackendConfig{Backend: "filesystem", Filesystem: filesystem.Config{Directory: t.TempDir()}}}
			bucketClient, err := bucket.NewClient(ctx, bucketCfg, "ruler-storage", logger, nil)
			require.NoError(t, err)

			store := bucketclient.NewBucketRuleStore(bucketClient, nil, logger)

			// Create an in-memory ring backend.
			kvStore, cleanUp := consul.NewInMemoryClient(ring.GetCodec(), logger, nil)
			t.Cleanup(func() { assert.NoError(t, cleanUp.Close()) })

			// Create rulers. The rulers are configured with a very long polling interval
			// so that they will not trigger after the initial sync. Once the ruler has started,
			// the initial sync already occurred.
			rulers := make([]*Ruler, numRulers)
			regs := make([]*prometheus.Registry, numRulers)

			for i := 0; i < len(rulers); i++ {
				rulerAddr := fmt.Sprintf("ruler-%d", i)

				rulerCfg := defaultRulerConfig(t)
				rulerCfg.PollInterval = time.Hour
				rulerCfg.OutboundSyncQueuePollInterval = 100 * time.Millisecond
				rulerCfg.InboundSyncQueuePollInterval = 100 * time.Millisecond
				rulerCfg.Ring.NumTokens = 128
				rulerCfg.Ring.Common.InstanceID = rulerAddr
				rulerCfg.Ring.Common.InstanceAddr = rulerAddr
				rulerCfg.Ring.Common.KVStore = kv.Config{Mock: kvStore}

				limits := validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
					defaults.RulerTenantShardSize = rulerShardSize
				})

				regs[i] = prometheus.NewPedanticRegistry()
				rulers[i] = prepareRuler(t, rulerCfg, store, withRulerAddrMap(rulerAddrMap), withRulerAddrAutomaticMapping(), withLimits(limits), withStart(), withPrometheusRegisterer(regs[i]))
			}

			// Pre-condition check: each ruler should have synced the rules once (at startup).
			for _, reg := range regs {
				verifySyncRulesMetric(t, reg, 1, 0)
			}

			// Pre-condition check: each ruler should have an updated view over the ring.
			for _, reg := range regs {
				verifyRingMembersMetric(t, reg, 2)
			}

			t.Run("NotifySyncRulesAsync() should trigger a re-sync after the initial rule groups of a tenant have been configured", func(t *testing.T) {
				// Create some rule groups in the storage.
				for i := 0; i < numRuleGroups; i++ {
					groupID := fmt.Sprintf("group-%d", i)
					record := fmt.Sprintf("count:metric_%d", i)
					expr := fmt.Sprintf("count(metric_%d)", i)

					require.NoError(t, store.SetRuleGroup(ctx, userID, namespace, createRuleGroup(groupID, userID, createRecordingRule(record, expr))))
				}

				// Call NotifySyncRulesAsync() on 1 ruler.
				rulers[0].NotifySyncRulesAsync("user-1")

				// Wait until rules syncing triggered on both rulers (with reason "API change").
				for _, reg := range regs {
					verifySyncRulesMetric(t, reg, 1, 1)
				}

				// GetRules() should return all configured rule groups. We use test.Poll() because
				// the per-tenant rules manager gets started asynchronously.
				for _, ruler := range rulers {
					test.Poll(t, time.Second, numRuleGroups, func() interface{} {
						actualRuleGroups, err := ruler.GetRules(user.InjectOrgID(ctx, userID), RulesRequest{Filter: AnyRule})
						require.NoError(t, err)
						return len(actualRuleGroups)
					})
				}
			})

			t.Run("NotifySyncRulesAsync() should trigger a re-sync after a single rule group of a tenant has been deleted", func(t *testing.T) {
				// Remove 1 rule group of a tenant.
				require.NoError(t, store.DeleteRuleGroup(ctx, userID, namespace, "group-0"))

				// Call NotifySyncRulesAsync() on 1 ruler.
				rulers[0].NotifySyncRulesAsync("user-1")

				// Wait until rules syncing triggered on both rulers (with reason "API change").
				for _, reg := range regs {
					verifySyncRulesMetric(t, reg, 1, 2)
				}

				// GetRules() should return all configured rule groups except the one just deleted.
				// We use test.Poll() because the rule syncing is asynchronous in each ruler.
				for _, ruler := range rulers {
					test.Poll(t, time.Second, numRuleGroups-1, func() interface{} {
						actualRuleGroups, err := ruler.GetRules(user.InjectOrgID(ctx, userID), RulesRequest{Filter: AnyRule})
						require.NoError(t, err)
						return len(actualRuleGroups)
					})
				}
			})

			t.Run("NotifySyncRulesAsync() should trigger a re-sync after all rule groups of a tenant have been deleted", func(t *testing.T) {
				// Remove all rules groups of a tenant.
				require.NoError(t, store.DeleteNamespace(ctx, userID, namespace))

				// Call NotifySyncRulesAsync() on 1 ruler.
				rulers[0].NotifySyncRulesAsync("user-1")

				// Wait until rules syncing triggered on both rulers (with reason "API change").
				for _, reg := range regs {
					verifySyncRulesMetric(t, reg, 1, 3)
				}

				// GetRules() should return no rule groups. We use test.Poll() because
				// the rule syncing is asynchronous in each ruler.
				for _, ruler := range rulers {
					test.Poll(t, time.Second, 0, func() interface{} {
						actualRuleGroups, err := ruler.GetRules(user.InjectOrgID(ctx, userID), RulesRequest{Filter: AnyRule})
						require.NoError(t, err)
						return len(actualRuleGroups)
					})
				}
			})

			// Post-condition check: there should have been no other rules syncing other than the initial one
			// and the one driven by the API.
			for _, reg := range regs {
				verifySyncRulesMetric(t, reg, 1, 3)
			}
		})
	}
}

func TestRuler_NotifySyncRulesAsync_ShouldTriggerRulesSyncingAndCorrectlyHandleTheCaseTheTenantShardHasChanged(t *testing.T) {
	const (
		numRulers     = 2
		numRuleGroups = 100
		userID        = "user-1"
		namespace     = "test"
	)

	var (
		ctx          = context.Background()
		logger       = log.NewNopLogger()
		rulerAddrMap = map[string]*Ruler{}
	)

	// Create a filesystem backed storage.
	bucketCfg := bucket.Config{StorageBackendConfig: bucket.StorageBackendConfig{Backend: "filesystem", Filesystem: filesystem.Config{Directory: t.TempDir()}}}
	bucketClient, err := bucket.NewClient(ctx, bucketCfg, "ruler-storage", logger, nil)
	require.NoError(t, err)

	store := bucketclient.NewBucketRuleStore(bucketClient, nil, logger)

	// Create an in-memory ring backend.
	kvStore, cleanUp := consul.NewInMemoryClient(ring.GetCodec(), logger, nil)
	t.Cleanup(func() { assert.NoError(t, cleanUp.Close()) })

	// Init tenant limits: ruler shuffle sharding disabled.
	tenantLimits := map[string]*validation.Limits{userID: validation.MockDefaultLimits()}
	tenantLimits[userID].RulerTenantShardSize = 0

	// Create rulers. The rulers are configured with a very long polling interval
	// so that they will not trigger after the initial sync. Once the ruler has started,
	// the initial sync already occurred.
	rulers := make([]*Ruler, numRulers)
	regs := make([]*prometheus.Registry, numRulers)

	for i := 0; i < len(rulers); i++ {
		rulerAddr := fmt.Sprintf("ruler-%d", i)

		rulerCfg := defaultRulerConfig(t)
		rulerCfg.PollInterval = time.Hour
		rulerCfg.OutboundSyncQueuePollInterval = 100 * time.Millisecond
		rulerCfg.InboundSyncQueuePollInterval = 100 * time.Millisecond
		rulerCfg.Ring.NumTokens = 128
		rulerCfg.Ring.Common.InstanceID = rulerAddr
		rulerCfg.Ring.Common.InstanceAddr = rulerAddr
		rulerCfg.Ring.Common.KVStore = kv.Config{Mock: kvStore}

		limits, err := validation.NewOverrides(*validation.MockDefaultLimits(), validation.NewMockTenantLimits(tenantLimits))
		require.NoError(t, err)

		regs[i] = prometheus.NewPedanticRegistry()
		rulers[i] = prepareRuler(t, rulerCfg, store, withRulerAddrMap(rulerAddrMap), withRulerAddrAutomaticMapping(), withLimits(limits), withStart(), withPrometheusRegisterer(regs[i]))
	}

	// Pre-condition check: each ruler should have synced the rules once (at startup).
	for _, reg := range regs {
		verifySyncRulesMetric(t, reg, 1, 0)
	}

	// Pre-condition check: each ruler should have an updated view over the ring.
	for _, reg := range regs {
		verifyRingMembersMetric(t, reg, 2)
	}

	// Create some rule groups in the storage.
	for i := 0; i < numRuleGroups; i++ {
		groupID := fmt.Sprintf("group-%d", i)
		record := fmt.Sprintf("count:metric_%d", i)
		expr := fmt.Sprintf("count(metric_%d)", i)

		require.NoError(t, store.SetRuleGroup(ctx, userID, namespace, createRuleGroup(groupID, userID, createRecordingRule(record, expr))))
	}

	// Call NotifySyncRulesAsync() on 1 ruler.
	rulers[0].NotifySyncRulesAsync(userID)

	// Wait until rules syncing triggered on both rulers (with reason "API change").
	for _, reg := range regs {
		verifySyncRulesMetric(t, reg, 1, 1)
	}

	// GetRules() should return all configured rule groups. We use test.Poll() because
	// the per-tenant rules manager gets started asynchronously.
	for _, ruler := range rulers {
		test.Poll(t, time.Second, numRuleGroups, func() interface{} {
			actualRuleGroups, err := ruler.GetRules(user.InjectOrgID(ctx, userID), RulesRequest{Filter: AnyRule})
			require.NoError(t, err)
			return len(actualRuleGroups)
		})
	}

	// We expect rule groups to have been sharded between the rulers.
	test.Poll(t, time.Second, []int{numRuleGroups, len(rulers)}, func() interface{} {
		var actualRuleGroupsCount int
		var actualRulersWithRuleGroups int

		for _, ruler := range rulers {
			actualRuleGroups, err := ruler.getLocalRules(ctx, userID, RulesRequest{Filter: AnyRule})
			require.NoError(t, err)
			actualRuleGroupsCount += len(actualRuleGroups)

			if len(actualRuleGroups) > 0 {
				actualRulersWithRuleGroups++
			}
		}

		return []int{actualRuleGroupsCount, actualRulersWithRuleGroups}
	})

	// Change the tenant's ruler shard size to 1, so that only 1 ruler will load all the rule groups after the next sync.
	tenantLimits[userID].RulerTenantShardSize = 1

	// Call NotifySyncRulesAsync() on 1 ruler.
	rulers[0].NotifySyncRulesAsync(userID)

	// Wait until rules syncing triggered on both rulers (with reason "API change").
	for _, reg := range regs {
		verifySyncRulesMetric(t, reg, 1, 2)
	}

	// GetRules() should return all configured rule groups. We use test.Poll() because
	// the rule syncing is asynchronous in each ruler.
	for _, ruler := range rulers {
		test.Poll(t, time.Second, numRuleGroups, func() interface{} {
			actualRuleGroups, err := ruler.GetRules(user.InjectOrgID(ctx, userID), RulesRequest{Filter: AnyRule})
			require.NoError(t, err)
			return len(actualRuleGroups)
		})
	}

	// We expect rule groups to have been loaded only from 1 ruler (not important which one).
	test.Poll(t, time.Second, []int{0, numRuleGroups}, func() interface{} {
		var actualRuleGroupsCountPerRuler []int

		for _, ruler := range rulers {
			actualRuleGroups, err := ruler.getLocalRules(ctx, userID, RulesRequest{Filter: AnyRule})
			require.NoError(t, err)
			actualRuleGroupsCountPerRuler = append(actualRuleGroupsCountPerRuler, len(actualRuleGroups))
		}

		slices.Sort(actualRuleGroupsCountPerRuler)
		return actualRuleGroupsCountPerRuler
	})

	// Post-condition check: there should have been no other rules syncing other than the initial one
	// and the one driven by the API.
	for _, reg := range regs {
		verifySyncRulesMetric(t, reg, 1, 2)
	}
}

func TestRuler_NotifySyncRulesAsync_ShouldNotTriggerRulesSyncingOnAllRulersWhenDisabled(t *testing.T) {
	const (
		numRulers     = 2
		numRuleGroups = 10
		userID        = "user-1"
		namespace     = "test"
	)

	var (
		ctx          = context.Background()
		logger       = log.NewNopLogger()
		rulerAddrMap = map[string]*Ruler{}
	)

	// Create a filesystem backed storage.
	bucketCfg := bucket.Config{StorageBackendConfig: bucket.StorageBackendConfig{Backend: "filesystem", Filesystem: filesystem.Config{Directory: t.TempDir()}}}
	bucketClient, err := bucket.NewClient(ctx, bucketCfg, "ruler-storage", logger, nil)
	require.NoError(t, err)

	store := bucketclient.NewBucketRuleStore(bucketClient, nil, logger)

	// Create an in-memory ring backend.
	kvStore, cleanUp := consul.NewInMemoryClient(ring.GetCodec(), logger, nil)
	t.Cleanup(func() { assert.NoError(t, cleanUp.Close()) })

	// Create rulers. The rulers are configured with a very long polling interval
	// so that they will not trigger after the initial sync. Once the ruler has started,
	// the initial sync already occurred.
	rulers := make([]*Ruler, numRulers)
	regs := make([]*prometheus.Registry, numRulers)

	for i := 0; i < len(rulers); i++ {
		rulerAddr := fmt.Sprintf("ruler-%d", i)

		rulerCfg := defaultRulerConfig(t)
		rulerCfg.PollInterval = time.Hour
		rulerCfg.OutboundSyncQueuePollInterval = 100 * time.Millisecond
		rulerCfg.InboundSyncQueuePollInterval = 100 * time.Millisecond
		rulerCfg.Ring.NumTokens = 128
		rulerCfg.Ring.Common.InstanceID = rulerAddr
		rulerCfg.Ring.Common.InstanceAddr = rulerAddr
		rulerCfg.Ring.Common.KVStore = kv.Config{Mock: kvStore}

		limits := validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
			defaults.RulerSyncRulesOnChangesEnabled = false
		})

		regs[i] = prometheus.NewPedanticRegistry()
		rulers[i] = prepareRuler(t, rulerCfg, store, withLimits(limits), withRulerAddrMap(rulerAddrMap), withRulerAddrAutomaticMapping(), withStart(), withPrometheusRegisterer(regs[i]))
	}

	// Pre-condition check: each ruler should have synced the rules once (at startup).
	for _, reg := range regs {
		verifySyncRulesMetric(t, reg, 1, 0)
	}

	// Pre-condition check: each ruler should have an updated view over the ring.
	for _, reg := range regs {
		verifyRingMembersMetric(t, reg, 2)
	}

	// Create some rule groups in the storage.
	for i := 0; i < numRuleGroups; i++ {
		groupID := fmt.Sprintf("group-%d", i)
		record := fmt.Sprintf("count:metric_%d", i)
		expr := fmt.Sprintf("count(metric_%d)", i)

		require.NoError(t, store.SetRuleGroup(ctx, userID, namespace, createRuleGroup(groupID, userID, createRecordingRule(record, expr))))
	}

	// Call NotifySyncRulesAsync() on 1 ruler.
	rulers[0].NotifySyncRulesAsync("user-1")

	// Give rulers enough time to eventually re-sync based on config change, if it was enabled (but it's not).
	// Unfortunately there's no better to way than waiting some time, since we're waiting for a condition to NOT happen.
	time.Sleep(time.Second)

	// Ensure no rules syncing has been triggered in any ruler.
	for _, reg := range regs {
		verifySyncRulesMetric(t, reg, 1, 0)
	}

	// GetRules() should return no configured rule groups, because no re-sync happened.
	for _, ruler := range rulers {
		actualRuleGroups, err := ruler.GetRules(user.InjectOrgID(ctx, userID), RulesRequest{Filter: AnyRule})
		require.NoError(t, err)
		require.Empty(t, actualRuleGroups)
	}
}

// User shuffle shard token.
func userToken(user string, skip int) uint32 {
	r := rand.New(rand.NewSource(util.ShuffleShardSeed(user, "")))

	for ; skip > 0; skip-- {
		_ = r.Uint32()
	}
	return r.Uint32()
}

func sortTokens(tokens []uint32) []uint32 {
	sort.Slice(tokens, func(i, j int) bool {
		return tokens[i] < tokens[j]
	})
	return tokens
}

func TestRuler_DeleteTenantConfiguration_ShouldDeleteTenantConfigurationAndTriggerSync(t *testing.T) {
	ruleGroups := []ruleGroupKey{
		{user: "userA", namespace: "namespace", group: "group"},
		{user: "userB", namespace: "namespace1", group: "group"},
		{user: "userB", namespace: "namespace2", group: "group"},
	}

	obj := objstore.NewInMemBucket()
	rs := bucketclient.NewBucketRuleStore(obj, nil, log.NewNopLogger())

	// "upload" rule groups
	for _, key := range ruleGroups {
		desc := rulespb.ToProto(key.user, key.namespace, rulefmt.RuleGroup{Name: key.group, Rules: []rulefmt.RuleNode{
			{
				Record: yaml.Node{Value: "up", Kind: yaml.ScalarNode},
				Expr:   yaml.Node{Value: "up==1", Kind: yaml.ScalarNode},
			},
		}})
		require.NoError(t, rs.SetRuleGroup(context.Background(), key.user, key.namespace, desc))
	}

	require.Len(t, obj.Objects(), 3)

	// Configure ruler with an high poll interval so that it will just sync
	// once explicitly triggered by the change via API.
	cfg := defaultRulerConfig(t)
	cfg.PollInterval = time.Hour
	cfg.OutboundSyncQueuePollInterval = 100 * time.Millisecond
	cfg.InboundSyncQueuePollInterval = 100 * time.Millisecond
	cfg.Ring.Common.InstanceAddr = "ruler-1"

	reg := prometheus.NewPedanticRegistry()
	ruler := prepareRuler(t, cfg, rs, withStart(), withPrometheusRegisterer(reg), withRulerAddrAutomaticMapping())

	// Pre-condition check: the ruler should have synced the rules once (at startup).
	verifySyncRulesMetric(t, reg, 1, 0)

	t.Run("should return 401 on missing tenant ID", func(t *testing.T) {
		require.Equal(t, http.StatusUnauthorized, callDeleteTenantConfigurationAPI(ruler, ""))
	})

	t.Run("should return 200 and be a no-op if the tenant has no rule groups configured", func(t *testing.T) {
		require.Equal(t, http.StatusOK, callDeleteTenantConfigurationAPI(ruler, "user-with-no-rule-groups"))
		require.Len(t, obj.Objects(), 3)

		verifyExpectedDeletedRuleGroupsForUser(t, ruler, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, ruler, "userA", false)
		verifyExpectedDeletedRuleGroupsForUser(t, ruler, "userB", false)

		// Ensure rules re-sync has been triggered.
		verifySyncRulesMetric(t, reg, 1, 1)
	})

	t.Run("should return 200 and delete the rule groups configured for the tenant", func(t *testing.T) {
		require.Equal(t, http.StatusOK, callDeleteTenantConfigurationAPI(ruler, "userA"))
		require.Len(t, obj.Objects(), 2)

		verifyExpectedDeletedRuleGroupsForUser(t, ruler, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, ruler, "userA", true)                    // Just deleted.
		verifyExpectedDeletedRuleGroupsForUser(t, ruler, "userB", false)

		// Ensure rules re-sync has been triggered.
		verifySyncRulesMetric(t, reg, 1, 2)
	})

	t.Run("should return 200 and be idempotent if the tenant rule groups have already been deleted", func(t *testing.T) {
		// Deleting same user again works fine and reports no problems.
		require.Equal(t, http.StatusOK, callDeleteTenantConfigurationAPI(ruler, "userA"))
		require.Len(t, obj.Objects(), 2)

		verifyExpectedDeletedRuleGroupsForUser(t, ruler, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, ruler, "userA", true)                    // Already deleted before.
		verifyExpectedDeletedRuleGroupsForUser(t, ruler, "userB", false)

		// Ensure rules re-sync has been triggered.
		verifySyncRulesMetric(t, reg, 1, 3)
	})
}

func generateTokenForGroups(groups []*rulespb.RuleGroupDesc, offset uint32) []uint32 {
	var tokens []uint32

	for _, g := range groups {
		tokens = append(tokens, tokenForGroup(g)+offset)
	}

	return tokens
}

func callDeleteTenantConfigurationAPI(api *Ruler, userID string) (statusCode int) {
	ctx := context.Background()
	if userID != "" {
		ctx = user.InjectOrgID(ctx, userID)
	}

	req := &http.Request{}
	resp := httptest.NewRecorder()
	api.DeleteTenantConfiguration(resp, req.WithContext(ctx))

	return resp.Code
}

func verifyExpectedDeletedRuleGroupsForUser(t *testing.T, r *Ruler, userID string, expectedDeleted bool) {
	ctx := context.Background()

	t.Run("ListRuleGroupsForUserAndNamespace()", func(t *testing.T) {
		list, err := r.directStore.ListRuleGroupsForUserAndNamespace(ctx, userID, "")
		require.NoError(t, err)

		if expectedDeleted {
			require.Empty(t, list)
		} else {
			require.NotEmpty(t, list)
		}
	})

	t.Run("GetRules()", func(t *testing.T) {
		// The rules manager updates the rules asynchronously so we need to poll it.
		test.Poll(t, time.Second, expectedDeleted, func() interface{} {
			list, err := r.GetRules(user.InjectOrgID(ctx, userID), RulesRequest{Filter: AnyRule})
			require.NoError(t, err)

			return len(list) == 0
		})
	})
}

func verifySyncRulesMetric(t *testing.T, reg prometheus.Gatherer, initialCount, apiChangeCount int) {
	t.Helper()

	test.Poll(t, time.Second, nil, func() interface{} {
		return prom_testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_ruler_sync_rules_total Total number of times the ruler sync operation triggered.
			# TYPE cortex_ruler_sync_rules_total counter
			cortex_ruler_sync_rules_total{reason="initial"} %d
			cortex_ruler_sync_rules_total{reason="api-change"} %d
			cortex_ruler_sync_rules_total{reason="periodic"} 0
			cortex_ruler_sync_rules_total{reason="ring-change"} 0
		`, initialCount, apiChangeCount)), "cortex_ruler_sync_rules_total")
	})
}

func verifyRingMembersMetric(t *testing.T, reg prometheus.Gatherer, activeCount int) {
	test.Poll(t, time.Second, nil, func() interface{} {
		return prom_testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
				# HELP cortex_ring_members Number of members in the ring
				# TYPE cortex_ring_members gauge
				cortex_ring_members{name="ruler",state="ACTIVE"} %d
				cortex_ring_members{name="ruler",state="JOINING"} 0
				cortex_ring_members{name="ruler",state="LEAVING"} 0
				cortex_ring_members{name="ruler",state="PENDING"} 0
				cortex_ring_members{name="ruler",state="Unhealthy"} 0
			`, activeCount)), "cortex_ring_members")
	})
}

type ruleGroupKey struct {
	user, namespace, group string
}

func TestRuler_ListAllRules(t *testing.T) {
	cfg := defaultRulerConfig(t)

	r := prepareRuler(t, cfg, newMockRuleStore(mockRules), withStart())

	router := mux.NewRouter()
	router.Path("/ruler/rule_groups").Methods(http.MethodGet).HandlerFunc(r.ListAllRules)

	req := requestFor(t, http.MethodGet, "https://localhost:8080/ruler/rule_groups", nil, "")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	// Check status code and header
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/yaml", resp.Header.Get("Content-Type"))

	expectedResponseYaml := `user1:
    namespace1:
        - name: group1
          interval: 1m
          rules:
            - record: UP_RULE
              expr: up
            - alert: UP_ALERT
              expr: up < 1
user2:
    namespace1:
        - name: group1
          interval: 1m
          rules:
            - record: UP_RULE
              expr: up`

	require.YAMLEq(t, expectedResponseYaml, string(body))
}

type senderFunc func(alerts ...*notifier.Alert)

func (s senderFunc) Send(alerts ...*notifier.Alert) {
	s(alerts...)
}

func TestSendAlerts(t *testing.T) {
	testCases := []struct {
		in  []*promRules.Alert
		exp []*notifier.Alert
	}{
		{
			in: []*promRules.Alert{
				{
					Labels:      labels.FromStrings("l1", "v1"),
					Annotations: labels.FromStrings("a2", "v2"),
					ActiveAt:    time.Unix(1, 0),
					FiredAt:     time.Unix(2, 0),
					ValidUntil:  time.Unix(3, 0),
				},
			},
			exp: []*notifier.Alert{
				{
					Labels:       labels.FromStrings("l1", "v1"),
					Annotations:  labels.FromStrings("a2", "v2"),
					StartsAt:     time.Unix(2, 0),
					EndsAt:       time.Unix(3, 0),
					GeneratorURL: "http://localhost:9090/graph?g0.expr=up&g0.tab=1",
				},
			},
		},
		{
			in: []*promRules.Alert{
				{
					Labels:      labels.FromStrings("l1", "v1"),
					Annotations: labels.FromStrings("a2", "v2"),
					ActiveAt:    time.Unix(1, 0),
					FiredAt:     time.Unix(2, 0),
					ResolvedAt:  time.Unix(4, 0),
				},
			},
			exp: []*notifier.Alert{
				{
					Labels:       labels.FromStrings("l1", "v1"),
					Annotations:  labels.FromStrings("a2", "v2"),
					StartsAt:     time.Unix(2, 0),
					EndsAt:       time.Unix(4, 0),
					GeneratorURL: "http://localhost:9090/graph?g0.expr=up&g0.tab=1",
				},
			},
		},
		{
			in: []*promRules.Alert{},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			senderFunc := senderFunc(func(alerts ...*notifier.Alert) {
				if len(tc.in) == 0 {
					t.Fatalf("sender called with 0 alert")
				}
				require.Equal(t, tc.exp, alerts)
			})
			promRules.SendAlerts(senderFunc, "http://localhost:9090")(context.TODO(), "up", tc.in...)
		})
	}
}

func TestFilterRuleGroupsByEnabled(t *testing.T) {
	tests := map[string]struct {
		configs  map[string]rulespb.RuleGroupList
		limits   RulesLimits
		expected map[string]rulespb.RuleGroupList
	}{
		"should return nil on nil input": {
			configs:  nil,
			limits:   validation.MockDefaultOverrides(),
			expected: nil,
		},
		"should return an empty map on empty input": {
			configs:  map[string]rulespb.RuleGroupList{},
			limits:   validation.MockDefaultOverrides(),
			expected: map[string]rulespb.RuleGroupList{},
		},
		"should remove alerting rules if disabled for a given tenant": {
			configs: map[string]rulespb.RuleGroupList{
				"user-1": {
					createRuleGroup("group-1", "user-1", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-1", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
					createRuleGroup("group-3", "user-1", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
				"user-2": {
					createRuleGroup("group-1", "user-2", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-2", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
					createRuleGroup("group-3", "user-2", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
			},
			limits: validation.MockOverrides(func(_ *validation.Limits, tenantLimits map[string]*validation.Limits) {
				tenantLimits["user-1"] = validation.MockDefaultLimits()
				tenantLimits["user-1"].RulerRecordingRulesEvaluationEnabled = true
				tenantLimits["user-1"].RulerAlertingRulesEvaluationEnabled = false
			}),
			expected: map[string]rulespb.RuleGroupList{
				"user-1": {
					createRuleGroup("group-1", "user-1", createRecordingRule("record:1", "1"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-1", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
				},
				"user-2": {
					createRuleGroup("group-1", "user-2", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-2", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
					createRuleGroup("group-3", "user-2", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
			},
		},
		"should remove recording rules if disabled for a given tenant": {
			configs: map[string]rulespb.RuleGroupList{
				"user-1": {
					createRuleGroup("group-1", "user-1", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-1", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
					createRuleGroup("group-3", "user-1", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
				"user-2": {
					createRuleGroup("group-1", "user-2", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-2", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
					createRuleGroup("group-3", "user-2", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
			},
			limits: validation.MockOverrides(func(_ *validation.Limits, tenantLimits map[string]*validation.Limits) {
				tenantLimits["user-1"] = validation.MockDefaultLimits()
				tenantLimits["user-1"].RulerRecordingRulesEvaluationEnabled = false
				tenantLimits["user-1"].RulerAlertingRulesEvaluationEnabled = true
			}),
			expected: map[string]rulespb.RuleGroupList{
				"user-1": {
					createRuleGroup("group-1", "user-1", createAlertingRule("alert-2", "2")),
					createRuleGroup("group-3", "user-1", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
				"user-2": {
					createRuleGroup("group-1", "user-2", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-2", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
					createRuleGroup("group-3", "user-2", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
			},
		},
		"should remove all config for a user if both recording and alerting rules are disabled": {
			configs: map[string]rulespb.RuleGroupList{
				"user-1": {
					createRuleGroup("group-1", "user-1", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-1", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
					createRuleGroup("group-3", "user-1", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
				"user-2": {
					createRuleGroup("group-1", "user-2", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-2", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
					createRuleGroup("group-3", "user-2", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
			},
			limits: validation.MockOverrides(func(_ *validation.Limits, tenantLimits map[string]*validation.Limits) {
				tenantLimits["user-1"] = validation.MockDefaultLimits()
				tenantLimits["user-1"].RulerRecordingRulesEvaluationEnabled = false
				tenantLimits["user-1"].RulerAlertingRulesEvaluationEnabled = false
			}),
			expected: map[string]rulespb.RuleGroupList{
				"user-2": {
					createRuleGroup("group-1", "user-2", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-2", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
					createRuleGroup("group-3", "user-2", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
			},
		},
		"should remove configs for all users if both recording and alerting rules are disabled for every user": {
			configs: map[string]rulespb.RuleGroupList{
				"user-1": {
					createRuleGroup("group-1", "user-1", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-1", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
					createRuleGroup("group-3", "user-1", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
				"user-2": {
					createRuleGroup("group-1", "user-2", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-2", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
					createRuleGroup("group-3", "user-2", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
			},
			limits: validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
				defaults.RulerRecordingRulesEvaluationEnabled = false
				defaults.RulerAlertingRulesEvaluationEnabled = false
			}),
			expected: map[string]rulespb.RuleGroupList{},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			logger := log.NewNopLogger()

			actual := filterRuleGroupsByEnabled(testData.configs, testData.limits, logger)
			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestFilterRuleGroupsByNotMissing(t *testing.T) {
	tests := map[string]struct {
		configs  map[string]rulespb.RuleGroupList
		missing  rulespb.RuleGroupList
		expected map[string]rulespb.RuleGroupList
	}{
		"should return nil on nil input": {
			configs:  nil,
			expected: nil,
		},
		"should return an empty map on empty input": {
			configs:  map[string]rulespb.RuleGroupList{},
			expected: map[string]rulespb.RuleGroupList{},
		},
		"should remove the input missing rule groups": {
			configs: map[string]rulespb.RuleGroupList{
				"user-1": {
					createRuleGroup("group-1", "user-1", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-1", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
					createRuleGroup("group-3", "user-1", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
				"user-2": {
					createRuleGroup("group-1", "user-2", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-2", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
					createRuleGroup("group-3", "user-2", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
			},
			missing: rulespb.RuleGroupList{
				createRuleGroup("group-3", "user-1"),
				createRuleGroup("group-2", "user-2"),
			},
			expected: map[string]rulespb.RuleGroupList{
				"user-1": {
					createRuleGroup("group-1", "user-1", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-1", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
				},
				"user-2": {
					createRuleGroup("group-1", "user-2", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-3", "user-2", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
			},
		},
		"should remove an user from the rule groups configs if all their rule groups are missing": {
			configs: map[string]rulespb.RuleGroupList{
				"user-1": {
					createRuleGroup("group-1", "user-1", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-1", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
					createRuleGroup("group-3", "user-1", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
				"user-2": {
					createRuleGroup("group-1", "user-2", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-2", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
					createRuleGroup("group-3", "user-2", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
			},
			missing: rulespb.RuleGroupList{
				createRuleGroup("group-1", "user-1"),
				createRuleGroup("group-2", "user-1"),
				createRuleGroup("group-3", "user-1"),
			},
			expected: map[string]rulespb.RuleGroupList{
				"user-2": {
					createRuleGroup("group-1", "user-2", createRecordingRule("record:1", "1"), createAlertingRule("alert-2", "2"), createRecordingRule("record:3", "3")),
					createRuleGroup("group-2", "user-2", createRecordingRule("record:4", "4"), createRecordingRule("record:5", "5")),
					createRuleGroup("group-3", "user-2", createAlertingRule("alert-6", "6"), createAlertingRule("alert-7", "7")),
				},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			logger := log.NewNopLogger()

			actual := filterRuleGroupsByNotMissing(testData.configs, testData.missing, logger)
			assert.Equal(t, testData.expected, actual)
		})
	}
}

func BenchmarkFilterRuleGroupsByEnabled(b *testing.B) {
	const (
		numTenants                    = 1000
		numRuleGroupsPerTenant        = 10
		numRecordingRulesPerRuleGroup = 10
		numAlertingRulesPerRuleGroup  = 10
	)

	var (
		logger = log.NewNopLogger()
	)

	b.Logf("total number of rules: %d", numTenants*numRuleGroupsPerTenant*(numRecordingRulesPerRuleGroup+numAlertingRulesPerRuleGroup))

	// Utility used to create the ruler configs.
	buildConfigs := func() map[string]rulespb.RuleGroupList {
		configs := make(map[string]rulespb.RuleGroupList, numTenants)

		for t := 0; t < numTenants; t++ {
			tenantID := fmt.Sprintf("tenant-%d", t)
			configs[tenantID] = make(rulespb.RuleGroupList, 0, numRuleGroupsPerTenant)

			for g := 0; g < numRuleGroupsPerTenant; g++ {
				group := &rulespb.RuleGroupDesc{
					User:  tenantID,
					Name:  fmt.Sprintf("group-%d", g),
					Rules: make([]*rulespb.RuleDesc, 0, numRecordingRulesPerRuleGroup+numAlertingRulesPerRuleGroup),
				}

				for r := 0; r < numRecordingRulesPerRuleGroup; r++ {
					group.Rules = append(group.Rules, createRecordingRule(fmt.Sprintf("record:%d", r), "count(up)"))
				}

				for r := 0; r < numAlertingRulesPerRuleGroup; r++ {
					group.Rules = append(group.Rules, createAlertingRule(fmt.Sprintf("alert-%d", r), "count(up)"))
				}

				configs[tenantID] = append(configs[tenantID], group)
			}
		}

		return configs
	}

	tests := map[string]struct {
		limits RulesLimits
	}{
		"all rules enabled": {
			limits: validation.MockDefaultOverrides(),
		},
		"recording rules disabled": {
			limits: validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
				defaults.RulerRecordingRulesEvaluationEnabled = false
			}),
		},
		"alerting rules disabled": {
			limits: validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
				defaults.RulerAlertingRulesEvaluationEnabled = false
			}),
		},
		"all rules disabled": {
			limits: validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
				defaults.RulerRecordingRulesEvaluationEnabled = false
				defaults.RulerAlertingRulesEvaluationEnabled = false
			}),
		},
	}

	for testName, testData := range tests {
		b.Run(testName, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				// The CPU/mem required to build the initial config is taken in account too.
				// Unfortunately if build it upfront and then reset the timer after building it,
				// the filterRuleGroupsByEnabled() (in-place replacement version) is so fast that
				// the benchmark will run a very high b.N  which will in turn exhaust the memory.
				filterRuleGroupsByEnabled(buildConfigs(), testData.limits, logger)
			}
		})
	}
}

func createRecordingRule(record, expr string) *rulespb.RuleDesc {
	return &rulespb.RuleDesc{
		Record: record,
		Expr:   expr,
	}
}

func createAlertingRule(alert, expr string) *rulespb.RuleDesc {
	return &rulespb.RuleDesc{
		Alert: alert,
		Expr:  expr,
	}
}

// createRuleGroup creates a rule group filling in all fields, so that we can use it in tests to check if all fields
// are copied when a rule group is cloned.
func createRuleGroup(name, user string, rules ...*rulespb.RuleDesc) *rulespb.RuleGroupDesc {
	return &rulespb.RuleGroupDesc{
		Name:      name,
		Namespace: "test",
		Interval:  time.Minute,
		Rules:     rules,
		User:      user,
	}
}

func TestConfig_Validate(t *testing.T) {
	t.Run("invalid tenant shard size", func(t *testing.T) {
		cfg := defaultRulerConfig(t)
		limits := validation.MockDefaultLimits()
		limits.RulerTenantShardSize = -1

		err := cfg.Validate(*limits)
		require.ErrorIs(t, err, errInvalidTenantShardSize)
	})

	t.Run("invalid client TLS config", func(t *testing.T) {
		cfg := defaultRulerConfig(t)
		cfg.ClientTLSConfig.GRPCCompression = "bogus"
		limits := validation.MockDefaultLimits()

		err := cfg.Validate(*limits)
		require.Error(t, err)
	})

	t.Run("invalid query frontend config", func(t *testing.T) {
		cfg := defaultRulerConfig(t)
		cfg.QueryFrontend.QueryResultResponseFormat = "bogus"
		limits := validation.MockDefaultLimits()

		err := cfg.Validate(*limits)
		require.Error(t, err)
	})

	t.Run("invalid concurrency evaluation percentage", func(t *testing.T) {
		cfg := defaultRulerConfig(t)
		cfg.IndependentRuleEvaluationConcurrencyMinDurationPercentage = -1.0
		limits := validation.MockDefaultLimits()

		err := cfg.Validate(*limits)
		require.ErrorIs(t, err, errInnvalidRuleEvaluationConcurrencyMinDurationPercentage)
	})
}
