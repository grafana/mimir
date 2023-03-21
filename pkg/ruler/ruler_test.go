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
	"os"
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
	"github.com/grafana/dskit/test"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/ruler/rulestore"
	"github.com/grafana/mimir/pkg/ruler/rulestore/bucketclient"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
)

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

type mockRulerClientsPool struct {
	ClientsPool
	cfg           Config
	rulerAddrMap  map[string]*Ruler
	numberOfCalls atomic.Int32
}

type mockRulerClient struct {
	ruler         *Ruler
	numberOfCalls *atomic.Int32
}

func (c *mockRulerClient) Rules(ctx context.Context, in *RulesRequest, _ ...grpc.CallOption) (*RulesResponse, error) {
	c.numberOfCalls.Inc()
	return c.ruler.Rules(ctx, in)
}

func (p *mockRulerClientsPool) GetClientFor(addr string) (RulerClient, error) {
	for _, r := range p.rulerAddrMap {
		if r.lifecycler.GetInstanceAddr() == addr {
			return &mockRulerClient{
				ruler:         r,
				numberOfCalls: &p.numberOfCalls,
			}, nil
		}
	}

	return nil, fmt.Errorf("unable to find ruler for add %s", addr)
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
	limits       RulesLimits
	logger       log.Logger
	registerer   prometheus.Registerer
	rulerAddrMap map[string]*Ruler
	start        bool
}

func applyPrepareOptions(opts ...prepareOption) prepareOptions {
	defaultLogger := log.NewLogfmtLogger(os.Stdout)
	defaultLogger = level.NewFilter(defaultLogger, level.AllowInfo())

	applied := prepareOptions{
		// Default limits in the ruler tests.
		limits: validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
			defaults.RulerEvaluationDelay = 0
			defaults.RulerMaxRuleGroupsPerTenant = 20
			defaults.RulerMaxRulesPerRuleGroup = 15
		}),
		logger:     defaultLogger,
		registerer: prometheus.NewPedanticRegistry(),
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

func prepareRuler(t *testing.T, cfg Config, storage rulestore.RuleStore, opts ...prepareOption) *Ruler {
	options := applyPrepareOptions(opts...)
	manager := prepareRulerManager(t, cfg, opts...)

	ruler, err := newRuler(cfg, manager, options.registerer, options.logger, storage, options.limits, newMockClientsPool(cfg, options.logger, options.registerer, options.rulerAddrMap))
	require.NoError(t, err)

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
	options := applyPrepareOptions(opts...)

	noopQueryable := storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return storage.NoopQuerier(), nil
	})
	noopQueryFunc := func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
		return nil, nil
	}

	// Mock the pusher
	pusher := newPusherMock()
	pusher.MockPush(&mimirpb.WriteResponse{}, nil)

	managerFactory := DefaultTenantManagerFactory(cfg, pusher, noopQueryable, noopQueryFunc, options.limits, options.registerer)
	manager, err := NewDefaultMultiTenantManager(cfg, managerFactory, prometheus.NewRegistry(), options.logger, nil)
	require.NoError(t, err)

	return manager
}

var _ MultiTenantManager = &DefaultMultiTenantManager{}

func TestNotifierSendsUserIDHeader(t *testing.T) {
	var wg sync.WaitGroup

	// We do expect 1 API call for the user create with the getOrCreateNotifier()
	wg.Add(1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
						Rules:         []*rulespb.RuleDesc{mockRecordingRuleDesc("UP_RULE", "up"), mockAlertingRuleDesc("UP_ALERT", "up < 1")},
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
				&rulespb.RuleGroupDesc{User: "user1", Namespace: "namespace", Name: "first", Interval: 10 * time.Second},
				&rulespb.RuleGroupDesc{User: "user1", Namespace: "namespace", Name: "second", Interval: 10 * time.Second},
			},
			"user2": {
				&rulespb.RuleGroupDesc{User: "user2", Namespace: "namespace", Name: "third", Interval: 10 * time.Second},
			},
		},
		"ruler2": map[string]rulespb.RuleGroupList{
			"user1": {
				&rulespb.RuleGroupDesc{User: "user1", Namespace: "namespace", Name: "third", Interval: 10 * time.Second},
			},
			"user2": {
				&rulespb.RuleGroupDesc{User: "user2", Namespace: "namespace", Name: "first", Interval: 10 * time.Second},
				&rulespb.RuleGroupDesc{User: "user2", Namespace: "namespace", Name: "second", Interval: 10 * time.Second},
			},
		},
		"ruler3": map[string]rulespb.RuleGroupList{
			"user3": {
				&rulespb.RuleGroupDesc{User: "user3", Namespace: "namespace", Name: "third", Interval: 10 * time.Second},
			},
			"user2": {
				&rulespb.RuleGroupDesc{User: "user2", Namespace: "namespace", Name: "forth", Interval: 10 * time.Second},
				&rulespb.RuleGroupDesc{User: "user2", Namespace: "namespace", Name: "fifty", Interval: 10 * time.Second},
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
			kvStore, cleanUp := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, cleanUp.Close()) })
			allRulesByUser := map[string]rulespb.RuleGroupList{}
			allRulesByRuler := map[string]rulespb.RuleGroupList{}
			allTokensByRuler := map[string][]uint32{}
			rulerAddrMap := map[string]*Ruler{}
			storage := newMockRuleStore(allRulesByUser)

			createRuler := func(id string) *Ruler {
				cfg := defaultRulerConfig(t)

				cfg.Ring = RingConfig{
					Common: util.CommonRingConfig{
						InstanceID:   id,
						InstanceAddr: id,
						KVStore: kv.Config{
							Mock: kvStore,
						},
					},
				}

				r := prepareRuler(t, cfg, storage, withRulerAddrMap(rulerAddrMap), withLimits(validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
					defaults.RulerEvaluationDelay = 0
					defaults.RulerTenantShardSize = tc.shuffleShardSize
				})))

				rulerAddrMap[id] = r
				if r.ring != nil {
					require.NoError(t, services.StartAndAwaitRunning(context.Background(), r.ring))
					t.Cleanup(r.ring.StopAsync)
				}
				return r
			}

			for rID, r := range expectedRulesByRuler {
				createRuler(rID)
				for user, rules := range r {
					allRulesByUser[user] = append(allRulesByUser[user], rules...)
					allRulesByRuler[rID] = append(allRulesByRuler[rID], rules...)
					allTokensByRuler[rID] = generateTokenForGroups(rules, 1)
				}
			}

			err := kvStore.CAS(context.Background(), RulerRingKey, func(in interface{}) (out interface{}, retry bool, err error) {
				d, _ := in.(*ring.Desc)
				if d == nil {
					d = ring.NewDesc()
				}
				for rID, tokens := range allTokensByRuler {
					d.AddIngester(rID, rulerAddrMap[rID].lifecycler.GetInstanceAddr(), "", tokens, ring.ACTIVE, time.Now())
				}
				return d, true, nil
			})
			require.NoError(t, err)
			// Wait a bit to make sure ruler's ring is updated.
			time.Sleep(100 * time.Millisecond)

			forEachRuler := func(f func(rID string, r *Ruler)) {
				for rID, r := range rulerAddrMap {
					f(rID, r)
				}
			}

			// Sync Rules
			forEachRuler(func(_ string, r *Ruler) {
				r.syncRules(context.Background(), rulerSyncReasonInitial)
			})

			for u := range allRulesByUser {
				ctx := user.InjectOrgID(context.Background(), u)
				forEachRuler(func(_ string, r *Ruler) {
					rules, err := r.GetRules(ctx)
					require.NoError(t, err)
					require.Equal(t, len(allRulesByUser[u]), len(rules))

					mockPoolClient := r.clientsPool.(*mockRulerClientsPool)
					if tc.shuffleShardSize > 0 {
						require.Equal(t, int32(tc.shuffleShardSize), mockPoolClient.numberOfCalls.Load())
					} else {
						require.Equal(t, int32(len(rulerAddrMap)), mockPoolClient.numberOfCalls.Load())
					}
					mockPoolClient.numberOfCalls.Store(0)
				})
			}

			totalLoadedRules := 0
			totalConfiguredRules := 0

			forEachRuler(func(rID string, r *Ruler) {
				localRules, err := r.listRules(context.Background(), rulerSyncReasonPeriodic)
				require.NoError(t, err)
				for _, rules := range localRules {
					totalLoadedRules += len(rules)
				}
				totalConfiguredRules += len(allRulesByRuler[rID])
			})

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
				desc.AddIngester(ruler1, ruler1Addr, "", []uint32{0}, ring.ACTIVE, time.Now())
			},
			expectedRules: expectedRulesMap{ruler1: allRules},
		},

		"single ruler, with ring setup, single user enabled": {
			shuffleShardSize: 0,
			enabledUsers:     []string{user1},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", []uint32{0}, ring.ACTIVE, time.Now())
			},
			expectedRules: expectedRulesMap{ruler1: map[string]rulespb.RuleGroupList{
				user1: {user1Group1, user1Group2},
			}},
		},

		"single ruler with ring setup, single user disabled": {
			shuffleShardSize: 0,
			disabledUsers:    []string{user1},
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", []uint32{0}, ring.ACTIVE, time.Now())
			},
			expectedRules: expectedRulesMap{ruler1: map[string]rulespb.RuleGroupList{
				user2: {user2Group1},
				user3: {user3Group1},
			}},
		},

		"shard size 0, multiple ACTIVE rulers": {
			shuffleShardSize: 0,
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
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
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
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
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
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
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now())
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
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.LEAVING, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
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
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{user1Group1Token + 1, user2Group1Token + 1}), ring.JOINING, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group2Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
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
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{0}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				ruler1: allRules,
			},
		},

		"shard size 1, multiple rulers": {
			shuffleShardSize: 1,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, userToken(user2, 0) + 1, userToken(user3, 0) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group1Token + 1, user1Group2Token + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
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
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, userToken(user2, 0) + 1, userToken(user3, 0) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{user1Group1Token + 1, user1Group2Token + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				ruler1: noRules,
				ruler2: allRules,
			},
		},

		"shard size 1, two rulers, distributed users": {
			shuffleShardSize: 1,

			setupRing: func(desc *ring.Desc) {
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1}), ring.ACTIVE, time.Now())
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
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, user1Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, user1Group2Token + 1, userToken(user2, 1) + 1, userToken(user3, 1) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
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
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, userToken(user2, 1) + 1, user1Group1Token + 1, user1Group2Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, userToken(user3, 1) + 1, user2Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
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
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, user1Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, user1Group2Token + 1, userToken(user2, 1) + 1, userToken(user3, 1) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
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
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1, user1Group1Token + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, user1Group2Token + 1, userToken(user2, 1) + 1, userToken(user3, 1) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 0) + 1, user2Group1Token + 1, user3Group1Token + 1}), ring.ACTIVE, time.Now())
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
				desc.AddIngester(ruler1, ruler1Addr, "", sortTokens([]uint32{userToken(user1, 0) + 1}), ring.ACTIVE, time.Now())
				desc.AddIngester(ruler2, ruler2Addr, "", sortTokens([]uint32{userToken(user1, 1) + 1, user1Group2Token + 1, userToken(user2, 1) + 1, userToken(user3, 0) + 1}), ring.JOINING, time.Now())
				desc.AddIngester(ruler3, ruler3Addr, "", sortTokens([]uint32{userToken(user2, 0) + 1, userToken(user3, 1) + 1}), ring.ACTIVE, time.Now())
			},

			expectedRules: expectedRulesMap{
				ruler1: map[string]rulespb.RuleGroupList{
					user1: {user1Group1, user1Group2},
				},
				ruler2: map[string]rulespb.RuleGroupList{}, // ruler2 owns token for user1group2, but user1 will only be handled by ruler1 because ruler2 is not running yet.
				ruler3: map[string]rulespb.RuleGroupList{
					user2: {user2Group1},
					user3: {user3Group1},
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
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
				t.Cleanup(rulerRing.StopAsync)
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
			loadedRules1, err := r1.listRules(context.Background(), rulerSyncReasonPeriodic)
			require.NoError(t, err)

			expected := expectedRulesMap{
				ruler1: loadedRules1,
			}

			addToExpected := func(id string, r *Ruler) {
				// Only expect rules from other rulers when using ring, and they are present in the ring.
				if r != nil && rulerRing != nil && rulerRing.HasInstance(id) {
					loaded, err := r.listRules(context.Background(), rulerSyncReasonPeriodic)
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

func TestDeleteTenantRuleGroups(t *testing.T) {
	ruleGroups := []ruleGroupKey{
		{user: "userA", namespace: "namespace", group: "group"},
		{user: "userB", namespace: "namespace1", group: "group"},
		{user: "userB", namespace: "namespace2", group: "group"},
	}

	obj := objstore.NewInMemBucket()
	rs := bucketclient.NewBucketRuleStore(obj, nil, log.NewNopLogger())

	// "upload" rule groups
	for _, key := range ruleGroups {
		desc := rulespb.ToProto(key.user, key.namespace, rulefmt.RuleGroup{Name: key.group})
		require.NoError(t, rs.SetRuleGroup(context.Background(), key.user, key.namespace, desc))
	}

	require.Len(t, obj.Objects(), 3)

	cfg := defaultRulerConfig(t)
	api, err := NewRuler(cfg, nil, nil, log.NewNopLogger(), rs, nil)
	require.NoError(t, err)

	{
		req := &http.Request{}
		resp := httptest.NewRecorder()
		api.DeleteTenantConfiguration(resp, req)

		require.Equal(t, http.StatusUnauthorized, resp.Code)
	}

	{
		callDeleteTenantAPI(t, api, "user-with-no-rule-groups")
		require.Len(t, obj.Objects(), 3)

		verifyExpectedDeletedRuleGroupsForUser(t, api, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userA", false)
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userB", false)
	}

	{
		callDeleteTenantAPI(t, api, "userA")
		require.Len(t, obj.Objects(), 2)

		verifyExpectedDeletedRuleGroupsForUser(t, api, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userA", true)                    // Just deleted.
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userB", false)
	}

	// Deleting same user again works fine and reports no problems.
	{
		callDeleteTenantAPI(t, api, "userA")
		require.Len(t, obj.Objects(), 2)

		verifyExpectedDeletedRuleGroupsForUser(t, api, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userA", true)                    // Already deleted before.
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userB", false)
	}

	{
		callDeleteTenantAPI(t, api, "userB")
		require.Empty(t, obj.Objects())

		verifyExpectedDeletedRuleGroupsForUser(t, api, "user-with-no-rule-groups", true) // Has no rule groups
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userA", true)                    // Deleted previously
		verifyExpectedDeletedRuleGroupsForUser(t, api, "userB", true)                    // Just deleted
	}
}

func generateTokenForGroups(groups []*rulespb.RuleGroupDesc, offset uint32) []uint32 {
	var tokens []uint32

	for _, g := range groups {
		tokens = append(tokens, tokenForGroup(g)+offset)
	}

	return tokens
}

func callDeleteTenantAPI(t *testing.T, api *Ruler, userID string) {
	ctx := user.InjectOrgID(context.Background(), userID)

	req := &http.Request{}
	resp := httptest.NewRecorder()
	api.DeleteTenantConfiguration(resp, req.WithContext(ctx))

	require.Equal(t, http.StatusOK, resp.Code)
}

func verifyExpectedDeletedRuleGroupsForUser(t *testing.T, r *Ruler, userID string, expectedDeleted bool) {
	list, err := r.store.ListRuleGroupsForUserAndNamespace(context.Background(), userID, "")
	require.NoError(t, err)

	if expectedDeleted {
		require.Empty(t, list)
	} else {
		require.NotEqual(t, 0, len(list))
	}
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
		tc := tc
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			senderFunc := senderFunc(func(alerts ...*notifier.Alert) {
				if len(tc.in) == 0 {
					t.Fatalf("sender called with 0 alert")
				}
				require.Equal(t, tc.exp, alerts)
			})
			rules.SendAlerts(senderFunc, "http://localhost:9090")(context.TODO(), "up", tc.in...)
		})
	}
}

func TestFilterRuleGroupsByEnabled(t *testing.T) {
	tests := map[string]struct {
		configs  map[string]rulespb.RuleGroupList
		limits   RulesLimits
		expected map[string]rulespb.RuleGroupList
	}{
		"should return an empty map on empty input": {
			configs:  nil,
			limits:   validation.MockDefaultOverrides(),
			expected: nil,
		},
		"should remove alerting rules if disabled for a given tenant": {
			configs: map[string]rulespb.RuleGroupList{
				"user-1": {
					mockRuleGroup("group-1", "user-1", mockRecordingRuleDesc("record:1", "1"), mockAlertingRuleDesc("alert-2", "2"), mockRecordingRuleDesc("record:3", "3")),
					mockRuleGroup("group-2", "user-1", mockRecordingRuleDesc("record:4", "4"), mockRecordingRuleDesc("record:5", "5")),
					mockRuleGroup("group-3", "user-1", mockAlertingRuleDesc("alert-6", "6"), mockAlertingRuleDesc("alert-7", "7")),
				},
				"user-2": {
					mockRuleGroup("group-1", "user-2", mockRecordingRuleDesc("record:1", "1"), mockAlertingRuleDesc("alert-2", "2"), mockRecordingRuleDesc("record:3", "3")),
					mockRuleGroup("group-2", "user-2", mockRecordingRuleDesc("record:4", "4"), mockRecordingRuleDesc("record:5", "5")),
					mockRuleGroup("group-3", "user-2", mockAlertingRuleDesc("alert-6", "6"), mockAlertingRuleDesc("alert-7", "7")),
				},
			},
			limits: validation.MockOverrides(func(defaults *validation.Limits, tenantLimits map[string]*validation.Limits) {
				tenantLimits["user-1"] = validation.MockDefaultLimits()
				tenantLimits["user-1"].RulerRecordingRulesEvaluationEnabled = true
				tenantLimits["user-1"].RulerAlertingRulesEvaluationEnabled = false
			}),
			expected: map[string]rulespb.RuleGroupList{
				"user-1": {
					mockRuleGroup("group-1", "user-1", mockRecordingRuleDesc("record:1", "1"), mockRecordingRuleDesc("record:3", "3")),
					mockRuleGroup("group-2", "user-1", mockRecordingRuleDesc("record:4", "4"), mockRecordingRuleDesc("record:5", "5")),
				},
				"user-2": {
					mockRuleGroup("group-1", "user-2", mockRecordingRuleDesc("record:1", "1"), mockAlertingRuleDesc("alert-2", "2"), mockRecordingRuleDesc("record:3", "3")),
					mockRuleGroup("group-2", "user-2", mockRecordingRuleDesc("record:4", "4"), mockRecordingRuleDesc("record:5", "5")),
					mockRuleGroup("group-3", "user-2", mockAlertingRuleDesc("alert-6", "6"), mockAlertingRuleDesc("alert-7", "7")),
				},
			},
		},
		"should remove recording rules if disabled for a given tenant": {
			configs: map[string]rulespb.RuleGroupList{
				"user-1": {
					mockRuleGroup("group-1", "user-1", mockRecordingRuleDesc("record:1", "1"), mockAlertingRuleDesc("alert-2", "2"), mockRecordingRuleDesc("record:3", "3")),
					mockRuleGroup("group-2", "user-1", mockRecordingRuleDesc("record:4", "4"), mockRecordingRuleDesc("record:5", "5")),
					mockRuleGroup("group-3", "user-1", mockAlertingRuleDesc("alert-6", "6"), mockAlertingRuleDesc("alert-7", "7")),
				},
				"user-2": {
					mockRuleGroup("group-1", "user-2", mockRecordingRuleDesc("record:1", "1"), mockAlertingRuleDesc("alert-2", "2"), mockRecordingRuleDesc("record:3", "3")),
					mockRuleGroup("group-2", "user-2", mockRecordingRuleDesc("record:4", "4"), mockRecordingRuleDesc("record:5", "5")),
					mockRuleGroup("group-3", "user-2", mockAlertingRuleDesc("alert-6", "6"), mockAlertingRuleDesc("alert-7", "7")),
				},
			},
			limits: validation.MockOverrides(func(defaults *validation.Limits, tenantLimits map[string]*validation.Limits) {
				tenantLimits["user-1"] = validation.MockDefaultLimits()
				tenantLimits["user-1"].RulerRecordingRulesEvaluationEnabled = false
				tenantLimits["user-1"].RulerAlertingRulesEvaluationEnabled = true
			}),
			expected: map[string]rulespb.RuleGroupList{
				"user-1": {
					mockRuleGroup("group-1", "user-1", mockAlertingRuleDesc("alert-2", "2")),
					mockRuleGroup("group-3", "user-1", mockAlertingRuleDesc("alert-6", "6"), mockAlertingRuleDesc("alert-7", "7")),
				},
				"user-2": {
					mockRuleGroup("group-1", "user-2", mockRecordingRuleDesc("record:1", "1"), mockAlertingRuleDesc("alert-2", "2"), mockRecordingRuleDesc("record:3", "3")),
					mockRuleGroup("group-2", "user-2", mockRecordingRuleDesc("record:4", "4"), mockRecordingRuleDesc("record:5", "5")),
					mockRuleGroup("group-3", "user-2", mockAlertingRuleDesc("alert-6", "6"), mockAlertingRuleDesc("alert-7", "7")),
				},
			},
		},
		"should remove all config for a user if both recording and alerting rules are disabled": {
			configs: map[string]rulespb.RuleGroupList{
				"user-1": {
					mockRuleGroup("group-1", "user-1", mockRecordingRuleDesc("record:1", "1"), mockAlertingRuleDesc("alert-2", "2"), mockRecordingRuleDesc("record:3", "3")),
					mockRuleGroup("group-2", "user-1", mockRecordingRuleDesc("record:4", "4"), mockRecordingRuleDesc("record:5", "5")),
					mockRuleGroup("group-3", "user-1", mockAlertingRuleDesc("alert-6", "6"), mockAlertingRuleDesc("alert-7", "7")),
				},
				"user-2": {
					mockRuleGroup("group-1", "user-2", mockRecordingRuleDesc("record:1", "1"), mockAlertingRuleDesc("alert-2", "2"), mockRecordingRuleDesc("record:3", "3")),
					mockRuleGroup("group-2", "user-2", mockRecordingRuleDesc("record:4", "4"), mockRecordingRuleDesc("record:5", "5")),
					mockRuleGroup("group-3", "user-2", mockAlertingRuleDesc("alert-6", "6"), mockAlertingRuleDesc("alert-7", "7")),
				},
			},
			limits: validation.MockOverrides(func(defaults *validation.Limits, tenantLimits map[string]*validation.Limits) {
				tenantLimits["user-1"] = validation.MockDefaultLimits()
				tenantLimits["user-1"].RulerRecordingRulesEvaluationEnabled = false
				tenantLimits["user-1"].RulerAlertingRulesEvaluationEnabled = false
			}),
			expected: map[string]rulespb.RuleGroupList{
				"user-2": {
					mockRuleGroup("group-1", "user-2", mockRecordingRuleDesc("record:1", "1"), mockAlertingRuleDesc("alert-2", "2"), mockRecordingRuleDesc("record:3", "3")),
					mockRuleGroup("group-2", "user-2", mockRecordingRuleDesc("record:4", "4"), mockRecordingRuleDesc("record:5", "5")),
					mockRuleGroup("group-3", "user-2", mockAlertingRuleDesc("alert-6", "6"), mockAlertingRuleDesc("alert-7", "7")),
				},
			},
		},
		"should remove configs for all users if both recording and alerting rules are disabled for every user": {
			configs: map[string]rulespb.RuleGroupList{
				"user-1": {
					mockRuleGroup("group-1", "user-1", mockRecordingRuleDesc("record:1", "1"), mockAlertingRuleDesc("alert-2", "2"), mockRecordingRuleDesc("record:3", "3")),
					mockRuleGroup("group-2", "user-1", mockRecordingRuleDesc("record:4", "4"), mockRecordingRuleDesc("record:5", "5")),
					mockRuleGroup("group-3", "user-1", mockAlertingRuleDesc("alert-6", "6"), mockAlertingRuleDesc("alert-7", "7")),
				},
				"user-2": {
					mockRuleGroup("group-1", "user-2", mockRecordingRuleDesc("record:1", "1"), mockAlertingRuleDesc("alert-2", "2"), mockRecordingRuleDesc("record:3", "3")),
					mockRuleGroup("group-2", "user-2", mockRecordingRuleDesc("record:4", "4"), mockRecordingRuleDesc("record:5", "5")),
					mockRuleGroup("group-3", "user-2", mockAlertingRuleDesc("alert-6", "6"), mockAlertingRuleDesc("alert-7", "7")),
				},
			},
			limits: validation.MockOverrides(func(defaults *validation.Limits, tenantLimits map[string]*validation.Limits) {
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
					group.Rules = append(group.Rules, mockRecordingRuleDesc(fmt.Sprintf("record:%d", r), "count(up)"))
				}

				for r := 0; r < numAlertingRulesPerRuleGroup; r++ {
					group.Rules = append(group.Rules, mockAlertingRuleDesc(fmt.Sprintf("alert-%d", r), "count(up)"))
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
			limits: validation.MockOverrides(func(defaults *validation.Limits, tenantLimits map[string]*validation.Limits) {
				defaults.RulerRecordingRulesEvaluationEnabled = false
			}),
		},
		"alerting rules disabled": {
			limits: validation.MockOverrides(func(defaults *validation.Limits, tenantLimits map[string]*validation.Limits) {
				defaults.RulerAlertingRulesEvaluationEnabled = false
			}),
		},
		"all rules disabled": {
			limits: validation.MockOverrides(func(defaults *validation.Limits, tenantLimits map[string]*validation.Limits) {
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

func mockRecordingRuleDesc(record, expr string) *rulespb.RuleDesc {
	return &rulespb.RuleDesc{
		Record: record,
		Expr:   expr,
	}
}

func mockAlertingRuleDesc(alert, expr string) *rulespb.RuleDesc {
	return &rulespb.RuleDesc{
		Alert: alert,
		Expr:  expr,
	}
}

// mockRuleGroup creates a rule group filling in all fields, so that we can use it in tests to check if all fields
// are copied when a rule group is cloned.
func mockRuleGroup(name, user string, rules ...*rulespb.RuleDesc) *rulespb.RuleGroupDesc {
	return &rulespb.RuleGroupDesc{
		Name:          name,
		Namespace:     "test",
		Interval:      time.Minute,
		Rules:         rules,
		User:          user,
		SourceTenants: []string{user},
	}
}
