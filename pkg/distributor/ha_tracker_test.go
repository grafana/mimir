// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/ha_tracker_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	utiltest "github.com/grafana/mimir/pkg/util/test"
)

type dnsProviderMock struct {
	resolved []string
}

func (p *dnsProviderMock) Resolve(_ context.Context, addrs []string) error {
	p.resolved = addrs
	return nil
}

func (p dnsProviderMock) Addresses() []string {
	return p.resolved
}

func checkReplicaTimestamp(t *testing.T, duration time.Duration, c *defaultHaTracker, user, cluster, replica string, expected time.Time, elected time.Time) {
	t.Helper()

	// Round the expected timestamp with milliseconds precision
	// to match "received at" precision
	expected = expected.Truncate(time.Millisecond)
	// change elected time to UTC
	elected = elected.UTC().Truncate(time.Millisecond)

	test.Poll(t, duration, nil, func() interface{} {
		var r ReplicaDesc
		c.electedLock.RLock()
		info := c.clusters[user][cluster]
		if info != nil {
			r = info.elected
		}
		c.electedLock.RUnlock()
		if info == nil {
			return fmt.Errorf("no data for user %s cluster %s", user, cluster)
		}
		if r.GetReplica() != replica {
			return fmt.Errorf("replicas did not match: %s != %s", r.GetReplica(), replica)
		}
		if r.GetDeletedAt() > 0 {
			return fmt.Errorf("replica is marked for deletion")
		}
		if !timestamp.Time(r.GetReceivedAt()).Equal(expected) {
			return fmt.Errorf("timestamps did not match: %+v != %+v", timestamp.Time(r.GetReceivedAt()), expected)
		}
		if timestamp.Time(r.GetElectedAt()).Sub(elected) > time.Second {
			return fmt.Errorf("elected timestamps did not match: %+v != %+v", timestamp.Time(r.GetElectedAt()), elected)
		}

		return nil
	})
}

func waitForHaTrackerCacheEntryRemoval(t require.TestingT, tracker *defaultHaTracker, user string, cluster string, duration time.Duration, tick time.Duration) bool {
	condition := assert.Eventually(t, func() bool {
		tracker.electedLock.RLock()
		defer tracker.electedLock.RUnlock()

		info := tracker.clusters[user][cluster]
		return info == nil
	}, duration, tick)
	return condition
}

func merge(r1, r2 *ReplicaDesc) (*ReplicaDesc, *ReplicaDesc) {
	change, err := r1.Merge(r2, false)
	if err != nil {
		panic(err)
	}

	if change == nil {
		return r1, nil
	}

	changeRDesc := change.(*ReplicaDesc)
	return r1, changeRDesc
}
func TestReplicaDescMerge(t *testing.T) {
	now := time.Now().Unix()

	const (
		replica1 = "r1"
		replica2 = "r2"
		replica3 = "r3"
	)

	firstReplica := func() *ReplicaDesc {
		return &ReplicaDesc{
			Replica:        replica1,
			ReceivedAt:     now,
			DeletedAt:      0,
			ElectedAt:      now,
			ElectedChanges: 1,
		}
	}

	firstReplicaWithHigherReceivedAt := func() *ReplicaDesc {
		return &ReplicaDesc{
			Replica:        replica1,
			ReceivedAt:     now + 5,
			DeletedAt:      0,
			ElectedAt:      now,
			ElectedChanges: 1,
		}
	}

	secondReplica := func() *ReplicaDesc {
		return &ReplicaDesc{
			Replica:        replica2,
			ReceivedAt:     now,
			DeletedAt:      0,
			ElectedAt:      now + 5,
			ElectedChanges: 2,
		}
	}

	thirdReplica := func() *ReplicaDesc {
		return &ReplicaDesc{
			Replica:        replica3,
			ReceivedAt:     now,
			DeletedAt:      0,
			ElectedAt:      now + 10,
			ElectedChanges: 3,
		}
	}

	expectedFirstAndFirstHigherReceivedAtMerge := func() *ReplicaDesc {
		return &ReplicaDesc{
			Replica:        replica1,
			ReceivedAt:     now + 5,
			DeletedAt:      0,
			ElectedAt:      now,
			ElectedChanges: 1,
		}
	}

	expectedFirstAndSecondMerge := func() *ReplicaDesc {
		return &ReplicaDesc{
			Replica:        replica2,
			ReceivedAt:     now,
			DeletedAt:      0,
			ElectedAt:      now + 5,
			ElectedChanges: 2,
		}
	}

	testsMerge := []struct {
		name           string
		rDesc1         *ReplicaDesc
		rDesc2         *ReplicaDesc
		expectedRDesc  *ReplicaDesc
		expectedChange *ReplicaDesc
	}{
		{
			name:           "Merge ReplicaDesc: Same replica name, different receivedAt should return ReplicaDesc with most recent receivedAt timestamp",
			rDesc1:         firstReplica(),
			rDesc2:         firstReplicaWithHigherReceivedAt(),
			expectedRDesc:  expectedFirstAndFirstHigherReceivedAtMerge(),
			expectedChange: expectedFirstAndFirstHigherReceivedAtMerge(),
		},
		{
			name:           "Merge ReplicaDesc: Different replica name, different electedAt should return ReplicaDesc with most recent electedAt timestamp",
			rDesc1:         firstReplica(),
			rDesc2:         secondReplica(),
			expectedRDesc:  expectedFirstAndSecondMerge(),
			expectedChange: expectedFirstAndSecondMerge(),
		},
		{
			name: "idempotency: no change after applying same ReplicaDesc again.",
			rDesc1: func() *ReplicaDesc {
				out, _ := merge(firstReplica(), secondReplica())
				return out
			}(),
			rDesc2:         firstReplica(),
			expectedRDesc:  expectedFirstAndSecondMerge(),
			expectedChange: nil,
		},
		{
			name:   "commutativity: Merge(first, second) == Merge(second, first)",
			rDesc1: firstReplica(),
			rDesc2: secondReplica(),
			expectedRDesc: func() *ReplicaDesc {
				expected, _ := merge(secondReplica(), firstReplica())
				return expected
			}(),
			expectedChange: expectedFirstAndSecondMerge(),
		},
		{
			name: "associativity: Merge(Merge(first, second), third) == Merge(first, Merge(second, third))",
			rDesc1: func() *ReplicaDesc {
				ours1, _ := merge(firstReplica(), secondReplica())
				ours1, _ = merge(ours1, thirdReplica())
				return ours1
			}(),
			rDesc2: nil,
			expectedRDesc: func() *ReplicaDesc {
				ours2, _ := merge(secondReplica(), thirdReplica())
				ours2, _ = merge(firstReplica(), ours2)
				return ours2
			}(),
			expectedChange: nil,
		},
		{
			name:           "Merge should return no change when replica is the same",
			rDesc1:         firstReplica(),
			rDesc2:         firstReplica(),
			expectedRDesc:  firstReplica(),
			expectedChange: nil,
		},
	}

	for _, tt := range testsMerge {
		t.Run(tt.name, func(t *testing.T) {
			rDesc, ch := merge(tt.rDesc1, tt.rDesc2)
			assert.Equal(t, tt.expectedRDesc, rDesc)
			assert.Equal(t, tt.expectedChange, ch)
		})
	}
}

func createMemberlistKVConfig(c codec.Codec) *memberlist.KVConfig {
	var config memberlist.KVConfig
	flagext.DefaultValues(&config)
	config.Codecs = []codec.Codec{c}
	return &config
}

func createMemberlistKVStore(ctx context.Context, t *testing.T, cfg *memberlist.KVConfig, logger log.Logger) kv.Config {
	memberListSvc := memberlist.NewKVInitService(
		cfg,
		logger,
		&dnsProviderMock{},
		prometheus.NewPedanticRegistry(),
	)
	require.NoError(t, services.StartAndAwaitRunning(ctx, memberListSvc))
	t.Cleanup(func() {
		assert.NoError(t, services.StopAndAwaitTerminated(ctx, memberListSvc))
	})

	return kv.Config{Store: "memberlist", StoreConfig: kv.StoreConfig{
		MemberlistKV: memberListSvc.GetMemberlistKV,
	}}
}

func TestHaTrackerWithMemberList(t *testing.T) {
	const (
		cluster                  = "cluster"
		replica1                 = "r1"
		replica2                 = "r2"
		updateTimeout            = time.Millisecond * 100
		failoverTimeout          = 2 * time.Millisecond
		failoverTimeoutPlus100ms = failoverTimeout + 100*time.Millisecond
	)

	ctx := context.Background()
	codec := GetReplicaDescCodec()
	cfg := createMemberlistKVConfig(codec)
	kvStore := createMemberlistKVStore(ctx, t, cfg, log.NewNopLogger())

	tracker, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kvStore,
	}, trackerLimits{
		maxClusters:            100,
		updateTimeout:          updateTimeout,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        failoverTimeout,
	}, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, tracker))

	t.Cleanup(func() {
		assert.NoError(t, services.StopAndAwaitTerminated(ctx, tracker))
	})

	now := time.Now()

	// Write the first time.
	err = tracker.checkReplica(ctx, "user", cluster, replica1, now, now)
	assert.NoError(t, err)

	// Throw away a sample from replica2.
	err = tracker.checkReplica(ctx, "user", cluster, replica2, now, now)
	assert.Error(t, err)

	// Wait more than the overwrite timeout.
	now = now.Add(failoverTimeoutPlus100ms)

	// Another sample from replica2 to update its timestamp.
	err = tracker.checkReplica(ctx, "user", cluster, replica2, now, now)
	assert.Error(t, err)

	// Update KVStore - this should elect replica 2.
	tracker.updateKVStoreAll(ctx, now)

	checkReplicaTimestamp(t, 100*time.Millisecond, tracker, "user", cluster, replica2, now, now)

	// Now we should accept from replica 2.
	err = tracker.checkReplica(ctx, "user", cluster, replica2, now, now)
	assert.NoError(t, err)

	// We timed out accepting samples from replica 1 and should now reject them.
	err = tracker.checkReplica(ctx, "user", cluster, replica1, now, now)
	assert.Error(t, err)
}

func TestHaTrackerWithMemberlistWhenReplicaDescIsMarkedDeletedThenKVStoreUpdateIsNotFailing(t *testing.T) {
	const (
		cluster                  = "cluster"
		tenant                   = "tenant"
		replica1                 = "r1"
		replica2                 = "r2"
		updateTimeout            = time.Millisecond * 100
		failoverTimeout          = 2 * time.Millisecond
		failoverTimeoutPlus100ms = failoverTimeout + 100*time.Millisecond
	)

	ctx := context.Background()
	logger := utiltest.NewTestingLogger(t)
	codec := GetReplicaDescCodec()
	cfg := createMemberlistKVConfig(codec)

	// give some room to WatchPrefix to register
	cfg.NotifyInterval = 250 * time.Millisecond

	kvStore := createMemberlistKVStore(ctx, t, cfg, logger)

	tracker, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kvStore,
	}, trackerLimits{
		maxClusters:            100,
		updateTimeout:          updateTimeout,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        failoverTimeout,
	}, nil, logger)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, tracker))

	t.Cleanup(func() {
		assert.NoError(t, services.StopAndAwaitTerminated(ctx, tracker))
	})

	now := time.Now()

	// Write the first time.
	err = tracker.checkReplica(ctx, tenant, cluster, replica1, now, now)
	assert.NoError(t, err)

	key := fmt.Sprintf("%s/%s", tenant, cluster)

	// Mark the ReplicaDesc as deleted in the KVStore, which will also remove it from the tracker cache.
	err = tracker.client.CAS(ctx, key, func(in interface{}) (out interface{}, retry bool, err error) {
		d, ok := in.(*ReplicaDesc)
		if !ok || d == nil {
			return nil, false, nil
		}
		d.DeletedAt = timestamp.FromTime(time.Now())
		return d, true, nil
	})
	require.NoError(t, err)

	condition := waitForHaTrackerCacheEntryRemoval(t, tracker, tenant, cluster, 10*time.Second, 50*time.Millisecond)
	require.True(t, condition)

	now = now.Add(failoverTimeoutPlus100ms)
	// check replica2
	err = tracker.checkReplica(ctx, tenant, cluster, replica2, now, now)
	assert.NoError(t, err)

	// check replica1
	assert.ErrorAs(t, tracker.checkReplica(ctx, tenant, cluster, replica1, now, now), &replicasDidNotMatchError{})
}

func TestHATrackerCacheSyncOnStart(t *testing.T) {
	const cluster = "c1"
	const replicaOne = "r1"
	const replicaTwo = "r2"

	var c *defaultHaTracker
	var err error
	var now time.Time

	codec := GetReplicaDescCodec()
	kvStore, closer := consul.NewInMemoryClient(codec, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	mockCountingClient := kv.NewMockCountingClient(kvStore)
	c, err = newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kv.Config{Mock: mockCountingClient},
	}, trackerLimits{
		maxClusters:            100,
		updateTimeout:          time.Millisecond * 100,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        time.Millisecond * 2,
	}, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	// KV Store empty: The sync should try fetching the Keys only
	// client.List: 1
	// client.Get: 0
	assert.Equal(t, 1, int(mockCountingClient.ListCalls.Load()))
	assert.Equal(t, 0, int(mockCountingClient.GetCalls.Load()))

	now = time.Now()
	err = c.checkReplica(context.Background(), "user", cluster, replicaOne, now, now)
	assert.NoError(t, err)

	err = services.StopAndAwaitTerminated(context.Background(), c)
	assert.NoError(t, err)

	// Initializing a New Client to set calls to zero
	mockCountingClient = kv.NewMockCountingClient(kvStore)
	c, err = newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kv.Config{Mock: mockCountingClient},
	}, trackerLimits{
		maxClusters:            100,
		updateTimeout:          time.Millisecond * 100,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        time.Millisecond * 2,
	}, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(context.Background(), c)) })

	// KV Store has one entry: The sync should try fetching the Keys and updating the cache
	// client.List: 1
	// client.Get: 1
	assert.Equal(t, 1, int(mockCountingClient.ListCalls.Load()))
	assert.Equal(t, 1, int(mockCountingClient.GetCalls.Load()))

	now = time.Now()
	err = c.checkReplica(context.Background(), "user", cluster, replicaTwo, now, now)
	assert.Error(t, err)
	assert.ErrorIs(t, err, replicasDidNotMatchError{replica: "r2", elected: "r1"})
}

// Test that values are set in the HATracker after WatchPrefix has found it in the KVStore.
func TestHATrackerWatchPrefixAssignment(t *testing.T) {
	cluster := "c1"
	replica := "r1"

	codec := GetReplicaDescCodec()
	kvStore, closer := consul.NewInMemoryClient(codec, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	mock := kv.PrefixClient(kvStore, "prefix")
	c, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kv.Config{Mock: mock},
	}, trackerLimits{
		maxClusters:            100,
		updateTimeout:          time.Millisecond,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        time.Millisecond * 2,
	}, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

	// Write the first time.
	now := time.Now()

	err = c.checkReplica(context.Background(), "user", cluster, replica, now, now)
	assert.NoError(t, err)

	// Check to see if the value in the trackers cache is correct.
	checkReplicaTimestamp(t, 2*time.Second, c, "user", cluster, replica, now, now)
}

func TestHATrackerCheckReplicaOverwriteTimeout(t *testing.T) {
	replica1 := "replica1"
	replica2 := "replica2"

	c, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kv.Config{Store: "inmemory"},
	}, trackerLimits{
		maxClusters:            100,
		updateTimeout:          100 * time.Millisecond,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        time.Second,
	}, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

	now := time.Now()

	// Write the first time.
	err = c.checkReplica(context.Background(), "user", "test", replica1, now, now)
	assert.NoError(t, err)

	// Throw away a sample from replica2.
	err = c.checkReplica(context.Background(), "user", "test", replica2, now, now)
	assert.Error(t, err)

	// Wait more than the overwrite timeout.
	now = now.Add(1100 * time.Millisecond)

	// Another sample from replica2 to update its timestamp.
	err = c.checkReplica(context.Background(), "user", "test", replica2, now, now)
	assert.Error(t, err)

	// Update KVStore - this should elect replica 2.
	c.updateKVStoreAll(context.Background(), now)

	// Validate Replica
	checkReplicaTimestamp(t, 100*time.Millisecond, c, "user", "test", replica2, now, now)

	// Now we should accept from replica 2.
	err = c.checkReplica(context.Background(), "user", "test", replica2, now, now)
	assert.NoError(t, err)

	// We timed out accepting samples from replica 1 and should now reject them.
	err = c.checkReplica(context.Background(), "user", "test", replica1, now, now)
	assert.Error(t, err)
}

func TestHATrackerCheckReplicaMultiCluster(t *testing.T) {
	replica1 := "replica1"
	replica2 := "replica2"

	reg := prometheus.NewPedanticRegistry()
	c, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kv.Config{Store: "inmemory"},
	}, trackerLimits{
		maxClusters:            100,
		updateTimeout:          100 * time.Millisecond,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        time.Second,
	}, reg, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

	now := time.Now()

	// Write the first time.
	err = c.checkReplica(context.Background(), "user", "c1", replica1, now, now)
	assert.NoError(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica1, now, now)
	assert.NoError(t, err)

	// Reject samples from replica 2 in each cluster.
	err = c.checkReplica(context.Background(), "user", "c1", replica2, now, now)
	assert.Error(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica2, now, now)
	assert.Error(t, err)

	// We should still accept from replica 1.
	err = c.checkReplica(context.Background(), "user", "c1", replica1, now, now)
	assert.NoError(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica1, now, now)
	assert.NoError(t, err)

	// We expect no CAS operation failures.
	metrics, err := reg.Gather()
	require.NoError(t, err)

	assert.Equal(t, uint64(0), getSumOfHistogramSampleCount(metrics, "cortex_kv_request_duration_seconds", labels.Selector{
		labels.MustNewMatcher(labels.MatchEqual, "operation", "CAS"),
		labels.MustNewMatcher(labels.MatchRegexp, "status_code", "5.*"),
	}))
	assert.Greater(t, getSumOfHistogramSampleCount(metrics, "cortex_kv_request_duration_seconds", labels.Selector{
		labels.MustNewMatcher(labels.MatchEqual, "operation", "CAS"),
		labels.MustNewMatcher(labels.MatchRegexp, "status_code", "2.*"),
	}), uint64(0))
}

func TestHATrackerCheckReplicaMultiClusterTimeout(t *testing.T) {
	replica1 := "replica1"
	replica2 := "replica2"

	reg := prometheus.NewPedanticRegistry()
	c, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kv.Config{Store: "inmemory"},
	}, trackerLimits{
		maxClusters:            100,
		updateTimeout:          100 * time.Millisecond,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        time.Second,
	}, reg, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

	now := time.Now()

	// Write the first time.
	err = c.checkReplica(context.Background(), "user", "c1", replica1, now, now)
	assert.NoError(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica1, now, now)
	assert.NoError(t, err)

	// Reject samples from replica 2 in each cluster.
	err = c.checkReplica(context.Background(), "user", "c1", replica2, now, now)
	assert.Error(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica2, now, now)
	assert.Error(t, err)

	// Accept a sample for replica1 in C2.
	now = now.Add(500 * time.Millisecond)
	err = c.checkReplica(context.Background(), "user", "c2", replica1, now, now)
	assert.NoError(t, err)

	// Reject samples from replica 2 in each cluster.
	err = c.checkReplica(context.Background(), "user", "c1", replica2, now, now)
	assert.Error(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica2, now, now)
	assert.Error(t, err)

	// Wait more than the failover timeout.
	now = now.Add(1100 * time.Millisecond)

	// Another sample from c1/replica2 to update its timestamp.
	err = c.checkReplica(context.Background(), "user", "c1", replica2, now, now)
	assert.Error(t, err)
	c.updateKVStoreAll(context.Background(), now)

	checkReplicaTimestamp(t, 100*time.Millisecond, c, "user", "c1", replica2, now, now)

	// Accept a sample from c1/replica2.
	err = c.checkReplica(context.Background(), "user", "c1", replica2, now, now)
	assert.NoError(t, err)

	// We should still accept from c2/replica1 but reject from c1/replica1.
	err = c.checkReplica(context.Background(), "user", "c1", replica1, now, now)
	assert.Error(t, err)
	err = c.checkReplica(context.Background(), "user", "c2", replica1, now, now)
	assert.NoError(t, err)

	// We expect no CAS operation failures.
	metrics, err := reg.Gather()
	require.NoError(t, err)

	assert.Equal(t, uint64(0), getSumOfHistogramSampleCount(metrics, "cortex_kv_request_duration_seconds", labels.Selector{
		labels.MustNewMatcher(labels.MatchEqual, "operation", "CAS"),
		labels.MustNewMatcher(labels.MatchRegexp, "status_code", "5.*"),
	}))
	assert.Greater(t, getSumOfHistogramSampleCount(metrics, "cortex_kv_request_duration_seconds", labels.Selector{
		labels.MustNewMatcher(labels.MatchEqual, "operation", "CAS"),
		labels.MustNewMatcher(labels.MatchRegexp, "status_code", "2.*"),
	}), uint64(0))
}

// Test that writes only happen every update timeout.
func TestHATrackerCheckReplicaUpdateTimeout(t *testing.T) {
	replica := "r1"
	cluster := "c1"
	user := "user"

	codec := GetReplicaDescCodec()
	kvStore, closer := consul.NewInMemoryClient(codec, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	mock := kv.PrefixClient(kvStore, "prefix")
	c, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kv.Config{Mock: mock},
	}, trackerLimits{
		maxClusters:            100,
		updateTimeout:          time.Second,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        time.Second,
	}, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

	// Write the first time.
	startTime := time.Now()
	err = c.checkReplica(context.Background(), user, cluster, replica, startTime, startTime)
	assert.NoError(t, err)

	checkReplicaTimestamp(t, 2*time.Second, c, user, cluster, replica, startTime, startTime)

	// Timestamp should not update here, since time has not advanced.
	err = c.checkReplica(context.Background(), user, cluster, replica, startTime, startTime)
	assert.NoError(t, err)

	checkReplicaTimestamp(t, 2*time.Second, c, user, cluster, replica, startTime, startTime)

	// Wait 500ms and the timestamp should still not update.
	updateTime := time.Unix(0, startTime.UnixNano()).Add(500 * time.Millisecond)
	c.updateKVStoreAll(context.Background(), updateTime)

	err = c.checkReplica(context.Background(), user, cluster, replica, updateTime, updateTime)
	assert.NoError(t, err)
	checkReplicaTimestamp(t, 2*time.Second, c, user, cluster, replica, startTime, startTime)

	receivedAt := updateTime

	// Now we've waited > 1s, so the timestamp should update.
	updateTime = time.Unix(0, startTime.UnixNano()).Add(1100 * time.Millisecond)
	c.updateKVStoreAll(context.Background(), updateTime)

	// Timestamp stored in KV should be time when we have received a request (called "checkReplica"), not current time (updateTime).
	checkReplicaTimestamp(t, 2*time.Second, c, user, cluster, replica, receivedAt, receivedAt)

	err = c.checkReplica(context.Background(), user, cluster, replica, updateTime, updateTime)
	assert.NoError(t, err)
}

func TestHATrackerCheckReplicaShouldFixZeroElectedAtTimestamp(t *testing.T) {
	const (
		replica = "r1"
		cluster = "c1"
		userID  = "user"
	)

	var (
		ctx   = context.Background()
		codec = GetReplicaDescCodec()
	)

	kvStore, closer := consul.NewInMemoryClient(codec, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	mock := kv.PrefixClient(kvStore, "prefix")
	c, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kv.Config{Mock: mock},
	}, trackerLimits{
		maxClusters:            100,
		updateTimeout:          time.Second,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        time.Second,
	}, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, c))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, c))
	})

	// Create an entry with zero ElectedAt timestamp.
	initialReceivedAt := time.Now()
	require.NoError(t, c.client.CAS(ctx, fmt.Sprintf("%s/%s", userID, cluster), func(in interface{}) (out interface{}, retry bool, err error) {
		return &ReplicaDesc{
			Replica:        replica,
			ReceivedAt:     timestamp.FromTime(initialReceivedAt),
			DeletedAt:      0,
			ElectedAt:      0,
			ElectedChanges: 0,
		}, true, nil
	}))

	// We expect a zero value for the ElectedAt timestamp.
	checkReplicaTimestamp(t, 2*time.Second, c, userID, cluster, replica, initialReceivedAt, time.Unix(0, 0))

	// Advance time and replica timestamp. This should fix the ElectedAt timestamp too.
	updatedReceivedAt := initialReceivedAt.Add(2 * time.Second)
	require.NoError(t, c.checkReplica(context.Background(), userID, cluster, replica, updatedReceivedAt, updatedReceivedAt))
	checkReplicaTimestamp(t, 2*time.Second, c, userID, cluster, replica, updatedReceivedAt, updatedReceivedAt)
}

// Test that writes only happen every write timeout.
func TestHATrackerCheckReplicaMultiUser(t *testing.T) {
	replica := "r1"
	cluster := "c1"

	codec := GetReplicaDescCodec()
	kvStore, closer := consul.NewInMemoryClient(codec, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	mock := kv.PrefixClient(kvStore, "prefix")
	c, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kv.Config{Mock: mock},
	}, trackerLimits{
		maxClusters:            100,
		updateTimeout:          100 * time.Millisecond,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        time.Second,
	}, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

	now := time.Now()

	// Write the first time for user 1.
	err = c.checkReplica(context.Background(), "user1", cluster, replica, now, now)
	assert.NoError(t, err)
	checkReplicaTimestamp(t, 2*time.Second, c, "user1", cluster, replica, now, now)

	// Write the first time for user 2.
	err = c.checkReplica(context.Background(), "user2", cluster, replica, now, now)
	assert.NoError(t, err)
	checkReplicaTimestamp(t, 2*time.Second, c, "user2", cluster, replica, now, now)

	// Now we've waited > 1s, so the timestamp should update.
	updated := now.Add(1100 * time.Millisecond)
	err = c.checkReplica(context.Background(), "user1", cluster, replica, updated, updated)
	assert.NoError(t, err)
	c.updateKVStoreAll(context.Background(), updated)

	checkReplicaTimestamp(t, 2*time.Second, c, "user1", cluster, replica, updated, updated)
	// No update for user2.
	checkReplicaTimestamp(t, 2*time.Second, c, "user2", cluster, replica, now, now)
}

func TestHATrackerCheckReplicaUpdateTimeoutJitter(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		updateTimeout     time.Duration
		updateJitter      time.Duration
		startTime         time.Time
		updateTime        time.Time
		expectedTimestamp time.Time
	}{
		"should not refresh the replica if the update timeout is not expired yet (without jitter)": {
			updateTimeout:     10 * time.Second,
			updateJitter:      0,
			startTime:         time.Unix(5, 0),
			updateTime:        time.Unix(14, 0),
			expectedTimestamp: time.Unix(5, 0),
		},
		"should refresh the replica if the update timeout is expired (without jitter)": {
			updateTimeout:     10 * time.Second,
			updateJitter:      0,
			startTime:         time.Unix(5, 0),
			updateTime:        time.Unix(15, 0),
			expectedTimestamp: time.Unix(15, 0),
		},
		"should not refresh the replica if the update timeout is not expired yet (with jitter)": {
			updateTimeout:     10 * time.Second,
			updateJitter:      2 * time.Second,
			startTime:         time.Unix(5, 0),
			updateTime:        time.Unix(16, 0),
			expectedTimestamp: time.Unix(5, 0),
		},
		"should refresh the replica if the update timeout is expired (with jitter)": {
			updateTimeout:     10 * time.Second,
			updateJitter:      2 * time.Second,
			startTime:         time.Unix(5, 0),
			updateTime:        time.Unix(17, 0),
			expectedTimestamp: time.Unix(17, 0),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Init HA tracker
			codec := GetReplicaDescCodec()
			kvStore, closer := consul.NewInMemoryClient(codec, log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			mock := kv.PrefixClient(kvStore, "prefix")
			c, err := newHaTracker(HATrackerConfig{
				EnableHATracker: true,
				KVStore:         kv.Config{Mock: mock},
			}, trackerLimits{
				maxClusters:            100,
				updateTimeout:          testData.updateTimeout,
				updateTimeoutJitterMax: 0,
				failoverTimeout:        time.Second,
			}, nil, log.NewNopLogger())
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
			defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

			// Init context used by the test
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			// Override the jitter so that it's not based on a random value
			// we can't control in tests
			c.computeUpdateTimeoutJitter = func(_ time.Duration) time.Duration {
				return testData.updateJitter
			}

			// Init the replica in the KV Store
			err = c.checkReplica(ctx, "user1", "cluster", "replica-1", testData.startTime, testData.startTime)
			require.NoError(t, err)
			checkReplicaTimestamp(t, 2*time.Second, c, "user1", "cluster", "replica-1", testData.startTime, testData.startTime)

			// Refresh the replica in the KV Store
			err = c.checkReplica(ctx, "user1", "cluster", "replica-1", testData.updateTime, testData.updateTime)
			require.NoError(t, err)
			c.updateKVStoreAll(context.Background(), testData.updateTime)

			// Assert on the received timestamp
			checkReplicaTimestamp(t, 2*time.Second, c, "user1", "cluster", "replica-1", testData.expectedTimestamp, testData.expectedTimestamp)
		})
	}
}

func TestHATrackerFindHALabels(t *testing.T) {
	replicaLabel, clusterLabel := "replica", "cluster"
	cases := []struct {
		labelsIn []mimirpb.LabelAdapter
		expected haReplica
	}{
		{
			[]mimirpb.LabelAdapter{
				{Name: "__name__", Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "sample", Value: "1"},
				{Name: replicaLabel, Value: "1"},
			},
			haReplica{cluster: "", replica: "1"},
		},
		{
			[]mimirpb.LabelAdapter{
				{Name: "__name__", Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "sample", Value: "1"},
				{Name: clusterLabel, Value: "cluster-2"},
			},
			haReplica{cluster: "cluster-2", replica: ""},
		},
		{
			[]mimirpb.LabelAdapter{
				{Name: "__name__", Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "sample", Value: "1"},
				{Name: replicaLabel, Value: "3"},
				{Name: clusterLabel, Value: "cluster-3"},
			},
			haReplica{cluster: "cluster-3", replica: "3"},
		},
	}

	for _, c := range cases {
		r := findHALabels(replicaLabel, clusterLabel, c.labelsIn)
		assert.Equal(t, c.expected, r)
	}
}

func TestHATrackerConfig_ShouldCustomizePrefixDefaultValue(t *testing.T) {
	haConfig := HATrackerConfig{}
	ringConfig := ring.Config{}
	flagext.DefaultValues(&haConfig)
	flagext.DefaultValues(&ringConfig)

	assert.Equal(t, "ha-tracker/", haConfig.KVStore.Prefix)
	assert.NotEqual(t, haConfig.KVStore.Prefix, ringConfig.KVStore.Prefix)
}

func TestHATrackerClustersLimit(t *testing.T) {
	const userID = "user"

	codec := GetReplicaDescCodec()
	kvStore, closer := consul.NewInMemoryClient(codec, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	mock := kv.PrefixClient(kvStore, "prefix")
	limits := trackerLimits{
		maxClusters:            2,
		updateTimeout:          time.Second,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        time.Second,
	}

	t1, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kv.Config{Mock: mock},
	}, limits, nil, log.NewNopLogger())

	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), t1))
	defer services.StopAndAwaitTerminated(context.Background(), t1) //nolint:errcheck

	now := time.Now()

	assert.NoError(t, t1.checkReplica(context.Background(), userID, "a", "a1", now, now))
	waitForClustersUpdate(t, 1, t1, userID)

	assert.NoError(t, t1.checkReplica(context.Background(), userID, "b", "b1", now, now))
	waitForClustersUpdate(t, 2, t1, userID)

	expectedErr := newTooManyClustersError(2)
	assert.EqualError(t, t1.checkReplica(context.Background(), userID, "c", "c1", now, now), expectedErr.Error())

	// Move time forward, and make sure that checkReplica for existing cluster works fine.
	now = now.Add(5 * time.Second) // higher than "update timeout"

	// Another sample to update internal timestamp.
	err = t1.checkReplica(context.Background(), userID, "b", "b2", now, now)
	assert.Error(t, err)
	// Update KVStore.
	t1.updateKVStoreAll(context.Background(), now)
	checkReplicaTimestamp(t, 2*time.Second, t1, userID, "b", "b2", now, now)

	assert.NoError(t, t1.checkReplica(context.Background(), userID, "b", "b2", now, now))
	waitForClustersUpdate(t, 2, t1, userID)

	// Mark cluster "a" for deletion (it was last updated 5 seconds ago)
	// We use seconds timestamp resolution here, to avoid cleaning up 'b'. (In KV store, we only store seconds).
	t1.cleanupOldReplicas(context.Background(), time.Unix(now.Unix(), 0))
	waitForClustersUpdate(t, 1, t1, userID)

	// Now adding cluster "c" works.
	assert.NoError(t, t1.checkReplica(context.Background(), userID, "c", "c1", now, now))
	waitForClustersUpdate(t, 2, t1, userID)

	// But yet another cluster doesn't.
	expectedErr = newTooManyClustersError(2)
	assert.EqualError(t, t1.checkReplica(context.Background(), userID, "a", "a2", now, now), expectedErr.Error())

	now = now.Add(5 * time.Second)

	// clean all replicas
	t1.cleanupOldReplicas(context.Background(), now)
	waitForClustersUpdate(t, 0, t1, userID)

	// Now "a" works again.
	assert.NoError(t, t1.checkReplica(context.Background(), userID, "a", "a1", now, now))
	waitForClustersUpdate(t, 1, t1, userID)
}

func waitForClustersUpdate(t *testing.T, expected int, tr *defaultHaTracker, userID string) {
	t.Helper()
	test.Poll(t, 2*time.Second, expected, func() interface{} {
		tr.electedLock.RLock()
		defer tr.electedLock.RUnlock()

		return len(tr.clusters[userID])
	})
}

type trackerLimits struct {
	maxClusters            int
	updateTimeout          time.Duration
	updateTimeoutJitterMax time.Duration
	failoverTimeout        time.Duration
	failoverSampleTimeout  time.Duration

	forUser map[string]trackerLimits
}

func (l trackerLimits) MaxHAClusters(userID string) int {
	if ul, ok := l.forUser[userID]; ok {
		l = ul
	}
	return l.maxClusters
}

func (l trackerLimits) HATrackerTimeouts(userID string) (update, updateJitterMax, failover, failoverSample time.Duration) {
	if ul, ok := l.forUser[userID]; ok {
		l = ul
	}
	return l.updateTimeout, l.updateTimeoutJitterMax, l.failoverTimeout, l.failoverSampleTimeout
}

func (l trackerLimits) DefaultHATrackerUpdateTimeout() time.Duration {
	return l.updateTimeout
}

func TestHATracker_MetricsCleanup(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	logger := utiltest.NewTestingLogger(t)

	kvStore, closer := consul.NewInMemoryClient(GetReplicaDescCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	mock := kv.PrefixClient(kvStore, "prefix")
	tr, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kv.Config{Mock: mock},
	}, trackerLimits{
		maxClusters:            100,
		updateTimeout:          1 * time.Second,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        time.Second,
	}, reg, logger)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), tr))

	metrics := []string{
		"cortex_ha_tracker_elected_replica_changes_total",
		"cortex_ha_tracker_elected_replica_timestamp_seconds",
		"cortex_ha_tracker_kv_store_cas_total",
	}

	tr.electedReplicaChanges.WithLabelValues("userA", "cluster1").Add(5)
	tr.electedReplicaChanges.WithLabelValues("userA", "cluster2").Add(8)
	tr.electedReplicaChanges.WithLabelValues("userB", "cluster").Add(10)
	tr.electedReplicaTimestamp.WithLabelValues("userA", "cluster1").Add(5)
	tr.electedReplicaTimestamp.WithLabelValues("userA", "cluster2").Add(8)
	tr.electedReplicaTimestamp.WithLabelValues("userB", "cluster").Add(10)
	tr.kvCASCalls.WithLabelValues("userA", "cluster1").Add(5)
	tr.kvCASCalls.WithLabelValues("userA", "cluster2").Add(8)
	tr.kvCASCalls.WithLabelValues("userB", "cluster").Add(10)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ha_tracker_elected_replica_changes_total The total number of times the elected replica has changed for a user ID/cluster.
		# TYPE cortex_ha_tracker_elected_replica_changes_total counter
		cortex_ha_tracker_elected_replica_changes_total{cluster="cluster",user="userB"} 10
		cortex_ha_tracker_elected_replica_changes_total{cluster="cluster1",user="userA"} 5
		cortex_ha_tracker_elected_replica_changes_total{cluster="cluster2",user="userA"} 8

		# HELP cortex_ha_tracker_elected_replica_timestamp_seconds The timestamp stored for the currently elected replica, from the KVStore.
		# TYPE cortex_ha_tracker_elected_replica_timestamp_seconds gauge
		cortex_ha_tracker_elected_replica_timestamp_seconds{cluster="cluster",user="userB"} 10
		cortex_ha_tracker_elected_replica_timestamp_seconds{cluster="cluster1",user="userA"} 5
		cortex_ha_tracker_elected_replica_timestamp_seconds{cluster="cluster2",user="userA"} 8

		# HELP cortex_ha_tracker_kv_store_cas_total The total number of CAS calls to the KV store for a user ID/cluster.
		# TYPE cortex_ha_tracker_kv_store_cas_total counter
		cortex_ha_tracker_kv_store_cas_total{cluster="cluster",user="userB"} 10
		cortex_ha_tracker_kv_store_cas_total{cluster="cluster1",user="userA"} 5
		cortex_ha_tracker_kv_store_cas_total{cluster="cluster2",user="userA"} 8
	`), metrics...))

	tr.cleanupHATrackerMetricsForUser("userA")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ha_tracker_elected_replica_changes_total The total number of times the elected replica has changed for a user ID/cluster.
		# TYPE cortex_ha_tracker_elected_replica_changes_total counter
		cortex_ha_tracker_elected_replica_changes_total{cluster="cluster",user="userB"} 10

		# HELP cortex_ha_tracker_elected_replica_timestamp_seconds The timestamp stored for the currently elected replica, from the KVStore.
		# TYPE cortex_ha_tracker_elected_replica_timestamp_seconds gauge
		cortex_ha_tracker_elected_replica_timestamp_seconds{cluster="cluster",user="userB"} 10

		# HELP cortex_ha_tracker_kv_store_cas_total The total number of CAS calls to the KV store for a user ID/cluster.
		# TYPE cortex_ha_tracker_kv_store_cas_total counter
		cortex_ha_tracker_kv_store_cas_total{cluster="cluster",user="userB"} 10
	`), metrics...))
}

func TestHATracker_RecordElectedReplicaStatus(t *testing.T) {
	cluster := "c1"
	userID := "user"

	reg := prometheus.NewPedanticRegistry()
	logger := utiltest.NewTestingLogger(t)

	kvStore, closer := consul.NewInMemoryClient(GetReplicaDescCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	mock := kv.PrefixClient(kvStore, "prefix")
	c, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kv.Config{Mock: mock},
	}, trackerLimits{
		maxClusters:            100,
		updateTimeout:          1 * time.Second,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        time.Second,
	}, reg, logger)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

	c.updateCache(userID, cluster, &ReplicaDesc{
		Replica: "r1",
	})

	// There should be no metrics recorded, since it's not enabled.
	require.Error(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ha_tracker_elected_replica_status The current elected replica for a user ID/cluster.
		# TYPE cortex_ha_tracker_elected_replica_status counter
		cortex_ha_tracker_elected_replica_status{cluster="c1",replica="r1",user="user"} 1
	`), "cortex_ha_tracker_elected_replica_status"))

	// Enable the metric and change the replica
	c.cfg.EnableElectedReplicaMetric = true
	c.updateCache(userID, cluster, &ReplicaDesc{
		Replica: "r2",
	})
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ha_tracker_elected_replica_status The current elected replica for a user ID/cluster.
		# TYPE cortex_ha_tracker_elected_replica_status counter
		cortex_ha_tracker_elected_replica_status{cluster="c1",replica="r2",user="user"} 1
	`), "cortex_ha_tracker_elected_replica_status"))

	// Change the replica
	c.cfg.EnableElectedReplicaMetric = true
	c.updateCache(userID, cluster, &ReplicaDesc{
		Replica: "r3",
	})
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ha_tracker_elected_replica_status The current elected replica for a user ID/cluster.
		# TYPE cortex_ha_tracker_elected_replica_status counter
		cortex_ha_tracker_elected_replica_status{cluster="c1",replica="r3",user="user"} 1
	`), "cortex_ha_tracker_elected_replica_status"))

}

func TestHATrackerCheckReplicaCleanup(t *testing.T) {
	replica := "r1"
	cluster := "c1"
	userID := "user"
	ctx := user.InjectOrgID(context.Background(), userID)

	reg := prometheus.NewPedanticRegistry()
	logger := utiltest.NewTestingLogger(t)

	kvStore, closer := consul.NewInMemoryClient(GetReplicaDescCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	mock := kv.PrefixClient(kvStore, "prefix")
	c, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kv.Config{Mock: mock},
	}, trackerLimits{
		maxClusters:            100,
		updateTimeout:          1 * time.Second,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        time.Second,
	}, reg, logger)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))
	defer services.StopAndAwaitTerminated(context.Background(), c) //nolint:errcheck

	now := time.Now()

	err = c.checkReplica(context.Background(), userID, cluster, replica, now, now)
	assert.NoError(t, err)
	checkReplicaTimestamp(t, 2*time.Second, c, userID, cluster, replica, now, now)

	// Replica is not marked for deletion yet.
	checkReplicaDeletionState(t, time.Second, c, userID, cluster, true, true, false)
	checkUserClusters(t, time.Second, c, userID, 1)

	// This will mark replica for deletion (with time.Now())
	c.cleanupOldReplicas(ctx, now.Add(1*time.Second))

	// Verify marking for deletion.
	checkReplicaDeletionState(t, time.Second, c, userID, cluster, false, true, true)
	checkUserClusters(t, time.Second, c, userID, 0)

	// This will "revive" the replica.
	now = time.Now()
	err = c.checkReplica(context.Background(), userID, cluster, replica, now, now)
	assert.NoError(t, err)
	checkReplicaTimestamp(t, 2*time.Second, c, userID, cluster, replica, now, now) // This also checks that entry is not marked for deletion.
	checkUserClusters(t, time.Second, c, userID, 1)

	// This will mark replica for deletion again (with new time.Now())
	c.cleanupOldReplicas(ctx, now.Add(1*time.Second))
	checkReplicaDeletionState(t, time.Second, c, userID, cluster, false, true, true)
	checkUserClusters(t, time.Second, c, userID, 0)

	// Delete entry marked for deletion completely.
	c.cleanupOldReplicas(ctx, time.Now().Add(5*time.Second))
	checkReplicaDeletionState(t, time.Second, c, userID, cluster, false, false, false)
	checkUserClusters(t, time.Second, c, userID, 0)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ha_tracker_replicas_cleanup_marked_for_deletion_total Number of elected replicas marked for deletion.
		# TYPE cortex_ha_tracker_replicas_cleanup_marked_for_deletion_total counter
		cortex_ha_tracker_replicas_cleanup_marked_for_deletion_total 2

		# HELP cortex_ha_tracker_replicas_cleanup_deleted_total Number of elected replicas deleted from KV store.
		# TYPE cortex_ha_tracker_replicas_cleanup_deleted_total counter
		cortex_ha_tracker_replicas_cleanup_deleted_total 1

		# HELP cortex_ha_tracker_replicas_cleanup_delete_failed_total Number of elected replicas that failed to be marked for deletion, or deleted.
		# TYPE cortex_ha_tracker_replicas_cleanup_delete_failed_total counter
		cortex_ha_tracker_replicas_cleanup_delete_failed_total 0
	`), "cortex_ha_tracker_replicas_cleanup_marked_for_deletion_total",
		"cortex_ha_tracker_replicas_cleanup_deleted_total",
		"cortex_ha_tracker_replicas_cleanup_delete_failed_total",
	))
}

func checkUserClusters(t *testing.T, duration time.Duration, c *defaultHaTracker, user string, expectedClusters int) {
	t.Helper()
	test.Poll(t, duration, nil, func() interface{} {
		c.electedLock.RLock()
		cl := len(c.clusters[user])
		c.electedLock.RUnlock()

		if cl != expectedClusters {
			return fmt.Errorf("expected clusters: %d, got %d", expectedClusters, cl)
		}

		return nil
	})
}

func checkReplicaDeletionState(t *testing.T, duration time.Duration, c *defaultHaTracker, user, cluster string, expectedExistsInMemory, expectedExistsInKV, expectedMarkedForDeletion bool) {
	key := fmt.Sprintf("%s/%s", user, cluster)

	test.Poll(t, duration, nil, func() interface{} {
		c.electedLock.RLock()
		_, exists := c.clusters[user][cluster]
		c.electedLock.RUnlock()

		if exists != expectedExistsInMemory {
			return fmt.Errorf("exists in memory: expected=%v, got=%v", expectedExistsInMemory, exists)
		}

		return nil
	})

	val, err := c.client.Get(context.Background(), key)
	require.NoError(t, err)

	existsInKV := val != nil
	require.Equal(t, expectedExistsInKV, existsInKV, "exists in KV")

	if val != nil {
		markedForDeletion := val.(*ReplicaDesc).DeletedAt > 0
		require.Equal(t, expectedMarkedForDeletion, markedForDeletion, "KV entry marked for deletion")
	}
}

// fromLabelPairsToLabels converts dto.LabelPair into labels.Labels.
func fromLabelPairsToLabels(pairs []*dto.LabelPair) labels.Labels {
	builder := labels.NewScratchBuilder(len(pairs))
	for _, pair := range pairs {
		builder.Add(pair.GetName(), pair.GetValue())
	}
	builder.Sort()
	return builder.Labels()
}

// getSumOfHistogramSampleCount returns the sum of samples count of histograms matching the provided metric name
// and optional label matchers. Returns 0 if no metric matches.
func getSumOfHistogramSampleCount(families []*dto.MetricFamily, metricName string, matchers labels.Selector) uint64 {
	sum := uint64(0)

	for _, metric := range families {
		if metric.GetName() != metricName {
			continue
		}

		if metric.GetType() != dto.MetricType_HISTOGRAM {
			continue
		}

		for _, series := range metric.GetMetric() {
			if !matchers.Matches(fromLabelPairsToLabels(series.GetLabel())) {
				continue
			}

			histogram := series.GetHistogram()
			sum += histogram.GetSampleCount()
		}
	}

	return sum
}

func TestHATrackerChangeInElectedReplicaClearsLastSeenTimestamp(t *testing.T) {
	const userID = "user"

	codec := GetReplicaDescCodec()
	kvStore, closer := consul.NewInMemoryClient(codec, log.NewLogfmtLogger(os.Stdout), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	mock := kv.PrefixClient(kvStore, "prefix")

	// Start two trackers.
	t1, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kv.Config{Mock: mock},
	}, trackerLimits{
		maxClusters:            2,
		updateTimeout:          5 * time.Second,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        5 * time.Second,
	}, nil, log.NewNopLogger())
	require.NoError(t, err)

	t2, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kv.Config{Mock: mock},
	}, trackerLimits{
		maxClusters:            2,
		updateTimeout:          5 * time.Second,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        5 * time.Second,
	}, nil, log.NewNopLogger())
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), t1))
	defer services.StopAndAwaitTerminated(context.Background(), t1) //nolint:errcheck

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), t2))
	defer services.StopAndAwaitTerminated(context.Background(), t2) //nolint:errcheck

	now := time.Now()

	const cluster = "cluster"
	const firstReplica = "first"
	const secondReplica = "second"

	assert.NoError(t, t1.checkReplica(context.Background(), userID, cluster, firstReplica, now, now))
	// Both trackers will see "first" replica as current.
	checkReplicaTimestamp(t, 2*time.Second, t1, userID, cluster, firstReplica, now, now)
	checkReplicaTimestamp(t, 2*time.Second, t2, userID, cluster, firstReplica, now, now)

	// Ten seconds later, t1 receives request from first replica again
	now = now.Add(10 * time.Second)
	assert.NoError(t, t1.checkReplica(context.Background(), userID, cluster, firstReplica, now, now))

	// And t2 receives request from second replica.
	assert.Error(t, t2.checkReplica(context.Background(), userID, cluster, secondReplica, now, now))
	secondReplicaReceivedAtT2 := now

	// Now t2 updates the KV store... and overwrite elected replica.
	t2.updateKVStoreAll(context.Background(), now)

	// t1 is reading updates from KV store, and should see second replica being the elected one.
	checkReplicaTimestamp(t, 2*time.Second, t1, userID, cluster, secondReplica, secondReplicaReceivedAtT2, secondReplicaReceivedAtT2)

	// Furthermore, t1 has never seen "second" replica, so it should not have "electedLastSeenTimestamp" set.
	{
		t1.electedLock.RLock()
		info := t1.clusters[userID][cluster]
		t1.electedLock.RUnlock()

		require.Zero(t, info.electedLastSeenTimestamp)
	}

	// Continuing the test, say both t1 and t2 receive request from "first" replica now.
	// They both reject it.
	now = now.Add(9 * time.Second)
	firstReceivedAtT1 := now
	assert.Error(t, t1.checkReplica(context.Background(), userID, cluster, firstReplica, now, now))
	now = now.Add(1 * time.Second)
	firstReceivedAtT2 := now
	assert.Error(t, t2.checkReplica(context.Background(), userID, cluster, firstReplica, now, now))

	// Now t1 updates the KV Store.
	t1.updateKVStoreAll(context.Background(), now)

	// t2 is reading updates from KV store, and should see "second" replica being the elected one.
	checkReplicaTimestamp(t, 2*time.Second, t2, userID, cluster, firstReplica, firstReceivedAtT1, firstReceivedAtT1)

	// Since t2 has seen new elected replica too, we should have non-zero "electedLastSeenTimestamp".
	{
		t2.electedLock.RLock()
		info := t2.clusters[userID][cluster]
		t2.electedLock.RUnlock()

		require.Equal(t, firstReceivedAtT2.UnixMilli(), info.electedLastSeenTimestamp)
	}
}

func TestHATracker_UserSpecificTimeouts(t *testing.T) {
	const userID = "test-user"
	const cluster = "test-cluster"
	const replica = "test-replica"
	const otherReplica = "test-other-replica"

	codec := GetReplicaDescCodec()
	kvStore, closer := consul.NewInMemoryClient(codec, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })
	mock := kv.PrefixClient(kvStore, "prefix")

	defaultUpdateTimeout := 15 * time.Second
	defaultFailoverTimeout := 30 * time.Second
	userSpecificUpdateTimeout := 5 * time.Second
	userSpecificFailoverTimeout := 10 * time.Second // Must be > UpdateTimeout + UpdateTimeoutJitterMax + 1s

	limitsForUser := map[string]trackerLimits{}
	limitsForUser[userID] = trackerLimits{
		updateTimeout:   userSpecificUpdateTimeout,
		failoverTimeout: userSpecificFailoverTimeout,
	}

	tracker, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kv.Config{Mock: mock},
	}, trackerLimits{
		maxClusters: 100,
		// Default timeouts
		updateTimeout:   defaultUpdateTimeout,
		failoverTimeout: defaultFailoverTimeout,
		forUser:         limitsForUser,
	}, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), tracker))
	defer services.StopAndAwaitTerminated(context.Background(), tracker) //nolint:errcheck

	userTracker := tracker.forUser(userID)

	// Verify that the user-specific timeouts are used instead of the default
	assert.Equal(t, userSpecificUpdateTimeout, userTracker.updateTimeout)
	assert.Equal(t, userSpecificFailoverTimeout, userTracker.failoverTimeout)

	now := time.Now()

	// First request should succeed (establishes the replica as elected)
	err = userTracker.updateKVStore(context.Background(), cluster, replica, now, now, now.UnixMilli())
	assert.NoError(t, err)
	tracker.electedLock.Lock()
	assert.Equal(t, replica, tracker.clusters[userID][cluster].elected.Replica)
	firstReceivedAt := now.UnixMilli()
	assert.Equal(t, firstReceivedAt, tracker.clusters[userID][cluster].elected.ReceivedAt)
	tracker.electedLock.Unlock()

	// Second request within user-specific timeout should not update KV store
	now = now.Add(2 * time.Second)
	err = userTracker.updateKVStore(context.Background(), cluster, replica, now, now, now.UnixMilli())
	assert.NoError(t, err)
	tracker.electedLock.Lock()
	assert.Equal(t, replica, tracker.clusters[userID][cluster].elected.Replica)
	assert.Equal(t, firstReceivedAt, tracker.clusters[userID][cluster].elected.ReceivedAt)
	tracker.electedLock.Unlock()

	// Request after user-specific timeout should trigger an update
	now = now.Add(userSpecificUpdateTimeout) // Beyond user-specific timeout
	err = userTracker.updateKVStore(context.Background(), cluster, replica, now, now, now.UnixMilli())
	assert.NoError(t, err)
	tracker.electedLock.Lock()
	assert.Equal(t, replica, tracker.clusters[userID][cluster].elected.Replica)
	assert.Equal(t, now.UnixMilli(), tracker.clusters[userID][cluster].elected.ReceivedAt) // Timestamp is updated
	tracker.electedLock.Unlock()

	// Failover shouldn't happen before FailoverTimeout
	err = userTracker.updateKVStore(context.Background(), cluster, otherReplica, now, now, now.UnixMilli())
	assert.NoError(t, err)
	tracker.electedLock.Lock()
	assert.Equal(t, replica, tracker.clusters[userID][cluster].elected.Replica)
	tracker.electedLock.Unlock()

	// After FailoverTimeout, new value should be set
	now = now.Add(userSpecificFailoverTimeout)
	err = userTracker.updateKVStore(context.Background(), cluster, otherReplica, now, now, now.UnixMilli())
	assert.NoError(t, err)
	tracker.electedLock.Lock()
	assert.Equal(t, otherReplica, tracker.clusters[userID][cluster].elected.Replica)
	tracker.electedLock.Unlock()
}

func TestHATracker_UseSampleTimeForFailoverEnabled(t *testing.T) {
	const userID = "test-user"
	const cluster = "test-cluster"
	const replica1 = "replica-1"
	const replica2 = "replica-2"

	ctx := context.Background()
	codec := GetReplicaDescCodec()
	cfg := createMemberlistKVConfig(codec)
	kvStore := createMemberlistKVStore(ctx, t, cfg, log.NewNopLogger())

	failoverTimeout := 30 * time.Second

	tracker, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kvStore,
	}, trackerLimits{
		maxClusters:            100,
		updateTimeout:          15 * time.Second,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        failoverTimeout,
		failoverSampleTimeout:  failoverTimeout,
	}, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, tracker))
	defer services.StopAndAwaitTerminated(ctx, tracker) //nolint:errcheck

	now := time.Now()
	sampleDelay := 20 * time.Second
	sampleTime := now.Add(-sampleDelay)

	// Establish replica1 as elected
	err = tracker.checkReplica(ctx, userID, cluster, replica1, now, sampleTime)
	require.NoError(t, err)

	// Wait past failover timeout
	now2 := now.Add(failoverTimeout)
	sampleTime2 := sampleTime.Add(failoverTimeout)

	// Attempt failover to replica2
	err = tracker.checkReplica(ctx, userID, cluster, replica2, now2, sampleTime2)
	require.Error(t, err)

	// Update KV store to try and trigger failover
	tracker.updateKVStoreAll(ctx, now2)

	// Now replica1 should still be elected
	checkReplicaTimestamp(t, 2*time.Second, tracker, userID, cluster, replica1, now, now)

	// Wait past sample delay
	now3 := now2.Add(sampleDelay)
	sampleTime3 := sampleTime2.Add(sampleDelay)

	// Attempt failover to replica2 again
	err = tracker.checkReplica(ctx, userID, cluster, replica2, now3, sampleTime3)
	require.Error(t, err)

	// Update KV store to try and trigger failover
	tracker.updateKVStoreAll(ctx, now2)

	// Now replica2 should be elected
	checkReplicaTimestamp(t, 2*time.Second, tracker, userID, cluster, replica2, now3, now3)

	// Verify replica2 is now accepted
	err = tracker.checkReplica(ctx, userID, cluster, replica2, now3, sampleTime3)
	require.NoError(t, err)
}

func TestHATracker_UseSampleTimeForFailoverDisabled(t *testing.T) {
	const userID = "test-user"
	const cluster = "test-cluster"
	const replica1 = "replica-1"
	const replica2 = "replica-2"

	ctx := context.Background()
	codec := GetReplicaDescCodec()
	cfg := createMemberlistKVConfig(codec)
	kvStore := createMemberlistKVStore(ctx, t, cfg, log.NewNopLogger())

	failoverTimeout := 30 * time.Second

	tracker, err := newHaTracker(HATrackerConfig{
		EnableHATracker: true,
		KVStore:         kvStore,
	}, trackerLimits{
		maxClusters:            100,
		updateTimeout:          15 * time.Second,
		updateTimeoutJitterMax: 0,
		failoverTimeout:        failoverTimeout,
		failoverSampleTimeout:  0,
	}, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, tracker))
	defer services.StopAndAwaitTerminated(ctx, tracker) //nolint:errcheck

	now := time.Now()
	sampleDelay := 20 * time.Second
	sampleTime := now.Add(-sampleDelay)

	// Establish replica1 as elected
	err = tracker.checkReplica(ctx, userID, cluster, replica1, now, sampleTime)
	require.NoError(t, err)

	// Wait past failover timeout
	now2 := now.Add(failoverTimeout)
	sampleTime2 := sampleTime.Add(failoverTimeout)

	// Attempt failover to replica2
	err = tracker.checkReplica(ctx, userID, cluster, replica2, now2, sampleTime2)
	require.Error(t, err)

	// Update KV store to try and trigger failover
	tracker.updateKVStoreAll(ctx, now2)

	// Now replica2 should be elected
	checkReplicaTimestamp(t, 2*time.Second, tracker, userID, cluster, replica2, now2, now2)

	// Verify replica2 is now accepted
	err = tracker.checkReplica(ctx, userID, cluster, replica2, now2, now2)
	require.NoError(t, err)
}
