// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/ha_tracker.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/timestamp"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	mimirsync "github.com/grafana/mimir/pkg/util/sync"
)

type haTrackerLimits interface {
	// MaxHAClusters returns the max number of clusters that the HA tracker should track for a user.
	// Samples from additional clusters are rejected.
	MaxHAClusters(user string) int
	// HATrackerTimeouts returns timeouts that may be specific for the user.
	HATrackerTimeouts(user string) (update, updateJitterMax, failover, failoverSample time.Duration)
	DefaultHATrackerUpdateTimeout() time.Duration
}

type haTracker interface {
	services.Service
	http.Handler

	checkReplica(ctx context.Context, userID, cluster, replica string, now, sampleTime time.Time) error
	cleanupHATrackerMetricsForUser(userID string)
}

// ProtoReplicaDescFactory makes new InstanceDescs
func ProtoReplicaDescFactory() proto.Message {
	return NewReplicaDesc()
}

// NewReplicaDesc returns an empty *distributor.ReplicaDesc.
func NewReplicaDesc() *ReplicaDesc {
	return &ReplicaDesc{}
}

// Merge merges other ReplicaDesc into this one.
// The decision is made based on the ReceivedAt timestamp, if the Replica name is the same and at the ElectedAt if the
// Replica name is different
func (r *ReplicaDesc) Merge(other memberlist.Mergeable, _ bool) (change memberlist.Mergeable, error error) {
	return r.mergeWithTime(other)
}

func (r *ReplicaDesc) mergeWithTime(mergeable memberlist.Mergeable) (memberlist.Mergeable, error) {
	if mergeable == nil {
		return nil, nil
	}

	other, ok := mergeable.(*ReplicaDesc)
	if !ok {
		return nil, fmt.Errorf("expected *distributor.ReplicaDesc, got %T", mergeable)
	}

	if other == nil {
		return nil, nil
	}

	changed := false
	if other.Replica == r.Replica {
		// Keeping the one with the most recent receivedAt timestamp
		if other.ReceivedAt > r.ReceivedAt {
			*r = *other
			changed = true
		} else if r.ReceivedAt == other.ReceivedAt && r.DeletedAt == 0 && other.DeletedAt != 0 {
			*r = *other
			changed = true
		}
	} else {
		// keep the most recent ElectedAt to reach consistency
		if other.ElectedAt > r.ElectedAt {
			*r = *other
			changed = true
		} else if other.ElectedAt == r.ElectedAt {
			// if the timestamps are equal we compare ReceivedAt
			if other.ReceivedAt > r.ReceivedAt {
				*r = *other
				changed = true
			}
		}
	}

	// No changes
	if !changed {
		return nil, nil
	}

	out := NewReplicaDesc()
	*out = *r
	return out, nil
}

// MergeContent describes content of this Mergeable.
// Given that ReplicaDesc can have only one instance at a time, it returns the ReplicaDesc it contains. By doing this we choose
// to not make use of the subset invalidation feature of memberlist
func (r *ReplicaDesc) MergeContent() []string {
	result := []string(nil)
	if len(r.Replica) != 0 {
		result = append(result, r.String())
	}
	return result
}

// RemoveTombstones is noOp because we will handle replica deletions outside the context of memberlist.
func (r *ReplicaDesc) RemoveTombstones(_ time.Time) (total, removed int) {
	return
}

// Clone returns a deep copy of the ReplicaDesc.
func (r *ReplicaDesc) Clone() memberlist.Mergeable {
	return proto.Clone(r).(*ReplicaDesc)
}

// HATrackerConfig contains the configuration required to
// create an HA Tracker.
type HATrackerConfig struct {
	EnableHATracker bool `yaml:"enable_ha_tracker"`

	// This is a potentially high cardinality metric, so it is disabled by default.
	EnableElectedReplicaMetric bool `yaml:"enable_elected_replica_metric"`

	// KVStore is the backend storage for the HA tracker. Memberlist is the recommended backend.
	// Supported values: consul, etcd, inmemory, memberlist, multi. Memberlist is recommended. Consul and etcd are still available and supported.
	KVStore kv.Config `yaml:"kvstore" doc:"description=Backend storage to use for the HA tracker. Supported values are: consul, etcd, inmemory, memberlist, multi. Memberlist is recommended."`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *HATrackerConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.EnableHATracker, "distributor.ha-tracker.enable", false, "Enable the distributors HA tracker so that it can accept samples from Prometheus HA replicas gracefully (requires labels).")
	f.BoolVar(&cfg.EnableElectedReplicaMetric, "distributor.ha-tracker.enable-elected-replica-metric", false, "Enable the elected_replica_status metric, which shows the current elected replica. It is disabled by default due to the possible high cardinality of the metric.")

	// We want the ability to use different instances for the ring and
	// for HA cluster tracking. We also customize the default keys prefix, in
	// order to not clash with the ring key if they both share the same KVStore
	// backend (i.e. run on the same Consul cluster).
	// Set the default to memberlist before registering flags.
	if cfg.KVStore.Store == "" {
		cfg.KVStore.Store = "memberlist"
	}
	cfg.KVStore.RegisterFlagsWithPrefix("distributor.ha-tracker.", "ha-tracker/", f)
}

func GetReplicaDescCodec() codec.Proto {
	return codec.NewProtoCodec("replicaDesc", ProtoReplicaDescFactory)
}

// Track the replica we're accepting samples from
// for each HA cluster we know about.
type defaultHaTracker struct {
	services.Service

	logger log.Logger
	cfg    HATrackerConfig
	client kv.Client
	limits haTrackerLimits

	electedLock sync.RWMutex                         // protects clusters maps
	clusters    map[string]map[string]*haClusterInfo // Known clusters with elected replicas per user. First key = user, second key = cluster name.

	electedReplicaStatus          *prometheus.CounterVec
	electedReplicaChanges         *prometheus.CounterVec
	electedReplicaTimestamp       *prometheus.GaugeVec
	lastElectionTimestamp         *prometheus.GaugeVec
	totalReelections              *prometheus.GaugeVec
	electedReplicaPropagationTime prometheus.Histogram
	kvCASCalls                    *prometheus.CounterVec

	cleanupRuns                      prometheus.Counter
	replicasMarkedForDeletion        prometheus.Counter
	deletedReplicas                  prometheus.Counter
	markingForDeletionsFailed        prometheus.Counter
	replicasDescFailedTypeAssertions prometheus.Counter

	// computeUpdateTimeoutJitter overrides the computeUpdateTimeoutJitter function
	// for deterministic tests.
	computeUpdateTimeoutJitter func(maxJitter time.Duration) time.Duration
}

// For one cluster, the information we need to do ha-tracking.
type haClusterInfo struct {
	elected                      ReplicaDesc // latest info from KVStore
	electedLastSeenTimestamp     int64       // timestamp in milliseconds
	nonElectedLastSeenReplica    string
	nonElectedLastSeenTimestamp  int64 // timestamp in milliseconds
	nonElectedLastSeenSampleTime int64 // last seen earliest sample time in milliseconds
}

// newHaTracker returns a new HA cluster tracker using memberlist (recommended),
// consul, etcd, or an in-memory KV store. Consul and etcd are deprecated for
// the HA tracker. Tracker must be started via StartAsync().
func newHaTracker(cfg HATrackerConfig, limits haTrackerLimits, reg prometheus.Registerer, logger log.Logger) (*defaultHaTracker, error) {
	t := &defaultHaTracker{
		logger:   log.With(logger, "component", "ha-tracker"),
		cfg:      cfg,
		limits:   limits,
		clusters: map[string]map[string]*haClusterInfo{},

		electedReplicaStatus: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ha_tracker_elected_replica_status",
			Help: "The current elected replica for a user ID/cluster.",
		}, []string{"user", "cluster", "replica"}),
		electedReplicaChanges: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ha_tracker_elected_replica_changes_total",
			Help: "The total number of times the elected replica has changed for a user ID/cluster.",
		}, []string{"user", "cluster"}),
		electedReplicaTimestamp: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ha_tracker_elected_replica_timestamp_seconds",
			Help: "The timestamp stored for the currently elected replica, from the KVStore.",
		}, []string{"user", "cluster"}),
		lastElectionTimestamp: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ha_tracker_last_election_timestamp_seconds",
			Help: "The timestamp stored for the most recent election, from the KVStore.",
		}, []string{"user", "cluster"}),
		totalReelections: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ha_tracker_reelections_total",
			Help: "The total number of reelections for a user ID/cluster, from the KVStore.",
		}, []string{"user", "cluster"}),
		electedReplicaPropagationTime: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "cortex_ha_tracker_elected_replica_change_propagation_time_seconds",
			Help:                            "The time it for the distributor to update the replica change.",
			Buckets:                         prometheus.DefBuckets,
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: time.Hour,
		}),
		kvCASCalls: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ha_tracker_kv_store_cas_total",
			Help: "The total number of CAS calls to the KV store for a user ID/cluster.",
		}, []string{"user", "cluster"}),

		cleanupRuns: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ha_tracker_replicas_cleanup_started_total",
			Help: "Number of elected replicas cleanup loops started.",
		}),
		replicasMarkedForDeletion: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ha_tracker_replicas_cleanup_marked_for_deletion_total",
			Help: "Number of elected replicas marked for deletion.",
		}),
		deletedReplicas: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ha_tracker_replicas_cleanup_deleted_total",
			Help: "Number of elected replicas deleted from KV store.",
		}),
		markingForDeletionsFailed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ha_tracker_replicas_cleanup_delete_failed_total",
			Help: "Number of elected replicas that failed to be marked for deletion, or deleted.",
		}),
		replicasDescFailedTypeAssertions: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ha_tracker_replica_desc_failed_type_assertions_total",
			Help: "Number of failed replicaDesc type assertions in the HA tracker.",
		}),
	}
	client, err := kv.NewClient(
		cfg.KVStore,
		GetReplicaDescCodec(),
		kv.RegistererWithKVName(prometheus.WrapRegistererWithPrefix("cortex_", reg), "distributor-hatracker"),
		logger,
	)
	if err != nil {
		return nil, err
	}
	t.client = client

	t.Service = services.NewBasicService(t.syncHATrackerStateOnStart, t.loop, nil)
	return t, nil
}

func computeUpdateTimeoutJitter(maxJitter time.Duration) time.Duration {
	if maxJitter > 0 {
		return time.Duration(rand.Int63n(int64(2*maxJitter))) - maxJitter
	}
	return 0
}

func (h *defaultHaTracker) syncHATrackerStateOnStart(ctx context.Context) error {
	level.Info(h.logger).Log("msg", "sync HA state on start: Listing keys from KV Store")
	keys, err := h.client.List(ctx, "")
	if err != nil {
		level.Error(h.logger).Log("msg", "sync HA state on start: failed to list the keys ", "err", err)
		return err
	}

	if len(keys) == 0 {
		level.Warn(h.logger).Log("msg", "sync HA state on start: no keys for HA tracker prefix in the KV store."+
			"Skipping cache sync")
		return nil
	}

	for i := 0; i < len(keys); i++ {
		if ctx.Err() != nil {
			return fmt.Errorf("syncing HA tracker state on startup: %w", context.Cause(ctx))
		}

		val, err := h.client.Get(ctx, keys[i])
		if err != nil {
			level.Warn(h.logger).Log("msg", "sync HA state on start: failed to get replica value", "key", keys[i], "err", err)
			return nil
		}
		desc, ok := val.(*ReplicaDesc)
		if !ok {
			level.Error(h.logger).Log("msg", "sync HA state on start: Skipping key: "+keys[i]+", got invalid ReplicaDesc")
			continue
		}
		h.processKVStoreEntry(keys[i], desc)
	}
	level.Info(h.logger).Log("msg", "sync HA state on start: HA cache sync finished successfully")
	return nil
}

// Follows pattern used by ring for WatchKey.
func (h *defaultHaTracker) loop(ctx context.Context) error {
	// Start cleanup loop. It will stop when context is done.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.updateKVLoop(ctx)
	}()

	// Request callbacks from KVStore when data changes.
	// The KVStore config we gave when creating h should have contained a prefix,
	// which would have given us a prefixed KVStore client. So, we can pass an empty string here.
	// WatchPrefix blocks until ctx is done or the function provided returns false.
	h.client.WatchPrefix(ctx, "", func(key string, value interface{}) bool {
		replica, ok := value.(*ReplicaDesc)
		if !ok {
			// Always return true to ensure WatchPrefix() is never interrupted, otherwise the HA tracker stops to receive updates.
			level.Error(h.logger).Log("msg", "unexpected data type receive when watching for HA tracker updates", "return type", fmt.Sprintf("%T", value), "key", key)
			h.replicasDescFailedTypeAssertions.Inc()
			return true
		}
		h.processKVStoreEntry(key, replica)
		return true
	})

	const waitTimeout = 10 * time.Second
	if err := mimirsync.WaitWithTimeout(&wg, waitTimeout); err != nil {
		level.Error(h.logger).Log("msg", "wait group was not done after a long timeout but we expected it to be done in a few milliseconds", "timeout", waitTimeout, "err", err)
		return fmt.Errorf("waiting for updateKVLoop to be done: %w", err)
	}

	return nil
}

func (h *defaultHaTracker) processKVStoreEntry(key string, replica *ReplicaDesc) {
	segments := strings.SplitN(key, "/", 2)

	// Valid key would look like cluster/replica, and a key without a / such as `ring` would be invalid.
	if len(segments) != 2 {
		return
	}

	user := segments[0]
	cluster := segments[1]

	if replica.DeletedAt > 0 {
		h.cleanupDeletedReplica(user, cluster)
		return
	}

	// Store the received information into our cache
	h.electedLock.Lock()
	h.updateCache(user, cluster, replica)
	h.electedLock.Unlock()
	h.electedReplicaPropagationTime.Observe(time.Since(timestamp.Time(replica.ReceivedAt)).Seconds())
}

func (h *defaultHaTracker) cleanupDeletedReplica(user string, cluster string) {
	h.electedReplicaStatus.DeletePartialMatch(prometheus.Labels{"user": user, "cluster": cluster})
	h.electedReplicaChanges.DeleteLabelValues(user, cluster)
	h.electedReplicaTimestamp.DeleteLabelValues(user, cluster)
	h.lastElectionTimestamp.DeleteLabelValues(user, cluster)
	h.totalReelections.DeleteLabelValues(user, cluster)

	h.electedLock.Lock()
	defer h.electedLock.Unlock()
	userClusters := h.clusters[user]
	if userClusters != nil {
		delete(userClusters, cluster)
		if len(userClusters) == 0 {
			delete(h.clusters, user)
		}
	}
}

const (
	cleanupCyclePeriod         = 30 * time.Minute
	cleanupCycleJitterVariance = 0.2 // for 30 minutes, this is ±6 min

	// If we have received the last sample for the given cluster before this timeout, we will mark the selected replica for deletion.
	// If the selected replica is marked for deletion for this time, it is deleted completely.
	deletionTimeout = 30 * time.Minute
)

func (h *defaultHaTracker) updateKVLoop(ctx context.Context) {
	cleanupTick := time.NewTicker(util.DurationWithJitter(cleanupCyclePeriod, cleanupCycleJitterVariance))
	defer cleanupTick.Stop()
	updateTimeout := h.limits.DefaultHATrackerUpdateTimeout()
	tick := time.NewTicker(updateTimeout)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case t := <-tick.C:
			h.updateKVStoreAll(ctx, t)
		case t := <-cleanupTick.C:
			h.cleanupRuns.Inc()
			h.cleanupOldReplicas(ctx, t.Add(-deletionTimeout))
		}
	}
}

// Loop over all entries in our cache and update KVStore where it is out of date,
// electing a new replica if necessary.
func (h *defaultHaTracker) updateKVStoreAll(ctx context.Context, now time.Time) {
	h.electedLock.RLock()
	defer h.electedLock.RUnlock()
	// Note the maps may change when we release the lock while talking to KVStore;
	// the Go language allows this: https://golang.org/ref/spec#For_range note 3.
	for userID, clusters := range h.clusters {
		uh := h.forUser(userID)
		for cluster, entry := range clusters {
			if uh.withinUpdateTimeout(now, entry.elected.ReceivedAt) {
				continue // Some other process updated it recently; nothing to do.
			}
			var replica string
			var receivedAt int64
			var sampleTime time.Time
			if uh.withinUpdateTimeout(now, entry.electedLastSeenTimestamp) {
				// We have seen the elected replica recently; carry on with that choice.
				replica = entry.elected.Replica
				receivedAt = entry.electedLastSeenTimestamp
				sampleTime = now
			} else if uh.withinUpdateTimeout(now, entry.nonElectedLastSeenTimestamp) {
				// Not seen elected but have seen another: attempt to fail over.
				replica = entry.nonElectedLastSeenReplica
				receivedAt = entry.nonElectedLastSeenTimestamp
				if uh.failoverSampleTimeout > 0 {
					sampleTime = timestamp.Time(entry.nonElectedLastSeenSampleTime)
				} else {
					sampleTime = now
				}
			} else {
				continue // we don't have any recent timestamps
			}
			// Release lock while we talk to KVStore, which could take a while.
			h.electedLock.RUnlock()
			err := uh.updateKVStore(ctx, cluster, replica, now, sampleTime, receivedAt)
			h.electedLock.RLock()
			if err != nil {
				// Failed to store - log it but carry on
				level.Error(h.logger).Log("msg", "failed to update KVStore", "err", err)
			}
		}
	}
}

// Replicas marked for deletion before deadline will be deleted.
// Replicas with last-received timestamp before deadline will be marked for deletion.
func (h *defaultHaTracker) cleanupOldReplicas(ctx context.Context, deadline time.Time) {
	keys, err := h.client.List(ctx, "")
	if err != nil {
		level.Warn(h.logger).Log("msg", "cleanup: failed to list replica keys", "err", err)
		return
	}

	for _, key := range keys {
		if ctx.Err() != nil {
			return
		}

		val, err := h.client.Get(ctx, key)
		if err != nil {
			level.Warn(h.logger).Log("msg", "cleanup: failed to get replica value", "key", key, "err", err)
			continue
		}

		desc, ok := val.(*ReplicaDesc)
		if !ok {
			level.Error(h.logger).Log("msg", "cleanup: got invalid replica descriptor", "key", key)
			continue
		}

		if desc.DeletedAt > 0 {
			if timestamp.Time(desc.DeletedAt).After(deadline) {
				continue
			}

			// We're blindly deleting a key here. It may happen that the value was updated since we have read it a few lines above,
			// in which case distributors will have updated value in memory, but Delete will remove it from the KV store anyway.
			// That's not great, but should not be a problem. If the KV store sends Watch notification for Delete, distributors will
			// delete it from memory, and recreate it on the next sample with matching replica.
			//
			// If KV store doesn't send Watch notification for Delete, distributors *with* replica in memory will keep using it,
			// while distributors *without* replica in memory will try to write it to KV store -- which will update *all*
			// watching distributors.
			err = h.client.Delete(ctx, key)
			if err != nil {
				level.Error(h.logger).Log("msg", "cleanup: failed to delete old replica", "key", key, "err", err)
				h.markingForDeletionsFailed.Inc()
			} else {
				level.Info(h.logger).Log("msg", "cleanup: deleted old replica", "key", key)
				h.deletedReplicas.Inc()
			}
			continue
		}

		// Not marked as deleted yet.
		if desc.DeletedAt == 0 && timestamp.Time(desc.ReceivedAt).Before(deadline) {
			err := h.client.CAS(ctx, key, func(in interface{}) (out interface{}, retry bool, err error) {
				d, ok := in.(*ReplicaDesc)
				if !ok || d == nil || d.DeletedAt > 0 || !timestamp.Time(desc.ReceivedAt).Before(deadline) {
					return nil, false, nil
				}

				d.DeletedAt = timestamp.FromTime(time.Now())
				return d, true, nil
			})

			if err != nil {
				h.markingForDeletionsFailed.Inc()
				level.Error(h.logger).Log("msg", "cleanup: failed to mark replica as deleted", "key", key, "err", err)
			} else {
				h.replicasMarkedForDeletion.Inc()
				level.Info(h.logger).Log("msg", "cleanup: marked replica as deleted", "key", key)
			}
		}
	}
}

// checkReplica checks the cluster and replica against the local cache to see
// if we should accept the incoming sample. It will return replicasNotMatchError
// if we shouldn't store this sample but are accepting samples from another
// replica for the cluster.
// Updates to and from the KV store are handled in the background, except
// if we have no cached data for this cluster in which case we create the
// record and store it in-band.
func (h *defaultHaTracker) checkReplica(ctx context.Context, userID, cluster, replica string, now, sampleTime time.Time) error {
	h.electedLock.Lock()
	if entry := h.clusters[userID][cluster]; entry != nil {
		var err error
		if entry.elected.Replica == replica {
			// Sample received is from elected replica: update timestamp and carry on.
			entry.electedLastSeenTimestamp = timestamp.FromTime(now)
		} else {
			// Sample received is from non-elected replica: record details and reject.
			entry.nonElectedLastSeenReplica = replica
			entry.nonElectedLastSeenTimestamp = timestamp.FromTime(now)
			entry.nonElectedLastSeenSampleTime = timestamp.FromTime(sampleTime)
			err = newReplicasDidNotMatchError(replica, entry.elected.Replica)
		}
		h.electedLock.Unlock()
		return err
	}

	// We don't know about this cluster yet.
	nClusters := len(h.clusters[userID])
	h.electedLock.Unlock()
	// If we have reached the limit for number of clusters, error out now.
	if limit := h.limits.MaxHAClusters(userID); limit > 0 && nClusters+1 > limit {
		return newTooManyClustersError(limit)
	}

	uh := h.forUser(userID)
	err := uh.updateKVStore(ctx, cluster, replica, now, sampleTime, now.UnixMilli())
	if err != nil {
		level.Error(h.logger).Log("msg", "failed to update KVStore - rejecting sample", "err", err)
		return err
	}
	// Cache will now have the value - recurse to check it again.
	return h.checkReplica(ctx, userID, cluster, replica, now, sampleTime)
}

type defaultHaTrackerForUser struct {
	*defaultHaTracker
	userID                string
	updateTimeout         time.Duration
	updateTimeoutJitter   time.Duration
	failoverTimeout       time.Duration
	failoverSampleTimeout time.Duration
}

func (h *defaultHaTracker) forUser(userID string) defaultHaTrackerForUser {
	uh := defaultHaTrackerForUser{
		defaultHaTracker: h,
		userID:           userID,
	}

	var updateJitterMax time.Duration
	uh.updateTimeout, updateJitterMax, uh.failoverTimeout, uh.failoverSampleTimeout = h.limits.HATrackerTimeouts(userID)

	computeJitter := h.computeUpdateTimeoutJitter
	if computeJitter == nil {
		computeJitter = computeUpdateTimeoutJitter
	}
	uh.updateTimeoutJitter = computeJitter(updateJitterMax)

	return uh
}

func (h *defaultHaTrackerForUser) withinUpdateTimeout(now time.Time, receivedAt int64) bool {
	return now.Sub(timestamp.Time(receivedAt)) < h.updateTimeout+h.updateTimeoutJitter
}

// Must be called with electedLock held.
func (h *defaultHaTracker) updateCache(userID, cluster string, desc *ReplicaDesc) {
	if h.clusters[userID] == nil {
		h.clusters[userID] = map[string]*haClusterInfo{}
	}
	entry := h.clusters[userID][cluster]
	if entry == nil {
		entry = &haClusterInfo{}
		h.clusters[userID][cluster] = entry
	}
	if desc.Replica != entry.elected.Replica {
		if h.cfg.EnableElectedReplicaMetric {
			h.electedReplicaStatus.DeletePartialMatch(prometheus.Labels{"user": userID, "cluster": cluster})
			h.electedReplicaStatus.WithLabelValues(userID, cluster, desc.Replica).Inc()
		}
		h.electedReplicaChanges.WithLabelValues(userID, cluster).Inc()
		h.lastElectionTimestamp.WithLabelValues(userID, cluster).Set(float64(desc.ElectedAt / 1000))
		h.totalReelections.WithLabelValues(userID, cluster).Set(float64(desc.ElectedChanges))
		level.Info(h.logger).Log("msg", "updating replica in cache", "user", userID, "cluster", cluster, "old_replica", entry.elected.Replica, "new_replica", desc.Replica, "received_at", timestamp.Time(desc.ReceivedAt))
		if entry.nonElectedLastSeenReplica == desc.Replica {
			entry.electedLastSeenTimestamp = entry.nonElectedLastSeenTimestamp
		} else {
			// clear electedLastSeenTimestamp since we don't know when we have seen this replica.
			entry.electedLastSeenTimestamp = 0
		}
	}
	entry.elected = *desc
	h.electedReplicaTimestamp.WithLabelValues(userID, cluster).Set(float64(desc.ReceivedAt / 1000))
}

// If we do set the value then err will be nil and desc will contain the value we set.
// If there is already a valid value in the store, return nil, nil.
func (h *defaultHaTrackerForUser) updateKVStore(ctx context.Context, cluster, replica string, now, sampleTime time.Time, receivedAt int64) error {
	key := fmt.Sprintf("%s/%s", h.userID, cluster)
	var desc *ReplicaDesc
	var electedAtTime, electedChanges int64
	err := h.client.CAS(ctx, key, func(in interface{}) (out interface{}, retry bool, err error) {
		var ok bool
		if desc, ok = in.(*ReplicaDesc); ok && desc.DeletedAt == 0 {
			// If the entry in KVStore is up-to-date, just stop the loop.
			if h.withinUpdateTimeout(now, desc.ReceivedAt) {
				return nil, false, nil
			}
			electedAtTime = desc.ElectedAt
			electedChanges = desc.ElectedChanges

			// In the past we had a bug that caused ElectedAt timestamp to not be set (so it was the default zero value).
			// The bug is fixed now, but we may still have very old entries around with a zero ElectedAt timestamp.
			// To avoid misunderstandings, we proactively fix it by setting the ElectedAt to now.
			if electedAtTime == 0 {
				electedAtTime = timestamp.FromTime(now)
			}

			// If our replica is different, wait until the failover time
			if desc.Replica != replica {
				if now.Sub(timestamp.Time(desc.ReceivedAt)) < h.failoverTimeout {
					level.Info(h.logger).Log("msg", "replica differs, but it's too early to failover", "user", h.userID, "cluster", cluster, "replica", replica, "elected", desc.Replica, "received_at", timestamp.Time(desc.ReceivedAt))
					return nil, false, nil
				}
				if h.failoverSampleTimeout > 0 {
					if sampleTime.Sub(timestamp.Time(desc.ReceivedAt)) < h.failoverSampleTimeout {
						level.Info(h.logger).Log("msg", "replica differs, but it's too early to failover based on earliest sample time", "user", h.userID, "cluster", cluster, "replica", replica, "elected", desc.Replica, "received_at", timestamp.Time(desc.ReceivedAt), "sample_time", sampleTime, "failover_sample_timeout", h.failoverSampleTimeout)
						return nil, false, nil
					}
				}
				level.Info(h.logger).Log("msg", "replica differs, attempting to update kv", "user", h.userID, "cluster", cluster, "replica", replica, "elected", desc.Replica, "received_at", timestamp.Time(desc.ReceivedAt), "sample_time", sampleTime, "failover_sample_timeout", h.failoverSampleTimeout)
				electedAtTime = timestamp.FromTime(now)
				electedChanges = desc.ElectedChanges + 1
			}
		} else {
			// The replica doesn't exist in the KV store yet, or it's marked for deletion. We have to re-create it,
			// making sure the ElectedAt timestamp is set.
			electedAtTime = timestamp.FromTime(now)
		}

		// Attempt to update KVStore to our timestamp and replica.
		desc = &ReplicaDesc{
			Replica:        replica,
			ReceivedAt:     receivedAt,
			DeletedAt:      0,
			ElectedAt:      electedAtTime,
			ElectedChanges: electedChanges,
		}
		return desc, true, nil
	})
	h.kvCASCalls.WithLabelValues(h.userID, cluster).Inc()
	// Add data stored or received from KVStore, if available.
	if err == nil && desc != nil {
		h.electedLock.Lock()
		h.updateCache(h.userID, cluster, desc)
		h.electedLock.Unlock()
	}
	return err
}

func findHALabels(replicaLabel, clusterLabel string, labels []mimirpb.LabelAdapter) (string, string) {
	var cluster, replica string
	var pair mimirpb.LabelAdapter

	for _, pair = range labels {
		switch pair.Name {
		case replicaLabel:
			replica = pair.Value
			if cluster != "" && replica != "" {
				return cluster, replica
			}
		case clusterLabel:
			cluster = pair.Value
			if cluster != "" && replica != "" {
				return cluster, replica
			}
		}
	}

	return cluster, replica
}

func (h *defaultHaTracker) cleanupHATrackerMetricsForUser(userID string) {
	filter := prometheus.Labels{"user": userID}

	h.electedReplicaStatus.DeletePartialMatch(filter)
	h.electedReplicaChanges.DeletePartialMatch(filter)
	h.electedReplicaTimestamp.DeletePartialMatch(filter)
	h.lastElectionTimestamp.DeletePartialMatch(filter)
	h.totalReelections.DeletePartialMatch(filter)
	h.kvCASCalls.DeletePartialMatch(filter)
}
