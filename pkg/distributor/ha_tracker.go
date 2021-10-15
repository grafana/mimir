// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/ha_tracker.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/pkg/timestamp"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
)

var (
	errNegativeUpdateTimeoutJitterMax = errors.New("HA tracker max update timeout jitter shouldn't be negative")
	errInvalidFailoverTimeout         = "HA Tracker failover timeout (%v) must be at least 1s greater than update timeout - max jitter (%v)"
)

type haTrackerLimits interface {
	// MaxHAClusters returns max number of clusters that HA tracker should track for a user.
	// Samples from additional clusters are rejected.
	MaxHAClusters(user string) int
}

// ProtoReplicaDescFactory makes new InstanceDescs
func ProtoReplicaDescFactory() proto.Message {
	return NewReplicaDesc()
}

// NewReplicaDesc returns an empty *distributor.ReplicaDesc.
func NewReplicaDesc() *ReplicaDesc {
	return &ReplicaDesc{}
}

// HATrackerConfig contains the configuration require to
// create a HA Tracker.
type HATrackerConfig struct {
	EnableHATracker bool `yaml:"enable_ha_tracker"`
	// We should only update the timestamp if the difference
	// between the stored timestamp and the time we received a sample at
	// is more than this duration.
	UpdateTimeout          time.Duration `yaml:"ha_tracker_update_timeout"`
	UpdateTimeoutJitterMax time.Duration `yaml:"ha_tracker_update_timeout_jitter_max"`
	// We should only failover to accepting samples from a replica
	// other than the replica written in the KVStore if the difference
	// between the stored timestamp and the time we received a sample is
	// more than this duration
	FailoverTimeout time.Duration `yaml:"ha_tracker_failover_timeout"`

	KVStore kv.Config `yaml:"kvstore" doc:"description=Backend storage to use for the ring. Please be aware that memberlist is not supported by the HA tracker since gossip propagation is too slow for HA purposes."`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *HATrackerConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.EnableHATracker, "distributor.ha-tracker.enable", false, "Enable the distributors HA tracker so that it can accept samples from Prometheus HA replicas gracefully (requires labels).")
	f.DurationVar(&cfg.UpdateTimeout, "distributor.ha-tracker.update-timeout", 15*time.Second, "Update the timestamp in the KV store for a given cluster/replica only after this amount of time has passed since the current stored timestamp.")
	f.DurationVar(&cfg.UpdateTimeoutJitterMax, "distributor.ha-tracker.update-timeout-jitter-max", 5*time.Second, "Maximum jitter applied to the update timeout, in order to spread the HA heartbeats over time.")
	f.DurationVar(&cfg.FailoverTimeout, "distributor.ha-tracker.failover-timeout", 30*time.Second, "If we don't receive any samples from the accepted replica for a cluster in this amount of time we will failover to the next replica we receive a sample from. This value must be greater than the update timeout")

	// We want the ability to use different Consul instances for the ring and
	// for HA cluster tracking. We also customize the default keys prefix, in
	// order to not clash with the ring key if they both share the same KVStore
	// backend (ie. run on the same consul cluster).
	cfg.KVStore.RegisterFlagsWithPrefix("distributor.ha-tracker.", "ha-tracker/", f)
}

// Validate config and returns error on failure
func (cfg *HATrackerConfig) Validate() error {
	if cfg.UpdateTimeoutJitterMax < 0 {
		return errNegativeUpdateTimeoutJitterMax
	}

	minFailureTimeout := cfg.UpdateTimeout + cfg.UpdateTimeoutJitterMax + time.Second
	if cfg.FailoverTimeout < minFailureTimeout {
		return fmt.Errorf(errInvalidFailoverTimeout, cfg.FailoverTimeout, minFailureTimeout)
	}

	return nil
}

func GetReplicaDescCodec() codec.Proto {
	return codec.NewProtoCodec("replicaDesc", ProtoReplicaDescFactory)
}

// Track the replica we're accepting samples from
// for each HA cluster we know about.
type haTracker struct {
	services.Service

	logger              log.Logger
	cfg                 HATrackerConfig
	client              kv.Client
	updateTimeoutJitter time.Duration
	limits              haTrackerLimits

	electedLock sync.RWMutex                         // protects clusters maps
	clusters    map[string]map[string]*haClusterInfo // Known clusters with elected replicas per user. First key = user, second key = cluster name.

	electedReplicaChanges         *prometheus.CounterVec
	electedReplicaTimestamp       *prometheus.GaugeVec
	electedReplicaPropagationTime prometheus.Histogram
	kvCASCalls                    *prometheus.CounterVec

	cleanupRuns               prometheus.Counter
	replicasMarkedForDeletion prometheus.Counter
	deletedReplicas           prometheus.Counter
	markingForDeletionsFailed prometheus.Counter
}

// For one cluster, the information we need to do ha-tracking.
type haClusterInfo struct {
	elected                     ReplicaDesc // latest info from KVStore
	electedLastSeenTimestamp    int64
	nonElectedLastSeenReplica   string
	nonElectedLastSeenTimestamp int64
}

// NewClusterTracker returns a new HA cluster tracker using either Consul
// or in-memory KV store. Tracker must be started via StartAsync().
func newHATracker(cfg HATrackerConfig, limits haTrackerLimits, reg prometheus.Registerer, logger log.Logger) (*haTracker, error) {
	var jitter time.Duration
	if cfg.UpdateTimeoutJitterMax > 0 {
		jitter = time.Duration(rand.Int63n(int64(2*cfg.UpdateTimeoutJitterMax))) - cfg.UpdateTimeoutJitterMax
	}

	t := &haTracker{
		logger:              logger,
		cfg:                 cfg,
		updateTimeoutJitter: jitter,
		limits:              limits,
		clusters:            map[string]map[string]*haClusterInfo{},

		electedReplicaChanges: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ha_tracker_elected_replica_changes_total",
			Help: "The total number of times the elected replica has changed for a user ID/cluster.",
		}, []string{"user", "cluster"}),
		electedReplicaTimestamp: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ha_tracker_elected_replica_timestamp_seconds",
			Help: "The timestamp stored for the currently elected replica, from the KVStore.",
		}, []string{"user", "cluster"}),
		electedReplicaPropagationTime: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ha_tracker_elected_replica_change_propagation_time_seconds",
			Help:    "The time it for the distributor to update the replica change.",
			Buckets: prometheus.DefBuckets,
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
	}

	if cfg.EnableHATracker {
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
	}

	t.Service = services.NewBasicService(nil, t.loop, nil)
	return t, nil
}

// Follows pattern used by ring for WatchKey.
func (c *haTracker) loop(ctx context.Context) error {
	if !c.cfg.EnableHATracker {
		// don't do anything, but wait until asked to stop.
		<-ctx.Done()
		return nil
	}

	// Start cleanup loop. It will stop when context is done.
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.updateKVLoop(ctx)
	}()

	// Request callbacks from KVStore when data changes.
	// The KVStore config we gave when creating c should have contained a prefix,
	// which would have given us a prefixed KVStore client. So, we can pass empty string here.
	c.client.WatchPrefix(ctx, "", func(key string, value interface{}) bool {
		replica := value.(*ReplicaDesc)
		segments := strings.SplitN(key, "/", 2)

		// Valid key would look like cluster/replica, and a key without a / such as `ring` would be invalid.
		if len(segments) != 2 {
			return true
		}

		user := segments[0]
		cluster := segments[1]

		if replica.DeletedAt > 0 {
			c.electedReplicaChanges.DeleteLabelValues(user, cluster)
			c.electedReplicaTimestamp.DeleteLabelValues(user, cluster)

			c.electedLock.Lock()
			defer c.electedLock.Unlock()
			userClusters := c.clusters[user]
			if userClusters != nil {
				delete(userClusters, cluster)
				if len(userClusters) == 0 {
					delete(c.clusters, user)
				}
			}
			return true
		}

		// Store the received information into our cache
		c.electedLock.Lock()
		c.updateCache(user, cluster, replica)
		c.electedLock.Unlock()
		c.electedReplicaPropagationTime.Observe(time.Since(timestamp.Time(replica.ReceivedAt)).Seconds())
		return true
	})

	wg.Wait()
	return nil
}

const (
	cleanupCyclePeriod         = 30 * time.Minute
	cleanupCycleJitterVariance = 0.2 // for 30 minutes, this is ±6 min

	// If we have received last sample for given cluster before this timeout, we will mark selected replica for deletion.
	// If selected replica is marked for deletion for this time, it is deleted completely.
	deletionTimeout = 30 * time.Minute
)

func (c *haTracker) updateKVLoop(ctx context.Context) {
	cleanupTick := time.NewTicker(util.DurationWithJitter(cleanupCyclePeriod, cleanupCycleJitterVariance))
	defer cleanupTick.Stop()
	tick := time.NewTicker(c.cfg.UpdateTimeout)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case t := <-tick.C:
			c.updateKVStoreAll(ctx, t)
		case t := <-cleanupTick.C:
			c.cleanupRuns.Inc()
			c.cleanupOldReplicas(ctx, t.Add(-deletionTimeout))
		}
	}
}

// Loop over all entries in our cache and update KVStore where it is out of date,
// electing a new replica if necessary.
func (c *haTracker) updateKVStoreAll(ctx context.Context, now time.Time) {
	c.electedLock.RLock()
	defer c.electedLock.RUnlock()
	// Note the maps may change when we release the lock while talking to KVStore;
	// the Go language allows this: https://golang.org/ref/spec#For_range note 3.
	for userID, clusters := range c.clusters {
		for cluster, entry := range clusters {
			if c.withinUpdateTimeout(now, entry.elected.ReceivedAt) {
				continue // Some other process updated it recently; nothing to do.
			}
			var replica string
			if c.withinUpdateTimeout(now, entry.electedLastSeenTimestamp) {
				// We have seen the elected replica recently; carry on with that choice.
				replica = entry.elected.Replica
			} else if c.withinUpdateTimeout(now, entry.nonElectedLastSeenTimestamp) {
				// Not seen elected but have seen another: attempt to fail over.
				replica = entry.nonElectedLastSeenReplica
			} else {
				continue // we don't have any recent timestamps
			}
			// Release lock while we talk to KVStore, which could take a while.
			c.electedLock.RUnlock()
			err := c.updateKVStore(ctx, userID, cluster, replica, now)
			c.electedLock.RLock()
			if err != nil {
				// Failed to store - log it but carry on
				level.Error(c.logger).Log("msg", "failed to update KVStore", "err", err)
			}
		}
	}
}

// Replicas marked for deletion before deadline will be deleted.
// Replicas with last-received timestamp before deadline will be marked for deletion.
func (c *haTracker) cleanupOldReplicas(ctx context.Context, deadline time.Time) {
	keys, err := c.client.List(ctx, "")
	if err != nil {
		level.Warn(c.logger).Log("msg", "cleanup: failed to list replica keys", "err", err)
		return
	}

	for _, key := range keys {
		if ctx.Err() != nil {
			return
		}

		val, err := c.client.Get(ctx, key)
		if err != nil {
			level.Warn(c.logger).Log("msg", "cleanup: failed to get replica value", "key", key, "err", err)
			continue
		}

		desc, ok := val.(*ReplicaDesc)
		if !ok {
			level.Error(c.logger).Log("msg", "cleanup: got invalid replica descriptor", "key", key)
			continue
		}

		if desc.DeletedAt > 0 {
			if timestamp.Time(desc.DeletedAt).After(deadline) {
				continue
			}

			// We're blindly deleting a key here. It may happen that value was updated since we have read it few lines above,
			// in which case Distributors will have updated value in memory, but Delete will remove it from KV store anyway.
			// That's not great, but should not be a problem. If KV store sends Watch notification for Delete, distributors will
			// delete it from memory, and recreate on next sample with matching replica.
			//
			// If KV store doesn't send Watch notification for Delete, distributors *with* replica in memory will keep using it,
			// while distributors *without* replica in memory will try to write it to KV store -- which will update *all*
			// watching distributors.
			err = c.client.Delete(ctx, key)
			if err != nil {
				level.Error(c.logger).Log("msg", "cleanup: failed to delete old replica", "key", key, "err", err)
				c.markingForDeletionsFailed.Inc()
			} else {
				level.Info(c.logger).Log("msg", "cleanup: deleted old replica", "key", key)
				c.deletedReplicas.Inc()
			}
			continue
		}

		// Not marked as deleted yet.
		if desc.DeletedAt == 0 && timestamp.Time(desc.ReceivedAt).Before(deadline) {
			err := c.client.CAS(ctx, key, func(in interface{}) (out interface{}, retry bool, err error) {
				d, ok := in.(*ReplicaDesc)
				if !ok || d == nil || d.DeletedAt > 0 || !timestamp.Time(desc.ReceivedAt).Before(deadline) {
					return nil, false, nil
				}

				d.DeletedAt = timestamp.FromTime(time.Now())
				return d, true, nil
			})

			if err != nil {
				c.markingForDeletionsFailed.Inc()
				level.Error(c.logger).Log("msg", "cleanup: failed to mark replica as deleted", "key", key, "err", err)
			} else {
				c.replicasMarkedForDeletion.Inc()
				level.Info(c.logger).Log("msg", "cleanup: marked replica as deleted", "key", key)
			}
		}
	}
}

// checkReplica checks the cluster and replica against the local cache to see
// if we should accept the incomming sample. It will return replicasNotMatchError
// if we shouldn't store this sample but are accepting samples from another
// replica for the cluster.
// Updates to and from the KV store are handled in the background, except
// if we have no cached data for this cluster in which case we create the
// record and store it in-band.
func (c *haTracker) checkReplica(ctx context.Context, userID, cluster, replica string, now time.Time) error {
	// If HA tracking isn't enabled then accept the sample
	if !c.cfg.EnableHATracker {
		return nil
	}

	c.electedLock.Lock()
	if entry := c.clusters[userID][cluster]; entry != nil {
		var err error
		if entry.elected.Replica == replica {
			// Sample received is from elected replica: update timestamp and carry on.
			entry.electedLastSeenTimestamp = timestamp.FromTime(now)
		} else {
			// Sample received is from non-elected replica: record details and reject.
			entry.nonElectedLastSeenReplica = replica
			entry.nonElectedLastSeenTimestamp = timestamp.FromTime(now)
			err = replicasNotMatchError{replica: replica, elected: entry.elected.Replica}
		}
		c.electedLock.Unlock()
		return err
	}

	// We don't know about this cluster yet.
	nClusters := len(c.clusters[userID])
	c.electedLock.Unlock()
	// If we have reached the limit for number of clusters, error out now.
	if limit := c.limits.MaxHAClusters(userID); limit > 0 && nClusters+1 > limit {
		return tooManyClustersError{limit: limit}
	}

	err := c.updateKVStore(ctx, userID, cluster, replica, now)
	c.kvCASCalls.WithLabelValues(userID, cluster).Inc()
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to update KVStore - rejecting sample", "err", err)
		return err
	}
	// Cache will now have the value - recurse to check it again.
	return c.checkReplica(ctx, userID, cluster, replica, now)
}

func (c *haTracker) withinUpdateTimeout(now time.Time, receivedAt int64) bool {
	return now.Sub(timestamp.Time(receivedAt)) < c.cfg.UpdateTimeout+c.updateTimeoutJitter
}

// Must be called with electedLock held.
func (c *haTracker) updateCache(userID, cluster string, desc *ReplicaDesc) {
	if c.clusters[userID] == nil {
		c.clusters[userID] = map[string]*haClusterInfo{}
	}
	entry := c.clusters[userID][cluster]
	if entry == nil {
		entry = &haClusterInfo{}
		c.clusters[userID][cluster] = entry
	}
	if desc.Replica != entry.elected.Replica {
		c.electedReplicaChanges.WithLabelValues(userID, cluster).Inc()
	}
	entry.elected = *desc
	c.electedReplicaTimestamp.WithLabelValues(userID, cluster).Set(float64(desc.ReceivedAt / 1000))
}

// If we do set the value then err will be nil and desc will contain the value we set.
// If there is already a valid value in the store, return nil, nil.
func (c *haTracker) updateKVStore(ctx context.Context, userID, cluster, replica string, now time.Time) error {
	key := fmt.Sprintf("%s/%s", userID, cluster)
	var desc *ReplicaDesc
	err := c.client.CAS(ctx, key, func(in interface{}) (out interface{}, retry bool, err error) {
		var ok bool
		if desc, ok = in.(*ReplicaDesc); ok && desc.DeletedAt == 0 {
			// If the entry in KVStore is up-to-date, just stop the loop.
			if c.withinUpdateTimeout(now, desc.ReceivedAt) ||
				// If our replica is different, wait until the failover time.
				desc.Replica != replica && now.Sub(timestamp.Time(desc.ReceivedAt)) < c.cfg.FailoverTimeout {
				return nil, false, nil
			}
		}

		// Attempt to update KVStore to our timestamp and replica.
		desc = &ReplicaDesc{
			Replica:    replica,
			ReceivedAt: timestamp.FromTime(now),
			DeletedAt:  0,
		}
		return desc, true, nil
	})
	// If cache is currently empty, add the data we either stored or received from KVStore
	if err == nil && desc != nil {
		c.electedLock.Lock()
		if c.clusters[userID][cluster] == nil {
			c.updateCache(userID, cluster, desc)
		}
		c.electedLock.Unlock()
	}
	return err
}

type replicasNotMatchError struct {
	replica, elected string
}

func (e replicasNotMatchError) Error() string {
	return fmt.Sprintf("replicas did not mach, rejecting sample: replica=%s, elected=%s", e.replica, e.elected)
}

// Needed for errors.Is to work properly.
func (e replicasNotMatchError) Is(err error) bool {
	_, ok1 := err.(replicasNotMatchError)
	_, ok2 := err.(*replicasNotMatchError)
	return ok1 || ok2
}

// IsOperationAborted returns whether the error has been caused by an operation intentionally aborted.
func (e replicasNotMatchError) IsOperationAborted() bool {
	return true
}

type tooManyClustersError struct {
	limit int
}

func (e tooManyClustersError) Error() string {
	return fmt.Sprintf("too many HA clusters (limit: %d)", e.limit)
}

// Needed for errors.Is to work properly.
func (e tooManyClustersError) Is(err error) bool {
	_, ok1 := err.(tooManyClustersError)
	_, ok2 := err.(*tooManyClustersError)
	return ok1 || ok2
}

func findHALabels(replicaLabel, clusterLabel string, labels []mimirpb.LabelAdapter) (string, string) {
	var cluster, replica string
	var pair mimirpb.LabelAdapter

	for _, pair = range labels {
		if pair.Name == replicaLabel {
			replica = pair.Value
		}
		if pair.Name == clusterLabel {
			cluster = pair.Value
		}
	}

	return cluster, replica
}

func (c *haTracker) cleanupHATrackerMetricsForUser(userID string) {
	filter := map[string]string{"user": userID}

	if err := util.DeleteMatchingLabels(c.electedReplicaChanges, filter); err != nil {
		level.Warn(c.logger).Log("msg", "failed to remove cortex_ha_tracker_elected_replica_changes_total metric for user", "user", userID, "err", err)
	}
	if err := util.DeleteMatchingLabels(c.electedReplicaTimestamp, filter); err != nil {
		level.Warn(c.logger).Log("msg", "failed to remove cortex_ha_tracker_elected_replica_timestamp_seconds metric for user", "user", userID, "err", err)
	}
	if err := util.DeleteMatchingLabels(c.kvCASCalls, filter); err != nil {
		level.Warn(c.logger).Log("msg", "failed to remove cortex_ha_tracker_kv_store_cas_total metric for user", "user", userID, "err", err)
	}
}
