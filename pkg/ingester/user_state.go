// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/user_state.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"sync"

	"github.com/prometheus/common/model"
	"github.com/segmentio/fasthash/fnv1a"
)

// DiscardedSamples metric labels
const (
	perUserSeriesLimit   = "per_user_series_limit"
	perMetricSeriesLimit = "per_metric_series_limit"
)

const numMetricCounterShards = 128

type metricCounterShard struct {
	mtx sync.Mutex
	m   map[string]int
}

type metricCounter struct {
	limiter *Limiter
	shards  []metricCounterShard

	ignoredMetrics map[string]struct{}
}

func newMetricCounter(limiter *Limiter, ignoredMetricsForSeriesCount map[string]struct{}) *metricCounter {
	shards := make([]metricCounterShard, 0, numMetricCounterShards)
	for i := 0; i < numMetricCounterShards; i++ {
		shards = append(shards, metricCounterShard{
			m: map[string]int{},
		})
	}
	return &metricCounter{
		limiter: limiter,
		shards:  shards,

		ignoredMetrics: ignoredMetricsForSeriesCount,
	}
}

func (m *metricCounter) decreaseSeriesForMetric(metricName string) {
	shard := m.getShard(metricName)
	shard.mtx.Lock()
	defer shard.mtx.Unlock()

	shard.m[metricName]--
	if shard.m[metricName] == 0 {
		delete(shard.m, metricName)
	}
}

func (m *metricCounter) getShard(metricName string) *metricCounterShard {
	shard := &m.shards[hashFP(model.Fingerprint(fnv1a.HashString64(metricName)))%numMetricCounterShards]
	return shard
}

func (m *metricCounter) canAddSeriesFor(userID, metric string) error {
	if _, ok := m.ignoredMetrics[metric]; ok {
		return nil
	}

	shard := m.getShard(metric)
	shard.mtx.Lock()
	defer shard.mtx.Unlock()

	return m.limiter.AssertMaxSeriesPerMetric(userID, shard.m[metric])
}

func (m *metricCounter) increaseSeriesForMetric(metric string) {
	shard := m.getShard(metric)
	shard.mtx.Lock()
	shard.m[metric]++
	shard.mtx.Unlock()
}

// hashFP simply moves entropy from the most significant 48 bits of the
// fingerprint into the least significant 16 bits (by XORing) so that a simple
// MOD on the result can be used to pick a mutex while still making use of
// changes in more significant bits of the fingerprint. (The fast fingerprinting
// function we use is prone to only change a few bits for similar metrics. We
// really want to make use of every change in the fingerprint to vary mutex
// selection.)
func hashFP(fp model.Fingerprint) uint32 {
	return uint32(fp ^ (fp >> 32) ^ (fp >> 16))
}
