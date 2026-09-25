// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// delayedSeriesTracker counts series that are kept out of the TSDB head by the delayed_series limit,
// so limits and usage metering still see them. It keeps only a hash and a last-seen time per series.
type delayedSeriesTracker struct {
	idleTimeout time.Duration

	mu    sync.Mutex
	users map[string]map[uint64]int64

	series  *prometheus.GaugeVec
	samples *prometheus.CounterVec
}

func newDelayedSeriesTracker(idleTimeout time.Duration, reg prometheus.Registerer) *delayedSeriesTracker {
	return &delayedSeriesTracker{
		idleTimeout: idleTimeout,
		users:       map[string]map[uint64]int64{},
		series: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_delayed_series",
			Help: "Number of series kept out of the TSDB head by the delayed_series limit and seen within the active series idle timeout.",
		}, []string{"user"}),
		samples: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_delayed_samples_total",
			Help: "Total number of samples not appended to the TSDB head because their series matched the delayed_series limit.",
		}, []string{"user"}),
	}
}

func (t *delayedSeriesTracker) observe(userID string, series []mimirpb.PreallocTimeseries, now time.Time) {
	if len(series) == 0 {
		return
	}

	nowUnix := now.Unix()
	samples := 0

	t.mu.Lock()
	hashes, ok := t.users[userID]
	if !ok {
		hashes = map[uint64]int64{}
		t.users[userID] = hashes
	}
	for _, s := range series {
		hashes[seriesHash(s.Labels)] = nowUnix
		samples += len(s.Samples) + len(s.Histograms)
	}
	t.mu.Unlock()

	t.samples.WithLabelValues(userID).Add(float64(samples))
}

func (t *delayedSeriesTracker) purge(now time.Time) {
	deadline := now.Add(-t.idleTimeout).Unix()

	t.mu.Lock()
	defer t.mu.Unlock()

	for userID, hashes := range t.users {
		for h, lastSeen := range hashes {
			if lastSeen < deadline {
				delete(hashes, h)
			}
		}
		if len(hashes) == 0 {
			delete(t.users, userID)
			t.series.DeleteLabelValues(userID)
			continue
		}
		t.series.WithLabelValues(userID).Set(float64(len(hashes)))
	}
}

func seriesHash(lbls []mimirpb.LabelAdapter) uint64 {
	h := xxhash.New()
	for _, l := range lbls {
		_, _ = h.WriteString(l.Name)
		_, _ = h.WriteString("\xff")
		_, _ = h.WriteString(l.Value)
		_, _ = h.WriteString("\xff")
	}
	return h.Sum64()
}
