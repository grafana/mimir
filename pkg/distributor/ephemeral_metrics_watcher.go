package distributor

import (
	"context"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/services"

	"github.com/grafana/mimir/pkg/util/ephemeral"
)

type ephemeralMetricsWatcher struct {
	services.Service

	kv  kv.Client
	log log.Logger

	userMetricsMtx sync.RWMutex
	userMetrics    map[string]*ephemeral.Metrics
}

func newEphemeralWatcher(kv kv.Client, logger log.Logger) *ephemeralMetricsWatcher {
	w := &ephemeralMetricsWatcher{
		kv:          kv,
		log:         logger,
		userMetrics: map[string]*ephemeral.Metrics{},
	}
	w.Service = services.NewBasicService(w.starting, w.running, nil)
	return w
}

func (w *ephemeralMetricsWatcher) starting(ctx context.Context) error {
	if w.kv == nil {
		return nil
	}

	keys, err := w.kv.List(ctx, "")
	if err != nil {
		level.Info(w.log).Log("msg", "failed to list users with ephememeral metrics from KV", "err", err)
		return nil
	}

	for _, user := range keys {
		v, err := w.kv.Get(ctx, user)
		if err != nil {
			level.Info(w.log).Log("msg", "failed to fetch ephememeral metrics map for user", "user", user, "err", err)
		}

		em, _ := v.(*ephemeral.Metrics)
		if em != nil {
			w.setMetricsMapForUser(user, em)
		}
	}
	return nil
}

func (w *ephemeralMetricsWatcher) setMetricsMapForUser(user string, em *ephemeral.Metrics) {
	w.userMetricsMtx.Lock()
	defer w.userMetricsMtx.Unlock()
	w.userMetrics[user] = em
}

func (w *ephemeralMetricsWatcher) running(ctx context.Context) error {
	if w.kv == nil {
		<-ctx.Done()
		return nil
	}

	w.kv.WatchPrefix(ctx, "", func(user string, in interface{}) bool {
		em, ok := in.(*ephemeral.Metrics)
		if !ok || em == nil {
			// keep watching
			return true
		}

		level.Info(w.log).Log("msg", "updated key", "user", user, "metrics", len(em.EphemeralMetrics()))

		// Keep this value. This is a copy of what's in KV store.
		w.setMetricsMapForUser(user, em)
		return true // keep watching
	})
	return nil
}

func (w *ephemeralMetricsWatcher) metricsMapForUser(user string) *ephemeral.Metrics {
	w.userMetricsMtx.RLock()
	defer w.userMetricsMtx.RUnlock()

	return w.userMetrics[user]
}
