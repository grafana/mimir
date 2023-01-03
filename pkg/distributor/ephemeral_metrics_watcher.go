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

func NewEphemeralWatcher(kv kv.Client, logger log.Logger) *ephemeralMetricsWatcher {
	w := &ephemeralMetricsWatcher{
		kv:          kv,
		log:         logger,
		userMetrics: map[string]*ephemeral.Metrics{},
	}
	w.Service = services.NewBasicService(nil, w.running, nil)
	return w
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

		w.userMetricsMtx.Lock()
		defer w.userMetricsMtx.Unlock()

		// Keep this value. This is a copy of what's in KV store.
		w.userMetrics[user] = em
		return true // keep watching
	})
	return nil
}

func (w *ephemeralMetricsWatcher) metricsMapForUser(user string) *ephemeral.Metrics {
	w.userMetricsMtx.RLock()
	defer w.userMetricsMtx.RUnlock()

	return w.userMetrics[user]
}
