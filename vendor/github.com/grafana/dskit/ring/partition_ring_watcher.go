package ring

import (
	"context"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/services"
)

type PartitionRingWatcherConfig struct {
}

type PartitionRingWatcher struct {
	services.Service

	cfg    PartitionRingWatcherConfig
	key    string
	logger log.Logger

	kv kv.Client

	ringMx sync.Mutex
	ring   *PartitionRing
}

func NewPartitionRingWatcher(cfg PartitionRingWatcherConfig, kv kv.Client, key string, logger log.Logger, reg prometheus.Registerer) (*PartitionRingWatcher, error) {
	r := &PartitionRingWatcher{
		key:    key,
		cfg:    cfg,
		kv:     kv,
		logger: logger,
	}

	r.Service = services.NewBasicService(r.starting, r.loop, nil).WithName("partitions-ring-watcher")
	return r, nil
}

func (w *PartitionRingWatcher) starting(ctx context.Context) error {
	// Get the initial ring state so that, as soon as the service will be running, the in-memory
	// ring would be already populated and there's no race condition between when the service is
	// running and the WatchKey() callback is called for the first time.
	value, err := w.kv.Get(ctx, w.key)
	if err != nil {
		return errors.Wrap(err, "unable to initialise ring state")
	}

	if value != nil {
		w.updatePartitionRing(value.(*PartitionRingDesc))
	} else {
		level.Info(w.logger).Log("msg", "partition ring doesn't exist in KV store yet")
	}
	return nil
}

func (w *PartitionRingWatcher) loop(ctx context.Context) error {
	w.kv.WatchKey(ctx, w.key, func(value interface{}) bool {
		if value == nil {
			level.Info(w.logger).Log("msg", "partition ring doesn't exist in KV store yet")
			return true
		}

		w.updatePartitionRing(value.(*PartitionRingDesc))
		return true
	})
	return nil
}

func (w *PartitionRingWatcher) updatePartitionRing(desc *PartitionRingDesc) {
	newRing := NewPartitionRing(*desc)

	w.ringMx.Lock()
	defer w.ringMx.Unlock()

	w.ring = newRing
}

// GetRing TODO: replace with more fine-grained methods.
func (w *PartitionRingWatcher) GetRing() *PartitionRing {
	w.ringMx.Lock()
	defer w.ringMx.Unlock()

	return w.ring
}
