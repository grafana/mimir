// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/objstore"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

type tenantDiscovery struct {
	bucketClient objstore.Bucket
	logger       log.Logger

	latest               atomic.Pointer[[]string]
	backgroundRefreshing atomic.Bool
	backgroundRefreshes  sync.WaitGroup

	refreshesFailed       prometheus.Counter
	lastSuccessfulRefresh prometheus.Gauge
}

func newTenantDiscovery(bucketClient objstore.Bucket, logger log.Logger, reg prometheus.Registerer) *tenantDiscovery {
	return &tenantDiscovery{
		bucketClient: bucketClient,
		logger:       logger,
		refreshesFailed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_block_cleanup_tenant_discovery_failed_total",
			Help: "Total number of failed tenant discoveries run by the blocks cleaner.",
		}),
		lastSuccessfulRefresh: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_block_cleanup_tenant_discovery_last_successful_run_timestamp_seconds",
			Help: "Unix timestamp of the last successful tenant discovery run by the blocks cleaner.",
		}),
	}
}

func (d *tenantDiscovery) tenants(ctx context.Context) ([]string, error) {
	if latest := d.latest.Load(); latest != nil {
		d.refreshInBackground(ctx)
		return *latest, nil
	}
	return d.refresh(ctx)
}

func (d *tenantDiscovery) refreshInBackground(ctx context.Context) {
	if !d.backgroundRefreshing.CompareAndSwap(false, true) {
		return
	}
	d.backgroundRefreshes.Go(func() {
		defer d.backgroundRefreshing.Store(false)
		if _, err := d.refresh(ctx); err != nil && !errors.Is(err, context.Canceled) {
			level.Warn(d.logger).Log("msg", "failed to refresh discovered tenants, keeping the previous ones", "err", err)
		}
	})
}

func (d *tenantDiscovery) refresh(ctx context.Context) ([]string, error) {
	tenants, err := mimir_tsdb.ListUsers(ctx, d.bucketClient)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			d.refreshesFailed.Inc()
		}
		return nil, fmt.Errorf("failed to discover users from bucket: %w", err)
	}

	d.latest.Store(&tenants)
	d.lastSuccessfulRefresh.SetToCurrentTime()
	return tenants, nil
}

func (d *tenantDiscovery) wait() {
	d.backgroundRefreshes.Wait()
}
