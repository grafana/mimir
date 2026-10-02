// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerclient"
)

type bandLimits struct{}

func (bandLimits) MaxActiveOrGlobalSeriesPerUser(string) int { return 0 }

type bandRejectObserver struct{}

func (bandRejectObserver) ObserveAsyncUsageTrackerRejection(string) {}

// AttachUsageTracker starts a usage-tracker client used only to read locality
// bands. It does nothing unless band shadow is enabled.
func (r *Rebalancer) AttachUsageTracker(clientCfg usagetrackerclient.Config, partitionRing *ring.MultiPartitionInstanceRing, instanceRing ring.ReadRing, registerer prometheus.Registerer) {
	if r == nil || !r.cfg.BandShadowEnabled || partitionRing == nil || instanceRing == nil {
		return
	}
	client := usagetrackerclient.NewUsageTrackerClient(
		"nautilus-rebalancer",
		clientCfg,
		partitionRing,
		instanceRing,
		bandLimits{},
		log.With(r.logger, "component", "band-shadow"),
		registerer,
		bandRejectObserver{},
	)
	r.SetBandReader(client, client, r.cfg.BandReadInterval)
}
