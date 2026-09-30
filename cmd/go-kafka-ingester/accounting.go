// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"sync/atomic"
	"time"

	"github.com/grafana/mimir/pkg/storage/seriesstore/metrics"
	"github.com/grafana/mimir/pkg/storage/seriesstore/store"
)

type accounting struct {
	activeSeriesUpdate time.Duration
	metadataRetainMs   int64
	headPeriod         time.Duration
	compactionInterval time.Duration
	ownedSeries        bool
	// Set once the startup replay finished, like the Go ingester leaving its Starting state.
	serving *atomic.Bool
}

// The Go ingester's hardcoded `HeadCompactionIntervalWhileStarting`.
const headCompactionIntervalWhileStarting = 30 * time.Second

// compactionSchedule is like the Go ingester's compaction loop: compactions wait for an interval,
// which is shorter while it replays at startup. The first check compacts, so a head rebuilt from
// segments, which has no min time yet, is compacted before it serves, like Go's head resumed from
// its WAL.
type compactionSchedule struct {
	last    time.Time
	hasLast bool
}

func (c *compactionSchedule) due(now time.Time, interval time.Duration, serving bool) bool {
	if !serving {
		interval = min(interval, headCompactionIntervalWhileStarting)
	}
	due := !c.hasLast || now.Sub(c.last) >= interval
	if due {
		c.last, c.hasLast = now, true
	}
	return due
}

// every runs work now and then every period, like a tokio interval.
func every(period time.Duration, work func()) {
	go func() {
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			work()
			<-ticker.C
		}
	}()
}

// spawnAccounting refreshes the ingester metrics on Mimir's schedules: active series every update
// period, the head, owned series and limit metrics every owned series interval, the ingestion rate
// every second, and purges metadata that was not seen recently.
func spawnAccounting(s *store.Store, config accounting) {
	every(config.activeSeriesUpdate, func() {
		metrics.ExportActiveSeries(s.ActiveSeriesReport())
	})
	schedule := &compactionSchedule{}
	every(config.headPeriod, func() {
		compact := schedule.due(time.Now(), config.compactionInterval, config.serving.Load())
		reports := s.HeadTick(compact, config.ownedSeries)
		overrides := s.Overrides()
		localLimits := make([]metrics.LocalSeriesLimit, len(reports))
		for index, report := range reports {
			tenantLimits := overrides.Tenant(report.Tenant)
			localLimits[index] = metrics.LocalSeriesLimit{Tenant: report.Tenant, Limit: uint64(overrides.MaxSeriesPerUser(&tenantLimits.Limits))}
		}
		metrics.ExportHead(reports, localLimits, overrides.InstanceLimits(), config.ownedSeries)
	})
	// Mimir's instance ingestion rate: an EWMA with alpha 0.2, ticked every second.
	rate := &metrics.IngestionRate{}
	every(time.Second, func() {
		metrics.IngestionRateGauge.Set(rate.Tick(s.IngestedSamples()))
	})
	// Mimir checks for stale metadata every five minutes.
	every(5*time.Minute, func() {
		s.PurgeMetadata(config.metadataRetainMs)
	})
}
