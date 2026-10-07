// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	tsdbStateActive = "active"
	tsdbStateFrozen = "frozen"
)

// readcacheTSDBCollector reports the physical storage managed by a readcache,
// including frozen TSDBs whose per-TSDB Prometheus registries are deliberately
// unregistered. Keeping this collector aggregate and state-only avoids adding
// tenant or partition cardinality while making storage-shape A/B comparisons
// complete.
type readcacheTSDBCollector struct {
	readcache *Readcache

	tsdbs      *prometheus.Desc
	headSeries *prometheus.Desc
	blocks     *prometheus.Desc
	blockBytes *prometheus.Desc
}

func newReadcacheTSDBCollector(r *Readcache) *readcacheTSDBCollector {
	stateLabel := []string{"state"}
	return &readcacheTSDBCollector{
		readcache: r,
		tsdbs: prometheus.NewDesc(
			"cortex_readcache_managed_tsdbs",
			"Number of physical TSDBs managed by this readcache, including active and frozen stores.",
			stateLabel, nil,
		),
		headSeries: prometheus.NewDesc(
			"cortex_readcache_managed_tsdb_head_series",
			"Number of in-memory series across physical TSDBs managed by this readcache.",
			stateLabel, nil,
		),
		blocks: prometheus.NewDesc(
			"cortex_readcache_managed_tsdb_blocks",
			"Number of loaded blocks across physical TSDBs managed by this readcache.",
			stateLabel, nil,
		),
		blockBytes: prometheus.NewDesc(
			"cortex_readcache_managed_tsdb_block_bytes",
			"Number of bytes used by loaded blocks across physical TSDBs managed by this readcache.",
			stateLabel, nil,
		),
	}
}

func (c *readcacheTSDBCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.tsdbs
	ch <- c.headSeries
	ch <- c.blocks
	ch <- c.blockBytes
}

func (c *readcacheTSDBCollector) Collect(ch chan<- prometheus.Metric) {
	activeDBs := c.activeTSDBs()
	frozenDBs := c.frozenTSDBs()

	// A freeze detaches a DB from a live partition before publishing the
	// same pointer in r.frozen. If a scrape overlaps that transition, the
	// two snapshots can both contain it. Frozen wins so fleet sums never
	// spike by double-counting one physical store.
	frozenSet := make(map[*partitionTSDB]struct{}, len(frozenDBs))
	for _, db := range frozenDBs {
		frozenSet[db] = struct{}{}
	}

	active := snapshotTSDBs(activeDBs, frozenSet)
	frozen := snapshotTSDBs(frozenDBs, nil)
	c.emit(ch, tsdbStateActive, active)
	c.emit(ch, tsdbStateFrozen, frozen)
}

type tsdbStorageSnapshot struct {
	tsdbs      int
	headSeries uint64
	blocks     int
	blockBytes int64
}

func (c *readcacheTSDBCollector) activeTSDBs() []*partitionTSDB {
	c.readcache.partitionMu.RLock()
	parts := make([]*partitionState, 0, len(c.readcache.partitions))
	for _, p := range c.readcache.partitions {
		parts = append(parts, p)
	}
	c.readcache.partitionMu.RUnlock()

	var out []*partitionTSDB
	for _, p := range parts {
		p.tenantsMu.RLock()
		for _, db := range p.tenants {
			out = append(out, db)
		}
		p.tenantsMu.RUnlock()
	}
	return out
}

func (c *readcacheTSDBCollector) frozenTSDBs() []*partitionTSDB {
	var out []*partitionTSDB
	c.readcache.frozenMu.RLock()
	for _, epochs := range c.readcache.frozen {
		for _, epoch := range epochs {
			for _, db := range epoch.tenants {
				out = append(out, db)
			}
		}
	}
	c.readcache.frozenMu.RUnlock()
	return out
}

func snapshotTSDBs(dbs []*partitionTSDB, exclude map[*partitionTSDB]struct{}) tsdbStorageSnapshot {
	var out tsdbStorageSnapshot
	for _, db := range dbs {
		if _, ok := exclude[db]; ok {
			continue
		}
		snapshot, ok := db.storageSnapshot()
		if !ok {
			continue
		}
		out.tsdbs += snapshot.tsdbs
		out.headSeries += snapshot.headSeries
		out.blocks += snapshot.blocks
		out.blockBytes += snapshot.blockBytes
	}
	return out
}

func (c *readcacheTSDBCollector) emit(ch chan<- prometheus.Metric, state string, snapshot tsdbStorageSnapshot) {
	ch <- prometheus.MustNewConstMetric(c.tsdbs, prometheus.GaugeValue, float64(snapshot.tsdbs), state)
	ch <- prometheus.MustNewConstMetric(c.headSeries, prometheus.GaugeValue, float64(snapshot.headSeries), state)
	ch <- prometheus.MustNewConstMetric(c.blocks, prometheus.GaugeValue, float64(snapshot.blocks), state)
	ch <- prometheus.MustNewConstMetric(c.blockBytes, prometheus.GaugeValue, float64(snapshot.blockBytes), state)
}
