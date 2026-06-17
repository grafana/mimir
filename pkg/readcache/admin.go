// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"fmt"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// AdminPathPrefix is the URL prefix under which the readcache admin
// page is mounted. Kept here so URL dispatch and link generation
// from outside the package stay in sync.
const AdminPathPrefix = "/readcache"

// ServeHTTP renders the readcache admin page. The page is intentionally
// scoped to THIS readcache pod: it shows the partitions this instance
// currently owns, their per-partition L (head series), and a breakdown
// by current vs historical hash ranges so an operator can see at a
// glance whether a hot partition is growth on a current range or
// residue from a range that just moved off it.
//
// The view answers two questions the rebalancer page can't:
//  1. "What is THIS pod doing right now?" — useful when paged on a
//     single pod (high memory, slow queries) without the full
//     rebalancer round context.
//  2. "Is the L_pid signal residue or growth?" — by splitting the
//     per-range counts into current (growth) vs historical (residue
//     awaiting compaction).
func (r *Readcache) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	data := r.buildAdminPageData()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := adminTemplate.Execute(w, data); err != nil {
		http.Error(w, fmt.Sprintf("template error: %v", err), http.StatusInternalServerError)
	}
}

// adminPageData is the rendering input for the readcache admin page.
type adminPageData struct {
	InstanceID          string
	KafkaTopic          string
	RebalancerAddress   string
	OwnedPartitionsFlag string
	GeneratedAt         string

	NumPartitions          int
	NumTenants             int
	TotalHeadSeries        int64
	TotalCurrentSeries     int64
	TotalResidueSeries     int64
	TotalCurrentSampleRate float64 // Σ EWMA samples/sec over current ranges
	TotalResidueSampleRate float64 // Σ EWMA samples/sec over historical ranges
	HashSpaceCurrent       float64 // 0..100
	HashSpaceResidue       float64 // 0..100

	Partitions []adminPartitionView
}

// adminPartitionView is the per-partition row of the admin page.
type adminPartitionView struct {
	PartitionID          int32
	Warm                 bool
	HeadSeries           int64
	CurrentSeries        int64
	ResidueSeries        int64
	CurrentSampleRate    float64 // Σ EWMA samples/sec over current ranges
	ResidueSampleRate    float64 // Σ EWMA samples/sec over historical ranges
	NumTenants           int
	CurrentRangeCount    int
	HistoricalRangeCount int
	CurrentHashPct       float64
	HistoricalHashPct    float64
	Tenants              []string
	Current              []adminRangeView
	Historical           []adminRangeView
}

// adminRangeView is one (range, count) entry on the admin page.
type adminRangeView struct {
	Lo     uint32
	Hi     uint32
	Series int64
	// SampleRate is the samples-per-second EWMA observed for this
	// (partition, range), advanced once per loadstats.TickInterval
	// (15s). It is what the rebalancer balances on from v4 onward.
	// Zero is "no samples in the last few half-lives"; newly
	// adopted ranges read 0 for the first 15s while the EWMA
	// initialises.
	SampleRate float64
	SizeB      uint64  // raw size in hash units (Hi-Lo+1)
	SizeP      float64 // size as % of 32-bit hash space
	// Example is one representative series in this range, rendered
	// as labels.Labels.String() (e.g. `{__name__="up", instance="…"}`)
	// by the load-stats walker. Empty if the walker has not seen
	// any series in this range yet, or if the range only exists
	// because the partition was just assigned and no walk has
	// landed. Helps operators sanity-check that distributor
	// routing matches what the rebalancer assigned.
	Example string
}

func (r *Readcache) buildAdminPageData() adminPageData {
	r.partitionMu.RLock()
	parts := make([]*partitionState, 0, len(r.partitions))
	for _, p := range r.partitions {
		parts = append(parts, p)
	}
	r.partitionMu.RUnlock()

	hashSpaceTotal := float64(uint64(math.MaxUint32) + 1)
	data := adminPageData{
		InstanceID:          r.cfg.InstanceID,
		KafkaTopic:          r.cfg.KafkaTopic,
		RebalancerAddress:   r.cfg.RebalancerAddress,
		OwnedPartitionsFlag: r.cfg.OwnedPartitions,
		GeneratedAt:         time.Now().UTC().Format(time.RFC3339),
		NumPartitions:       len(parts),
	}

	tenantSet := make(map[string]struct{})
	views := make([]adminPartitionView, 0, len(parts))

	for _, p := range parts {
		var tenants []string
		p.tenantsMu.RLock()
		for tenantID := range p.tenants {
			tenants = append(tenants, tenantID)
			tenantSet[tenantID] = struct{}{}
		}
		// HeadSeries is the source-of-truth L_pid for this partition:
		// the actual sum of NumSeries across this partition's
		// per-tenant heads. Same number the walker reports via
		// PartitionActiveSeries, computed here on the fly so the
		// admin page never lags the walker's last tick.
		var headSeries int64
		for _, db := range p.tenants {
			headSeries += int64(db.Head().NumSeries())
		}
		p.tenantsMu.RUnlock()
		sort.Strings(tenants)

		current, historical := p.ranges.adminSnapshot()

		pv := adminPartitionView{
			PartitionID:          p.partitionID,
			Warm:                 p.warm.Load(),
			HeadSeries:           headSeries,
			NumTenants:           len(tenants),
			Tenants:              tenants,
			CurrentRangeCount:    len(current),
			HistoricalRangeCount: len(historical),
		}
		pv.Current = make([]adminRangeView, len(current))
		for i, c := range current {
			pv.Current[i] = makeRangeView(c.Range, c.Count, c.SampleRate, c.Example, hashSpaceTotal)
			pv.CurrentSeries += c.Count
			pv.CurrentSampleRate += c.SampleRate
			pv.CurrentHashPct += pv.Current[i].SizeP
		}
		pv.Historical = make([]adminRangeView, len(historical))
		for i, h := range historical {
			pv.Historical[i] = makeRangeView(h.Range, h.Count, h.SampleRate, h.Example, hashSpaceTotal)
			pv.ResidueSeries += h.Count
			pv.ResidueSampleRate += h.SampleRate
			pv.HistoricalHashPct += pv.Historical[i].SizeP
		}

		views = append(views, pv)
	}

	// Sort partitions by descending HeadSeries so the most operationally
	// interesting rows surface first, with a partition-id tiebreak for
	// determinism between refreshes.
	sort.Slice(views, func(i, j int) bool {
		if views[i].HeadSeries != views[j].HeadSeries {
			return views[i].HeadSeries > views[j].HeadSeries
		}
		return views[i].PartitionID < views[j].PartitionID
	})

	for _, pv := range views {
		data.TotalHeadSeries += pv.HeadSeries
		data.TotalCurrentSeries += pv.CurrentSeries
		data.TotalResidueSeries += pv.ResidueSeries
		data.TotalCurrentSampleRate += pv.CurrentSampleRate
		data.TotalResidueSampleRate += pv.ResidueSampleRate
		data.HashSpaceCurrent += pv.CurrentHashPct
		data.HashSpaceResidue += pv.HistoricalHashPct
	}
	data.NumTenants = len(tenantSet)
	data.Partitions = views
	return data
}

func makeRangeView(hr assignment.HashRange, series int64, sampleRate float64, example string, hashSpaceTotal float64) adminRangeView {
	size := hr.Size()
	return adminRangeView{
		Lo:         hr.Lo,
		Hi:         hr.Hi,
		Series:     series,
		SampleRate: sampleRate,
		SizeB:      size,
		SizeP:      float64(size) / hashSpaceTotal * 100,
		Example:    example,
	}
}
