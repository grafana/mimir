// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"context"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
)

const (
	bandWidth             = 1 << 16
	bandDominance         = 0.5
	shadowPartition int32 = 0
)

// bandReader is the usage-tracker read the shadow loop uses. Counts in the
// response cover one tracker partition; the rebalancer scales them.
type bandReader interface {
	GetTenantBands(ctx context.Context, partition int32, userID string) (*usagetrackerpb.GetTenantBandsResponse, error)
}

// bandPage is the last shadow snapshot the admin page renders. A browser
// refresh reads this; it does not start another tracker read.
type bandPage struct {
	At          time.Time
	Partition   int32
	Partitions  int32
	Bytes       int
	Duration    time.Duration
	Error       string
	Tenants     []bandTenantView
	ByPartition []bandPartitionRow
}

type bandTenantView struct {
	UserID            string
	ShardSeries       uint64
	EstimatedSeries   uint64
	LocalitySeries    uint64
	EstimatedLocality uint64
	Residual          uint64
	Unhashed          uint64
	Ranges            []bandRangeView
	Proposal          *bandProposal
}

type bandRangeView struct {
	Label     string
	Readcache string
	Marks     []bandMark
	CutLeft   float64
	CutWidth  float64
	HasCut    bool
	Partition int32
	span      float64
}

type bandMark struct {
	Left      float64
	Width     float64
	Estimated uint64
	Hot       bool
	Title     string
}

type bandProposal struct {
	Kind            string
	Reason          string
	FromPartition   int32
	ToPartition     int32
	ParentLo        uint32
	ParentHi        uint32
	ChildLo         uint32
	ChildHi         uint32
	EstimatedSeries uint64
	ReadcacheSeries int64
	ReadcacheRate   float64
	Text            string
}

type bandPartitionRow struct {
	Partition int32
	Segments  []bandSegment
}

type bandSegment struct {
	Width   float64
	Label   string
	Color   string
	Outline bool
}

func (r *Rebalancer) observeBands(ctx context.Context) {
	if r.bands == nil {
		return
	}
	start := time.Now()
	resp, err := r.bands.GetTenantBands(ctx, shadowPartition, "")
	elapsed := time.Since(start)
	if r.metrics != nil && r.metrics.bandReadDuration != nil {
		r.metrics.bandReadDuration.Observe(elapsed.Seconds())
	}
	if err != nil {
		if r.metrics != nil && r.metrics.bandReadFailures != nil {
			r.metrics.bandReadFailures.Inc()
		}
		level.Warn(r.logger).Log("msg", "locality band read failed", "err", err)
		r.admin.setBandPage(bandPage{At: time.Now(), Error: err.Error(), Duration: elapsed})
		return
	}
	size := proto.Size(resp)
	if r.metrics != nil && r.metrics.bandReadBytes != nil {
		r.metrics.bandReadBytes.Set(float64(size))
	}
	_, stats, _, rates := r.admin.snapshot()
	page := buildBandPage(time.Now(), resp, r.store.latestActiveAssignment(r.now()), stats, rates, size, elapsed)
	r.admin.setBandPage(page)
	for _, tenant := range page.Tenants {
		p := tenant.Proposal
		if p == nil || p.Kind != "move" {
			continue
		}
		level.Info(r.logger).Log(
			"msg", "locality shadow cut, not applied",
			"tenant", tenant.UserID,
			"from_partition", p.FromPartition,
			"to_partition", p.ToPartition,
			"parent_lo", p.ParentLo,
			"parent_hi", p.ParentHi,
			"child_lo", p.ChildLo,
			"child_hi", p.ChildHi,
			"estimated_series", p.EstimatedSeries,
			"readcache_series", p.ReadcacheSeries,
			"readcache_rate", p.ReadcacheRate,
			"reason", p.Reason,
		)
	}
}

func (s *adminState) setBandPage(page bandPage) {
	s.mu.Lock()
	s.bandPage = page
	s.mu.Unlock()
}

func (s *adminState) snapshotBandPage() bandPage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bandPage
}

// buildBandPage scales one tracker partition's bands by the partition count
// and proposes a cut. The proposal is never applied.
func buildBandPage(at time.Time, resp *usagetrackerpb.GetTenantBandsResponse, asg *assignment.Assignment, stats map[partitionRangeKey]rangeStatsView, rates map[int32]float64, size int, elapsed time.Duration) bandPage {
	page := bandPage{At: at, Bytes: size, Duration: elapsed}
	if resp == nil {
		page.Error = "no band snapshot yet"
		return page
	}
	page.Partition = resp.Partition
	page.Partitions = resp.Partitions
	scale := uint64(resp.Partitions)
	if resp.Partitions <= 0 {
		scale = 1
	}
	partRanges := map[int32][]bandSegment{}
	for _, tenant := range resp.Tenants {
		if tenant == nil {
			continue
		}
		view := bandTenantView{
			UserID:            tenant.UserID,
			ShardSeries:       tenant.TotalSeries,
			EstimatedSeries:   tenant.TotalSeries * scale,
			LocalitySeries:    tenant.LocalitySeries,
			EstimatedLocality: tenant.LocalitySeries * scale,
		}
		var bandSum uint64
		for _, c := range tenant.Counts {
			bandSum += c
		}
		bandSum *= scale
		if view.EstimatedSeries > bandSum {
			view.Residual = view.EstimatedSeries - bandSum
		}
		if tenant.TotalSeries > tenant.LocalitySeries {
			view.Unhashed = (tenant.TotalSeries - tenant.LocalitySeries) * scale
		}
		view.Ranges, view.Proposal = proposeTenant(tenant, scale, asg, stats, rates)
		page.Tenants = append(page.Tenants, view)
		for _, rg := range view.Ranges {
			partRanges[rg.Partition] = append(partRanges[rg.Partition], bandSegment{
				Width:   rg.span,
				Label:   tenant.UserID + " " + rg.Label,
				Color:   partitionColor(rg.Partition),
				Outline: rg.HasCut,
			})
		}
	}
	pids := make([]int32, 0, len(partRanges))
	for pid := range partRanges {
		pids = append(pids, pid)
	}
	sort.Slice(pids, func(i, j int) bool { return pids[i] < pids[j] })
	for _, pid := range pids {
		segs := partRanges[pid]
		var sum float64
		for _, seg := range segs {
			sum += seg.Width
		}
		if sum > 0 {
			for i := range segs {
				segs[i].Width = segs[i].Width / sum * 100
			}
		}
		page.ByPartition = append(page.ByPartition, bandPartitionRow{Partition: pid, Segments: segs})
	}
	return page
}

type tenantRangeDraw struct {
	bandRangeView
	partition int32
	span      float64
}

func proposeTenant(tenant *usagetrackerpb.TenantBands, scale uint64, asg *assignment.Assignment, stats map[partitionRangeKey]rangeStatsView, rates map[int32]float64) ([]bandRangeView, *bandProposal) {
	if tenant == nil {
		return nil, nil
	}
	var entries []assignment.Entry
	if asg != nil {
		for _, e := range asg.Entries {
			if e.TenantID == tenant.UserID {
				entries = append(entries, e)
			}
		}
	}
	draws := make([]tenantRangeDraw, 0, len(entries))
	var best *bandProposal
	for _, entry := range entries {
		draw, proposal := proposeRange(tenant, scale, entry, stats, rates)
		draws = append(draws, tenantRangeDraw{bandRangeView: draw, partition: entry.PartitionID, span: float64(entry.Range.Size())})
		if betterProposal(proposal, best) {
			best = proposal
		}
	}
	views := make([]bandRangeView, len(draws))
	for i := range draws {
		views[i] = draws[i].bandRangeView
	}
	if best == nil {
		if len(tenant.Bands) == 0 {
			best = &bandProposal{Kind: "no-bands", Reason: "no retained bands", Text: "no retained bands"}
		} else if len(entries) == 0 {
			best = &bandProposal{Kind: "no-assignment", Reason: "no assignment for this tenant", Text: "no assignment yet"}
		}
	}
	return views, best
}

func proposeRange(tenant *usagetrackerpb.TenantBands, scale uint64, entry assignment.Entry, stats map[partitionRangeKey]rangeStatsView, rates map[int32]float64) (bandRangeView, *bandProposal) {
	stat := stats[partitionRangeKey{tenantID: tenant.UserID, partitionID: entry.PartitionID, hr: entry.Range}]
	view := bandRangeView{
		Label:     formatHexRange(entry.Range.Lo, entry.Range.Hi) + " on p" + itoa(entry.PartitionID),
		Readcache: "readcache " + itoa64(stat.Series) + " series, " + strconvFormat(stat.SampleRate) + " samples/s",
		Partition: entry.PartitionID,
		span:      float64(entry.Range.Size()),
	}
	var located uint64
	var hot uint64
	var hotLo, hotHi uint32
	var hotFound bool
	span := float64(entry.Range.Size())
	for i, band := range tenant.Bands {
		if i >= len(tenant.Counts) {
			break
		}
		lo := uint32(uint16(band)) << 16
		hi := lo + bandWidth - 1
		if !entry.Range.Overlaps(lo, hi) {
			continue
		}
		ovLo := lo
		if entry.Range.Lo > ovLo {
			ovLo = entry.Range.Lo
		}
		ovHi := hi
		if entry.Range.Hi < ovHi {
			ovHi = entry.Range.Hi
		}
		fraction := float64(uint64(ovHi)-uint64(ovLo)+1) / float64(bandWidth)
		estimated := uint64(math.Round(float64(tenant.Counts[i]*scale) * fraction))
		located += estimated
		left := 0.0
		width := 100.0
		if span > 0 {
			left = float64(uint64(ovLo)-uint64(entry.Range.Lo)) / span * 100
			width = float64(uint64(ovHi)-uint64(ovLo)+1) / span * 100
		}
		hotMark := false
		if !hotFound || estimated > hot {
			hot = estimated
			hotLo, hotHi = ovLo, ovHi
			hotFound = true
			hotMark = true
			for j := range view.Marks {
				view.Marks[j].Hot = false
			}
		}
		view.Marks = append(view.Marks, bandMark{
			Left: left, Width: width, Estimated: estimated, Hot: hotMark,
			Title: formatHexRange(ovLo, ovHi) + " ~" + itoa64(int64(estimated)) + " series",
		})
	}
	proposal := &bandProposal{
		FromPartition:   entry.PartitionID,
		ParentLo:        entry.Range.Lo,
		ParentHi:        entry.Range.Hi,
		EstimatedSeries: hot,
		ReadcacheSeries: stat.Series,
		ReadcacheRate:   stat.SampleRate,
	}
	if !hotFound || located == 0 {
		proposal.Kind = "no-bands"
		proposal.Reason = "no retained band overlaps this range"
		proposal.Text = proposal.Reason
		return view, proposal
	}
	dominates := float64(hot) > bandDominance*float64(located)
	whole := entry.Range.Size() <= bandWidth
	if !dominates {
		proposal.Kind = "flat"
		proposal.Reason = "retained mass is flat; no cut"
		proposal.Text = "mass is flat across " + formatHexRange(entry.Range.Lo, entry.Range.Hi) + "; no cut (not applied)"
		return view, proposal
	}
	childLo, childHi := hotLo, hotHi
	if whole {
		childLo, childHi = entry.Range.Lo, entry.Range.Hi
	}
	proposal.ChildLo, proposal.ChildHi = childLo, childHi
	if span > 0 {
		view.CutLeft = float64(uint64(childLo)-uint64(entry.Range.Lo)) / span * 100
		view.CutWidth = float64(uint64(childHi)-uint64(childLo)+1) / span * 100
		view.HasCut = true
	}
	dest, colder := coldestOther(entry.PartitionID, rates)
	if !colder {
		proposal.Kind = "no-colder"
		proposal.Reason = "no colder partition"
		proposal.Text = "would cut " + formatHexRange(childLo, childHi) + " out of " + formatHexRange(entry.Range.Lo, entry.Range.Hi) + " but no colder partition (not applied)"
		return view, proposal
	}
	proposal.Kind = "move"
	proposal.ToPartition = dest
	if whole {
		proposal.Reason = "range is already one band; would move it whole"
	} else {
		proposal.Reason = "hottest band dominates; would cut it out and move it"
	}
	proposal.Text = "would move " + formatHexRange(childLo, childHi) + " from p" + itoa(entry.PartitionID) + " to p" + itoa(dest) + " (not applied)"
	return view, proposal
}

func coldestOther(source int32, rates map[int32]float64) (int32, bool) {
	sourceRate, ok := rates[source]
	best := int32(-1)
	bestRate := 0.0
	for pid, rate := range rates {
		if pid == source {
			continue
		}
		if best < 0 || rate < bestRate {
			best = pid
			bestRate = rate
		}
	}
	if best < 0 {
		return 0, false
	}
	if ok && bestRate >= sourceRate {
		return best, false
	}
	return best, true
}

func partitionColor(pid int32) string {
	colors := []string{"#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc948", "#b07aa1", "#ff9da7", "#9c755f", "#bab0ac"}
	if pid < 0 {
		pid = 0
	}
	return colors[int(pid)%len(colors)]
}

func strconvFormat(f float64) string {
	return strconv.FormatFloat(f, 'f', 1, 64)
}

func itoa(v int32) string {
	return strconvItoa(int64(v))
}

func itoa64(v int64) string {
	return strconvItoa(v)
}

func betterProposal(a, b *bandProposal) bool {
	if a == nil {
		return false
	}
	if b == nil {
		return true
	}
	if a.Kind == "move" && b.Kind != "move" {
		return true
	}
	if a.Kind != "move" && b.Kind == "move" {
		return false
	}
	return a.EstimatedSeries > b.EstimatedSeries
}

func strconvItoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
