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
	Balance     bandBalance
}

// bandBalance is one coordinated shadow pass. Series are the retained
// bands folded through the assignment. A partition sheds only while it
// is over the mean, and each booked move is added to its destination
// before the next one is placed.
type bandBalance struct {
	Mean       uint64
	Partitions []bandPartitionFill
	Booked     int
}

type bandPartitionFill struct {
	Partition int32
	Before    uint64
	After     uint64
	Over      bool
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
	Label      string
	Width      string // range width in bands, for people who do not do hex in their head
	Lo, Hi     uint32
	Series     int64   // readcache series for the whole range, last slicer snapshot
	Rate       float64 // readcache samples/s, same snapshot
	Located    uint64  // estimated series in retained bands overlapping this range
	HotShare   float64 // hottest band's share of Located, 0..1
	Decision   string  // what the shadow would do with this range
	Kind       string  // proposal kind for this range
	Readcache  string
	HotCaption string
	Marks      []bandMark
	CutLeft    float64
	CutWidth   float64
	HasCut     bool
	Partition  int32
	span       float64
	// canMove is a dominant hot band this pass could cut. moveSeries is
	// the estimated series that cut would take with it.
	canMove    bool
	moveSeries uint64
	moveLo     uint32
	moveHi     uint32
}

type bandMark struct {
	Left      float64
	Width     float64
	Lo, Hi    uint32
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
	HotLo           uint32
	HotHi           uint32
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
		view.Ranges, view.Proposal = proposeTenant(tenant, scale, asg, stats)
		page.Tenants = append(page.Tenants, view)
	}
	page.Balance = balancePass(page.Tenants, rates)
	for _, view := range page.Tenants {
		for _, rg := range view.Ranges {
			partRanges[rg.Partition] = append(partRanges[rg.Partition], bandSegment{
				Width:   rg.span,
				Label:   view.UserID + " " + rg.Label,
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

func proposeTenant(tenant *usagetrackerpb.TenantBands, scale uint64, asg *assignment.Assignment, stats map[partitionRangeKey]rangeStatsView) ([]bandRangeView, *bandProposal) {
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
	if len(entries) == 0 {
		if len(tenant.Bands) == 0 {
			return nil, &bandProposal{Kind: "no-bands", Reason: "no retained bands", Text: "no retained bands"}
		}
		return nil, &bandProposal{Kind: "no-assignment", Reason: "no assignment for this tenant", Text: "no assignment yet"}
	}
	views := make([]bandRangeView, 0, len(entries))
	for _, entry := range entries {
		views = append(views, proposeRange(tenant, scale, entry, stats))
	}
	sort.SliceStable(views, func(i, j int) bool { return views[i].Lo < views[j].Lo })
	return views, nil
}

func proposeRange(tenant *usagetrackerpb.TenantBands, scale uint64, entry assignment.Entry, stats map[partitionRangeKey]rangeStatsView) bandRangeView {
	stat := stats[partitionRangeKey{tenantID: tenant.UserID, partitionID: entry.PartitionID, hr: entry.Range}]
	view := bandRangeView{
		Label:     formatHexRange(entry.Range.Lo, entry.Range.Hi) + " on p" + itoa(entry.PartitionID),
		Width:     formatBands(entry.Range.Size()),
		Lo:        entry.Range.Lo,
		Hi:        entry.Range.Hi,
		Series:    stat.Series,
		Rate:      stat.SampleRate,
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
			Left: left, Width: width, Lo: ovLo, Hi: ovHi, Estimated: estimated, Hot: hotMark,
			Title: formatHexRange(ovLo, ovHi) + " ~" + itoa64(int64(estimated)) + " series",
		})
	}
	view.Located = located
	if located > 0 {
		view.HotShare = float64(hot) / float64(located)
	}
	view.HotCaption = "no retained band overlaps this range"
	for _, m := range view.Marks {
		if !m.Hot {
			continue
		}
		n := itoa64(int64(m.Estimated))
		if m.Width >= 98 {
			view.HotCaption = "Red fills this bar: the hottest band covers the whole range (~" + n + " series)."
		} else {
			view.HotCaption = "Red is the hottest band (~" + n + " series), drawn where it sits in this range. Gray is the rest of the range."
		}
		break
	}
	if !hotFound || located == 0 {
		view.Kind = "no-bands"
		view.Decision = "no retained band overlaps this range"
		return view
	}
	dominates := float64(hot) > bandDominance*float64(located)
	if !dominates {
		view.Kind = "flat"
		view.Decision = "mass is flat across " + formatHexRange(entry.Range.Lo, entry.Range.Hi) + "; no cut (not applied)"
		return view
	}
	childLo, childHi := hotLo, hotHi
	series := hot
	if entry.Range.Size() <= bandWidth {
		childLo, childHi = entry.Range.Lo, entry.Range.Hi
		series = located
	}
	view.canMove = series > 0
	view.moveSeries = series
	view.moveLo, view.moveHi = childLo, childHi
	view.Kind = "cut"
	view.Decision = "dominant band; the pass has not booked it"
	return view
}

// balancePass folds retained-band series onto partitions and books cuts
// off partitions that sit over the mean. Each booking is visible to the
// next placement. A piece that fits in some partition's headroom lands on
// the coldest such partition. A piece bigger than every headroom lands on
// the current coldest partition, which later pieces then avoid.
func balancePass(tenants []bandTenantView, rates map[int32]float64) bandBalance {
	loads := map[int32]uint64{}
	for pid := range rates {
		loads[pid] = 0
	}
	for ti := range tenants {
		for _, rg := range tenants[ti].Ranges {
			loads[rg.Partition] += rg.Located
		}
	}
	if len(loads) == 0 {
		return bandBalance{}
	}
	pids := make([]int32, 0, len(loads))
	var total uint64
	for pid, n := range loads {
		pids = append(pids, pid)
		total += n
	}
	sort.Slice(pids, func(i, j int) bool { return pids[i] < pids[j] })
	mean := total / uint64(len(loads))
	before := make(map[int32]uint64, len(loads))
	for pid, n := range loads {
		before[pid] = n
	}

	booked := map[[2]int]bool{}
	skipped := map[[2]int]bool{}
	stuck := map[int32]bool{}
	var nBooked int
	for {
		src := int32(-1)
		var srcLoad uint64
		for _, pid := range pids {
			if stuck[pid] || loads[pid] <= mean {
				continue
			}
			if src < 0 || loads[pid] > srcLoad {
				src, srcLoad = pid, loads[pid]
			}
		}
		if src < 0 {
			break
		}
		bestTi, bestRi := -1, -1
		var bestSeries uint64
		var bestTenant string
		var bestLo uint32
		for ti := range tenants {
			for ri := range tenants[ti].Ranges {
				rg := tenants[ti].Ranges[ri]
				key := [2]int{ti, ri}
				if rg.Partition != src || !rg.canMove || booked[key] || skipped[key] {
					continue
				}
				if bestTi >= 0 && (rg.moveSeries < bestSeries ||
					(rg.moveSeries == bestSeries && (tenants[ti].UserID > bestTenant || (tenants[ti].UserID == bestTenant && rg.Lo >= bestLo)))) {
					continue
				}
				bestTi, bestRi = ti, ri
				bestSeries = rg.moveSeries
				bestTenant = tenants[ti].UserID
				bestLo = rg.Lo
			}
		}
		if bestTi < 0 || bestSeries == 0 {
			stuck[src] = true
			continue
		}
		// A cut bigger than the series still on the partition was
		// double-counted with one already booked. Try a smaller cut.
		if bestSeries > loads[src] {
			skipped[[2]int{bestTi, bestRi}] = true
			continue
		}
		dest := placePiece(src, bestSeries, loads, mean, pids)
		if dest < 0 {
			stuck[src] = true
			continue
		}
		loads[src] -= bestSeries
		loads[dest] += bestSeries
		booked[[2]int{bestTi, bestRi}] = true
		nBooked++
		bookCut(&tenants[bestTi], &tenants[bestTi].Ranges[bestRi], dest, bestSeries, mean)
	}

	for ti := range tenants {
		t := &tenants[ti]
		var reason *bandRangeView
		for ri := range t.Ranges {
			rg := &t.Ranges[ri]
			if booked[[2]int{ti, ri}] {
				continue
			}
			if rg.canMove {
				if before[rg.Partition] <= mean {
					rg.Kind = "under"
					rg.Decision = "p" + itoa(rg.Partition) + " is at or under the mean of " + strconv.FormatUint(mean, 10) + "; not moved"
				} else if loads[rg.Partition] <= mean {
					rg.Kind = "shed"
					rg.Decision = "hotter ranges on p" + itoa(rg.Partition) + " already brought it to the mean; not moved"
				} else {
					rg.Kind = "no-room"
					rg.Decision = "p" + itoa(rg.Partition) + " is still over the mean and this cut has nowhere to go (not applied)"
				}
				if reason == nil || !reason.canMove {
					reason = rg
				}
				continue
			}
			if reason == nil {
				reason = rg
			}
		}
		if t.Proposal != nil && t.Proposal.Kind == "move" {
			continue
		}
		if reason == nil {
			continue
		}
		t.Proposal = &bandProposal{
			Kind:            reason.Kind,
			Reason:          reason.Decision,
			FromPartition:   reason.Partition,
			ParentLo:        reason.Lo,
			ParentHi:        reason.Hi,
			HotLo:           reason.moveLo,
			HotHi:           reason.moveHi,
			EstimatedSeries: reason.moveSeries,
			ReadcacheSeries: reason.Series,
			ReadcacheRate:   reason.Rate,
			Text:            reason.Decision,
		}
	}

	out := make([]bandPartitionFill, 0, len(pids))
	for _, pid := range pids {
		if before[pid] == 0 && loads[pid] == 0 {
			continue
		}
		out = append(out, bandPartitionFill{
			Partition: pid,
			Before:    before[pid],
			After:     loads[pid],
			Over:      before[pid] > mean,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Before != out[j].Before {
			return out[i].Before > out[j].Before
		}
		return out[i].Partition < out[j].Partition
	})
	return bandBalance{Mean: mean, Partitions: out, Booked: nBooked}
}

func placePiece(source int32, series uint64, loads map[int32]uint64, mean uint64, pids []int32) int32 {
	fit := int32(-1)
	var fitLoad uint64
	cold := int32(-1)
	var coldLoad uint64
	for _, pid := range pids {
		if pid == source {
			continue
		}
		load := loads[pid]
		if cold < 0 || load < coldLoad || (load == coldLoad && pid < cold) {
			cold, coldLoad = pid, load
		}
		if load+series <= mean && (fit < 0 || load < fitLoad || (load == fitLoad && pid < fit)) {
			fit, fitLoad = pid, load
		}
	}
	if fit >= 0 {
		return fit
	}
	return cold
}

func bookCut(t *bandTenantView, rg *bandRangeView, dest int32, series, mean uint64) {
	rg.HasCut = true
	rg.Kind = "move"
	span := float64(uint64(rg.Hi) - uint64(rg.Lo) + 1)
	if span > 0 {
		rg.CutLeft = float64(uint64(rg.moveLo)-uint64(rg.Lo)) / span * 100
		rg.CutWidth = float64(uint64(rg.moveHi)-uint64(rg.moveLo)+1) / span * 100
	}
	text := "would move " + formatHexRange(rg.moveLo, rg.moveHi) + " (" + strconv.FormatUint(series, 10) + " series) from p" + itoa(rg.Partition) + " to p" + itoa(dest) + "; booked against the mean of " + strconv.FormatUint(mean, 10) + " (not applied)"
	rg.Decision = text
	p := &bandProposal{
		Kind:            "move",
		Reason:          "partition over the mean; cut booked onto a destination",
		FromPartition:   rg.Partition,
		ToPartition:     dest,
		ParentLo:        rg.Lo,
		ParentHi:        rg.Hi,
		ChildLo:         rg.moveLo,
		ChildHi:         rg.moveHi,
		HotLo:           rg.moveLo,
		HotHi:           rg.moveHi,
		EstimatedSeries: series,
		ReadcacheSeries: rg.Series,
		ReadcacheRate:   rg.Rate,
		Text:            text,
	}
	if t.Proposal == nil || t.Proposal.Kind != "move" || series > t.Proposal.EstimatedSeries {
		t.Proposal = p
	}
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

// formatBands says how wide a range is in bands (2^16 hashes each), with
// the share of the tenant's whole 2^32 hash space.
func formatBands(size uint64) string {
	bands := float64(size) / bandWidth
	share := float64(size) / float64(uint64(1)<<32) * 100
	var w string
	switch {
	case bands >= 100:
		w = strconv.FormatFloat(bands, 'f', 0, 64) + " bands"
	case bands >= 1:
		w = strconv.FormatFloat(bands, 'f', 1, 64) + " bands"
	default:
		w = "1/" + strconv.FormatFloat(1/bands, 'f', 0, 64) + " of a band"
	}
	var s string
	switch {
	case share >= 1:
		s = strconv.FormatFloat(share, 'f', 1, 64) + "%"
	case share >= 0.01:
		s = strconv.FormatFloat(share, 'f', 2, 64) + "%"
	default:
		s = "<0.01%"
	}
	return w + ", " + s + " of the hash space"
}

func itoa(v int32) string {
	return strconvItoa(int64(v))
}

func itoa64(v int64) string {
	return strconvItoa(v)
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
