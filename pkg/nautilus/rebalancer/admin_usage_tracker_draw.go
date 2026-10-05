// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"math"
	"sort"
	"strconv"
)

// The per-tenant page draws the tenant's whole hash space once: ranges at
// their real position and width on a strip, retained bands as a density
// histogram above it, and a zoomed copy around the selected move because a
// sub-band range is a fraction of a pixel at full scale.

const (
	drawWidth   = 1000.0 // SVG viewBox width
	histHeight  = 100.0
	stripTop    = 112.0
	stripHeight = 30.0
	drawHeight  = 150.0
	hashSpace   = float64(uint64(1) << 32)
)

type tenantDrawing struct {
	Strip      []drawRect
	Hist       []drawRect
	Outlines   []drawRect // one per booked move; the selected move is last and solid
	MaxDensity string
	Zoom       *zoomDrawing
	Rows       []bandTableRow
	RowsTotal  int
	Partitions []drawLegend
	Moves      []tenantMoveRow
	Selected   *tenantMoveRow
	MoveSeries string // series across every booked move for this tenant
}

type zoomDrawing struct {
	Lo, Hi     uint32
	Label      string
	Width      string
	Strip      []drawRect
	Hist       []drawRect
	Outlines   []drawRect
	MaxDensity string
}

type drawRect struct {
	X, Y, W, H float64
	Fill       string
	Class      string // "cut" for the selected move, "other" for the rest
	Title      string
}

type drawLegend struct {
	Partition int32
	Color     string
	Ranges    int
}

// tenantMoveRow is one booked move for the tenant page. Index is 1-based
// and is what ?move= selects.
type tenantMoveRow struct {
	Index     int
	Piece     string
	Width     string
	Parent    string
	From, To  int32
	FromColor string
	ToColor   string
	Series    string
	Selected  bool
	move      bandMove
}

type bandTableRow struct {
	Range     string
	Width     string
	Partition int32
	Color     string
	Series    string
	Rate      string
	Located   string
	HotShare  string
	Decision  string
	Moved     bool // the pass booked a move out of this range
	Selected  bool // parent of the selected move
	hotShare  float64
	located   uint64
}

// maxTableRows caps the ranges table. Tenants on the bootstrap path have
// tens of thousands of ranges; the drawing shows them all, the table shows
// the ones that matter.
const maxTableRows = 300

// minSeparatorWidth is how wide (in viewBox units) both neighbours must be
// before a boundary line is drawn between them. A line wider than the
// ranges it separates reads as a gap in the hash space, and there are no
// gaps: a tenant's ranges cover [0, 2^32). Where ranges are narrower than
// this the alternating shades carry the boundaries alone.
const minSeparatorWidth = 3.0

// partitionPalette has no red; red is the hot band. Colors are handed out
// per tenant in order of first appearance along the hash space, so
// neighbouring ranges differ even when the partition numbers happen to share
// a residue. The histogram is grey so it is never confused with a partition.
var partitionPalette = []string{
	"#4e79a7", "#f28e2b", "#59a14f", "#b07aa1", "#edc948", "#76b7b2", "#9c755f", "#ff9da7", "#17becf", "#8c564b",
	"#1f77b4", "#ff7f0e", "#2ca02c", "#9467bd", "#bcbd22", "#7f7f7f", "#aec7e8", "#ffbb78", "#98df8a", "#c5b0d5",
}

const (
	histFill      = "#8b8f96"
	hotFill       = "#e15759"
	unknownColor  = "#bbbbbb" // a destination partition this tenant has no range on
	separatorFill = "#ffffff"
)

// tenantColors assigns each partition the tenant touches a palette color by
// the order its first range appears on the strip.
func tenantColors(t *bandTenantView) map[int32]string {
	colors := map[int32]string{}
	for _, rg := range t.Ranges {
		if _, ok := colors[rg.Partition]; !ok {
			colors[rg.Partition] = partitionPalette[len(colors)%len(partitionPalette)]
		}
	}
	return colors
}

func colorFor(colors map[int32]string, pid int32) string {
	if c, ok := colors[pid]; ok {
		return c
	}
	return unknownColor
}

// tint lightens a #rrggbb color by blending it 45% toward white. Alternate
// ranges on the strip use the tint so two adjacent ranges on the same
// partition still show a boundary.
func tint(hex string) string {
	if len(hex) != 7 || hex[0] != '#' {
		return hex
	}
	v, err := strconv.ParseUint(hex[1:], 16, 32)
	if err != nil {
		return hex
	}
	mix := func(c uint64) uint64 { return c + (255-c)*45/100 }
	r, g, b := mix(v>>16&0xff), mix(v>>8&0xff), mix(v&0xff)
	return "#" + hex2(r) + hex2(g) + hex2(b)
}

func hex2(v uint64) string {
	s := strconv.FormatUint(v, 16)
	if len(s) < 2 {
		s = "0" + s
	}
	return s
}

type drawWindow struct {
	lo, hi float64 // inclusive hash bounds as floats
}

func (w drawWindow) span() float64 { return w.hi - w.lo + 1 }

// x maps a hash to the viewBox.
func (w drawWindow) x(h float64) float64 { return (h - w.lo) / w.span() * drawWidth }

// rect maps an inclusive hash interval to a viewBox rectangle at least one
// unit wide so it stays visible.
func (w drawWindow) rect(lo, hi uint32) (x, width float64, ok bool) {
	flo, fhi := float64(lo), float64(hi)
	if fhi < w.lo || flo > w.hi {
		return 0, 0, false
	}
	flo = math.Max(flo, w.lo)
	fhi = math.Min(fhi, w.hi)
	x = w.x(flo)
	width = (fhi - flo + 1) / w.span() * drawWidth
	if width < 1 {
		width = 1
	}
	if x+width > drawWidth {
		x = drawWidth - width
	}
	return x, width, true
}

// tenantMoves lists the pass's booked moves for one tenant, largest first.
func tenantMoves(t *bandTenantView, balance bandBalance, colors map[int32]string) []tenantMoveRow {
	var moves []bandMove
	for _, m := range balance.Moves {
		if m.Tenant == t.UserID {
			moves = append(moves, m)
		}
	}
	sort.SliceStable(moves, func(i, j int) bool {
		if moves[i].Series != moves[j].Series {
			return moves[i].Series > moves[j].Series
		}
		return moves[i].Lo < moves[j].Lo
	})
	rows := make([]tenantMoveRow, 0, len(moves))
	for i, m := range moves {
		rows = append(rows, tenantMoveRow{
			Index:     i + 1,
			Piece:     formatHexRange(m.Lo, m.Hi),
			Width:     formatBands(uint64(m.Hi) - uint64(m.Lo) + 1),
			Parent:    formatHexRange(m.ParentLo, m.ParentHi),
			From:      m.From,
			To:        m.To,
			FromColor: colorFor(colors, m.From),
			ToColor:   colorFor(colors, m.To),
			Series:    commas(m.Series),
			move:      m,
		})
	}
	return rows
}

// buildTenantDrawing draws the tenant. selected is the 1-based move index
// from the query string; out of range or zero selects the largest move.
func buildTenantDrawing(t *bandTenantView, balance bandBalance, selected int) tenantDrawing {
	full := drawWindow{lo: 0, hi: hashSpace - 1}
	colors := tenantColors(t)
	d := tenantDrawing{}
	d.Moves = tenantMoves(t, balance, colors)
	var total uint64
	for _, m := range d.Moves {
		total += m.move.Series
	}
	d.MoveSeries = commas(total)
	if len(d.Moves) > 0 {
		if selected < 1 || selected > len(d.Moves) {
			selected = 1
		}
		d.Moves[selected-1].Selected = true
		d.Selected = &d.Moves[selected-1]
	}
	d.Strip, d.Hist, d.Outlines, d.MaxDensity = drawInWindow(t, full, colors, d.Moves, d.Selected)
	d.Rows = buildBandTable(t, colors, d.Selected)
	d.RowsTotal = len(t.Ranges)
	d.Partitions = buildLegend(t, colors)
	if win, ok := zoomWindow(t, d.Selected); ok {
		z := &zoomDrawing{
			Lo:    uint32(win.lo),
			Hi:    uint32(win.hi),
			Label: formatHexRange(uint32(win.lo), uint32(win.hi)),
			Width: formatBands(uint64(win.span())),
		}
		z.Strip, z.Hist, z.Outlines, z.MaxDensity = drawInWindow(t, win, colors, d.Moves, d.Selected)
		d.Zoom = z
	}
	return d
}

func drawInWindow(t *bandTenantView, win drawWindow, colors map[int32]string, moves []tenantMoveRow, selected *tenantMoveRow) (strip, hist, outlines []drawRect, maxDensity string) {
	var hotLo, hotHi uint32
	var hasHot bool
	if selected != nil {
		hotLo, hotHi, hasHot = selected.move.HotLo, selected.move.HotHi, true
	} else if p := t.Proposal; p != nil && p.HotHi > p.HotLo {
		hotLo, hotHi, hasHot = p.HotLo, p.HotHi, true
	}

	// Density per band is what makes a narrow hot range stand out against
	// a wide lukewarm one. Estimated series on a mark are already scaled to
	// the mark's overlap, so divide by the overlap width in bands.
	type col struct {
		lo, hi  uint32
		density float64
		est     uint64
		hot     bool
	}
	var cols []col
	var maxD float64
	for _, rg := range t.Ranges {
		for _, m := range rg.Marks {
			bands := float64(uint64(m.Hi)-uint64(m.Lo)+1) / bandWidth
			if bands <= 0 {
				continue
			}
			dens := float64(m.Estimated) / bands
			if dens > maxD {
				maxD = dens
			}
			cols = append(cols, col{lo: m.Lo, hi: m.Hi, density: dens, est: m.Estimated, hot: hasHot && m.Lo == hotLo && m.Hi == hotHi})
		}
	}
	for _, c := range cols {
		x, w, ok := win.rect(c.lo, c.hi)
		if !ok {
			continue
		}
		h := 0.0
		if maxD > 0 {
			h = c.density / maxD * histHeight
		}
		if h < 1 {
			h = 1
		}
		fill := histFill
		if c.hot {
			fill = hotFill
		}
		hist = append(hist, drawRect{
			X: x, Y: histHeight - h, W: w, H: h, Fill: fill,
			Title: "band " + formatHexRange(c.lo, c.hi) + ": ~" + commas(c.est) + " series, " + strconv.FormatFloat(c.density, 'f', 0, 64) + " per band",
		})
	}
	maxDensity = strconv.FormatFloat(maxD, 'f', 0, 64)

	// Ranges are sorted by Lo, so alternating the tint along the slice
	// alternates it along the strip. Ranges narrower than a unit are
	// widened to one and overdraw each other; the selected move's parent is
	// drawn again at the end so the color under its outline is its own.
	var selectedRange *drawRect
	for i, rg := range t.Ranges {
		x, w, ok := win.rect(rg.Lo, rg.Hi)
		if !ok {
			continue
		}
		fill := colors[rg.Partition]
		if i%2 == 1 {
			fill = tint(fill)
		}
		r := drawRect{
			X: x, Y: stripTop, W: w, H: stripHeight, Fill: fill,
			Title: formatHexRange(rg.Lo, rg.Hi) + " on p" + itoa(rg.Partition) + ", " + rg.Width + "; readcache " + commas(uint64(max(rg.Series, 0))) + " series, " + strconvFormat(rg.Rate) + " samples/s; " + rg.Decision,
		}
		strip = append(strip, r)
		if selected != nil && rg.Lo == selected.move.ParentLo && rg.Hi == selected.move.ParentHi && rg.Partition == selected.move.From {
			cp := r
			selectedRange = &cp
		}
	}
	// A boundary line only where both neighbours are wide enough to still
	// show their color beside it. Otherwise lines pile up and read as gaps.
	n := len(strip)
	for i := 1; i < n; i++ {
		if strip[i-1].W >= minSeparatorWidth && strip[i].W >= minSeparatorWidth {
			strip = append(strip, drawRect{X: strip[i].X - 0.4, Y: stripTop, W: 0.8, H: stripHeight, Fill: separatorFill})
		}
	}
	if selectedRange != nil {
		strip = append(strip, *selectedRange)
	}

	for i := range moves {
		m := &moves[i]
		if m.Selected {
			continue
		}
		if x, w, ok := win.rect(m.move.Lo, m.move.Hi); ok {
			outlines = append(outlines, drawRect{X: x, Y: stripTop - 3, W: w, H: stripHeight + 6, Fill: "none", Class: "other", Title: "move " + itoa(int32(m.Index)) + ": " + m.Piece + " (" + m.Series + " series) p" + itoa(m.From) + " → p" + itoa(m.To)})
		}
	}
	if selected != nil {
		if x, w, ok := win.rect(selected.move.Lo, selected.move.Hi); ok {
			outlines = append(outlines, drawRect{X: x, Y: stripTop - 3, W: w, H: stripHeight + 6, Fill: "none", Class: "cut", Title: "selected move " + itoa(int32(selected.Index)) + ": " + selected.Piece + " (" + selected.Series + " series) p" + itoa(selected.From) + " → p" + itoa(selected.To)})
		}
	}
	return strip, hist, outlines, maxDensity
}

// zoomWindow picks the hash window to draw at readable scale: the selected
// move plus a margin, widened to include the parent range when the parent
// is small. Without a move it falls back to the range the tenant's
// proposal text is about, when there is one.
func zoomWindow(t *bandTenantView, selected *tenantMoveRow) (drawWindow, bool) {
	var lo, hi, parentLo, parentHi float64
	switch {
	case selected != nil:
		lo, hi = float64(selected.move.Lo), float64(selected.move.Hi)
		parentLo, parentHi = float64(selected.move.ParentLo), float64(selected.move.ParentHi)
	case t.Proposal != nil && t.Proposal.ParentHi > t.Proposal.ParentLo:
		lo, hi = float64(t.Proposal.ParentLo), float64(t.Proposal.ParentHi)
		parentLo, parentHi = lo, hi
	default:
		return drawWindow{}, false
	}
	pad := math.Max((hi-lo+1)*2, 2*bandWidth)
	lo -= pad
	hi += pad
	if parentHi-parentLo+1 <= 64*bandWidth {
		lo = math.Min(lo, parentLo-bandWidth)
		hi = math.Max(hi, parentHi+bandWidth)
	}
	lo = math.Max(lo, 0)
	hi = math.Min(hi, hashSpace-1)
	return drawWindow{lo: lo, hi: hi}, true
}

func buildBandTable(t *bandTenantView, colors map[int32]string, selected *tenantMoveRow) []bandTableRow {
	rows := make([]bandTableRow, 0, len(t.Ranges))
	for _, rg := range t.Ranges {
		sel := selected != nil && rg.Lo == selected.move.ParentLo && rg.Hi == selected.move.ParentHi && rg.Partition == selected.move.From
		rows = append(rows, bandTableRow{
			Range:     formatHexRange(rg.Lo, rg.Hi),
			Width:     rg.Width,
			Partition: rg.Partition,
			Color:     colors[rg.Partition],
			Series:    commas(uint64(max(rg.Series, 0))),
			Rate:      strconvFormat(rg.Rate),
			Located:   commas(rg.Located),
			HotShare:  strconv.FormatFloat(rg.HotShare*100, 'f', 0, 64) + "%",
			Decision:  rg.Decision,
			Moved:     rg.HasCut,
			Selected:  sel,
			hotShare:  rg.HotShare,
			located:   rg.Located,
		})
	}
	// Selected first, then moved ranges, then the most series. Hot share
	// alone is a poor key: every range narrower than a band is trivially 100%.
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Selected != rows[j].Selected {
			return rows[i].Selected
		}
		if rows[i].Moved != rows[j].Moved {
			return rows[i].Moved
		}
		if rows[i].located != rows[j].located {
			return rows[i].located > rows[j].located
		}
		return rows[i].hotShare > rows[j].hotShare
	})
	if len(rows) > maxTableRows {
		rows = rows[:maxTableRows]
	}
	return rows
}

func buildLegend(t *bandTenantView, colors map[int32]string) []drawLegend {
	counts := map[int32]int{}
	for _, rg := range t.Ranges {
		counts[rg.Partition]++
	}
	out := make([]drawLegend, 0, len(counts))
	for pid, n := range counts {
		out = append(out, drawLegend{Partition: pid, Color: colors[pid], Ranges: n})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })
	return out
}

// commas formats an integer with thousands separators.
func commas(v uint64) string {
	s := strconv.FormatUint(v, 10)
	if len(s) <= 3 {
		return s
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return string(out)
}

// signedCommas formats a difference with its sign.
func signedCommas(after, before uint64) string {
	switch {
	case after > before:
		return "+" + commas(after-before)
	case after < before:
		return "−" + commas(before-after)
	default:
		return "0"
	}
}
