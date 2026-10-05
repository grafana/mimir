// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"encoding/json"
	"html/template"
	"net/http"
	"sort"
	"strconv"
	"time"
)

// The usage-tracker page shows what the rebalancer reads from one tracker
// partition every band-read interval, how those series fold onto Kafka
// partitions, and the moves the shadow pass would book. Nothing on it is
// applied. The index has a summary row, a Pass section by partition, and a
// Tenants section; ?user= opens one tenant's hash space and ?move= picks
// which of that tenant's booked moves is outlined and zoomed.

var usageTrackerTemplate = template.Must(template.New("usage-tracker").Parse(`<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Signals from Usage Tracker{{if .Tenant}} — {{.Tenant.UserID}}{{end}}</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;font-size:13px;line-height:1.4;color:#1a1a2e;background:#f4f5f7;padding:16px}
h1{font-size:18px;font-weight:600;margin-bottom:8px;color:#0b1426}
h2{font-size:14px;font-weight:600;margin:16px 0 8px;color:#333;border-bottom:1px solid #ddd;padding-bottom:4px}
a{color:#1971c2}
.nav{display:flex;gap:8px;margin-bottom:12px}
.nav a{display:inline-block;background:#fff;border:1px solid #e0e0e0;border-radius:6px;padding:6px 12px;font-size:12px;font-weight:600;color:#1971c2;text-decoration:none}
.nav a:hover{background:#e7f5ff;border-color:#1971c2}
.meta{color:#666;font-size:12px;margin:4px 0}
.err{color:#e03131}
.notice{display:inline-block;background:#fff4e6;border:1px solid #ffa94d;color:#9c4a00;border-radius:6px;padding:4px 10px;font-size:12px;font-weight:600;margin:6px 0 10px}
.summary{display:flex;flex-wrap:wrap;gap:8px;margin:12px 0 16px}
.stat{background:#fff;border:1px solid #e0e0e0;border-radius:6px;padding:8px 14px;min-width:120px}
.stat-label{font-size:11px;color:#666;text-transform:uppercase;letter-spacing:.5px}
.stat-value{font-size:20px;font-weight:700;color:#0b1426;font-variant-numeric:tabular-nums}
.stat-value.warn{color:#e67700}
.stat-sub{font-size:11px;color:#888}
details>summary{cursor:pointer;list-style:none}
details>summary::-webkit-details-marker{display:none}
.section{margin:16px 0 8px}
.section>summary{font-size:14px;font-weight:600;color:#333;border-bottom:1px solid #ddd;padding-bottom:4px}
.section>summary::before{content:"▸ ";color:#888;font-size:12px}
.section[open]>summary::before{content:"▾ "}
.section[open]>summary{margin-bottom:8px}
.body{background:#fff;border:1px solid #e0e0e0;border-radius:6px;padding:12px}
.partitions{display:flex;flex-direction:column;gap:6px}
.partition{background:#fff;border:1px solid #e0e0e0;border-radius:6px;overflow:hidden}
.part-header{display:flex;align-items:center;gap:12px;padding:8px 12px;user-select:none}
.part-header:hover{background:#f8f9fa}
.part-id{font-weight:700;font-size:14px;min-width:48px;color:#0b1426}
.part-stats{display:flex;gap:16px;font-size:12px;color:#444;flex:1;flex-wrap:wrap;font-variant-numeric:tabular-nums}
.part-stats span{white-space:nowrap}
.part-stats .over{color:#e03131;font-weight:600}
.part-stats .delta-up{color:#e67700}
.part-stats .delta-down{color:#087f5b}
.bars{display:flex;flex-direction:column;gap:2px;width:160px;flex-shrink:0}
.bar{height:6px;background:#eee;border-radius:3px;overflow:hidden;position:relative}
.bar i{display:block;height:100%;border-radius:3px}
.bar .before{background:#adb5bd}
.bar .after{background:#4dabf7}
.bar .mean{position:absolute;top:-2px;bottom:-2px;width:1px;background:#e03131}
.part-moves{padding:4px 12px 10px;border-top:1px solid #f0f0f0}
.part-moves h3{font-size:12px;color:#666;margin:8px 0 4px;text-transform:uppercase;letter-spacing:.5px}
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:8px}
.card{display:block;background:#fff;border:1px solid #e0e0e0;border-radius:6px;padding:10px 12px;text-decoration:none;color:inherit}
.card:hover{border-color:#1971c2}
.card.move{border-left:4px solid #4dabf7}
.card h2{font-size:14px;margin:0 0 4px;border:none;padding:0}
.card p{margin:2px 0;font-size:12px;color:#444}
.proposal{font-size:14px;margin:8px 0;padding:8px 12px;background:#e7f5ff;border:1px solid #4dabf7;border-radius:6px}
.legend{font-size:12px;line-height:1.5;max-width:860px;color:#444}
.swatch{display:inline-block;width:14px;height:10px;vertical-align:-1px;margin-right:5px;border-radius:2px}
.chart{margin:12px 0 4px}
.chart svg{display:block;width:100%;height:150px}
.axis{display:flex;justify-content:space-between;font-family:"SF Mono",Consolas,monospace;font-size:11px;color:#666}
.ylabel{font-size:11px;color:#666;margin-bottom:2px}
.parts{font-size:12px;margin:6px 0 12px;color:#444}
.parts span{margin-right:12px;white-space:nowrap}
rect.cut{stroke:#111;stroke-width:2}
rect.other{stroke:#111;stroke-width:1;stroke-dasharray:3 2}
table{border-collapse:collapse;font-size:12px;margin-top:6px;width:100%}
th,td{text-align:left;padding:4px 10px 4px 0;border-bottom:1px solid #f0f0f0;vertical-align:top}
th{color:#666;font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.5px}
td.num,th.num{text-align:right;font-variant-numeric:tabular-nums}
td.mono{font-family:"SF Mono",Consolas,monospace}
tr.moved td{background:#f3f9ff}
tr.selected td{background:#e7f5ff;font-weight:600}
.table-wrap{max-height:560px;overflow:auto}
</style></head><body>
<div class="nav"><a href="{{.Prefix}}/">&larr; Rebalancer</a>{{if .Tenant}}<a href="{{.Prefix}}/usage-tracker">All tenants</a>{{end}}</div>
<h1>Signals from Usage Tracker{{if .Tenant}}: {{.Tenant.UserID}}{{end}}</h1>
<div class="notice">Shadow only — nothing on this page is applied.</div>
<p class="meta">Every {{.Interval}} the rebalancer reads one usage-tracker partition's locality bands and runs the pass below. Refreshing redraws the last read; it does not read the tracker again.</p>
{{if .Waiting}}<p>Waiting for the first read.</p>{{end}}
{{if .Page.Error}}<p class="err">{{.Page.Error}}</p>{{end}}
{{if not .Waiting}}
<p class="meta">Read at {{.When}} from tracker partition {{.Page.Partition}} of {{.Page.Partitions}}. Response {{.Bytes}} bytes in {{.Page.Duration}}. Counts from that one partition are scaled by {{.Page.Partitions}}.</p>

{{if .Tenant}}
{{with .Tenant}}
<div class="summary">
	<div class="stat" title="One tracker partition's series for this tenant, scaled by the partition count."><div class="stat-label">Estimated series</div><div class="stat-value">{{$.TenantStats.Estimated}}</div><div class="stat-sub">{{$.TenantStats.Locality}} carry a locality hash</div></div>
	<div class="stat" title="Series outside the 256 retained bands have no position on the drawing."><div class="stat-label">Outside retained bands</div><div class="stat-value">{{$.TenantStats.Residual}}</div></div>
	<div class="stat"><div class="stat-label">Ranges</div><div class="stat-value">{{len .Ranges}}</div><div class="stat-sub">on {{len $.Draw.Partitions}} partitions</div></div>
	<div class="stat"><div class="stat-label">Moves booked</div><div class="stat-value{{if $.Draw.Moves}} warn{{end}}">{{len $.Draw.Moves}}</div><div class="stat-sub">{{$.Draw.MoveSeries}} series would move</div></div>
</div>
{{end}}
{{with .Draw.Selected}}<p class="proposal">Move {{.Index}} of {{len $.Draw.Moves}}: <b>{{.Piece}}</b> ({{.Series}} series, {{.Width}}) from <span class="swatch" style="background:{{.FromColor}}"></span>p{{.From}} to <span class="swatch" style="background:{{.ToColor}}"></span>p{{.To}}. It is the hottest band of range {{.Parent}}. Not applied.</p>{{else}}{{if $.Tenant.Proposal}}<p class="proposal">{{$.Tenant.Proposal.Text}}</p>{{end}}{{end}}

<p class="legend">The tenant's whole hash space, 0 on the left to 2<sup>32</sup> on the right. The <b>strip</b> is the ranges the tenant owns, at their real position and width, colored by Kafka partition; every other range is a lighter shade of its partition's color. Ranges cover the whole space: a thin white line is a boundary between two wide ranges, not a gap, and where ranges are too narrow for a line the alternating shades mark the boundaries. The grey <b>columns</b> above are the retained bands where the tracker saw series; height is series per band, so a narrow hot range stands tall and a wide cool one stays low. <span class="swatch" style="background:#e15759"></span>The red column is the band the selected move is based on. <span class="swatch" style="background:#fff;border:2px solid #111"></span>The solid outline is the selected move; <span class="swatch" style="background:#fff;border:1px dashed #111"></span>dashed outlines are this tenant's other booked moves. Ranges thinner than a pixel are widened to one, so at full scale a neighbour can hide under an outline; the zoom shows the true widths. Hover anything for numbers.</p>

<div class="chart">
<div class="ylabel">series per band, tallest = {{.Draw.MaxDensity}}</div>
<svg viewBox="0 0 1000 150" preserveAspectRatio="none">
<line x1="0" y1="100" x2="1000" y2="100" stroke="#ddd" stroke-width="0.5"/>
{{range .Draw.Hist}}<rect x="{{.X}}" y="{{.Y}}" width="{{.W}}" height="{{.H}}" fill="{{.Fill}}"><title>{{.Title}}</title></rect>{{end}}
{{range .Draw.Strip}}<rect x="{{.X}}" y="{{.Y}}" width="{{.W}}" height="{{.H}}" fill="{{.Fill}}">{{if .Title}}<title>{{.Title}}</title>{{end}}</rect>{{end}}
{{range .Draw.Outlines}}<rect class="{{.Class}}" x="{{.X}}" y="{{.Y}}" width="{{.W}}" height="{{.H}}" fill="none"><title>{{.Title}}</title></rect>{{end}}
</svg>
<div class="axis"><span>00000000</span><span>40000000</span><span>80000000</span><span>c0000000</span><span>ffffffff</span></div>
</div>
<div class="parts">{{range .Draw.Partitions}}<span><span class="swatch" style="background:{{.Color}}"></span>p{{.Partition}} · {{.Ranges}} range{{if ne .Ranges 1}}s{{end}}</span>{{end}}</div>

{{with .Draw.Zoom}}
<h2>Zoomed to {{if $.Draw.Selected}}move {{$.Draw.Selected.Index}}{{else}}the range in question{{end}}</h2>
<p class="meta">{{.Label}} — {{.Width}}. Same drawing, same colors, over just this window.</p>
<div class="chart">
<div class="ylabel">series per band, tallest = {{.MaxDensity}}</div>
<svg viewBox="0 0 1000 150" preserveAspectRatio="none">
<line x1="0" y1="100" x2="1000" y2="100" stroke="#ddd" stroke-width="0.5"/>
{{range .Hist}}<rect x="{{.X}}" y="{{.Y}}" width="{{.W}}" height="{{.H}}" fill="{{.Fill}}"><title>{{.Title}}</title></rect>{{end}}
{{range .Strip}}<rect x="{{.X}}" y="{{.Y}}" width="{{.W}}" height="{{.H}}" fill="{{.Fill}}">{{if .Title}}<title>{{.Title}}</title>{{end}}</rect>{{end}}
{{range .Outlines}}<rect class="{{.Class}}" x="{{.X}}" y="{{.Y}}" width="{{.W}}" height="{{.H}}" fill="none"><title>{{.Title}}</title></rect>{{end}}
</svg>
<div class="axis"><span>{{printf "%08x" .Lo}}</span><span>{{printf "%08x" .Hi}}</span></div>
</div>
{{end}}

<details class="section">
<summary>Moves — {{len .Draw.Moves}} booked for this tenant, {{.Draw.MoveSeries}} series</summary>
<div class="body">
{{if .Draw.Moves}}
<p class="meta">Largest first. Pick one to outline and zoom to it.</p>
<div class="table-wrap"><table>
<tr><th>#</th><th>piece</th><th>width</th><th>from</th><th>to</th><th class="num">series</th><th>parent range</th></tr>
{{range .Draw.Moves}}<tr{{if .Selected}} class="selected"{{end}}><td><a href="{{$.Prefix}}/usage-tracker?user={{$.Tenant.UserID}}&amp;move={{.Index}}">{{.Index}}</a></td><td class="mono">{{.Piece}}</td><td>{{.Width}}</td><td><span class="swatch" style="background:{{.FromColor}}"></span>p{{.From}}</td><td><span class="swatch" style="background:{{.ToColor}}"></span>p{{.To}}</td><td class="num">{{.Series}}</td><td class="mono">{{.Parent}}</td></tr>
{{end}}
</table></div>
{{else}}<p class="meta">The pass booked no move for this tenant.{{if .Tenant.Proposal}} {{.Tenant.Proposal.Text}}{{end}}</p>{{end}}
</div>
</details>

<details class="section">
<summary>Ranges — {{.Draw.RowsTotal}}{{if gt .Draw.RowsTotal (len .Draw.Rows)}}, showing {{len .Draw.Rows}}{{end}}</summary>
<div class="body">
<p class="meta">{{if gt .Draw.RowsTotal (len .Draw.Rows)}}The selected move's range first, then ranges with a booked move, then the most series, up to {{len .Draw.Rows}} rows. The drawing above has all of them. {{end}}Located is the estimated series in retained bands that overlap the range. Hot share is the hottest band's part of that. The pass books a cut when that share is above 50% and the partition is over the mean; a range narrower than a band moves whole. Readcache numbers are the last slicer snapshot, minutes behind the read.</p>
<div class="table-wrap"><table>
<tr><th>range</th><th>width</th><th>partition</th><th class="num">readcache series</th><th class="num">samples/s</th><th class="num">located</th><th class="num">hot share</th><th>pass</th></tr>
{{range .Draw.Rows}}<tr{{if .Selected}} class="selected"{{else if .Moved}} class="moved"{{end}}><td class="mono">{{.Range}}</td><td>{{.Width}}</td><td><span class="swatch" style="background:{{.Color}}"></span>p{{.Partition}}</td><td class="num">{{.Series}}</td><td class="num">{{.Rate}}</td><td class="num">{{.Located}}</td><td class="num">{{.HotShare}}</td><td>{{.Decision}}</td></tr>
{{end}}
</table></div>
</div>
</details>

{{else}}
<div class="summary">
	<div class="stat" title="Partitions with any retained-band series folded onto them, before or after the pass. The rest of the active partitions hold none and are not listed."><div class="stat-label">Partitions</div><div class="stat-value">{{len .Pass.Rows}}</div><div class="stat-sub">of {{.Page.Balance.Active}} active · {{.Pass.OverCount}} over the mean before</div></div>
	<div class="stat" title="Total folded series divided by every active partition, including those with none. The pass sheds a partition only while it is above this."><div class="stat-label">Mean series</div><div class="stat-value">{{.Pass.Mean}}</div><div class="stat-sub">over {{.Page.Balance.Active}} active partitions</div></div>
	<div class="stat" title="Every booked cut. A tenant can have many."><div class="stat-label">Moves booked</div><div class="stat-value{{if .Page.Balance.Booked}} warn{{end}}">{{.Page.Balance.Booked}}</div><div class="stat-sub">{{.Pass.MovingSeries}} series would move</div></div>
	<div class="stat"><div class="stat-label">Tenants</div><div class="stat-value">{{len .Tenants}}</div><div class="stat-sub">{{.TenantsWithMoves}} with a booked move</div></div>
	<div class="stat" title="Retained-band series across all tenants, scaled by the partition count."><div class="stat-label">Series folded</div><div class="stat-value">{{.Pass.Total}}</div></div>
</div>

<details class="section">
<summary>Pass — {{.Page.Balance.Booked}} moves booked across {{len .Pass.Rows}} partitions</summary>
<div class="body">
<p class="meta">Every partition that holds retained-band series, by partition id; the other active partitions hold none and are left out. Before and after are folded series. The pass takes the hottest cut off the partition furthest over the mean, books it onto the coldest partition with room under the mean (or the coldest overall when nothing has room), and repeats until no partition over the mean has a cut left. A partition's load changes once per move that touches it, so many partitions can change with few moves. Expand a partition for the moves out of it and into it.</p>
<div class="partitions">
{{range .Pass.Rows}}
<details class="partition"><summary class="part-header">
	<span class="part-id">p{{.Partition}}</span>
	<span class="part-stats"><span>before {{.Before}}</span><span>after {{.After}}</span><span class="{{if .Up}}delta-up{{else if .Down}}delta-down{{end}}">{{.Delta}}</span>{{if .Over}}<span class="over">over the mean before</span>{{end}}<span>{{len .Out}} out · {{len .In}} in</span></span>
	<span class="bars" title="grey: before, blue: after, red line: mean"><span class="bar"><i class="before" style="width:{{.BarBefore}}%"></i><span class="mean" style="left:{{$.Pass.BarMean}}%"></span></span><span class="bar"><i class="after" style="width:{{.BarAfter}}%"></i><span class="mean" style="left:{{$.Pass.BarMean}}%"></span></span></span>
</summary>
<div class="part-moves">
{{if .Out}}<h3>Out of p{{.Partition}} ({{.OutSeries}} series)</h3><table>
<tr><th>tenant</th><th>piece</th><th>width</th><th>to</th><th class="num">series</th></tr>
{{range .Out}}<tr><td><a href="{{$.Prefix}}/usage-tracker?user={{.Tenant}}&amp;move={{.TenantIndex}}">{{.Tenant}}</a></td><td class="mono">{{.Piece}}</td><td>{{.Width}}</td><td>p{{.To}}</td><td class="num">{{.Series}}</td></tr>
{{end}}
</table>{{end}}
{{if .In}}<h3>Into p{{.Partition}} ({{.InSeries}} series)</h3><table>
<tr><th>tenant</th><th>piece</th><th>width</th><th>from</th><th class="num">series</th></tr>
{{range .In}}<tr><td><a href="{{$.Prefix}}/usage-tracker?user={{.Tenant}}&amp;move={{.TenantIndex}}">{{.Tenant}}</a></td><td class="mono">{{.Piece}}</td><td>{{.Width}}</td><td>p{{.From}}</td><td class="num">{{.Series}}</td></tr>
{{end}}
</table>{{end}}
{{if and (not .Out) (not .In)}}<p class="meta">No move touches this partition.</p>{{end}}
</div></details>
{{end}}
</div>
</div>
</details>

<details class="section">
<summary>Tenants — {{len .Tenants}}, {{.TenantsWithMoves}} with a booked move</summary>
<div class="body">
<p class="meta">Tenants with booked moves first, then by estimated series. Open one to see its hash space.</p>
<div class="cards">
{{range .Tenants}}
<a class="card{{if .Moves}} move{{end}}" href="{{$.Prefix}}/usage-tracker?user={{.UserID}}">
<h2>{{.UserID}}</h2>
<p>{{.Estimated}} estimated series · {{.Ranges}} ranges</p>
<p>{{if .Moves}}{{.Moves}} move{{if ne .Moves 1}}s{{end}} booked, {{.MoveSeries}} series{{else if .Text}}{{.Text}}{{end}}</p>
</a>
{{end}}
</div>
</div>
</details>
{{end}}
{{end}}
</body></html>`))

type usageTrackerHTMLData struct {
	Prefix           string
	Interval         string
	Waiting          bool
	When             string
	Bytes            string
	Page             bandPage
	Pass             passView
	Tenants          []tenantCard
	TenantsWithMoves int
	Tenant           *bandTenantView
	TenantStats      tenantStats
	Draw             tenantDrawing
}

type tenantStats struct {
	Estimated string
	Locality  string
	Residual  string
}

type tenantCard struct {
	UserID     string
	Estimated  string
	Ranges     int
	Moves      int
	MoveSeries string
	Text       string
	series     uint64
}

// passView is the Pass section: every partition with folded series, in
// partition order, with the moves out of it and into it.
type passView struct {
	Mean         string
	Total        string
	MovingSeries string
	OverCount    int
	BarMean      float64 // mean as a percent of the largest before/after
	Rows         []passPartitionView
}

type passPartitionView struct {
	Partition int32
	Before    string
	After     string
	Delta     string
	Up, Down  bool
	Over      bool
	BarBefore float64
	BarAfter  float64
	Out, In   []passMoveView
	OutSeries string
	InSeries  string
}

type passMoveView struct {
	Tenant      string
	TenantIndex int // 1-based index among this tenant's moves, largest first; what ?move= takes
	Piece       string
	Width       string
	From, To    int32
	Series      string
	series      uint64
}

func buildPassView(balance bandBalance) passView {
	// Each tenant's moves are numbered largest first on the tenant page;
	// give the pass rows the same numbers so links land on the right move.
	byTenant := map[string][]int{}
	for i, m := range balance.Moves {
		byTenant[m.Tenant] = append(byTenant[m.Tenant], i)
	}
	indexOf := make(map[int]int, len(balance.Moves))
	for _, idxs := range byTenant {
		sort.SliceStable(idxs, func(a, b int) bool {
			ma, mb := balance.Moves[idxs[a]], balance.Moves[idxs[b]]
			if ma.Series != mb.Series {
				return ma.Series > mb.Series
			}
			return ma.Lo < mb.Lo
		})
		for n, i := range idxs {
			indexOf[i] = n + 1
		}
	}

	out := map[int32][]passMoveView{}
	in := map[int32][]passMoveView{}
	var moving uint64
	for i, m := range balance.Moves {
		v := passMoveView{
			Tenant:      m.Tenant,
			TenantIndex: indexOf[i],
			Piece:       formatHexRange(m.Lo, m.Hi),
			Width:       formatBands(uint64(m.Hi) - uint64(m.Lo) + 1),
			From:        m.From,
			To:          m.To,
			Series:      commas(m.Series),
			series:      m.Series,
		}
		out[m.From] = append(out[m.From], v)
		in[m.To] = append(in[m.To], v)
		moving += m.Series
	}
	bySeries := func(vs []passMoveView) {
		sort.SliceStable(vs, func(a, b int) bool { return vs[a].series > vs[b].series })
	}

	var maxLoad, total uint64
	for _, p := range balance.Partitions {
		total += p.Before
		if p.Before > maxLoad {
			maxLoad = p.Before
		}
		if p.After > maxLoad {
			maxLoad = p.After
		}
	}
	pct := func(v uint64) float64 {
		if maxLoad == 0 {
			return 0
		}
		return float64(v) / float64(maxLoad) * 100
	}
	view := passView{
		Mean:         commas(balance.Mean),
		Total:        commas(total),
		MovingSeries: commas(moving),
		BarMean:      pct(balance.Mean),
	}
	for _, p := range balance.Partitions {
		o, n := out[p.Partition], in[p.Partition]
		bySeries(o)
		bySeries(n)
		var os, is uint64
		for _, v := range o {
			os += v.series
		}
		for _, v := range n {
			is += v.series
		}
		if p.Over {
			view.OverCount++
		}
		view.Rows = append(view.Rows, passPartitionView{
			Partition: p.Partition,
			Before:    commas(p.Before),
			After:     commas(p.After),
			Delta:     signedCommas(p.After, p.Before),
			Up:        p.After > p.Before,
			Down:      p.After < p.Before,
			Over:      p.Over,
			BarBefore: pct(p.Before),
			BarAfter:  pct(p.After),
			Out:       o,
			In:        n,
			OutSeries: commas(os),
			InSeries:  commas(is),
		})
	}
	sort.Slice(view.Rows, func(i, j int) bool { return view.Rows[i].Partition < view.Rows[j].Partition })
	return view
}

func buildTenantCards(page bandPage) ([]tenantCard, int) {
	moves := map[string]int{}
	moveSeries := map[string]uint64{}
	for _, m := range page.Balance.Moves {
		moves[m.Tenant]++
		moveSeries[m.Tenant] += m.Series
	}
	cards := make([]tenantCard, 0, len(page.Tenants))
	withMoves := 0
	for _, t := range page.Tenants {
		c := tenantCard{
			UserID:     t.UserID,
			Estimated:  commas(t.EstimatedSeries),
			Ranges:     len(t.Ranges),
			Moves:      moves[t.UserID],
			MoveSeries: commas(moveSeries[t.UserID]),
			series:     t.EstimatedSeries,
		}
		if c.Moves > 0 {
			withMoves++
		} else if t.Proposal != nil {
			c.Text = t.Proposal.Text
		}
		cards = append(cards, c)
	}
	sort.SliceStable(cards, func(i, j int) bool {
		if (cards[i].Moves > 0) != (cards[j].Moves > 0) {
			return cards[i].Moves > 0
		}
		if cards[i].series != cards[j].series {
			return cards[i].series > cards[j].series
		}
		return cards[i].UserID < cards[j].UserID
	})
	return cards, withMoves
}

func (r *Rebalancer) serveUsageTrackerHTML(w http.ResponseWriter, req *http.Request) {
	page := r.admin.snapshotBandPage()
	user := ""
	move := 0
	if req != nil && req.URL != nil {
		user = req.URL.Query().Get("user")
		move, _ = strconv.Atoi(req.URL.Query().Get("move"))
	}
	interval := "15s"
	if r.cfg.BandReadInterval > 0 {
		interval = r.cfg.BandReadInterval.String()
	}
	cards, withMoves := buildTenantCards(page)
	data := usageTrackerHTMLData{
		Prefix:           adminPathPrefix,
		Interval:         interval,
		Waiting:          page.At.IsZero() && page.Error == "",
		When:             page.At.UTC().Format(time.RFC3339),
		Bytes:            commas(uint64(max(page.Bytes, 0))),
		Page:             page,
		Pass:             buildPassView(page.Balance),
		Tenants:          cards,
		TenantsWithMoves: withMoves,
	}
	if user != "" {
		found := -1
		for i := range page.Tenants {
			if page.Tenants[i].UserID == user {
				found = i
				break
			}
		}
		if found < 0 {
			http.Error(w, "no usage-tracker snapshot for tenant "+user, http.StatusNotFound)
			return
		}
		data.Tenant = &page.Tenants[found]
		data.TenantStats = tenantStats{
			Estimated: commas(data.Tenant.EstimatedSeries),
			Locality:  commas(data.Tenant.EstimatedLocality),
			Residual:  commas(data.Tenant.Residual),
		}
		data.Draw = buildTenantDrawing(data.Tenant, page.Balance, move)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := usageTrackerTemplate.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (r *Rebalancer) serveUsageTrackerJSON(w http.ResponseWriter, _ *http.Request) {
	page := r.admin.snapshotBandPage()
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(page)
}
