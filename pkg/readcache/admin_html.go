// SPDX-License-Identifier: AGPL-3.0-only

package readcache

// adminHTML is the readcache admin page template. Intentionally
// "classic HTML" — server-rendered tables, monospace, minimal CSS,
// no JavaScript. Designed to render the same on a terminal browser
// as on Chrome.
const adminHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Mimir Readcache: {{.InstanceID}}</title>
<style>
body {
	font-family: "Courier New", Courier, monospace;
	font-size: 13px;
	color: #000;
	background: #fff;
	margin: 0;
	padding: 18px;
	max-width: 1200px;
}
h1 { font-size: 18px; margin: 0 0 4px; border-bottom: 2px solid #000; padding-bottom: 4px; }
h2 { font-size: 14px; margin: 18px 0 6px; border-bottom: 1px solid #000; padding-bottom: 2px; }
h3 { font-size: 13px; margin: 12px 0 4px; font-weight: bold; }
.muted { color: #555; }
.warn { color: #a00; font-weight: bold; }
.ok   { color: #060; }

table {
	border-collapse: collapse;
	margin: 4px 0 8px;
	font-family: "Courier New", Courier, monospace;
	font-size: 12px;
}
th, td {
	border: 1px solid #888;
	padding: 2px 8px;
	text-align: left;
	vertical-align: top;
}
th { background: #eee; font-weight: bold; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }
td.hex { font-family: "Courier New", Courier, monospace; }
td.example {
	font-family: "Courier New", Courier, monospace;
	font-size: 11px;
	color: #333;
	max-width: 520px;
	word-break: break-all;
	overflow-wrap: anywhere;
}

.facts { margin: 4px 0 12px; }
.facts dt { display: inline; font-weight: bold; }
.facts dd { display: inline; margin: 0 16px 0 4px; }
.facts dd::after { content: ""; }

details { margin: 6px 0; }
summary { cursor: pointer; padding: 2px 0; font-weight: bold; }
summary:hover { background: #f4f4f4; }
.partition-block {
	border: 1px solid #888;
	margin: 6px 0;
	padding: 6px 10px;
	background: #fafafa;
}
.partition-block.cold { background: #fff8e6; }
.range-tables { display: flex; gap: 24px; flex-wrap: wrap; }
.range-tables > div { min-width: 320px; }
.empty { color: #888; font-style: italic; padding: 4px 0; }
.footer { margin-top: 24px; font-size: 11px; color: #555; border-top: 1px solid #ccc; padding-top: 6px; }
</style>
</head>
<body>

<h1>Mimir Readcache: {{.InstanceID}}</h1>
<dl class="facts">
	<dt>Topic:</dt><dd>{{if .KafkaTopic}}{{.KafkaTopic}}{{else}}<span class="muted">(default)</span>{{end}}</dd>
	<dt>Rebalancer:</dt><dd>{{if .RebalancerAddress}}{{.RebalancerAddress}}{{else}}<span class="muted">(static)</span>{{end}}</dd>
	{{if .OwnedPartitionsFlag}}<dt>Static partitions:</dt><dd>{{.OwnedPartitionsFlag}}</dd>{{end}}
	<dt>Generated:</dt><dd>{{.GeneratedAt}}</dd>
</dl>

<h2>Summary</h2>
<table>
	<tr>
		<th>Owned partitions</th>
		<th>Distinct tenants</th>
		<th>Total head series (&Sigma; L_pid)</th>
		<th>Current-range series</th>
		<th>Historical-range series (residue)</th>
		<th>Residue / head</th>
		<th>Hash space (current)</th>
		<th>Hash space (residue)</th>
	</tr>
	<tr>
		<td class="num">{{.NumPartitions}}</td>
		<td class="num">{{.NumTenants}}</td>
		<td class="num">{{fmtSeries .TotalHeadSeries}}</td>
		<td class="num">{{fmtSeries .TotalCurrentSeries}}</td>
		<td class="num {{if gt .TotalResidueSeries 0}}warn{{end}}">{{fmtSeries .TotalResidueSeries}}</td>
		<td class="num">{{residueRatio .TotalResidueSeries .TotalHeadSeries}}</td>
		<td class="num">{{fmtPct .HashSpaceCurrent}}</td>
		<td class="num">{{fmtPct .HashSpaceResidue}}</td>
	</tr>
</table>

<p class="muted">
	<b>L_pid</b> is the per-partition active series count this readcache reports to the rebalancer's slicer.
	The walker buckets each head's series into one of this partition's <i>current</i> ranges (growth) or
	<i>historical</i> ranges (residue still in the head after a range moved off — drains on compaction).
	&Sigma; current + &Sigma; historical per partition should approximately equal L_pid; any gap is series that
	hash outside every tracked range and would be a bookkeeping bug.
</p>

<h2>Partitions</h2>

{{if eq .NumPartitions 0}}
<p class="empty">This readcache owns no partitions. Either the rebalancer has not yet sent an assignment, or
all partitions have been moved off to other instances.</p>
{{else}}

<table>
	<tr>
		<th>Partition</th>
		<th>Warm</th>
		<th>L_pid (head series)</th>
		<th>Current series</th>
		<th>Residue series</th>
		<th>Residue %</th>
		<th>Tenants</th>
		<th>Current ranges</th>
		<th>Historical ranges</th>
		<th>Hash % (cur)</th>
		<th>Hash % (res)</th>
	</tr>
	{{range .Partitions}}
	<tr>
		<td class="num"><a href="#p{{.PartitionID}}"><b>P{{.PartitionID}}</b></a></td>
		<td>{{if .Warm}}<span class="ok">yes</span>{{else}}<span class="warn">no</span>{{end}}</td>
		<td class="num">{{fmtSeries .HeadSeries}}</td>
		<td class="num">{{fmtSeries .CurrentSeries}}</td>
		<td class="num {{if gt .ResidueSeries 0}}warn{{end}}">{{fmtSeries .ResidueSeries}}</td>
		<td class="num">{{residueRatio .ResidueSeries .HeadSeries}}</td>
		<td class="num">{{.NumTenants}}</td>
		<td class="num">{{.CurrentRangeCount}}</td>
		<td class="num">{{.HistoricalRangeCount}}</td>
		<td class="num">{{fmtPct .CurrentHashPct}}</td>
		<td class="num">{{fmtPct .HistoricalHashPct}}</td>
	</tr>
	{{end}}
</table>

{{range .Partitions}}
<div class="partition-block{{if not .Warm}} cold{{end}}" id="p{{.PartitionID}}">
	<h3>Partition P{{.PartitionID}}
		&nbsp;&nbsp;L_pid={{fmtSeries .HeadSeries}}
		&nbsp;current={{fmtSeries .CurrentSeries}}
		&nbsp;residue={{fmtSeries .ResidueSeries}}
		{{if not .Warm}}<span class="warn">&nbsp;[COLD]</span>{{end}}
	</h3>

	<details {{if or (gt .CurrentRangeCount 0) (gt .HistoricalRangeCount 0)}}open{{end}}>
	<summary>Hash ranges ({{.CurrentRangeCount}} current, {{.HistoricalRangeCount}} historical)</summary>
	<div class="range-tables">
		<div>
			<b>Current ranges</b> &mdash; growth this partition is actively ingesting into.
			{{if eq .CurrentRangeCount 0}}
			<div class="empty">(no current ranges &mdash; this partition has nothing assigned to it on this instance)</div>
			{{else}}
			<table>
				<tr><th>Range (lo-hi)</th><th>Size</th><th>Hash %</th><th>Series</th><th>Example series</th></tr>
				{{range .Current}}
				<tr>
					<td class="hex">{{hexRange .Lo .Hi}}</td>
					<td class="num">{{.SizeB}}</td>
					<td class="num">{{fmtPct4 .SizeP}}</td>
					<td class="num">{{fmtSeries .Series}}</td>
					<td class="example">{{if .Example}}{{.Example}}{{else}}<span class="muted">(no series sampled yet)</span>{{end}}</td>
				</tr>
				{{end}}
			</table>
			{{end}}
		</div>

		<div>
			<b>Historical ranges</b> &mdash; residue from previously-owned ranges, drains on head compaction.
			{{if eq .HistoricalRangeCount 0}}
			<div class="empty">(no residue &mdash; nothing has been moved off this partition recently, or it has already drained)</div>
			{{else}}
			<table>
				<tr><th>Range (lo-hi)</th><th>Size</th><th>Hash %</th><th>Series</th><th>Example series</th></tr>
				{{range .Historical}}
				<tr>
					<td class="hex">{{hexRange .Lo .Hi}}</td>
					<td class="num">{{.SizeB}}</td>
					<td class="num">{{fmtPct4 .SizeP}}</td>
					<td class="num {{if gt .Series 0}}warn{{end}}">{{fmtSeries .Series}}</td>
					<td class="example">{{if .Example}}{{.Example}}{{else}}<span class="muted">(no series sampled yet)</span>{{end}}</td>
				</tr>
				{{end}}
			</table>
			{{end}}
		</div>
	</div>
	</details>

	{{if gt .NumTenants 0}}
	<details>
	<summary>Tenants ({{.NumTenants}})</summary>
	<div>
		{{range .Tenants}}<code>{{.}}</code>&nbsp; {{end}}
	</div>
	</details>
	{{end}}
</div>
{{end}}

{{end}}

<div class="footer">
	Page is server-rendered and does not auto-refresh; reload manually for fresh stats.
	Per-range series counts come from the latest walker tick; head series counts (L_pid) are computed live on this request.
</div>

</body>
</html>`
