// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

const adminHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Nautilus Rebalancer</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;font-size:13px;line-height:1.4;color:#1a1a2e;background:#f4f5f7;padding:16px}
h1{font-size:18px;font-weight:600;margin-bottom:12px;color:#0b1426}
h2{font-size:14px;font-weight:600;margin:16px 0 8px;color:#333;border-bottom:1px solid #ddd;padding-bottom:4px}
.summary{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:16px}
.stat{background:#fff;border:1px solid #e0e0e0;border-radius:6px;padding:8px 14px;min-width:120px}
.stat-label{font-size:11px;color:#666;text-transform:uppercase;letter-spacing:.5px}
.stat-value{font-size:20px;font-weight:700;color:#0b1426}
.stat-value.warn{color:#e67700}
.stat-value.good{color:#087f5b}
.heatmap-container{margin-bottom:16px;background:#fff;border:1px solid #e0e0e0;border-radius:6px;padding:12px}
.heatmap-label{font-size:11px;color:#666;text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px}
.heatmap{display:flex;height:24px;border-radius:3px;overflow:hidden}
.heatmap-cell{flex:1;min-width:0}
.partitions{display:flex;flex-direction:column;gap:6px;margin-bottom:16px}
.partition{background:#fff;border:1px solid #e0e0e0;border-radius:6px;overflow:hidden}
.part-header{display:flex;align-items:center;gap:12px;padding:8px 12px;cursor:pointer;user-select:none}
.part-header:hover{background:#f8f9fa}
.part-id{font-weight:700;font-size:14px;min-width:30px;color:#0b1426}
.part-instance{font-size:12px;color:#666;min-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.part-stats{display:flex;gap:16px;font-size:12px;color:#444;flex:1}
.part-stats span{white-space:nowrap}
.part-bar-container{width:120px;height:14px;background:#eee;border-radius:3px;overflow:hidden;flex-shrink:0}
.part-bar{height:100%;border-radius:3px;transition:width .3s}
.part-bar.hot{background:linear-gradient(90deg,#ff6b6b,#e03131)}
.part-bar.warm{background:linear-gradient(90deg,#ffd43b,#f59f00)}
.part-bar.cool{background:linear-gradient(90deg,#69db7c,#2b8a3e)}
.part-ranges{display:none;padding:4px 12px 8px;border-top:1px solid #f0f0f0}
.part-ranges.open{display:block}
.range-grid{display:flex;flex-wrap:wrap;gap:3px}
.range{font-size:11px;font-family:"SF Mono",Consolas,monospace;padding:2px 6px;border-radius:3px;background:#f1f3f5;border:1px solid #dee2e6;white-space:nowrap;line-height:1.6}
.range .rate{color:#495057;margin-left:4px}
.act-move{border-color:#4dabf7;background:#e7f5ff}
.act-merge{border-color:#b197fc;background:#f3f0ff}
.act-split{border-color:#ffa94d;background:#fff4e6}
.act-reassign{border-color:#ff8787;background:#fff5f5}
.legend{display:flex;gap:12px;margin:8px 0;font-size:11px;color:#666}
.legend-item{display:flex;align-items:center;gap:4px}
.legend-swatch{width:12px;height:12px;border-radius:2px;border:1px solid}
.log-section{background:#fff;border:1px solid #e0e0e0;border-radius:6px;padding:12px;margin-top:16px;max-height:500px;overflow-y:auto}
.round{border-bottom:1px solid #f0f0f0;padding:8px 0}
.round:last-child{border-bottom:none}
.round-header{display:flex;gap:16px;font-size:12px;color:#444;margin-bottom:4px;flex-wrap:wrap}
.round-header strong{color:#0b1426}
.round-actions{display:flex;flex-wrap:wrap;gap:3px}
.action-pill{font-size:10px;font-family:"SF Mono",Consolas,monospace;padding:1px 5px;border-radius:3px;white-space:nowrap}
.action-pill.act-move{border-color:#4dabf7;background:#e7f5ff;border:1px solid #4dabf7}
.action-pill.act-merge{border-color:#b197fc;background:#f3f0ff;border:1px solid #b197fc}
.action-pill.act-split{border-color:#ffa94d;background:#fff4e6;border:1px solid #ffa94d}
.action-pill.act-reassign{border-color:#ff8787;background:#fff5f5;border:1px solid #ff8787}
.no-data{text-align:center;padding:40px;color:#888;font-size:14px}
.generated{font-size:11px;color:#888;margin-top:12px;text-align:right}
details>summary{cursor:pointer;list-style:none}
details>summary::-webkit-details-marker{display:none}
</style>
</head>
<body>
<h1>Nautilus Rebalancer</h1>

{{if eq .NumPartitions 0}}
<div class="no-data">No assignment data yet. Waiting for first rebalance round.</div>
{{else}}
<div class="summary">
	<div class="stat" title="Σ L_pid (TSDB head series, max over owner ingesters per partition). Matches the denominator of cortex_ingester_memory_series.">
		<div class="stat-label">Head Series</div>
		<div class="stat-value">{{fmtSeries .TotalMemorySeries}}</div>
	</div>
	<div class="stat" title="Σ max(0, L_pid - meanL). Instantaneous imbalance across partitions before the recent-moves budget discount. The slicer's per-source movable budget equals this partition's contribution minus outstanding recentMoves series.">
		<div class="stat-label">Above Average</div>
		<div class="stat-value{{if gt .AboveAverage 0}} warn{{end}}">{{fmtSeries .AboveAverage}}</div>
	</div>
	<div class="stat">
		<div class="stat-label">Partitions</div>
		<div class="stat-value">{{.NumPartitions}}</div>
	</div>
	<div class="stat">
		<div class="stat-label">Hash Ranges</div>
		<div class="stat-value">{{.NumEntries}}</div>
	</div>
	<div class="stat" title="Σ L_pid / N_partitions. The target each partition is balanced toward.">
		<div class="stat-label">Mean L</div>
		<div class="stat-value">{{fmtSeries .MeanL}}</div>
	</div>
	<div class="stat">
		<div class="stat-label">Max L</div>
		<div class="stat-value warn">{{fmtSeries .MaxL}}</div>
	</div>
	<div class="stat">
		<div class="stat-label">Min L</div>
		<div class="stat-value good">{{fmtSeries .MinL}}</div>
	</div>
	<div class="stat" title="maxL / meanL (Slicer paper definition) over per-partition head series. 1.00× means perfectly balanced; values above 1.00× indicate the hottest partition exceeds the average by that factor on the cardinality axis.">
		<div class="stat-label">Imbalance (series)</div>
		<div class="stat-value{{if gt .ImbalanceRatio 1.5}} warn{{end}}">{{fmtImbalance .ImbalanceRatio}}</div>
	</div>
	<div class="stat" title="maxRate / meanRate over per-partition samples-per-second EWMA. This is the metric the slicer now optimizes; 1.00× means perfectly balanced on ingest throughput. Zero when no rate signal has been reported yet (cold start).">
		<div class="stat-label">Imbalance (rate)</div>
		<div class="stat-value{{if gt .RateImbalance 1.5}} warn{{end}}">{{fmtImbalance .RateImbalance}}</div>
	</div>
	<div class="stat" title="Σ samples/s / N_partitions across active partitions. Computed from the same per-partition aggregation runSlicer feeds to Pass 3.">
		<div class="stat-label">Mean rate</div>
		<div class="stat-value">{{fmtRate .MeanRate}}/s</div>
	</div>
	<div class="stat" title="Highest per-partition samples-per-second EWMA in the current snapshot. Compare against Mean rate to see absolute headroom on the busiest partition.">
		<div class="stat-label">Max rate</div>
		<div class="stat-value warn">{{fmtRate .MaxRate}}/s</div>
	</div>
	<div class="stat">
		<div class="stat-label">Last Moved</div>
		<div class="stat-value">{{fmtPct1 .MovedFraction}}</div>
	</div>
	<div class="stat" title="Window over which moves off a source partition count against its movable budget. Matches the ingester's TSDB head compaction interval; L on a source does not drop until compaction.">
		<div class="stat-label">Compaction Window</div>
		<div class="stat-value" style="font-size:13px">{{.CompactionInterval}}</div>
	</div>
</div>

<div class="heatmap-container">
	<div class="heatmap-label">Hash Space Head Series Distribution (0x00000000 → 0xffffffff)</div>
	<div class="heatmap" id="heatmap"></div>
</div>

<div class="legend">
	<div class="legend-item"><div class="legend-swatch act-move"></div> Move</div>
	<div class="legend-item"><div class="legend-swatch act-merge"></div> Merge</div>
	<div class="legend-item"><div class="legend-swatch act-split"></div> Split</div>
	<div class="legend-item"><div class="legend-swatch act-reassign"></div> Reassign</div>
</div>

<h2>Partitions (click to expand ranges)</h2>
<div class="partitions">
{{range .Partitions}}
<div class="partition">
	<details>
	<summary class="part-header">
		<span class="part-id">P{{.PartitionID}}</span>
		<span class="part-instance" title="{{.InstanceAddr}}">{{.InstanceID}}</span>
		<span class="part-stats">
			<span title="L_pid: max-over-owners of TotalActiveSeries, matches cortex_ingester_memory_series. The legacy load signal — kept for observability; the slicer now balances on rate (next field).">{{fmtSeries .MemorySeries}} L</span>
			<span title="Samples-per-second EWMA summed across the hash ranges currently on this partition. This is what the slicer balances on (Pass 3 in runSlicer).">{{fmtRate .SampleRate}}/s</span>
			<span title="Per-round movable budget: max(0, L_pid - meanL) - Σ recentMoves[pid].series. Source-side only; decreases as the slicer books moves during the CompactionInterval window.">{{fmtSeries .MovableSeries}} movable</span>
			<span title="Σ per-range series for ranges this partition currently owns. Should roughly match L; a gap indicates series still in the head from previously-owned ranges not yet compacted away.">{{fmtSeries .OwnedSeries}} owned</span>
			<span>{{.NumRanges}} ranges</span>
			<span>{{fmtPct .HashSpacePct}} hash</span>
		</span>
		<span class="part-bar-container">
			<span class="part-bar{{if gt .MemorySeries $.MaxL}} hot{{else if gt .MemorySeries $.MeanL}} warm{{else}} cool{{end}}"
			      style="width:{{lBar .MemorySeries $.MaxL}}%"></span>
		</span>
	</summary>
	<div class="part-ranges open">
		<div class="range-grid">
		{{range .Ranges}}
			<span class="range {{actionClass .LastAction}}" title="Size: {{fmtPct .SizePct}} · Head series: {{fmtSeries .Series}}">{{hexRange .Lo .Hi}}<span class="rate">{{fmtSeries .Series}}s</span></span>
		{{end}}
		</div>
	</div>
	</details>
</div>
{{end}}
</div>

<h2>Recent Rebalance Rounds</h2>
<div class="log-section">
{{if eq (len .Rounds) 0}}
<div class="no-data">No rebalance rounds recorded yet.</div>
{{else}}
{{range $i, $r := .Rounds}}
<div class="round">
	<div class="round-header">
		<strong>{{$r.Time.Format "15:04:05"}}</strong>
		<span>Σ L: {{fmtSeries $r.TotalL}}</span>
		<span>Imbalance: {{fmtImbalance $r.ImbalanceRatio}}</span>
		<span>Ranges: {{$r.NumEntries}}</span>
		<span>Moved: {{fmtPct1 $r.MovedFraction}}</span>
		<span>Actions: {{len $r.Actions}}</span>
		<span><a href="/nautilus/rebalancer/rounds/{{$i}}.json" title="Download full input/output trace for this round (for replay/verification)">trace.json</a></span>
	</div>
	{{if $r.Actions}}
	<div class="round-actions">
	{{range $r.Actions}}
		<span class="action-pill {{actionClass .Kind}}" title="{{.Detail}}">{{.Kind}} {{hexRange .Range.Lo .Range.Hi}}{{if and .FromPart .ToPart}} P{{.FromPart}}→P{{.ToPart}}{{end}}{{if .Series}} ({{fmtSeries .Series}}s){{end}}</span>
	{{end}}
	</div>
	{{end}}
</div>
{{end}}
{{end}}
</div>
{{end}}

{{if .ReadcacheConfigured}}
<h2>Readcache replicas (partition ownership)</h2>
<p style="font-size:12px;color:#666;margin:-4px 0 8px">Live leases from the readcache assignment log. Load is from the most recent readcache slicer round.</p>
<div id="resetBanner" style="display:none;background:#e6fcf5;border:1px solid #099268;color:#0b6e54;padding:8px 12px;border-radius:6px;margin-bottom:8px;font-size:12px"></div>
<form method="POST" action="readcache/reset" style="margin-bottom:8px;display:flex;align-items:center;gap:8px"
      onsubmit="return confirm('Force a round-robin (partition \u2192 readcache) assignment now?\n\nThis preempts every existing lease and broadcasts a fresh even-split snapshot. Use this only when the slicer is stuck (e.g. all partitions piled on one pod due to a zero-load tiebreak).');">
	<button type="submit" style="background:#fff5f5;color:#c92a2a;border:1px solid #c92a2a;border-radius:4px;padding:4px 10px;font-size:12px;cursor:pointer;font-weight:600">Reset to even split</button>
	<span style="font-size:11px;color:#666">Forces a fresh round-robin assignment across all healthy readcache instances. Operational escape hatch — use when the slicer is stuck (e.g. all partitions on one pod).</span>
</form>
<script>
(function() {
	var p = new URLSearchParams(window.location.search);
	if (p.get('reset') === 'ok') {
		var b = document.getElementById('resetBanner');
		if (b) { b.style.display = 'block'; b.textContent = 'Readcache assignment reset to even split. Subscribers will reconcile within one tick.'; }
	}
}());
</script>
<div class="partitions">
{{range .ReadcacheReplicas}}
<div class="partition">
	<details{{if .Partitions}} open{{end}}>
	<summary class="part-header">
		<span class="part-id">{{.InstanceID}}</span>
		<span class="part-instance" title="{{.InstanceAddr}}">{{if .InstanceAddr}}{{.InstanceAddr}}{{else}}(address unknown){{end}}</span>
		<span class="part-stats">
			<span>{{len .Partitions}} partition{{if ne (len .Partitions) 1}}s{{end}}</span>
			{{if gt .Load 0.0}}<span title="Slicer load from the last readcache round">{{fmtFloat .Load}} load</span>{{end}}
		</span>
	</summary>
	<div class="part-ranges open">
		{{if .Partitions}}
		<div class="range-grid">
		{{range .Partitions}}
			<span class="range" title="Kafka partition {{.}}">P{{.}}</span>
		{{end}}
		</div>
		{{else}}
		<div class="no-data" style="padding:8px 0">No partitions assigned</div>
		{{end}}
	</div>
	</details>
</div>
{{else}}
<div class="no-data">No readcache instances in the ring or assignment log yet.</div>
{{end}}
</div>

{{end}}

{{if .ReadcacheConfigured}}
<h2>Recent Readcache Slicer Rounds</h2>
<p style="font-size:12px;color:#666;margin:-4px 0 8px">History of (partition → readcache) move rounds, newest first. Each round shows the per-instance load, the moves the slicer chose to make, and the reason for each move. Rounds that produced no moves are intentionally omitted.</p>
<div class="log-section">
{{if eq (len .ReadcacheHistory) 0}}
<div class="no-data">No readcache slicer rounds with moves recorded yet.{{if .ReadcacheHasRound}} The most recent round produced no changes; the planned/current assignment for that quiescent round is shown below.{{end}}</div>
{{else}}
{{range $i, $rr := .ReadcacheHistory}}
<div class="round">
	<div class="round-header">
		<strong>{{$rr.Time.Format "15:04:05"}}</strong>
		<span>Moves: {{len $rr.Moves}}</span>
		<span>Instances: {{len $rr.PerInstance}}</span>
		<span>Partitions: {{len $rr.PerPartition}}</span>
	</div>
	<div class="round-actions" style="margin-bottom:6px">
	{{range $rr.Moves}}
		<span class="action-pill act-move" title="{{.Reason}}">P{{.PartitionID}} {{.From}}→{{.To}} ({{fmtRate .Load}}/s)</span>
	{{end}}
	</div>
	<details>
	<summary style="font-size:11px;color:#666;cursor:pointer">Per-instance load &amp; pod-to-partition assignments</summary>
	<div style="display:flex;gap:24px;margin-top:6px;flex-wrap:wrap">
		<div style="flex:1;min-width:240px">
			<div style="font-size:11px;color:#888;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Per-instance load (samples/s)</div>
			<table style="width:100%;border-collapse:collapse;font-size:12px">
			<tr style="text-align:left;border-bottom:1px solid #eee"><th>Instance</th><th style="text-align:right">Load</th></tr>
			{{range $inst, $load := $rr.PerInstance}}
			<tr style="border-bottom:1px solid #f5f5f5"><td>{{$inst}}</td><td style="text-align:right">{{fmtRate $load}}/s</td></tr>
			{{end}}
			</table>
		</div>
		<div style="flex:2;min-width:300px">
			<div style="font-size:11px;color:#888;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Pod-to-partition assignments</div>
			<table style="width:100%;border-collapse:collapse;font-size:12px">
			<tr style="text-align:left;border-bottom:1px solid #eee"><th>Partition</th><th>Current owner</th><th>Planned owner</th></tr>
			{{range $rr.PerPartition}}
			<tr style="border-bottom:1px solid #f5f5f5{{if and .CurrentOwner .PlannedOwner}}{{if ne .CurrentOwner .PlannedOwner}};background:#fff5f5{{end}}{{end}}">
				<td>P{{.PartitionID}}</td>
				<td>{{if .CurrentOwner}}{{.CurrentOwner}}{{else}}—{{end}}</td>
				<td>{{if .PlannedOwner}}{{.PlannedOwner}}{{else}}—{{end}}</td>
			</tr>
			{{end}}
			</table>
		</div>
	</div>
	{{if $rr.Moves}}
	<details style="margin-top:6px">
	<summary style="font-size:11px;color:#666;cursor:pointer">Move reasons</summary>
	<ul style="margin:4px 0 0 18px;font-size:11px;color:#555;line-height:1.6">
	{{range $rr.Moves}}
		<li><strong>P{{.PartitionID}}</strong> {{.From}}→{{.To}}: {{.Reason}}</li>
	{{end}}
	</ul>
	</details>
	{{end}}
	</details>
</div>
{{end}}
{{end}}
{{if and .ReadcacheHasRound (eq (len .ReadcacheHistory) 0)}}
<details style="margin-top:8px">
<summary style="font-size:11px;color:#666;cursor:pointer">Last (no-move) round snapshot</summary>
<table style="width:100%;border-collapse:collapse;font-size:12px;margin-top:6px">
<tr style="text-align:left;border-bottom:1px solid #eee"><th>Partition</th><th>Current owner</th><th>Planned owner</th></tr>
{{range .ReadcacheLastRound.PerPartition}}
<tr style="border-bottom:1px solid #f5f5f5">
	<td>P{{.PartitionID}}</td>
	<td>{{if .CurrentOwner}}{{.CurrentOwner}}{{else}}—{{end}}</td>
	<td>{{if .PlannedOwner}}{{.PlannedOwner}}{{else}}—{{end}}</td>
</tr>
{{end}}
</table>
</details>
{{end}}
</div>
{{end}}

<div class="generated">Generated {{.GeneratedAt}} · Auto-refresh: <a href="" onclick="setTimeout(function(){location.reload()},0);return false">now</a></div>

<script>
(function(){
	var raw = {{.HeatmapData}};
	if (!raw || !raw.length) return;
	var el = document.getElementById('heatmap');
	if (!el) return;
	var max = 0;
	for (var i = 0; i < raw.length; i++) if (raw[i] > max) max = raw[i];
	if (max === 0) max = 1;
	var frag = document.createDocumentFragment();
	for (var i = 0; i < raw.length; i++) {
		var d = document.createElement('div');
		d.className = 'heatmap-cell';
		var intensity = raw[i] / max;
		var r = Math.round(69 + intensity * (224 - 69));
		var g = Math.round(219 - intensity * (219 - 49));
		var b = Math.round(124 - intensity * (124 - 49));
		d.style.backgroundColor = 'rgb(' + r + ',' + g + ',' + b + ')';
		d.title = 'Bucket ' + i + ': ' + raw[i].toFixed(0) + ' series';
		frag.appendChild(d);
	}
	el.appendChild(frag);
})();
</script>
</body>
</html>`
