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
	<div class="stat" title="In-memory TSDB head series across all owned ranges. This is the same denominator as cortex_ingester_memory_series (= prometheus_tsdb_head_series), NOT cortex_ingester_active_series (which is a ~10min sliding window). Series stay counted here until head compaction GCs them.">
		<div class="stat-label">Head Series</div>
		<div class="stat-value">{{fmtSeries .TotalSeries}}</div>
	</div>
	<div class="stat" title="In-memory head series an ingester still holds for hash ranges it no longer owns. These will be GC'd by the next TSDB head compaction (~2h) but in the meantime represent real memory pressure on the source ingester. The slicer treats orphan series as load attributed to the partition's current owner.">
		<div class="stat-label">Orphan Series</div>
		<div class="stat-value{{if gt .TotalOrphan 0}} warn{{end}}">{{fmtSeries .TotalOrphan}}</div>
	</div>
	<div class="stat">
		<div class="stat-label">Samples / Sec</div>
		<div class="stat-value">{{fmtRate .TotalSamples}}</div>
	</div>
	<div class="stat">
		<div class="stat-label">Partitions</div>
		<div class="stat-value">{{.NumPartitions}}</div>
	</div>
	<div class="stat">
		<div class="stat-label">Hash Ranges</div>
		<div class="stat-value">{{.NumEntries}}</div>
	</div>
	<div class="stat">
		<div class="stat-label">Mean Part Load</div>
		<div class="stat-value">{{fmtLoad .MeanPartLoad}}</div>
	</div>
	<div class="stat">
		<div class="stat-label">Max Part Load</div>
		<div class="stat-value warn">{{fmtLoad .MaxPartLoad}}</div>
	</div>
	<div class="stat">
		<div class="stat-label">Min Part Load</div>
		<div class="stat-value good">{{fmtLoad .MinPartLoad}}</div>
	</div>
	<div class="stat">
		<div class="stat-label">Imbalance</div>
		<div class="stat-value{{if gt .ImbalanceRatio 0.5}} warn{{end}}">{{fmtImbalance .ImbalanceRatio}}</div>
	</div>
	<div class="stat">
		<div class="stat-label">Last Moved</div>
		<div class="stat-value">{{fmtPct1 .MovedFraction}}</div>
	</div>
	<div class="stat">
		<div class="stat-label">Load Weights</div>
		<div class="stat-value" style="font-size:13px;line-height:1.4">series {{fmtPct1 .WeightSeries}}<br>samples {{fmtPct1 .WeightSamples}}</div>
	</div>
</div>

<div class="heatmap-container">
	<div class="heatmap-label">Hash Space Combined Load Distribution (0x00000000 → 0xffffffff)</div>
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
			<span title="combined weighted load (per-range + orphan)">{{fmtLoad .TotalLoad}}</span>
			<span title="in-memory TSDB head series (matches cortex_ingester_memory_series, not cortex_ingester_active_series)">{{fmtSeries .TotalSeries}}s</span>
			{{if gt .OrphanSeries 0}}<span title="orphan head series held by owner ingester from previously-owned ranges, not yet GC'd by head compaction" style="color:#e67700">+{{fmtSeries .OrphanSeries}} orphan</span>{{end}}
			<span title="samples per second">{{fmtRate .TotalSamples}}/s</span>
			<span>{{.NumRanges}} ranges</span>
			<span>{{fmtPct .HashSpacePct}} hash</span>
		</span>
		<span class="part-bar-container">
			<span class="part-bar{{if gt .TotalLoad $.MaxPartLoad}} hot{{else if gt .TotalLoad $.MeanPartLoad}} warm{{else}} cool{{end}}"
			      style="width:{{loadBar .TotalLoad $.MaxPartLoad}}%"></span>
		</span>
	</summary>
	<div class="part-ranges open">
		<div class="range-grid">
		{{range .Ranges}}
			<span class="range {{actionClass .LastAction}}" title="Size: {{fmtPct .SizePct}} · Load: {{fmtLoad .Load}} · Samples: {{fmtRate .Samples}}/s · Head series: {{fmtSeries .Series}}">{{hexRange .Lo .Hi}}<span class="rate">{{fmtSeries .Series}}s · {{fmtRate .Samples}}/s</span></span>
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
{{range .Rounds}}
<div class="round">
	<div class="round-header">
		<strong>{{.Time.Format "15:04:05"}}</strong>
		<span>Load: {{fmtLoad .TotalLoad}}</span>
		<span>Imbalance: {{fmtImbalance .ImbalanceRatio}}</span>
		<span>Ranges: {{.NumEntries}}</span>
		<span>Moved: {{fmtPct1 .MovedFraction}}</span>
		<span>Actions: {{len .Actions}}</span>
	</div>
	{{if .Actions}}
	<div class="round-actions">
	{{range .Actions}}
		<span class="action-pill {{actionClass .Kind}}" title="{{.Detail}}">{{.Kind}} {{hexRange .Range.Lo .Range.Hi}}{{if and .FromPart .ToPart}} P{{.FromPart}}→P{{.ToPart}}{{end}}</span>
	{{end}}
	</div>
	{{end}}
</div>
{{end}}
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
		d.title = 'Bucket ' + i + ': ' + (raw[i] * 100).toFixed(2) + '% load';
		frag.appendChild(d);
	}
	el.appendChild(frag);
})();
</script>
</body>
</html>`
