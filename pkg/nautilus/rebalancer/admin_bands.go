// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"encoding/json"
	"html/template"
	"net/http"
	"time"
)

var bandTemplate = template.Must(template.New("bands").Parse(`<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Locality bands</title>
<style>
body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 24px; color: #1f1f1f; }
a { color: #4e79a7; }
.meta { color: #666; font-size: 13px; }
.err { color: #e15759; }
.tenant { margin: 28px 0; }
.range { margin: 10px 0 16px; }
.bar { position: relative; height: 28px; background: #f3f3f3; border-radius: 4px; overflow: hidden; }
.mark { position: absolute; top: 0; bottom: 0; background: #4e79a7; opacity: 0.55; }
.mark.hot { background: #e15759; opacity: 0.9; }
.cut { position: absolute; top: 0; bottom: 0; border: 2px solid #111; box-sizing: border-box; pointer-events: none; }
.proposal { font-size: 14px; margin: 6px 0; }
.part { display: flex; height: 26px; margin: 4px 0; background: #f6f6f6; }
.seg { height: 100%; overflow: hidden; font-size: 11px; color: #fff; padding: 4px; box-sizing: border-box; white-space: nowrap; }
.seg.outline { outline: 2px solid #111; outline-offset: -2px; }
</style></head><body>
<p><a href="{{.Prefix}}/">← rebalancer</a></p>
<h1>Locality bands</h1>
<p class="meta">Shadow only. Nothing here is applied. The page renders the last read; refreshing it does not read the tracker again.</p>
{{if .Waiting}}<p>Waiting for the first band read.</p>{{end}}
{{if .Page.Error}}<p class="err">{{.Page.Error}}</p>{{end}}
{{if not .Waiting}}
<p class="meta">Read at {{.When}} from tracker partition {{.Page.Partition}} of {{.Page.Partitions}}. Response {{.Page.Bytes}} bytes in {{.Page.Duration}}.</p>
<p class="meta">Counts are one tracker's share. Estimated series = that count × {{.Page.Partitions}}. Bands outside the retained 256 are the residual against the tenant series total, not empty hash space. Readcache series and samples/s are the last slicer snapshot, which is minutes behind this read.</p>
{{range .Page.Tenants}}
<section class="tenant">
<h2>{{.UserID}}</h2>
<p class="meta">shard {{.ShardSeries}} series × partitions = {{.EstimatedSeries}} estimated. Locality hashes cover {{.EstimatedLocality}}. {{.Unhashed}} estimated series have no hash. {{.Residual}} estimated series are outside the retained 256 bands.</p>
{{if .Proposal}}<p class="proposal">{{.Proposal.Text}}</p>{{end}}
{{range .Ranges}}
<div class="range">
<div class="meta">{{.Label}} — {{.Readcache}}</div>
<div class="bar">
{{range .Marks}}<div class="mark{{if .Hot}} hot{{end}}" style="left:{{.Left}}%;width:{{.Width}}%" title="{{.Title}}"></div>{{end}}
{{if .HasCut}}<div class="cut" style="left:{{.CutLeft}}%;width:{{.CutWidth}}%"></div>{{end}}
</div>
</div>
{{end}}
</section>
{{end}}
<h2>Partitions</h2>
<p class="meta">Each row is one Kafka partition. Segments are the ranges on it, sized by hash width. An outline is a range this snapshot would cut.</p>
{{range .Page.ByPartition}}
<div class="meta">partition {{.Partition}}</div>
<div class="part">
{{range .Segments}}<div class="seg{{if .Outline}} outline{{end}}" style="width:{{.Width}}%;background:{{.Color}}" title="{{.Label}}">{{.Label}}</div>{{end}}
</div>
{{end}}
{{end}}
</body></html>`))

type bandHTMLData struct {
	Prefix  string
	Waiting bool
	When    string
	Page    bandPage
}

func (r *Rebalancer) serveBandsHTML(w http.ResponseWriter, _ *http.Request) {
	page := r.admin.snapshotBandPage()
	data := bandHTMLData{
		Prefix:  adminPathPrefix,
		Waiting: page.At.IsZero() && page.Error == "",
		When:    page.At.UTC().Format(time.RFC3339),
		Page:    page,
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := bandTemplate.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (r *Rebalancer) serveBandsJSON(w http.ResponseWriter, _ *http.Request) {
	page := r.admin.snapshotBandPage()
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(page)
}
