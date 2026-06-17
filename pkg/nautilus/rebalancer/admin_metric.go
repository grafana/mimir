// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// metricLookupDefaultWindow is the default wall-clock lookback for
// the admin metric-lookup tool. It approximates the read-path padding
// the distributor applies (CreationGracePeriod, default 10m, plus the
// query range itself), so the resolved partition set matches what a
// short query for this metric would fan out to.
const metricLookupDefaultWindow = 15 * time.Minute

// metricTileView is one assignment-log lease whose hash range
// overlaps the metric's hash range during the lookup window.
type metricTileView struct {
	Lo          uint32    `json:"lo"`
	Hi          uint32    `json:"hi"`
	PartitionID int32     `json:"partition_id"`
	From        time.Time `json:"from"`
	To          time.Time `json:"to"`
	Status      string    `json:"status"` // active | expired | future
}

// metricPartitionView is one resolved partition with the readcache
// leases that overlap the lookup window — the "why" for each
// readcache a query for this metric would hit.
type metricPartitionView struct {
	PartitionID int32                          `json:"partition_id"`
	Owners      []readcacheassignment.LogEntry `json:"owners"`
}

// metricLookupData is the full result of one metric lookup, shared
// between the HTML template and the JSON encoding.
type metricLookupData struct {
	AdminPathPrefix string `json:"-"`
	User            string `json:"user"`
	Metric          string `json:"metric"`

	Lo           uint32  `json:"lo"`
	Hi           uint32  `json:"hi"`
	HashSpacePct float64 `json:"hash_space_pct"`

	Now      time.Time     `json:"now"`
	WindowW0 time.Time     `json:"window_w0"`
	WindowW1 time.Time     `json:"window_w1"`
	Window   time.Duration `json:"-"`

	Tiles      []metricTileView      `json:"tiles"`
	Partitions []metricPartitionView `json:"partitions"`

	NumPartitionsNow    int `json:"num_partitions_now"`
	NumPartitionsWindow int `json:"num_partitions_window"`
	DistinctReadcaches  int `json:"distinct_readcaches"`
}

// serveMetricLookup answers "which hash range does this metric map
// to, and which partitions/readcaches does a query for it touch?"
// straight from the rebalancer's authoritative logs.
//
//	GET /nautilus/rebalancer/metric?user=<tenant>&metric=<name>[&window=15m][&format=json]
//
// The window is the wall-clock lookback the resolution unions over,
// mirroring the distributor's interval-aware routing (which pads the
// query's sample-time range by CreationGracePeriod/OOO window). The
// instantaneous partition count is reported alongside so churn
// amplification (window vs instant) is directly visible.
func (r *Rebalancer) serveMetricLookup(w http.ResponseWriter, req *http.Request) {
	user := strings.TrimSpace(req.URL.Query().Get("user"))
	metric := strings.TrimSpace(req.URL.Query().Get("metric"))
	if user == "" || metric == "" {
		http.Error(w, "both 'user' and 'metric' query parameters are required", http.StatusBadRequest)
		return
	}

	window := metricLookupDefaultWindow
	if ws := req.URL.Query().Get("window"); ws != "" {
		d, err := time.ParseDuration(ws)
		if err != nil || d <= 0 {
			http.Error(w, fmt.Sprintf("invalid window %q: want a positive Go duration like 15m", ws), http.StatusBadRequest)
			return
		}
		window = d
	}

	now := r.now()
	data := r.buildMetricLookupData(user, metric, now, window)

	if req.URL.Query().Get("format") == "json" || strings.Contains(req.Header.Get("Accept"), "application/json") {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(data); err != nil {
			http.Error(w, fmt.Sprintf("encode error: %v", err), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := metricLookupTemplate.Execute(w, data); err != nil {
		http.Error(w, fmt.Sprintf("template error: %v", err), http.StatusInternalServerError)
	}
}

// buildMetricLookupData resolves (user, metric) against the hash and
// readcache assignment logs over the half-open wall-clock window
// [now-window, now+1ms) — the same half-open intersection semantics
// the distributor's read path uses.
func (r *Rebalancer) buildMetricLookupData(user, metric string, now time.Time, window time.Duration) metricLookupData {
	lo, hi := mimirpb.MetricNameHashRange(user, metric)
	w0 := now.Add(-window)
	w1 := now.Add(time.Millisecond)

	data := metricLookupData{
		AdminPathPrefix: adminPathPrefix,
		User:            user,
		Metric:          metric,
		Lo:              lo,
		Hi:              hi,
		HashSpacePct:    float64(uint64(hi)-uint64(lo)+1) / float64(uint64(math.MaxUint32)+1) * 100,
		Now:             now,
		WindowW0:        w0,
		WindowW1:        w1,
		Window:          window,
	}

	// Tiles: every hash-log lease overlapping the metric's range
	// during the window, annotated with its lifecycle status at now.
	partitionsWindow := make(map[int32]struct{})
	partitionsNow := make(map[int32]struct{})
	for _, e := range r.store.snapshot() {
		if !e.Range.Overlaps(lo, hi) {
			continue
		}
		if !e.From.Before(w1) || !e.To.After(w0) {
			continue
		}
		status := "active"
		switch {
		case !e.To.After(now):
			status = "expired"
		case e.From.After(now):
			status = "future"
		default:
			partitionsNow[e.PartitionID] = struct{}{}
		}
		partitionsWindow[e.PartitionID] = struct{}{}
		data.Tiles = append(data.Tiles, metricTileView{
			Lo:          e.Range.Lo,
			Hi:          e.Range.Hi,
			PartitionID: e.PartitionID,
			From:        e.From,
			To:          e.To,
			Status:      status,
		})
	}
	sort.Slice(data.Tiles, func(i, j int) bool {
		if data.Tiles[i].Lo != data.Tiles[j].Lo {
			return data.Tiles[i].Lo < data.Tiles[j].Lo
		}
		return data.Tiles[i].From.Before(data.Tiles[j].From)
	})

	// Partitions -> readcache owners during the window, exactly as
	// the distributor's OwnersDuring/EntriesDuring would resolve.
	rcLog := readcacheassignment.NewLogFromEntries(r.readcacheStore.snapshot())
	distinctOwners := make(map[string]struct{})
	for pid := range partitionsWindow {
		owners := rcLog.EntriesDuring(pid, w0, w1)
		for _, o := range owners {
			distinctOwners[o.InstanceID] = struct{}{}
		}
		data.Partitions = append(data.Partitions, metricPartitionView{
			PartitionID: pid,
			Owners:      owners,
		})
	}
	sort.Slice(data.Partitions, func(i, j int) bool {
		return data.Partitions[i].PartitionID < data.Partitions[j].PartitionID
	})

	data.NumPartitionsNow = len(partitionsNow)
	data.NumPartitionsWindow = len(partitionsWindow)
	data.DistinctReadcaches = len(distinctOwners)
	return data
}

var metricLookupTemplate = template.Must(template.New("metric").Funcs(template.FuncMap{
	"hexRange": formatHexRange,
	"fmtPct": func(f float64) string {
		return formatFloat(f, 4) + "%"
	},
	"fmtTS": func(t time.Time) string {
		return t.UTC().Format("15:04:05.000")
	},
}).Parse(`<!DOCTYPE html>
<html>
<head>
<title>Metric hash range lookup</title>
<style>
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;margin:24px;color:#222;font-size:13px}
h1{font-size:18px}h2{font-size:14px;margin-top:24px}
table{border-collapse:collapse;margin-top:8px}
th,td{border:1px solid #ddd;padding:4px 10px;text-align:left;font-variant-numeric:tabular-nums}
th{background:#f5f5f5;font-size:11px;text-transform:uppercase;letter-spacing:.5px;color:#666}
code{background:#f0f0f0;padding:1px 4px;border-radius:3px}
.stats{display:flex;gap:16px;margin:12px 0;flex-wrap:wrap}
.stat{background:#f8f9fa;border:1px solid #e9ecef;border-radius:6px;padding:8px 14px}
.stat-label{font-size:11px;color:#666;text-transform:uppercase;letter-spacing:.5px}
.stat-value{font-size:16px;font-weight:600}
.expired{color:#999}.future{color:#1971c2}.active{color:#0b6e54;font-weight:600}
a{color:#1971c2}
</style>
</head>
<body>
<p><a href="{{.AdminPathPrefix}}/">&larr; rebalancer admin</a></p>
<h1>Metric hash range lookup</h1>
<p>user <code>{{.User}}</code> &middot; metric <code>{{.Metric}}</code> &middot; window {{.Window}} ([{{fmtTS .WindowW0}}, {{fmtTS .WindowW1}}) UTC, now {{fmtTS .Now}})</p>

<div class="stats">
	<div class="stat" title="MetricNameHashRange(user, metric): the contiguous slice of locality-hash keyspace every series of this metric falls into.">
		<div class="stat-label">Hash range</div>
		<div class="stat-value"><code>{{hexRange .Lo .Hi}}</code></div>
	</div>
	<div class="stat" title="Fraction of the full 32-bit keyspace covered by the metric's hash range.">
		<div class="stat-label">Keyspace share</div>
		<div class="stat-value">{{fmtPct .HashSpacePct}}</div>
	</div>
	<div class="stat" title="Partitions whose currently-active tiles overlap the hash range. This is what an instantaneous query would touch.">
		<div class="stat-label">Partitions (now)</div>
		<div class="stat-value">{{.NumPartitionsNow}}</div>
	</div>
	<div class="stat" title="Partitions that owned any overlapping tile at any point in the window — what the distributor's interval-aware routing resolves. The gap vs 'now' is churn amplification.">
		<div class="stat-label">Partitions (window)</div>
		<div class="stat-value">{{.NumPartitionsWindow}}</div>
	</div>
	<div class="stat" title="Distinct readcache instances owning the window's partitions — the query fan-out.">
		<div class="stat-label">Readcaches</div>
		<div class="stat-value">{{.DistinctReadcaches}}</div>
	</div>
</div>

<h2>Tiles overlapping the hash range during the window</h2>
{{if not .Tiles}}<p>No assignment-log leases overlap this hash range in the window.</p>{{else}}
<table>
<tr><th>Tile</th><th>Partition</th><th>Lease from</th><th>Lease to</th><th>Status at now</th></tr>
{{range .Tiles}}
<tr>
	<td><code>{{hexRange .Lo .Hi}}</code></td>
	<td>P{{.PartitionID}}</td>
	<td>{{fmtTS .From}}</td>
	<td>{{fmtTS .To}}</td>
	<td class="{{.Status}}">{{.Status}}</td>
</tr>
{{end}}
</table>
{{end}}

<h2>Resolved partitions &rarr; readcache owners during the window</h2>
{{if not .Partitions}}<p>No partitions resolved.</p>{{else}}
<table>
<tr><th>Partition</th><th>Readcache leases overlapping the window</th></tr>
{{range .Partitions}}
<tr>
	<td>P{{.PartitionID}}</td>
	<td>{{range $i, $o := .Owners}}{{if $i}}, {{end}}<code>{{$o.InstanceID}}</code> [{{fmtTS $o.From}}, {{fmtTS $o.To}}){{else}}<em>none</em>{{end}}</td>
</tr>
{{end}}
</table>
{{end}}
</body>
</html>
`))
