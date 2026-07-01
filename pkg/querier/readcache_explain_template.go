// SPDX-License-Identifier: AGPL-3.0-only

package querier

import "html/template"

// explainPageData is the rendering input for the readcache-lookup
// explain page.
type explainPageData struct {
	// Form values (echoed back so the form is sticky).
	Query         string
	Tenant        string
	Start         string
	End           string
	Step          string
	LookbackDelta string

	GeneratedAt   string
	Submitted     bool
	Error         string
	ResolvedStart string
	ResolvedEnd   string

	Selectors          []explainSelectorView
	TotalCalls         int
	DistinctPartitions int
	DistinctOwners     int
}

type explainSelectorView struct {
	Expr           string
	Matchers       string
	FromStr        string
	ToStr          string
	RangeStr       string
	OffsetStr      string
	Mode           string
	WindowStr      string
	Unavailable    string
	RoutingEnabled bool
	CallCount      int
	Partitions     []explainPartitionView
}

type explainPartitionView struct {
	PartitionID int32
	Owners      []explainOwnerView
}

type explainOwnerView struct {
	Owner      string
	InstanceID string
	LeaseStr   string
}

var readcacheExplainTemplate = template.Must(template.New("readcache-explain").Parse(`<!DOCTYPE html>
<html>
<head>
	<meta charset="utf-8">
	<title>Querier · Readcache lookups</title>
	<style>
		body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 1.5rem; color: #1a1a1a; }
		h1 { font-size: 1.4rem; }
		h2 { font-size: 1.1rem; margin-top: 1.5rem; }
		form { margin-bottom: 1rem; }
		label { display: block; font-weight: 600; margin: 0.5rem 0 0.15rem; font-size: 0.85rem; }
		textarea, input[type=text] { width: 100%; max-width: 900px; box-sizing: border-box; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.85rem; padding: 0.4rem; border: 1px solid #ccc; border-radius: 4px; }
		textarea { height: 4rem; }
		.row { display: flex; gap: 1rem; flex-wrap: wrap; max-width: 900px; }
		.row > div { flex: 1; min-width: 180px; }
		button { margin-top: 0.75rem; padding: 0.45rem 1rem; font-size: 0.9rem; background: #3b5bdb; color: #fff; border: 0; border-radius: 4px; cursor: pointer; }
		.hint { color: #666; font-size: 0.8rem; margin-top: 0.15rem; }
		.error { background: #fff0f0; border: 1px solid #e03131; color: #c92a2a; padding: 0.6rem 0.8rem; border-radius: 4px; max-width: 900px; }
		.summary { background: #f1f3f5; border-radius: 6px; padding: 0.6rem 0.9rem; display: inline-block; margin: 0.5rem 0; }
		.summary b { font-size: 1.1rem; }
		.selector { border: 1px solid #dee2e6; border-radius: 6px; padding: 0.8rem 1rem; margin: 0.8rem 0; }
		.selector code { background: #f8f9fa; padding: 0.1rem 0.3rem; border-radius: 3px; }
		.meta { color: #495057; font-size: 0.82rem; margin: 0.25rem 0; }
		table { border-collapse: collapse; margin-top: 0.5rem; width: 100%; max-width: 900px; }
		th, td { text-align: left; padding: 0.3rem 0.6rem; border-bottom: 1px solid #eee; font-size: 0.85rem; }
		th { background: #f8f9fa; }
		.pill { display: inline-block; background: #e7f5ff; color: #1971c2; border-radius: 10px; padding: 0 0.5rem; font-size: 0.78rem; }
		.warn { color: #e8590c; }
		.owners { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.82rem; }
		.disabled { color: #868e96; }
	</style>
</head>
<body>
	<h1>Readcache lookups for a query</h1>
	<p class="hint">Enter a PromQL query and time range to see every readcache <code>QueryStream</code> call the querier would make (one per partition &times; owner), without executing them. The query is analyzed as pasted &mdash; query-frontend shard expansion is not applied.</p>

	<form method="GET" action="` + ReadcacheExplainPathPrefix + `">
		<label for="query">PromQL query</label>
		<textarea id="query" name="query" placeholder="sum(rate(http_requests_total[5m]))">{{ .Query }}</textarea>
		<div class="row">
			<div>
				<label for="tenant">Tenant (X-Scope-OrgID)</label>
				<input type="text" id="tenant" name="tenant" value="{{ .Tenant }}" placeholder="12345">
			</div>
			<div>
				<label for="start">Start</label>
				<input type="text" id="start" name="start" value="{{ .Start }}" placeholder="unix secs or RFC3339 (default end-1h)">
			</div>
			<div>
				<label for="end">End</label>
				<input type="text" id="end" name="end" value="{{ .End }}" placeholder="unix secs or RFC3339 (default now)">
			</div>
			<div>
				<label for="step">Step (optional, not used for routing)</label>
				<input type="text" id="step" name="step" value="{{ .Step }}" placeholder="e.g. 15s">
			</div>
		</div>
		<button type="submit">Explain lookups</button>
	</form>

	{{ if .Error }}<div class="error">{{ .Error }}</div>{{ end }}

	{{ if .Submitted }}{{ if not .Error }}
		<div class="summary">
			<b>{{ .TotalCalls }}</b> readcache QueryStream call(s) &nbsp;·&nbsp;
			<b>{{ .DistinctPartitions }}</b> partition(s) &nbsp;·&nbsp;
			<b>{{ .DistinctOwners }}</b> distinct readcache(s) &nbsp;·&nbsp;
			{{ len .Selectors }} selector(s)
		</div>
		<p class="meta">Resolved range: {{ .ResolvedStart }} → {{ .ResolvedEnd }} · lookback delta {{ .LookbackDelta }} · generated {{ .GeneratedAt }}</p>

		{{ range .Selectors }}
			<div class="selector">
				<div><code>{{ .Expr }}</code> &nbsp; <span class="pill">{{ .CallCount }} call(s)</span>
					{{ if not .RoutingEnabled }}<span class="warn">&nbsp;tenant not routed to readcache — the querier would use the ingester path</span>{{ end }}
				</div>
				<div class="meta">matchers: {{ .Matchers }}</div>
				<div class="meta">select window: {{ .FromStr }} → {{ .ToStr }}{{ if .RangeStr }} · range {{ .RangeStr }}{{ end }}{{ if .OffsetStr }} · offset {{ .OffsetStr }}{{ end }}</div>
				<div class="meta">routing mode: {{ .Mode }}{{ if .WindowStr }} · ownership window {{ .WindowStr }}{{ end }}</div>
				{{ if .Unavailable }}
					<div class="error">readcache routing could not resolve this selector: {{ .Unavailable }}</div>
				{{ else }}
					<table>
						<thead><tr><th>Partition</th><th># calls</th><th>Readcache owner(s) — one QueryStream each</th></tr></thead>
						<tbody>
						{{ range .Partitions }}
							<tr>
								<td>p{{ .PartitionID }}</td>
								<td>{{ len .Owners }}</td>
								<td class="owners">
									{{ range .Owners }}{{ .Owner }}{{ if .LeaseStr }} <span class="disabled">[{{ .LeaseStr }}]</span>{{ end }}<br>{{ end }}
								</td>
							</tr>
						{{ end }}
						</tbody>
					</table>
				{{ end }}
			</div>
		{{ end }}
	{{ end }}{{ end }}
</body>
</html>
`))
