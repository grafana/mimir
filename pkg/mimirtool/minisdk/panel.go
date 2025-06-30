// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana-tools/sdk/blob/master/panel.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: 2016 Alexander I.Grafov <grafov@gmail.com>.
// Provenance-includes-copyright: 2016-2019 The Grafana SDK authors

package minisdk

type panelType uint8

const (
	graph panelType = iota + 1
	table
	text
	singleStat
	stat
	dashList
	barGauge
	heatmap
	timeseries
	row
	gauge
	barChart
	trend
	traces
	logs
	news
	alertList
	canvas
	pieChart
	annotationList
	histogram
	statusHistory
	candlestick
	stateTimeline
	flameGraph
	geomap
	nodeGraph
	xyChart
)

func (p panelType) String() string {
	switch p {
	case graph:
		return "graph"
	case table:
		return "table"
	case text:
		return "text"
	case singleStat:
		return "singlestat"
	case stat:
		return "stat"
	case dashList:
		return "dashlist"
	case barGauge:
		return "bargauge"
	case heatmap:
		return "heatmap"
	case timeseries:
		return "timeseries"
	case row:
		return "row"
	case gauge:
		return "gauge"
	case barChart:
		return "barchart"
	case trend:
		return "trend"
	case traces:
		return "traces"
	case logs:
		return "logs"
	case news:
		return "news"
	case alertList:
		return "alertlist"
	case canvas:
		return "canvas"
	case pieChart:
		return "piechart"
	case annotationList:
		return "annolist"
	case histogram:
		return "histogram"
	case statusHistory:
		return "status-history"
	case candlestick:
		return "candlestick"
	case stateTimeline:
		return "state-timeline"
	case flameGraph:
		return "flamegraph"
	case geomap:
		return "geomap"
	case nodeGraph:
		return "nodeGraph"
	case xyChart:
		return "xychart"
	default:
		return "unknown"
	}
}

var knownTypes = map[string]struct{}{
	graph.String():          {},
	table.String():          {},
	text.String():           {},
	singleStat.String():     {},
	stat.String():           {},
	dashList.String():       {},
	barGauge.String():       {},
	heatmap.String():        {},
	timeseries.String():     {},
	row.String():            {},
	gauge.String():          {},
	barChart.String():       {},
	trend.String():          {},
	traces.String():         {},
	logs.String():           {},
	news.String():           {},
	alertList.String():      {},
	canvas.String():         {},
	pieChart.String():       {},
	annotationList.String(): {},
	histogram.String():      {},
	statusHistory.String():  {},
	candlestick.String():    {},
	stateTimeline.String():  {},
	flameGraph.String():     {},
	geomap.String():         {},
	nodeGraph.String():      {},
	xyChart.String():        {},
}

// Panel represents panels of different types defined in a Grafana dashboard.
type Panel struct {
	Targets   []Target `json:"targets,omitempty"`
	SubPanels []Panel  `json:"panels"`
	Type      string   `json:"type"`
}

// Target describes an expression with which the Panel fetches data from a data source.
// A Panel may have zero, one or multiple targets.
// Not all Panel types support targets. For example, a text Panel cannot have any targets.
// Panel types that do support target are not guaranteed to have any. For example, a gauge Panel can be created without
// any configured queries. In this case, the Panel will have no targets.
type Target struct {
	Datasource *DatasourceRef `json:"datasource,omitempty"`
	Expr       string         `json:"expr,omitempty"`
}

func (p *Panel) hasCustomType() bool {
	_, ok := knownTypes[p.Type]

	return !ok
}

// SupportsTargets returns true if the Panel type supports targets.
// The fact that a Panel type supports targets does not guarantee that the Panel has any; it is valid for a Panel to
// have no targets, even if it supports them.
func (p *Panel) SupportsTargets() bool {
	// If the Panel is of a custom type, then we can't be sure whether it supports targets or not, as we don't know its
	// structure. In this case, we return true as we'd prefer for consumers to check for targets that don't exist rather
	// than not check for targets that do exist.
	if p.hasCustomType() {
		return true
	}

	switch p.Type {
	case
		graph.String(),
		table.String(),
		singleStat.String(),
		stat.String(),
		barGauge.String(),
		heatmap.String(),
		timeseries.String(),
		barChart.String(),
		pieChart.String(),
		stateTimeline.String(),
		statusHistory.String(),
		histogram.String(),
		candlestick.String(),
		canvas.String(),
		flameGraph.String(),
		geomap.String(),
		nodeGraph.String(),
		trend.String(),
		xyChart.String(),
		gauge.String():
		return true
	default:
		return false
	}
}

// GetTargets for the Panel. GetTargets returns nil if the Panel has no targets.
func (p *Panel) GetTargets() *[]Target {
	return &p.Targets
}
