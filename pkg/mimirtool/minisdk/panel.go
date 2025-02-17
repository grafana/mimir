// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana-tools/sdk/blob/master/panel.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: 2016 Alexander I.Grafov <grafov@gmail.com>.
// Provenance-includes-copyright: 2016-2019 The Grafana SDK authors

package minisdk

import (
	"encoding/json"
	"fmt"
)

// Each panel may be one of these types.
const (
	CustomType panelType = iota
	DashlistType
	GraphType
	TableType
	TextType
	PluginlistType
	AlertlistType
	SinglestatType
	StatType
	RowType
	BarGaugeType
	HeatmapType
	TimeseriesType
	GaugeType
)

type (
	// Panel represents panels of different types defined in Grafana.
	Panel struct {
		CommonPanel
		// Should be initialized only one type of panels.
		// OfType field defines which of types below will be used.
		*GraphPanel
		*TablePanel
		*TextPanel
		*SinglestatPanel
		*StatPanel
		*DashlistPanel
		*PluginlistPanel
		*RowPanel
		*AlertlistPanel
		*BarGaugePanel
		*HeatmapPanel
		*TimeseriesPanel
		*GaugePanel
		*CustomPanel
	}
	panelType   int8
	CommonPanel struct {
		Datasource *DatasourceRef `json:"datasource,omitempty"` // metrics
		ID         uint           `json:"id"`
		OfType     panelType      `json:"-"`     // it required for defining type of the panel
		Title      string         `json:"title"` // general
		Type       string         `json:"type"`
	}
	GraphPanel struct {
		Targets []Target `json:"targets,omitempty"`
	}
	TablePanel struct {
		Targets []Target `json:"targets,omitempty"`
	}
	TextPanel       struct{}
	SinglestatPanel struct {
		Targets []Target `json:"targets,omitempty"`
	}
	StatPanel struct {
		Targets []Target `json:"targets,omitempty"`
	}
	DashlistPanel   struct{}
	PluginlistPanel struct{}
	AlertlistPanel  struct{}
	BarGaugePanel   struct {
		Targets []Target `json:"targets,omitempty"`
	}
	RowPanel struct {
		Panels []Panel `json:"panels"`
	}
	HeatmapPanel struct {
		Targets []Target `json:"targets,omitempty"`
	}
	TimeseriesPanel struct {
		Targets []Target `json:"targets,omitempty"`
	}
	GaugePanel struct {
		Targets []Target `json:"targets,omitempty"`
	}
	CustomPanel struct {
		Targets []Target `json:"targets,omitempty"`
	}
)

// Target describes an expression with which the Panel fetches data from a data source.
// A Panel may have zero, one or multiple targets.
// Not all Panel types support targets. For example, a text Panel cannot have any targets.
// Panel types that do support target are not guaranteed to have any. For example, a gauge Panel can be created without
// any configured queries. In this case, the Panel will have no targets.
type Target struct {
	Datasource *DatasourceRef `json:"datasource,omitempty"`
	Expr       string         `json:"expr,omitempty"`
}

// SupportsTargets returns true if the Panel type supports targets.
// The fact that a Panel type supports targets does not guarantee that the Panel has any; it is valid for a Panel to
// have no targets, even if it supports them.
func (p *Panel) SupportsTargets() bool {
	switch p.OfType {
	case
		GraphType,
		TableType,
		SinglestatType,
		StatType,
		BarGaugeType,
		HeatmapType,
		TimeseriesType,
		GaugeType,
		CustomType:
		return true
	default:
		return false
	}
}

// GetTargets for the Panel. GetTargets returns nil if the Panel has no targets.
func (p *Panel) GetTargets() *[]Target {
	switch p.OfType {
	case GraphType:
		return &p.GraphPanel.Targets
	case SinglestatType:
		return &p.SinglestatPanel.Targets
	case StatType:
		return &p.StatPanel.Targets
	case TableType:
		return &p.TablePanel.Targets
	case BarGaugeType:
		return &p.BarGaugePanel.Targets
	case HeatmapType:
		return &p.HeatmapPanel.Targets
	case TimeseriesType:
		return &p.TimeseriesPanel.Targets
	case GaugeType:
		return &p.GaugePanel.Targets
	case CustomType:
		return &p.CustomPanel.Targets
	default:
		return nil
	}
}

type probePanel struct {
	CommonPanel
}

func (p *Panel) UnmarshalJSON(b []byte) (err error) {
	var probe probePanel
	if err = json.Unmarshal(b, &probe); err != nil {
		return err
	}

	p.CommonPanel = probe.CommonPanel
	switch probe.Type {
	case "graph":
		var graph GraphPanel
		p.OfType = GraphType
		if err = json.Unmarshal(b, &graph); err == nil {
			p.GraphPanel = &graph
		}
	case "table":
		var table TablePanel
		p.OfType = TableType
		if err = json.Unmarshal(b, &table); err == nil {
			p.TablePanel = &table
		}
	case "text":
		var text TextPanel
		p.OfType = TextType
		if err = json.Unmarshal(b, &text); err == nil {
			p.TextPanel = &text
		}
	case "singlestat":
		var singlestat SinglestatPanel
		p.OfType = SinglestatType
		if err = json.Unmarshal(b, &singlestat); err == nil {
			p.SinglestatPanel = &singlestat
		}
	case "stat":
		var stat StatPanel
		p.OfType = StatType
		if err = json.Unmarshal(b, &stat); err == nil {
			p.StatPanel = &stat
		}
	case "dashlist":
		var dashlist DashlistPanel
		p.OfType = DashlistType
		if err = json.Unmarshal(b, &dashlist); err == nil {
			p.DashlistPanel = &dashlist
		}
	case "bargauge":
		var bargauge BarGaugePanel
		p.OfType = BarGaugeType
		if err = json.Unmarshal(b, &bargauge); err == nil {
			p.BarGaugePanel = &bargauge
		}
	case "heatmap":
		var heatmap HeatmapPanel
		p.OfType = HeatmapType
		if err = json.Unmarshal(b, &heatmap); err == nil {
			p.HeatmapPanel = &heatmap
		}
	case "timeseries":
		var timeseries TimeseriesPanel
		p.OfType = TimeseriesType
		if err = json.Unmarshal(b, &timeseries); err == nil {
			p.TimeseriesPanel = &timeseries
		}
	case "row":
		var rowpanel RowPanel
		p.OfType = RowType
		if err = json.Unmarshal(b, &rowpanel); err == nil {
			p.RowPanel = &rowpanel
		}
	case "gauge":
		var gauge GaugePanel
		p.OfType = GaugeType
		if err = json.Unmarshal(b, &gauge); err == nil {
			p.GaugePanel = &gauge
		}
	default:
		var custom CustomPanel
		p.OfType = CustomType
		if err = json.Unmarshal(b, &custom); err == nil {
			p.CustomPanel = &custom
		}
	}

	if err != nil && (probe.Title != "" || probe.Type != "") {
		err = fmt.Errorf("%w (panel %q of type %q)", err, probe.Title, probe.Type)
	}

	return err
}
