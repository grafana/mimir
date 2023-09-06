// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana-tools/sdk/blob/master/panel.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: 2016 Alexander I.Grafov <grafov@gmail.com>.
// Provenance-includes-copyright: 2016-2019 The Grafana SDK authors

package minisdk

import (
	"encoding/json"
	"fmt"

	log "github.com/sirupsen/logrus"
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
	LogsType
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
		*LogsPanel
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
	TextPanel struct {
		Targets []Target `json:"targets,omitempty"`
	}
	SinglestatPanel struct {
		Targets []Target `json:"targets,omitempty"`
	}
	StatPanel struct {
		Targets []Target `json:"targets,omitempty"`
	}
	DashlistPanel struct {
		Targets []Target `json:"targets,omitempty"`
	}
	PluginlistPanel struct {
		Targets []Target `json:"targets,omitempty"`
	}
	AlertlistPanel struct {
		Targets []Target `json:"targets,omitempty"`
	}
	BarGaugePanel struct {
		Targets []Target `json:"targets,omitempty"`
	}
	RowPanel struct {
		Panels  []Panel  `json:"panels"`
		Targets []Target `json:"targets,omitempty"`
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
	LogsPanel struct {
		Targets []Target `json:"targets,omitempty"`
	}
	CustomPanel struct {
		Targets []Target `json:"targets,omitempty"`
	}
)

// for an any panel
type Target struct {
	Datasource *DatasourceRef `json:"datasource,omitempty"`
	Expr       string         `json:"expr,omitempty"`
}

// GetTargets is iterate over all panel targets. It just returns nil if
// no targets defined for panel of concrete type.
func (p *Panel) GetTargets(datasourceUID string) *[]Target {
	// filtering datasources
	if datasourceUID != "" {
		if p.Datasource != nil {
			log.Debugln("GetTargets", "Incoming panel ID", p.ID, "'", p.Title, "' of type", p.Type)
			//log.Debugln("GetTargets", "p.Datasource.LegacyName", p.Datasource.LegacyName)
			//log.Debugln("GetTargets", "p.Datasource.Type", p.Datasource.Type)
			//log.Debugln("GetTargets", "p.Datasource.UID", p.Datasource.UID)
			// legacy datasource ("datasource":"xxxxx")
			if p.Datasource.LegacyName != "" && p.Datasource.LegacyName != datasourceUID {
				log.Debugln("GetTargets", "Legacy datasource", p.Datasource.LegacyName, "not matching target ds", datasourceUID)
				return nil
			} else {
				// normal datasource (with type and uid)
				// we'll filter mixed targets (p.Datasource.Type  "datasource") later
				if p.Datasource.Type != "datasource" && p.Datasource.UID != datasourceUID {
					log.Debugln("GetTargets", "Datasource UID", p.Datasource.UID, "not matching target ds", datasourceUID)
					return nil
				}
			}
		} else {
			// if datasourceUID is defined we'll filter out null datasource too
			return nil
		}
	}
	log.Debugln("GetTargets", "Filtered panel ID", p.ID, "'", p.Title, "' of type", p.Type)
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
	case RowType:
		return &p.RowPanel.Targets
	case TextType:
		return &p.TextPanel.Targets
	case LogsType:
		return &[]Target{}
	case DashlistType:
		return &p.DashlistPanel.Targets
	case PluginlistType:
		return &p.PluginlistPanel.Targets
	case AlertlistType:
		return &p.AlertlistPanel.Targets
	case CustomType:
		return &p.CustomPanel.Targets
	default:
		return nil
	}
}

type probePanel struct {
	CommonPanel
	//	json.RawMessage
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
	case "logs":
		var logs LogsPanel
		p.OfType = LogsType
		if err = json.Unmarshal(b, &logs); err == nil {
			p.LogsPanel = &logs
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
