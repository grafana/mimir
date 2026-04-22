// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"fmt"
	"html/template"
	"math"
)

var adminTemplate = template.Must(template.New("admin").Funcs(template.FuncMap{
	"hexRange": func(lo, hi uint32) string {
		return formatHexRange(lo, hi)
	},
	"fmtRate": func(f float64) string {
		if f < 1 {
			return "<1"
		}
		if f >= 1e6 {
			return formatFloat(f/1e6, 1) + "M"
		}
		if f >= 1e3 {
			return formatFloat(f/1e3, 1) + "K"
		}
		return formatFloat(f, 0)
	},
	"fmtSeries": func(n int64) string {
		f := float64(n)
		if f >= 1e6 {
			return formatFloat(f/1e6, 1) + "M"
		}
		if f >= 1e3 {
			return formatFloat(f/1e3, 1) + "K"
		}
		return formatFloat(f, 0)
	},
	"fmtLoad": func(f float64) string {
		return formatFloat(f*100, 2) + "%"
	},
	"fmtPct": func(f float64) string {
		return formatFloat(f, 2) + "%"
	},
	"fmtPct1": func(f float64) string {
		return formatFloat(f*100, 2) + "%"
	},
	"fmtFloat": func(f float64) string {
		return formatFloat(f, 1)
	},
	"fmtImbalance": func(f float64) string {
		return formatFloat(f*100, 1) + "%"
	},
	"fmtTime": func(t interface{}) string {
		switch v := t.(type) {
		case string:
			return v
		default:
			return ""
		}
	},
	"actionClass": func(a ActionKind) string {
		switch a {
		case ActionMove:
			return "act-move"
		case ActionMerge:
			return "act-merge"
		case ActionSplit:
			return "act-split"
		case ActionReassign:
			return "act-reassign"
		default:
			return ""
		}
	},
	"loadBar": func(load, maxLoad float64) float64 {
		if maxLoad <= 0 {
			return 0
		}
		return math.Min(load/maxLoad*100, 100)
	},
	"lBar": func(l, maxL int64) float64 {
		if maxL <= 0 {
			return 0
		}
		return math.Min(float64(l)/float64(maxL)*100, 100)
	},
}).Parse(adminHTML))

func formatFloat(f float64, decimals int) string {
	if decimals == 0 {
		return fmt.Sprintf("%.0f", f)
	}
	return fmt.Sprintf("%.*f", decimals, f)
}

func formatHexRange(lo, hi uint32) string {
	return fmt.Sprintf("%08x–%08x", lo, hi)
}
