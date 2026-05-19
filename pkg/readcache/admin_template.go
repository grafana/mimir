// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"fmt"
	"html/template"
)

var adminTemplate = template.Must(template.New("readcache-admin").Funcs(template.FuncMap{
	"hexRange": func(lo, hi uint32) string {
		return fmt.Sprintf("%08x-%08x", lo, hi)
	},
	"fmtSeries": func(n int64) string {
		f := float64(n)
		if f >= 1e6 {
			return fmt.Sprintf("%.2fM", f/1e6)
		}
		if f >= 1e3 {
			return fmt.Sprintf("%.2fK", f/1e3)
		}
		return fmt.Sprintf("%d", n)
	},
	"fmtPct": func(f float64) string {
		return fmt.Sprintf("%.2f%%", f)
	},
	"fmtPct4": func(f float64) string {
		return fmt.Sprintf("%.4f%%", f)
	},
	"residueRatio": func(residue, head int64) string {
		if head <= 0 {
			return "0.0%"
		}
		return fmt.Sprintf("%.1f%%", float64(residue)/float64(head)*100)
	},
}).Parse(adminHTML))
