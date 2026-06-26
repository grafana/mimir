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
	// fmtOffset renders a Kafka offset, showing the unknown sentinel
	// (-1, set before the reader has started or when it has been torn
	// down) as an em dash rather than a misleading "-1".
	"fmtOffset": func(o int64) string {
		if o < 0 {
			return "\u2014"
		}
		return fmt.Sprintf("%d", o)
	},
	// offsetSpan renders the number of offsets between start and end,
	// i.e. roughly how many records this TSDB's partition consumed
	// while owned. Empty when either bound is unknown.
	"offsetSpan": func(start, end int64) string {
		if start < 0 || end < 0 || end < start {
			return ""
		}
		return fmt.Sprintf("%d", end-start)
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
	// fmtRate renders a samples-per-second EWMA in human-friendly
	// units (K/M with two decimals, "0" for the EWMA-not-yet-warm
	// case). The walker advances every loadstats.TickInterval
	// (15s); single-digit values are stable signal, not noise.
	"fmtRate": func(f float64) string {
		if f <= 0 {
			return "0"
		}
		if f >= 1e6 {
			return fmt.Sprintf("%.2fM/s", f/1e6)
		}
		if f >= 1e3 {
			return fmt.Sprintf("%.2fK/s", f/1e3)
		}
		if f < 1 {
			return fmt.Sprintf("%.2f/s", f)
		}
		return fmt.Sprintf("%.1f/s", f)
	},
}).Parse(adminHTML))
