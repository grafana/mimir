// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"time"

	"github.com/grafana/mimir/tools/headcount/model"
)

// shiftConfig moves cfg's [Start, End) window so that it begins at start,
// keeping its length. The population generated from the shifted config has
// the same label sets as the one generated from cfg, with every interval
// moved by the same offset: model.New derives every timestamp from Start
// and End and every random draw from Seed, so the shift is exact.
func shiftConfig(cfg model.Config, start time.Time) model.Config {
	delta := start.Sub(cfg.Start)
	cfg.Start = cfg.Start.Add(delta)
	cfg.End = cfg.End.Add(delta)
	return cfg
}

// alignDown rounds t down to a multiple of step since the Unix epoch, so
// that every writer run and every truth query uses the same grid of
// sample timestamps.
func alignDown(t time.Time, step time.Duration) time.Time {
	ms := t.UnixMilli()
	stepMs := step.Milliseconds()
	return time.UnixMilli(ms - ms%stepMs).UTC()
}

// stepTimes returns every sample timestamp s_k = start + k*step with
// start <= s_k < end, in milliseconds.
func stepTimes(start, end time.Time, step time.Duration) []int64 {
	var out []int64
	stepMs := step.Milliseconds()
	for t := start.UnixMilli(); t < end.UnixMilli(); t += stepMs {
		out = append(out, t)
	}
	return out
}

// liveAt returns the indexes of the series that have a sample at
// timestamp t, which is every series with an interval containing t.
// Interval ends are exclusive, so a series whose interval ends exactly at
// t has no sample at t.
func liveAt(series []model.Series, t int64) []int {
	var out []int
	for i := range series {
		if series[i].Live(t, t+1) {
			out = append(out, i)
		}
	}
	return out
}

// sampledTruth counts the series matching keep that have at least one
// sample timestamp on the grid (start aligned, every step) inside
// [lo, hi]. This is exactly what a PromQL instant query at hi with
// lookback hi-lo+1ms sees after the writer has replayed the model, and
// differs from model.Truth over the same window only when a live
// interval overlaps the window without containing a grid point, which
// happens when a gap leaves a remainder shorter than one step at the
// end of an interval.
func sampledTruth(series []model.Series, keep func(model.Series) bool, grid time.Duration, lo, hi int64) int {
	stepMs := grid.Milliseconds()
	count := 0
	for _, s := range series {
		if !keep(s) {
			continue
		}
		for _, iv := range s.Intervals {
			a, b := max(iv.Start, lo), min(iv.End, hi+1) // Samples live in [a, b).
			if a >= b {
				continue
			}
			first := a
			if r := a % stepMs; r != 0 {
				first = a - r + stepMs
			}
			if first < b {
				count++
				break
			}
		}
	}
	return count
}
