// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/mimir/tools/headcount/cardpoc"
	"github.com/grafana/mimir/tools/headcount/model"
)

// server answers the demo's questions from one compacted snapshot. Each
// day is one block range; its per-name truth is computed once at start.
type server struct {
	compacted string
	pop       *model.Model
	mimir     *mimir

	days      []cardpoc.BlockRange
	dayTruth  []map[string]int // per day: name -> true series count
	spikeName string
}

func newServer(compacted string, pop *model.Model, m *mimir) (*server, error) {
	days, err := cardpoc.BlockRanges(compacted)
	if err != nil {
		return nil, err
	}
	s := &server{compacted: compacted, pop: pop, mimir: m, days: days, dayTruth: make([]map[string]int, len(days))}
	for i := range days {
		s.dayTruth[i] = map[string]int{}
	}
	for _, series := range pop.Series {
		name := series.Labels.Get("__name__")
		for i, d := range days {
			if series.Live(d.MinT, d.MaxT) {
				s.dayTruth[i][name]++
			}
		}
	}
	if cfg := pop.Config(); cfg.SpikeMetric >= 0 {
		prefix := fmt.Sprintf("metric_%06d_", cfg.SpikeMetric)
		for _, series := range pop.Series {
			if name := series.Labels.Get("__name__"); strings.HasPrefix(name, prefix) {
				s.spikeName = name
				break
			}
		}
	}
	return s, nil
}

func (s *server) register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/days", s.handleDays)
	mux.HandleFunc("GET /api/growth", s.handleGrowth)
	mux.HandleFunc("GET /api/breakdown", s.handleBreakdown)
	mux.HandleFunc("GET /api/promql/growth", s.handlePromQLGrowth)
	mux.HandleFunc("GET /api/promql/breakdown", s.handlePromQLBreakdown)
	mux.HandleFunc("GET /api/window", s.handleWindow)
	mux.HandleFunc("GET /api/storegateway/growth", s.handleStoreGatewayGrowth)
	mux.HandleFunc("GET /api/hourly", s.handleHourly)
	mux.HandleFunc("GET /api/promql/hourly", s.handlePromQLHourly)
	mux.HandleFunc("GET /api/promql/window", s.handlePromQLWindow)
}

type dayJSON struct {
	Index int    `json:"index"`
	Start string `json:"start"`
	End   string `json:"end"`
}

func (s *server) handleDays(w http.ResponseWriter, _ *http.Request) {
	out := struct {
		Days      []dayJSON `json:"days"`
		SpikeName string    `json:"spike_name"`
		SpikeDay  int       `json:"spike_day"`
		Names     int       `json:"names"`
		Series    int       `json:"series"`
	}{SpikeName: s.spikeName, SpikeDay: s.pop.Config().SpikeDay, Names: s.pop.Config().MetricNames, Series: len(s.pop.Series)}
	for i, d := range s.days {
		out.Days = append(out.Days, dayJSON{i, rfc3339(d.MinT), rfc3339(d.MaxT)})
	}
	writeJSON(w, out)
}

type growthRow struct {
	Name      string `json:"name"`
	Base      int    `json:"base"`
	Day       int    `json:"day"`
	Growth    int    `json:"growth"`
	TruthBase int    `json:"truth_base"`
	TruthDay  int    `json:"truth_day"`
}

type headcountCost struct {
	ElapsedMS          float64 `json:"elapsed_ms"`
	Blocks             int     `json:"blocks"`
	IndexHeaderBytes   int64   `json:"index_header_bytes"`
	ObjectStorageBytes int64   `json:"object_storage_bytes"`
	SeriesTouched      int     `json:"series_touched,omitempty"`
	PostingsBytes      int64   `json:"postings_bytes,omitempty"`
}

// handleGrowth ranks metric names by series growth from day base to day,
// both counted from index-headers only.
func (s *server) handleGrowth(w http.ResponseWriter, r *http.Request) {
	day, base, limit, err := s.dayBaseLimit(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	start := time.Now()
	dayCounts, err := cardpoc.NameCountsForRange(s.compacted, s.days[day])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	baseCounts, err := cardpoc.NameCountsForRange(s.compacted, s.days[base])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	elapsed := time.Since(start)

	rows := growthRows(baseCounts.Counts, dayCounts.Counts)
	exact, changed := 0, 0
	for _, row := range rows {
		if row.Base == s.dayTruth[base][row.Name] && row.Day == s.dayTruth[day][row.Name] {
			exact++
		}
		if row.Growth != 0 {
			changed++
		}
	}
	withTruth := func(rs []growthRow) []growthRow {
		out := append([]growthRow(nil), rs...)
		for i := range out {
			out[i].TruthBase, out[i].TruthDay = s.dayTruth[base][out[i].Name], s.dayTruth[day][out[i].Name]
		}
		return out
	}
	top := withTruth(rows[:min(limit, len(rows))])
	var falls []growthRow
	for i := len(rows) - 1; i >= 0 && len(falls) < 5 && rows[i].Growth < 0; i-- {
		falls = append(falls, rows[i])
	}
	falls = withTruth(falls)
	writeJSON(w, map[string]any{
		"rows":        top,
		"falls":       falls,
		"changed":     changed,
		"names":       len(rows),
		"names_exact": exact,
		"total_day":   dayCounts.Total(),
		"total_base":  baseCounts.Total(),
		"cost": headcountCost{
			ElapsedMS:        ms(elapsed),
			Blocks:           len(dayCounts.Blocks) + len(baseCounts.Blocks),
			IndexHeaderBytes: dayCounts.IndexHeaderBytes + baseCounts.IndexHeaderBytes,
		},
	})
}

// growthRows returns every name in either map with its growth, largest
// growth first.
func growthRows(base, day map[string]int) []growthRow {
	names := map[string]bool{}
	for n := range base {
		names[n] = true
	}
	for n := range day {
		names[n] = true
	}
	rows := make([]growthRow, 0, len(names))
	for n := range names {
		rows = append(rows, growthRow{Name: n, Base: base[n], Day: day[n], Growth: day[n] - base[n]})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Growth != rows[j].Growth {
			return rows[i].Growth > rows[j].Growth
		}
		return rows[i].Name < rows[j].Name
	})
	return rows
}

type valueRow struct {
	Value string `json:"value"`
	Count int    `json:"count"`
	Truth int    `json:"truth"`
}

// handleBreakdown breaks one metric down by one label over one day, with
// an optional series budget.
func (s *server) handleBreakdown(w http.ResponseWriter, r *http.Request) {
	day, _, limit, err := s.dayBaseLimit(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	metric, label := r.URL.Query().Get("metric"), r.URL.Query().Get("label")
	if metric == "" || label == "" {
		http.Error(w, "metric and label are required", http.StatusBadRequest)
		return
	}
	budget, _ := strconv.Atoi(r.URL.Query().Get("budget"))

	bd, err := cardpoc.LabelBreakdown(s.compacted, s.days[day], metric, label, cardpoc.Budget{MaxSeries: budget})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	truth := s.pop.TruthBy(matchName(metric), label, s.days[day].MinT, s.days[day].MaxT)
	truthTotal := 0
	for _, n := range truth {
		truthTotal += n
	}

	rows := make([]valueRow, 0, len(bd.Counts))
	for v, n := range bd.Counts {
		rows = append(rows, valueRow{Value: v, Count: n, Truth: truth[v]})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Count != rows[j].Count {
			return rows[i].Count > rows[j].Count
		}
		return rows[i].Value < rows[j].Value
	})
	writeJSON(w, map[string]any{
		"rows":         rows[:min(limit, len(rows))],
		"values":       len(bd.Counts),
		"total":        bd.Total(),
		"lower_bound":  bd.LowerBound,
		"truth_values": len(truth),
		"truth_total":  truthTotal,
		"cost": headcountCost{
			ElapsedMS: ms(bd.Elapsed),
			// The breakdown reads postings and series labels from the full
			// index, which is object storage in a real store-gateway.
			ObjectStorageBytes: bd.PostingsBytes,
			SeriesTouched:      bd.SeriesTouched,
			PostingsBytes:      bd.PostingsBytes,
		},
	})
}

// handlePromQLGrowth asks Mimir for per-name counts on both days and ranks
// growth the same way, so the two rankings can be compared.
func (s *server) handlePromQLGrowth(w http.ResponseWriter, r *http.Request) {
	day, base, limit, err := s.dayBaseLimit(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	seriesLimit, _ := strconv.Atoi(r.URL.Query().Get("series_limit"))
	const q = `count by (__name__) (last_over_time({__name__=~".+"}[1d]))`

	dayRes := s.mimir.query(q, s.days[day].MaxT-1, seriesLimit)
	baseRes := s.mimir.query(q, s.days[base].MaxT-1, seriesLimit)
	out := map[string]any{"query": q, "day": dayRes.summary(), "base": baseRes.summary()}
	if dayRes.Err == "" && baseRes.Err == "" {
		rows := growthRows(baseRes.byLabel("__name__"), dayRes.byLabel("__name__"))
		out["rows"] = rows[:min(limit, len(rows))]
	}
	writeJSON(w, out)
}

// handlePromQLBreakdown asks Mimir for the same breakdown as
// handleBreakdown, with an optional series limit applied to the tenant.
func (s *server) handlePromQLBreakdown(w http.ResponseWriter, r *http.Request) {
	day, _, limit, err := s.dayBaseLimit(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	metric, label := r.URL.Query().Get("metric"), r.URL.Query().Get("label")
	if metric == "" || label == "" || strings.ContainsAny(metric+label, `"{}\`) {
		http.Error(w, "metric and label are required and must be plain names", http.StatusBadRequest)
		return
	}
	seriesLimit, _ := strconv.Atoi(r.URL.Query().Get("series_limit"))
	q := fmt.Sprintf(`count by (%s) (last_over_time({__name__=%q}[1d]))`, label, metric)

	res := s.mimir.query(q, s.days[day].MaxT-1, seriesLimit)
	out := map[string]any{"query": q, "result": res.summary()}
	if res.Err == "" {
		counts := res.byLabel(label)
		rows := make([]valueRow, 0, len(counts))
		for v, n := range counts {
			rows = append(rows, valueRow{Value: v, Count: n})
		}
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].Count != rows[j].Count {
				return rows[i].Count > rows[j].Count
			}
			return rows[i].Value < rows[j].Value
		})
		out["rows"] = rows[:min(limit, len(rows))]
		out["values"] = len(counts)
	}
	writeJSON(w, out)
}

// hourlyMetric returns the metric query parameter, or the spike metric.
func (s *server) hourlyMetric(r *http.Request) (string, error) {
	metric := r.URL.Query().Get("metric")
	if metric == "" {
		metric = s.spikeName
	}
	if metric == "" || strings.ContainsAny(metric, `"{}\`) {
		return "", fmt.Errorf("metric is required and must be a plain name")
	}
	return metric, nil
}

// handleHourly counts one metric per hour of one day from chunk metas.
func (s *server) handleHourly(w http.ResponseWriter, r *http.Request) {
	day, _, _, err := s.dayBaseLimit(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	metric, err := s.hourlyMetric(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	res, err := cardpoc.RunE8(s.compacted, s.pop, s.days[day], time.Hour, metric)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{
		"metric": metric,
		"starts": res.Starts,
		"counts": res.Counts,
		"truth":  res.Truth,
		"exact":  res.Pass(),
		// Chunk metas live in the series section of the full index, which
		// is object storage in a real store-gateway.
		"cost": headcountCost{ElapsedMS: ms(res.Elapsed), SeriesTouched: res.SeriesRead},
	})
}

// handlePromQLHourly asks Mimir for the same hourly counts with a range
// query whose steps line up with the hours.
func (s *server) handlePromQLHourly(w http.ResponseWriter, r *http.Request) {
	day, _, _, err := s.dayBaseLimit(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	metric, err := s.hourlyMetric(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	q := fmt.Sprintf(`count(last_over_time({__name__=%q}[1h]))`, metric)
	hour := time.Hour.Milliseconds()
	// Each step at t answers (t-1h, t], so t = hour end minus 1 ms answers
	// exactly [hour start, hour end).
	res := s.mimir.queryRange(q, s.days[day].MinT+hour-1, s.days[day].MaxT-1, time.Hour, 0)
	counts := make([]int, int((s.days[day].MaxT-s.days[day].MinT)/hour))
	for i, ts := range res.Times {
		if idx := int((ts + 1 - s.days[day].MinT - hour) / hour); idx >= 0 && idx < len(counts) {
			counts[idx] = int(res.Values[i])
		}
	}
	out := map[string]any{"query": q, "counts": counts, "latency_ms": ms(res.Latency), "error": res.Err}
	for _, k := range []string{"fetched_series_count", "fetched_chunk_bytes", "fetched_index_bytes"} {
		if v, ok := res.Stats[k]; ok {
			out[k] = v
		}
	}
	writeJSON(w, out)
}

// handleStoreGatewayGrowth asks Mimir's own store-gateway for each day's
// per-name counts, through its metric_name_counts endpoint, and ranks growth
// the same way handleGrowth does, so the page can show that a Mimir
// component gives the same answer as the library.
func (s *server) handleStoreGatewayGrowth(w http.ResponseWriter, r *http.Request) {
	day, base, limit, err := s.dayBaseLimit(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	dayRes := s.mimir.metricNameCounts(s.days[day].MinT, s.days[day].MaxT)
	baseRes := s.mimir.metricNameCounts(s.days[base].MinT, s.days[base].MaxT)
	out := map[string]any{
		"latency_ms": ms(dayRes.Latency + baseRes.Latency),
		"blocks":     dayRes.Blocks + baseRes.Blocks,
		"error":      dayRes.Err + baseRes.Err,
	}
	if dayRes.Err == "" && baseRes.Err == "" {
		rows := growthRows(baseRes.Counts, dayRes.Counts)
		out["rows"] = rows[:min(limit, len(rows))]
	}
	writeJSON(w, out)
}

type windowRow struct {
	Name   string `json:"name"`
	Summed int    `json:"summed"`
	Exact  int    `json:"exact"`
	Truth  int    `json:"truth"`
}

// windowDays parses from and to, inclusive day indices, as a window.
func (s *server) windowDays(r *http.Request) (minT, maxT int64, days int, err error) {
	from, err1 := strconv.Atoi(r.URL.Query().Get("from"))
	to, err2 := strconv.Atoi(r.URL.Query().Get("to"))
	if err1 != nil || err2 != nil || from < 0 || to >= len(s.days) || from > to {
		return 0, 0, 0, fmt.Errorf("from and to must be day indices with 0 <= from <= to < %d", len(s.days))
	}
	return s.days[from].MinT, s.days[to].MaxT, to - from + 1, nil
}

// handleWindow counts every metric name over several days, both by adding
// the days' index-header counts and exactly by hash union.
func (s *server) handleWindow(w http.ResponseWriter, r *http.Request) {
	minT, maxT, _, err := s.windowDays(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	limit := 10
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 {
		limit = v
	}
	wc, err := cardpoc.NameCountsForWindow(s.compacted, minT, maxT)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	truth := s.pop.TruthBy(nil, "__name__", minT, maxT)

	rows := make([]windowRow, 0, len(wc.Exact))
	summedTotal, exactTotal, truthTotal, exactNames := 0, 0, 0, 0
	for name, exact := range wc.Exact {
		rows = append(rows, windowRow{Name: name, Summed: wc.Summed[name], Exact: exact, Truth: truth[name]})
		summedTotal += wc.Summed[name]
		exactTotal += exact
		if exact == truth[name] {
			exactNames++
		}
	}
	for _, n := range truth {
		truthTotal += n
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Exact != rows[j].Exact {
			return rows[i].Exact > rows[j].Exact
		}
		return rows[i].Name < rows[j].Name
	})
	writeJSON(w, map[string]any{
		"rows":         rows[:min(limit, len(rows))],
		"names":        len(rows),
		"names_exact":  exactNames,
		"summed_total": summedTotal,
		"exact_total":  exactTotal,
		"truth_total":  truthTotal,
		"ranges":       len(wc.Ranges),
		"summed_cost":  headcountCost{ElapsedMS: ms(wc.SummedTime), IndexHeaderBytes: wc.SummedHeaders},
		"exact_cost":   headcountCost{ElapsedMS: ms(wc.ExactTime), ObjectStorageBytes: wc.IndexBytes, SeriesTouched: wc.SeriesRead},
	})
}

// handlePromQLWindow asks Mimir for the tenant-wide per-name table over
// the same days, with Mimir's default limits or with none.
func (s *server) handlePromQLWindow(w http.ResponseWriter, r *http.Request) {
	_, maxT, days, err := s.windowDays(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	seriesLimit := 0
	if r.URL.Query().Get("limits") == "default" {
		seriesLimit = defaultLimits
	}
	q := fmt.Sprintf(`count by (__name__) (last_over_time({__name__=~".+"}[%dd]))`, days)
	res := s.mimir.query(q, maxT-1, seriesLimit)
	total := 0
	for _, v := range res.Values {
		total += int(v)
	}
	writeJSON(w, map[string]any{"query": q, "result": res.summary(), "total": total})
}

// dayBaseLimit parses the day, base and limit query parameters. base
// defaults to the day before day, limit to 20.
func (s *server) dayBaseLimit(r *http.Request) (day, base, limit int, err error) {
	q := r.URL.Query()
	day, err = strconv.Atoi(q.Get("day"))
	if err != nil || day < 0 || day >= len(s.days) {
		return 0, 0, 0, fmt.Errorf("day must be an index in [0, %d)", len(s.days))
	}
	base = day - 1
	if v := q.Get("base"); v != "" {
		if base, err = strconv.Atoi(v); err != nil || base < 0 || base >= len(s.days) {
			return 0, 0, 0, fmt.Errorf("base must be an index in [0, %d)", len(s.days))
		}
	}
	if base < 0 {
		base = day
	}
	limit = 20
	if v := q.Get("limit"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil || limit <= 0 {
			return 0, 0, 0, fmt.Errorf("limit must be positive")
		}
	}
	return day, base, limit, nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func rfc3339(ms int64) string { return time.UnixMilli(ms).UTC().Format(time.RFC3339) }
