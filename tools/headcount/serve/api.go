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
	}{SpikeName: s.spikeName, SpikeDay: s.pop.Config().SpikeDay}
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
	exact := 0
	for _, row := range rows {
		if row.Base == s.dayTruth[base][row.Name] && row.Day == s.dayTruth[day][row.Name] {
			exact++
		}
	}
	top := rows[:min(limit, len(rows))]
	for i := range top {
		top[i].TruthBase, top[i].TruthDay = s.dayTruth[base][top[i].Name], s.dayTruth[day][top[i].Name]
	}
	writeJSON(w, map[string]any{
		"rows":        top,
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
