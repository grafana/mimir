// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/mimir/tools/headcount/cardpoc"
	"github.com/grafana/mimir/tools/headcount/model"
)

// server answers the demo's questions over any whole-hour window, once
// through Mimir's prototype routes (Headcount) and once with PromQL through
// the same Mimir (the baseline), and checks both against the model's truth.
type server struct {
	pop       *model.Model
	mimir     *mimir
	ranges    []cardpoc.BlockRange // the compacted blocks' ranges, one per day
	spikeName string
}

func newServer(compacted string, pop *model.Model, m *mimir) (*server, error) {
	ranges, err := cardpoc.BlockRanges(compacted)
	if err != nil {
		return nil, err
	}
	if len(ranges) == 0 {
		return nil, fmt.Errorf("no blocks in %s", compacted)
	}
	s := &server{pop: pop, mimir: m, ranges: ranges}
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
	mux.HandleFunc("GET /api/meta", s.handleMeta)
	mux.HandleFunc("GET /api/names", s.handleNames)
	mux.HandleFunc("GET /api/promql/names", s.handlePromQLNames)
	mux.HandleFunc("GET /api/breakdown", s.handleBreakdown)
	mux.HandleFunc("GET /api/promql/breakdown", s.handlePromQLBreakdown)
	mux.HandleFunc("GET /api/window", s.handleWindow)
	mux.HandleFunc("GET /api/promql/window", s.handlePromQLWindow)
	mux.HandleFunc("GET /api/buckets", s.handleBuckets)
	mux.HandleFunc("GET /api/promql/buckets", s.handlePromQLBuckets)
}

const hourMs = int64(time.Hour / time.Millisecond)

func (s *server) dataStart() int64 { return s.ranges[0].MinT }
func (s *server) dataEnd() int64   { return s.ranges[len(s.ranges)-1].MaxT }

func (s *server) handleMeta(w http.ResponseWriter, _ *http.Request) {
	type rangeJSON struct {
		Start int64 `json:"start"`
		End   int64 `json:"end"`
	}
	out := struct {
		Start     int64       `json:"start"`
		End       int64       `json:"end"`
		Blocks    []rangeJSON `json:"block_ranges"`
		SpikeName string      `json:"spike_name"`
		SpikeDay  int         `json:"spike_day"`
		Names     int         `json:"names"`
		Series    int         `json:"series"`
	}{Start: s.dataStart() / 1000, End: s.dataEnd() / 1000, SpikeName: s.spikeName, SpikeDay: s.pop.Config().SpikeDay, Names: s.pop.Config().MetricNames, Series: len(s.pop.Series)}
	for _, r := range s.ranges {
		out.Blocks = append(out.Blocks, rangeJSON{r.MinT / 1000, r.MaxT / 1000})
	}
	writeJSON(w, out)
}

// window parses start and end, in Unix seconds, as [minT, maxT). Both must
// be whole hours inside the data.
func (s *server) window(r *http.Request) (minT, maxT int64, err error) {
	start, err1 := strconv.ParseInt(r.URL.Query().Get("start"), 10, 64)
	end, err2 := strconv.ParseInt(r.URL.Query().Get("end"), 10, 64)
	minT, maxT = start*1000, end*1000
	switch {
	case err1 != nil || err2 != nil:
		return 0, 0, fmt.Errorf("start and end are required, in Unix seconds")
	case maxT <= minT:
		return 0, 0, fmt.Errorf("end must be after start")
	case minT%hourMs != 0 || maxT%hourMs != 0:
		return 0, 0, fmt.Errorf("start and end must be whole hours")
	case minT < s.dataStart() || maxT > s.dataEnd():
		return 0, 0, fmt.Errorf("the window must be inside the data, %s to %s", rfc3339(s.dataStart()), rfc3339(s.dataEnd()))
	}
	return minT, maxT, nil
}

// readInfo says how Mimir answered and what it cost.
type readInfo struct {
	Method        string  `json:"method"`
	Route         string  `json:"route"`
	ElapsedMS     float64 `json:"elapsed_ms"`
	Calls         int     `json:"calls"`
	Blocks        int     `json:"blocks"`
	StoreGateways int     `json:"store_gateways"`
	SeriesRead    int     `json:"series_read,omitempty"`
	IndexBytes    int64   `json:"index_bytes,omitempty"`
	BucketBytes   int64   `json:"bucket_bytes,omitempty"`
}

func (a readInfo) add(b readInfo) readInfo {
	if a.Method == "" {
		a.Method, a.Route = b.Method, b.Route
	}
	a.ElapsedMS += b.ElapsedMS
	a.Calls += b.Calls
	a.Blocks += b.Blocks
	a.StoreGateways = max(a.StoreGateways, b.StoreGateways)
	a.SeriesRead += b.SeriesRead
	a.IndexBytes += b.IndexBytes
	a.BucketBytes += b.BucketBytes
	return a
}

// isBlockRange reports whether [minT, maxT) is exactly one block range.
func (s *server) isBlockRange(minT, maxT int64) bool {
	for _, r := range s.ranges {
		if r.MinT == minT && r.MaxT == maxT {
			return true
		}
	}
	return false
}

// seriesCountsInfo describes one series_counts answer.
func seriesCountsInfo(res mimirSeriesCounts, step bool) readInfo {
	method := "chunk metas inside one block range"
	switch {
	case res.Dedup:
		method = "full index, dedup across block ranges"
	case step:
		method = "chunk metas, per bucket"
	}
	return readInfo{
		Method: method, Route: "series_counts", ElapsedMS: ms(res.Latency), Calls: 1,
		Blocks: res.Blocks, StoreGateways: res.StoreGateways,
		SeriesRead: res.SeriesCounted, IndexBytes: res.IndexBytes, BucketBytes: res.BucketBytes,
	}
}

// nameCounts asks Mimir for every metric name's count over [minT, maxT). A
// window of exactly one block range is read from the index-headers; any
// other window from the full index.
func (s *server) nameCounts(minT, maxT int64) (map[string]int, readInfo, string) {
	if s.isBlockRange(minT, maxT) {
		res := s.mimir.metricNameCounts(minT, maxT)
		return res.Counts, readInfo{
			Method: "index-headers only, one block range", Route: "metric_name_counts", ElapsedMS: ms(res.Latency), Calls: 1,
			Blocks: res.Blocks, StoreGateways: res.StoreGateways,
		}, res.Err
	}
	res := s.mimir.seriesCounts(minT, maxT, nil)
	return res.Counts, seriesCountsInfo(res, false), res.Err
}

// previous is the window of the same length just before [minT, maxT),
// clipped to the data.
func (s *server) previous(minT, maxT int64) (int64, int64) {
	return max(s.dataStart(), 2*minT-maxT), minT
}

type windowJSON struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

func win(minT, maxT int64) windowJSON { return windowJSON{minT / 1000, maxT / 1000} }

type growthRow struct {
	Name      string `json:"name"`
	Base      int    `json:"base"`
	Day       int    `json:"day"`
	Growth    int    `json:"growth"`
	TruthBase int    `json:"truth_base"`
	TruthDay  int    `json:"truth_day"`
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

// handleNames ranks metric names by series growth from the previous window
// to this one.
func (s *server) handleNames(w http.ResponseWriter, r *http.Request) {
	minT, maxT, err := s.window(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	limit := intParam(r, "limit", 10)
	pMinT, pMaxT := s.previous(minT, maxT)
	counts, read, msg := s.nameCounts(minT, maxT)
	if msg != "" {
		mimirErr(w, msg)
		return
	}
	prev := map[string]int{}
	var prevRead readInfo
	if pMaxT > pMinT {
		if prev, prevRead, msg = s.nameCounts(pMinT, pMaxT); msg != "" {
			mimirErr(w, msg)
			return
		}
	}
	truth := s.pop.TruthBy(nil, "__name__", minT, maxT)
	prevTruth := s.pop.TruthBy(nil, "__name__", pMinT, pMaxT)

	rows := growthRows(prev, counts)
	exact, changed := 0, 0
	for _, row := range rows {
		if row.Day == truth[row.Name] && row.Base == prevTruth[row.Name] {
			exact++
		}
		if row.Growth != 0 {
			changed++
		}
	}
	withTruth := func(rs []growthRow) []growthRow {
		out := append([]growthRow(nil), rs...)
		for i := range out {
			out[i].TruthBase, out[i].TruthDay = prevTruth[out[i].Name], truth[out[i].Name]
		}
		return out
	}
	var falls []growthRow
	for i := len(rows) - 1; i >= 0 && len(falls) < 5 && rows[i].Growth < 0; i-- {
		falls = append(falls, rows[i])
	}
	writeJSON(w, map[string]any{
		"window":      win(minT, maxT),
		"previous":    win(pMinT, pMaxT),
		"rows":        withTruth(rows[:min(limit, len(rows))]),
		"falls":       withTruth(falls),
		"changed":     changed,
		"names":       len(rows),
		"names_exact": exact,
		"total":       sumCounts(counts),
		"total_prev":  sumCounts(prev),
		"read":        read,
		"prev_read":   prevRead,
	})
}

// promqlRange is a PromQL range selector for [minT, maxT): evaluated at
// maxT-1 ms, last_over_time over it sees exactly the samples in the window.
func promqlRange(minT, maxT int64) string { return fmt.Sprintf("%ds", (maxT-minT)/1000) }

// handlePromQLNames asks the same as handleNames with PromQL.
func (s *server) handlePromQLNames(w http.ResponseWriter, r *http.Request) {
	minT, maxT, err := s.window(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	limit := intParam(r, "limit", 10)
	pMinT, pMaxT := s.previous(minT, maxT)
	q := fmt.Sprintf(`count by (__name__) (last_over_time({__name__=~".+"}[%s]))`, promqlRange(minT, maxT))
	res := s.mimir.query(q, maxT-1, 0)
	out := map[string]any{"query": q, "window": res.summary()}
	prev := map[string]int{}
	if pMaxT > pMinT {
		pq := fmt.Sprintf(`count by (__name__) (last_over_time({__name__=~".+"}[%s]))`, promqlRange(pMinT, pMaxT))
		pres := s.mimir.query(pq, pMaxT-1, 0)
		out["previous"] = pres.summary()
		if pres.Err == "" {
			prev = pres.byLabel("__name__")
		}
	}
	if res.Err == "" {
		rows := growthRows(prev, res.byLabel("__name__"))
		out["rows"] = rows[:min(limit, len(rows))]
	}
	writeJSON(w, out)
}

type valueRow struct {
	Value string `json:"value"`
	Count int    `json:"count"`
	Truth int    `json:"truth"`
}

func sortedValues(counts, truth map[string]int, limit int) []valueRow {
	rows := make([]valueRow, 0, len(counts))
	for v, n := range counts {
		rows = append(rows, valueRow{Value: v, Count: n, Truth: truth[v]})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Count != rows[j].Count {
			return rows[i].Count > rows[j].Count
		}
		return rows[i].Value < rows[j].Value
	})
	return rows[:min(limit, len(rows))]
}

// plainName reads a metric or label name parameter that is safe to put in a
// PromQL selector.
func plainName(r *http.Request, key string) (string, error) {
	v := r.URL.Query().Get(key)
	if v == "" || strings.ContainsAny(v, `"{}\`) {
		return "", fmt.Errorf("%s is required and must be a plain name", key)
	}
	return v, nil
}

// handleBreakdown breaks one metric down by one label over the window, with
// an optional series budget, and gives the previous window's value count.
func (s *server) handleBreakdown(w http.ResponseWriter, r *http.Request) {
	minT, maxT, err := s.window(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	metric, err := plainName(r, "metric")
	if err == nil {
		var label string
		if label, err = plainName(r, "label"); err == nil {
			s.breakdown(w, r, minT, maxT, metric, label)
			return
		}
	}
	http.Error(w, err.Error(), http.StatusBadRequest)
}

func (s *server) breakdown(w http.ResponseWriter, r *http.Request, minT, maxT int64, metric, label string) {
	budget := intParam(r, "budget", 0)
	match := fmt.Sprintf(`{__name__=%q}`, metric)
	params := url.Values{"match[]": {match}, "group_by": {label}}
	if budget > 0 {
		params.Set("budget", strconv.Itoa(budget))
	}
	res := s.mimir.seriesCounts(minT, maxT, params)
	if res.Err != "" {
		mimirErr(w, res.Err)
		return
	}
	truth := s.pop.TruthBy(matchName(metric), label, minT, maxT)
	out := map[string]any{
		"window":       win(minT, maxT),
		"rows":         sortedValues(res.Counts, truth, intParam(r, "limit", 5)),
		"values":       len(res.Counts),
		"total":        sumCounts(res.Counts),
		"lower_bound":  res.LowerBound,
		"truth_values": len(truth),
		"truth_total":  sumCounts(truth),
		"read":         seriesCountsInfo(res, false),
	}
	if pMinT, pMaxT := s.previous(minT, maxT); pMaxT > pMinT && budget == 0 {
		prev := s.mimir.seriesCounts(pMinT, pMaxT, url.Values{"match[]": {match}, "group_by": {label}})
		if prev.Err != "" {
			mimirErr(w, prev.Err)
			return
		}
		out["previous"], out["prev_values"] = win(pMinT, pMaxT), len(prev.Counts)
	}
	writeJSON(w, out)
}

// handlePromQLBreakdown asks the same breakdown with PromQL, with an
// optional series limit applied to the tenant.
func (s *server) handlePromQLBreakdown(w http.ResponseWriter, r *http.Request) {
	minT, maxT, err := s.window(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	metric, err1 := plainName(r, "metric")
	label, err2 := plainName(r, "label")
	if err1 != nil || err2 != nil {
		http.Error(w, "metric and label are required and must be plain names", http.StatusBadRequest)
		return
	}
	q := fmt.Sprintf(`count by (%s) (last_over_time({__name__=%q}[%s]))`, label, metric, promqlRange(minT, maxT))
	res := s.mimir.query(q, maxT-1, intParam(r, "series_limit", 0))
	out := map[string]any{"query": q, "result": res.summary()}
	if res.Err == "" {
		counts := res.byLabel(label)
		out["rows"] = sortedValues(counts, nil, intParam(r, "limit", 5))
		out["values"] = len(counts)
	}
	writeJSON(w, out)
}

type windowRow struct {
	Name   string `json:"name"`
	Summed int    `json:"summed"`
	Exact  int    `json:"exact"`
	Truth  int    `json:"truth"`
}

// pieces cuts [minT, maxT) at block range boundaries.
func (s *server) pieces(minT, maxT int64) [][2]int64 {
	var out [][2]int64
	for _, r := range s.ranges {
		if lo, hi := max(minT, r.MinT), min(maxT, r.MaxT); lo < hi {
			out = append(out, [2]int64{lo, hi})
		}
	}
	return out
}

// handleWindow counts every metric name over the window twice: once by
// adding each block range's count, which counts a series once per block
// range it lives in, and once with Mimir's dedup across the blocks.
func (s *server) handleWindow(w http.ResponseWriter, r *http.Request) {
	minT, maxT, err := s.window(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	limit := intParam(r, "limit", 8)
	summed := map[string]int{}
	var summedRead readInfo
	pieces := s.pieces(minT, maxT)
	for _, p := range pieces {
		counts, read, msg := s.nameCounts(p[0], p[1])
		if msg != "" {
			mimirErr(w, msg)
			return
		}
		for n, c := range counts {
			summed[n] += c
		}
		summedRead = summedRead.add(read)
	}
	res := s.mimir.seriesCounts(minT, maxT, nil)
	if res.Err != "" {
		mimirErr(w, res.Err)
		return
	}
	truth := s.pop.TruthBy(nil, "__name__", minT, maxT)
	rows := make([]windowRow, 0, len(res.Counts))
	exactNames := 0
	for name, n := range res.Counts {
		rows = append(rows, windowRow{Name: name, Summed: summed[name], Exact: n, Truth: truth[name]})
		if n == truth[name] {
			exactNames++
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Exact != rows[j].Exact {
			return rows[i].Exact > rows[j].Exact
		}
		return rows[i].Name < rows[j].Name
	})
	writeJSON(w, map[string]any{
		"window":       win(minT, maxT),
		"pieces":       len(pieces),
		"rows":         rows[:min(limit, len(rows))],
		"names":        len(rows),
		"names_exact":  exactNames,
		"summed_total": sumCounts(summed),
		"exact_total":  sumCounts(res.Counts),
		"truth_total":  sumCounts(truth),
		"summed_read":  summedRead,
		"exact_read":   seriesCountsInfo(res, false),
	})
}

// handlePromQLWindow asks for the tenant-wide per-name table over the same
// window, with Mimir's default limits or with none.
func (s *server) handlePromQLWindow(w http.ResponseWriter, r *http.Request) {
	minT, maxT, err := s.window(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	seriesLimit := 0
	if r.URL.Query().Get("limits") == "default" {
		seriesLimit = defaultLimits
	}
	q := fmt.Sprintf(`count by (__name__) (last_over_time({__name__=~".+"}[%s]))`, promqlRange(minT, maxT))
	res := s.mimir.query(q, maxT-1, seriesLimit)
	total := 0
	for _, v := range res.Values {
		total += int(v)
	}
	writeJSON(w, map[string]any{"query": q, "result": res.summary(), "total": total})
}

// bucketStep reads the step parameter, in seconds, or picks one hour for a
// window of a day or less and one day otherwise. A given step must be whole
// hours and divide the window.
func bucketStep(r *http.Request, minT, maxT int64) (int64, error) {
	if v := r.URL.Query().Get("step"); v != "" {
		sec, err := strconv.ParseInt(v, 10, 64)
		step := sec * 1000
		if err != nil || step <= 0 || step%hourMs != 0 || (maxT-minT)%step != 0 {
			return 0, fmt.Errorf("step must be whole hours, in seconds, and divide the window")
		}
		return step, nil
	}
	if maxT-minT <= 24*hourMs {
		return hourMs, nil
	}
	return 24 * hourMs, nil
}

// handleBuckets counts one metric per bucket of the window. Each block
// range the window covers gets one series_counts call with a step, unless a
// bucket crosses a block boundary: then each bucket gets its own call, which
// Mimir answers with dedup.
func (s *server) handleBuckets(w http.ResponseWriter, r *http.Request) {
	minT, maxT, err := s.window(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	metric, err := plainName(r, "metric")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	step, err := bucketStep(r, minT, maxT)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	match := fmt.Sprintf(`{__name__=%q}`, metric)
	var (
		starts []int64
		counts []int
		read   readInfo
	)
	for b := minT; b < maxT; b += step {
		starts = append(starts, b)
	}
	pieces := s.pieces(minT, maxT)
	aligned := true
	for _, p := range pieces {
		aligned = aligned && (p[0]-minT)%step == 0 && (p[1]-p[0])%step == 0
	}
	if aligned {
		for _, p := range pieces {
			res := s.mimir.seriesCounts(p[0], p[1], url.Values{"match[]": {match}, "step": {fmt.Sprintf("%ds", step/1000)}})
			if res.Err != "" {
				mimirErr(w, res.Err)
				return
			}
			c := res.Buckets[metric]
			if c == nil {
				c = make([]int, (p[1]-p[0])/step)
			}
			counts = append(counts, c...)
			read = read.add(seriesCountsInfo(res, true))
		}
		if len(pieces) > 1 {
			read.Method = "chunk metas, per bucket, one call per block range"
		}
	} else {
		for _, b := range starts {
			res := s.mimir.seriesCounts(b, min(b+step, maxT), url.Values{"match[]": {match}})
			if res.Err != "" {
				mimirErr(w, res.Err)
				return
			}
			counts = append(counts, res.Counts[metric])
			read = read.add(seriesCountsInfo(res, false))
		}
		read.Method = "one call per bucket, since buckets cross block boundaries"
	}
	truth := make([]int, len(starts))
	exact := true
	for i, b := range starts {
		truth[i] = s.pop.Truth(matchName(metric), b, min(b+step, maxT))
		exact = exact && truth[i] == counts[i]
	}
	writeJSON(w, map[string]any{
		"window": win(minT, maxT), "step_s": step / 1000, "starts": starts,
		"counts": counts, "truth": truth, "exact": exact, "read": read,
	})
}

// handlePromQLBuckets asks the same with a range query whose steps line up
// with the buckets.
func (s *server) handlePromQLBuckets(w http.ResponseWriter, r *http.Request) {
	minT, maxT, err := s.window(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	metric, err := plainName(r, "metric")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	step, err := bucketStep(r, minT, maxT)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	q := fmt.Sprintf(`count(last_over_time({__name__=%q}[%ds]))`, metric, step/1000)
	// Each step at t answers (t-step, t], so t = bucket end minus 1 ms
	// answers exactly [bucket start, bucket end).
	res := s.mimir.queryRange(q, minT+step-1, maxT-1, time.Duration(step)*time.Millisecond, 0)
	counts := make([]int, int((maxT-minT+step-1)/step))
	for i, ts := range res.Times {
		if idx := int((ts + 1 - minT - step) / step); idx >= 0 && idx < len(counts) {
			counts[idx] = int(res.Values[i])
		}
	}
	out := map[string]any{"query": q, "step_s": step / 1000, "counts": counts, "latency_ms": ms(res.Latency), "error": res.Err}
	for _, k := range []string{"fetched_series_count", "fetched_chunk_bytes", "fetched_index_bytes"} {
		if v, ok := res.Stats[k]; ok {
			out[k] = v
		}
	}
	writeJSON(w, out)
}

// mimirErr writes a Mimir route failure as a 502, so the page can tell it
// apart from a bad request.
func mimirErr(w http.ResponseWriter, msg string) {
	http.Error(w, "mimir: "+msg, http.StatusBadGateway)
}

func intParam(r *http.Request, key string, def int) int {
	if v, err := strconv.Atoi(r.URL.Query().Get(key)); err == nil && v > 0 {
		return v
	}
	return def
}

func sumCounts(m map[string]int) int {
	n := 0
	for _, c := range m {
		n += c
	}
	return n
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func rfc3339(ms int64) string { return time.UnixMilli(ms).UTC().Format(time.RFC3339) }
