// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

// mimir runs PromQL against a Mimir serving the same snapshot, and reads
// the cost of each query from the query stats line Mimir logs for it.
type mimir struct {
	baseURL       string
	logPath       string
	runtimeConfig string

	mu         sync.Mutex // one query at a time, so each stats line is ours.
	lastLimit  int
	limitKnown bool
}

// queryResult is one instant query's outcome and cost.
type queryResult struct {
	Err       string
	Latency   time.Duration
	Stats     map[string]string // from the query stats log line; nil if not found
	Series    []map[string]string
	Values    []float64
	HTTPError int
}

func (q queryResult) summary() map[string]any {
	out := map[string]any{
		"latency_ms": ms(q.Latency),
		"error":      q.Err,
		"results":    len(q.Series),
	}
	for _, k := range []string{"fetched_series_count", "fetched_chunk_bytes", "fetched_chunks_count", "fetched_index_bytes", "query_wall_time_seconds", "sharded_queries"} {
		if v, ok := q.Stats[k]; ok {
			out[k] = v
		}
	}
	return out
}

// byLabel returns each result's value keyed by one of its labels.
func (q queryResult) byLabel(name string) map[string]int {
	out := make(map[string]int, len(q.Series))
	for i, s := range q.Series {
		out[s[name]] = int(q.Values[i])
	}
	return out
}

// defaultLimits, passed as a series limit, gives the tenant Mimir's
// default limits instead of none.
const defaultLimits = -1

// query runs an instant query at time atMS. With seriesLimit > 0 the
// tenant's max_fetched_series_per_query is set to it first; with 0 the
// tenant has no fetch limits; with defaultLimits it has Mimir's defaults.
func (m *mimir) query(q string, atMS int64, seriesLimit int) queryResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.setSeriesLimit(seriesLimit); err != nil {
		return queryResult{Err: fmt.Sprintf("setting series limit: %v", err)}
	}

	logOffset := m.logSize()
	at := time.UnixMilli(atMS).UTC().Format("2006-01-02T15:04:05.000Z")
	u := m.baseURL + "/prometheus/api/v1/query?" + url.Values{"query": {q}, "time": {at}}.Encode()

	start := time.Now()
	resp, err := http.Get(u)
	if err != nil {
		return queryResult{Err: err.Error(), Latency: time.Since(start)}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	res := queryResult{Latency: time.Since(start), HTTPError: resp.StatusCode}
	if err != nil {
		res.Err = err.Error()
		return res
	}

	var parsed struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  [2]any            `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		res.Err = fmt.Sprintf("decoding response: %v", err)
		return res
	}
	if parsed.Status != "success" {
		res.Err = parsed.Error
	}
	for _, r := range parsed.Data.Result {
		v, _ := strconv.ParseFloat(fmt.Sprint(r.Value[1]), 64)
		res.Series = append(res.Series, r.Metric)
		res.Values = append(res.Values, v)
	}
	res.Stats = m.statsFor(q, logOffset)
	return res
}

// rangeResult is one range query's outcome for a query that returns a
// single series, such as a count().
type rangeResult struct {
	Err     string
	Latency time.Duration
	Stats   map[string]string
	Times   []int64 // milliseconds
	Values  []float64
}

// queryRange runs a range query from startMS to endMS at step, with the
// tenant's limits as in query, and returns the first result series.
func (m *mimir) queryRange(q string, startMS, endMS int64, step time.Duration, seriesLimit int) rangeResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.setSeriesLimit(seriesLimit); err != nil {
		return rangeResult{Err: fmt.Sprintf("setting series limit: %v", err)}
	}
	logOffset := m.logSize()
	ts := func(ms int64) string { return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z") }
	u := m.baseURL + "/prometheus/api/v1/query_range?" + url.Values{
		"query": {q}, "start": {ts(startMS)}, "end": {ts(endMS)}, "step": {fmt.Sprintf("%ds", int(step.Seconds()))},
	}.Encode()

	start := time.Now()
	resp, err := http.Get(u)
	if err != nil {
		return rangeResult{Err: err.Error(), Latency: time.Since(start)}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	res := rangeResult{Latency: time.Since(start)}
	if err != nil {
		res.Err = err.Error()
		return res
	}
	var parsed struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			Result []struct {
				Values [][2]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		res.Err = fmt.Sprintf("decoding response: %v", err)
		return res
	}
	if parsed.Status != "success" {
		res.Err = parsed.Error
	}
	if len(parsed.Data.Result) > 0 {
		for _, v := range parsed.Data.Result[0].Values {
			sec, _ := strconv.ParseFloat(fmt.Sprint(v[0]), 64)
			val, _ := strconv.ParseFloat(fmt.Sprint(v[1]), 64)
			res.Times = append(res.Times, int64(sec*1000+0.5))
			res.Values = append(res.Values, val)
		}
	}
	res.Stats = m.statsFor(q, logOffset)
	return res
}

// getRoute calls one of Mimir's experimental cardinality routes over
// [minT, maxT) and decodes the JSON answer into out. It returns the latency
// and an error message, empty on success.
func (m *mimir) getRoute(route string, minT, maxT int64, params url.Values, out any) (time.Duration, string) {
	q := url.Values{"start": {fmt.Sprintf("%.3f", float64(minT)/1000)}, "end": {fmt.Sprintf("%.3f", float64(maxT)/1000)}}
	for k, v := range params {
		q[k] = v
	}
	start := time.Now()
	resp, err := http.Get(m.baseURL + "/prometheus/api/v1/cardinality/" + route + "?" + q.Encode())
	if err != nil {
		return time.Since(start), err.Error()
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	latency := time.Since(start)
	switch {
	case err != nil:
		return latency, err.Error()
	case resp.StatusCode == http.StatusNotFound && bytes.Contains(body, []byte("404 page not found")):
		return latency, "this Mimir build has no " + route + " route"
	case resp.StatusCode != http.StatusOK:
		return latency, strings.TrimSpace(string(body))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return latency, fmt.Sprintf("decoding response: %v", err)
	}
	return latency, ""
}

// mimirSeriesCounts is one call to the cardinality/estimate route.
type mimirSeriesCounts struct {
	Err     string
	Latency time.Duration
	// Read is how Mimir answered: index_header or full_index.
	Read          string
	Dedup         bool
	Blocks        int
	StoreGateways int
	SeriesCounted int
	IndexBytes    int64 // series entries and postings decoded
	BucketBytes   int64 // read from the bucket rather than a cache
	Counts        map[string]int
	Buckets       map[string][]int // only with a step
}

// estimate asks Mimir for series counts over [minT, maxT). params may set
// match[], group_by, step and max_index_bytes. Mimir picks the read.
func (m *mimir) estimate(minT, maxT int64, params url.Values) mimirSeriesCounts {
	var parsed struct {
		Read                 string `json:"read"`
		Dedup                bool   `json:"dedup"`
		Blocks               int    `json:"blocks"`
		StoreGateways        int    `json:"store_gateways"`
		SeriesCounted        int    `json:"series_counted"`
		IndexBytes           int64  `json:"index_bytes"`
		PostingsFetchedBytes int64  `json:"postings_fetched_bytes"`
		SeriesFetchedBytes   int64  `json:"series_fetched_bytes"`
		Counts               []struct {
			Value  string `json:"value"`
			Count  int    `json:"count"`
			Counts []int  `json:"counts"`
		} `json:"counts"`
	}
	res := mimirSeriesCounts{}
	if res.Latency, res.Err = m.getRoute("estimate", minT, maxT, params, &parsed); res.Err != "" {
		return res
	}
	res.Read, res.Dedup = parsed.Read, parsed.Dedup
	res.Blocks, res.StoreGateways, res.SeriesCounted = parsed.Blocks, parsed.StoreGateways, parsed.SeriesCounted
	res.IndexBytes, res.BucketBytes = parsed.IndexBytes, parsed.PostingsFetchedBytes+parsed.SeriesFetchedBytes
	res.Counts = make(map[string]int, len(parsed.Counts))
	for _, c := range parsed.Counts {
		res.Counts[c.Value] = c.Count
		if c.Counts != nil {
			if res.Buckets == nil {
				res.Buckets = map[string][]int{}
			}
			res.Buckets[c.Value] = c.Counts
		}
	}
	return res
}

// setSeriesLimit rewrites the runtime config for the tenant and waits for
// Mimir to report the new value, unless it is already in effect.
func (m *mimir) setSeriesLimit(limit int) error {
	if m.runtimeConfig == "" || (m.limitKnown && m.lastLimit == limit) {
		return nil
	}
	cfg := fmt.Sprintf("overrides:\n  anonymous:\n    max_fetched_chunks_per_query: 0\n    max_fetched_chunk_bytes_per_query: 0\n    max_fetched_series_per_query: %d\n", limit)
	want := fmt.Sprintf("max_fetched_series_per_query: %d", limit)
	if limit == defaultLimits {
		cfg, want = "overrides: {}\n", ""
	}
	if err := os.WriteFile(m.runtimeConfig, []byte(cfg), 0o644); err != nil {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(m.baseURL + "/runtime_config")
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			// With default limits the tenant has no override block left.
			loaded := bytes.Contains(b, []byte(want))
			if limit == defaultLimits {
				loaded = !bytes.Contains(b, []byte("anonymous:"))
			}
			if loaded {
				m.lastLimit, m.limitKnown = limit, true
				return nil
			}
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("mimir did not load the runtime config for series limit %d within 30s", limit)
}

func (m *mimir) logSize() int64 {
	if m.logPath == "" {
		return 0
	}
	st, err := os.Stat(m.logPath)
	if err != nil {
		return 0
	}
	return st.Size()
}

// statsFor returns the fields of the first query stats line for q written
// to the log after offset. The query-frontend writes it once the query
// finishes, so it may lag the response slightly.
func (m *mimir) statsFor(q string, offset int64) map[string]string {
	if m.logPath == "" {
		return nil
	}
	needle := "param_query=" + strconv.Quote(q)
	for attempt := 0; attempt < 20; attempt++ {
		if line := findLine(m.logPath, offset, `msg="query stats"`, needle); line != "" {
			return logfmtFields(line)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func findLine(path string, offset int64, needles ...string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return ""
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		ok := true
		for _, n := range needles {
			if !strings.Contains(line, n) {
				ok = false
				break
			}
		}
		if ok {
			return line
		}
	}
	return ""
}

// logfmtFields splits the unquoted key=value fields of a logfmt line;
// quoted values are skipped, which is enough for the numeric stats.
func logfmtFields(line string) map[string]string {
	out := map[string]string{}
	for _, f := range strings.Fields(line) {
		k, v, ok := strings.Cut(f, "=")
		if ok && !strings.HasPrefix(v, `"`) {
			out[k] = v
		}
	}
	return out
}

func matchName(name string) []*labels.Matcher {
	return []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", name)}
}
