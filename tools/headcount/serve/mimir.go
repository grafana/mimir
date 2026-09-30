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
