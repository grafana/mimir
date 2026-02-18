// SPDX-License-Identifier: AGPL-3.0-only

package bench

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/util/promqlext"
)

const (
	// numSegments is the number of segments to split queries into for sampling.
	// This value provides a good balance between sampling granularity and performance
	// for typical query sets (1000-100000 queries). Each segment is sampled independently
	// to ensure representative coverage across the entire query distribution.
	numSegments = 100
)

// QueryLoader loads and parses queries from query-frontend "query stats" logs.
// The queries are contained in a logcli log file in JSONL format.
// It supports filtering by tenant ID and/or specific query IDs, as well as sampling.
//
// QueryLoader expects query logs in Loki's newline-delimited JSON (JSONL) format,
// where each line contains a JSON object with query parameters in the labels field.
//
// You can obtain query logs using logcli with a command like:
//
//	logcli query -q --timezone=UTC --limit=1000000 \
//	  --from='2025-10-15T15:15:21.0Z' \
//	  --to='2025-10-15T16:15:21.0Z' \
//	  --output=jsonl \
//	  '{namespace="mimir", name="query-frontend"} |= "query stats" | logfmt | path=~".*/query(_range)?"' \
//	  > logs.json
//
// Each log line should have the following structure:
//
//	{
//	  "labels": {
//	    "param_query": "sum(rate(metric[5m]))",
//	    "param_start": "1697452800",
//	    "param_end": "1697456400",
//	    "param_step": "60",
//	    "user": "tenant-123"
//	  },
//	  "timestamp": "2025-10-15T15:30:00.123456789Z"
//	}
type QueryLoader struct {
	cache sync.Map
}

// NewQueryLoader creates a new QueryLoader.
func NewQueryLoader() *QueryLoader {
	return &QueryLoader{}
}

// QueryLoaderConfig contains the parameters for loading queries.
type QueryLoaderConfig struct {
	// Filepath is the path to the Loki log file in JSONL format.
	Filepath string
	// TenantID is the tenant ID to filter queries by. If empty, all queries are used.
	TenantID string
	// QueryIDs is a list of query IDs (line numbers) to load.
	// Mutually exclusive with sampling (SampleFraction < 1.0).
	QueryIDs []int
	// SampleFraction is the fraction of queries to sample (0.0 to 1.0).
	// Queries are split into segments and sampled uniformly across segments.
	SampleFraction float64
	// Seed is the random seed for query sampling.
	Seed int64
}

// PrepareQueries loads queries from a Loki log file in JSONL format, filters by tenant ID
// and/or query IDs if specified, and samples them according to the given parameters.
// Results are cached to avoid re-processing. Queries are returned with their VectorSelectors already parsed.
//
// The file should contain newline-delimited JSON logs with query parameters in the labels field,
// as produced by logcli (see package documentation for details).
func (qc *QueryLoader) PrepareQueries(config QueryLoaderConfig) ([]Query, ParsingStats, error) {
	// Validate that QueryIDs and sampling are mutually exclusive
	if len(config.QueryIDs) > 0 && config.SampleFraction < 1.0 {
		return nil, ParsingStats{}, fmt.Errorf("QueryIDs and SampleFraction < 1.0 are mutually exclusive")
	}

	// Create cache key from parameters
	cacheKey := fmt.Sprintf("%s|%s|%v|%f|%d", config.Filepath, config.TenantID, config.QueryIDs, config.SampleFraction, config.Seed)

	// Check cache first
	if cached, ok := qc.cache.Load(cacheKey); ok {
		return cached.([]Query), ParsingStats{}, nil
	}

	// Load queries from file (with early filtering by query IDs if specified)
	queries, stats, err := loadQueryLogsFromFile(config.Filepath, config.QueryIDs)
	if err != nil {
		return nil, ParsingStats{}, err
	}

	// Filter by tenant ID if specified
	if config.TenantID != "" {
		filtered := queries[:0]
		for i := range queries {
			if queries[i].User == config.TenantID {
				filtered = append(filtered, queries[i])
			}
		}
		queries = filtered
	}

	// Sample queries only if not filtering by specific IDs
	if len(config.QueryIDs) == 0 {
		queries = sampleQueries(queries, config.SampleFraction, config.Seed)
	}

	// Store in cache before returning
	qc.cache.Store(cacheKey, queries)

	return queries, stats, nil
}

type ParsingStats struct {
	MalformedLines int
}

// Query represents a parsed query from Loki logs.
type Query struct {
	QueryID         int                 // Line number from the file (1-indexed)
	Query           string              // Raw PromQL query string
	VectorSelectors [][]*labels.Matcher // Extracted label matchers from vector selectors in the query
	Start           time.Time
	End             time.Time
	Step            time.Duration
	Timestamp       time.Time
	User            string
	valid           bool // internal flag to track if query should be included
}

// loadQueryLogsFromFile parses queries from a Loki log file in newline-delimited JSON (JSONL) format.
// Each line should be a JSON object with query information in the labels field (see package documentation).
// If queryIDs is non-empty, only queries with matching line numbers are returned.
// Returns the parsed queries and statistics about the parsing process.
func loadQueryLogsFromFile(filepath string, queryIDs []int) ([]Query, ParsingStats, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, ParsingStats{}, fmt.Errorf("failed to open query file %q: %w", filepath, err)
	}
	defer f.Close()

	// Create a set for fast lookups if filtering by query IDs
	var queryIDSet map[int]struct{}
	if len(queryIDs) > 0 {
		queryIDSet = make(map[int]struct{}, len(queryIDs))
		for _, id := range queryIDs {
			queryIDSet[id] = struct{}{}
		}
	}

	var queries []Query
	var stats ParsingStats
	scanner := bufio.NewScanner(f)
	scanner.Buffer(nil, 10*1024*1024) // 10MB max line size
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// Skip lines that don't match query ID filter
		if queryIDSet != nil {
			if _, ok := queryIDSet[lineNum]; !ok {
				continue
			}
		}

		var q Query
		if err = json.Unmarshal(line, &q); err != nil || !q.valid {
			// Skip malformed lines
			stats.MalformedLines++
			continue
		}

		q.QueryID = lineNum
		queries = append(queries, q)
	}

	if err := scanner.Err(); err != nil {
		return nil, ParsingStats{}, fmt.Errorf("error reading file: %w", err)
	}

	return queries, stats, nil
}

// extractLabelMatchers extracts label matchers from a PromQL query string.
// It parses the PromQL expression and returns a separate set of matchers for each vector selector.
// Returns a slice of slices, where each inner slice represents one vector selector's matchers.
func extractLabelMatchers(query string) ([][]*labels.Matcher, error) {
	p := promqlext.NewPromQLParser()
	expr, err := p.ParseExpr(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PromQL query: %w", err)
	}

	// Collect matchers from each vector selector separately
	var allMatcherSets [][]*labels.Matcher

	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if n, ok := node.(*parser.VectorSelector); ok {
			var matchers []*labels.Matcher

			// Add all label matchers
			// This includes the __name__ matcher even if it was provided as a metric name (e.g. metric{...})
			for _, lm := range n.LabelMatchers {
				if lm != nil {
					matchers = append(matchers, lm)
				}
			}

			if len(matchers) > 0 {
				allMatcherSets = append(allMatcherSets, matchers)
			}
		}
		return nil
	})

	return allMatcherSets, nil
}

// UnmarshalJSON implements custom JSON unmarshaling for Query.
// It expects the Loki log format with query parameters in the labels field:
//
//	{"labels": {"param_query": "...", "param_start": "...", ...}, "timestamp": "..."}
//
// This format is produced by the command documented in the queryFileFlag help text.
func (q *Query) UnmarshalJSON(b []byte) error {
	var d struct {
		Labels struct {
			Query  string `json:"param_query"`
			Start  string `json:"param_start"`
			Step   string `json:"param_step"`
			End    string `json:"param_end"`
			Method string `json:"method"`
			User   string `json:"user"`
		} `json:"labels"`
		Timestamp string `json:"timestamp"`
	}

	if err := json.Unmarshal(b, &d); err != nil {
		return err
	}

	// Skip if no query present
	if d.Labels.Query == "" {
		return fmt.Errorf("no query present in log entry")
	}

	vectorSelectors, err := extractLabelMatchers(d.Labels.Query)
	if err != nil {
		return fmt.Errorf("failed to extract label matchers: %w", err)
	}

	q.valid = true
	q.Query = d.Labels.Query
	q.User = d.Labels.User
	q.VectorSelectors = vectorSelectors

	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339Nano, d.Timestamp)
	if err != nil {
		return fmt.Errorf("failed to parse timestamp: %w", err)
	}
	q.Timestamp = timestamp

	// Parse start time if present
	if d.Labels.Start != "" {
		start, err := parseTime(d.Labels.Start)
		if err != nil {
			return fmt.Errorf("failed to parse start time: %w", err)
		}
		q.Start = start
	}

	// Parse end time if present
	if d.Labels.End != "" {
		end, err := parseTime(d.Labels.End)
		if err != nil {
			return fmt.Errorf("failed to parse end time: %w", err)
		}
		q.End = end
	}

	// Parse step if present
	if d.Labels.Step != "" {
		step, err := parseDuration(d.Labels.Step)
		if err != nil {
			return fmt.Errorf("failed to parse step: %w", err)
		}
		q.Step = step
	}

	return nil
}

// parseTime parses a time string in RFC3339Nano format.
func parseTime(str string) (time.Time, error) {
	timestamp, err := time.Parse(time.RFC3339Nano, str)
	if err != nil {
		return time.Time{}, err
	}
	return timestamp, nil
}

// parseDuration parses a duration string that can be either a float (seconds) or Prometheus duration format.
func parseDuration(s string) (time.Duration, error) {
	if step, err := strconv.ParseFloat(s, 64); err == nil {
		return time.Second * time.Duration(step), nil
	}

	stepD, err := model.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	return time.Duration(stepD), nil
}

// sampleQueries samples queries by splitting them into segments and taking a continuous
// sample from each segment. This ensures representative coverage across the query set.
func sampleQueries(queries []Query, sampleFraction float64, seed int64) []Query {
	if sampleFraction >= 1.0 {
		return queries
	}
	if sampleFraction <= 0.0 {
		return nil
	}

	totalQueries := len(queries)
	if totalQueries == 0 {
		return queries
	}

	rng := rand.New(rand.NewSource(seed))

	// Calculate segment size
	segmentSize := totalQueries / numSegments
	if segmentSize == 0 {
		segmentSize = 1
	}

	// Calculate sample size per segment
	sampleSize := int(math.Ceil(float64(segmentSize) * sampleFraction))
	if sampleSize < 1 {
		sampleSize = 1
	}

	var sampledQueries []Query

	// Sample from each segment
	for seg := 0; seg < numSegments; seg++ {
		segmentStart := seg * segmentSize
		segmentEnd := segmentStart + segmentSize
		if segmentEnd > totalQueries {
			segmentEnd = totalQueries
		}

		// If segment is empty, skip it
		currentSegmentSize := segmentEnd - segmentStart
		if currentSegmentSize == 0 {
			break
		}

		// Adjust sample size if segment is smaller than expected
		currentSampleSize := sampleSize
		if currentSampleSize > currentSegmentSize {
			currentSampleSize = currentSegmentSize
		}

		// Pick random starting point within the segment for continuous sample
		maxStartOffset := currentSegmentSize - currentSampleSize
		startOffset := 0
		if maxStartOffset > 0 {
			startOffset = rng.Intn(maxStartOffset + 1)
		}

		// Extract continuous sample
		sampleStart := segmentStart + startOffset
		sampleEnd := sampleStart + currentSampleSize

		sampledQueries = append(sampledQueries, queries[sampleStart:sampleEnd]...)
	}

	return sampledQueries
}
