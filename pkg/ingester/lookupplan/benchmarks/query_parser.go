// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks

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
)

const (
	// numSegments is the number of segments to split queries into for sampling.
	numSegments = 100
)

// vectorQueryCache caches prepared vector queries to avoid re-processing the same file with same parameters.
// Cache key is a string combining filepath, sample fraction, and seed.
var vectorQueryCache sync.Map

// vectorSelectorQuery represents a single vector selector extracted from a query.
type vectorSelectorQuery struct {
	originalQuery *Query
	matchers      []*labels.Matcher
}

// Query represents a parsed query from Loki logs.
type Query struct {
	Query     string
	Start     time.Time
	End       time.Time
	Step      time.Duration
	Timestamp time.Time
	OrgID     string
	valid     bool // internal flag to track if query should be included
}

// LoadQueryLogsFromFile parses queries from a Loki log file in newline-delimited JSON format.
// Each line should be a JSON object with query information in the labels field.
func LoadQueryLogsFromFile(filepath string) ([]Query, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open query file: %w", err)
	}
	defer f.Close()

	var queries []Query
	scanner := bufio.NewScanner(f)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var q Query
		if err := json.Unmarshal(line, &q); err != nil {
			// Skip malformed lines
			continue
		}

		if q.valid {
			queries = append(queries, q)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	return queries, nil
}

// UnmarshalJSON implements custom JSON unmarshaling for Query.
func (q *Query) UnmarshalJSON(b []byte) error {
	var d struct {
		Labels struct {
			Query  string `json:"param_query"`
			Start  string `json:"param_start"`
			Step   string `json:"param_step"`
			End    string `json:"param_end"`
			Method string `json:"method"`
		} `json:"labels"`
		Timestamp string `json:"timestamp"`
	}

	if err := json.Unmarshal(b, &d); err != nil {
		return err
	}

	// Skip if no query present
	if d.Labels.Query == "" {
		return nil
	}

	q.valid = true
	q.Query = d.Labels.Query

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

// parseTime parses a time string that can be either a Unix timestamp (float) or RFC3339Nano format.
func parseTime(str string) (time.Time, error) {
	t, err := strconv.ParseFloat(str, 64)
	if err != nil {
		timestamp, err := time.Parse(time.RFC3339Nano, str)
		if err != nil {
			return time.Time{}, err
		}
		return timestamp, nil
	}

	s, ns := math.Modf(t)
	ns = math.Round(ns*1000) / 1000
	return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
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

// PrepareVectorQueries loads queries from a file, extracts label matchers from each query,
// and samples them according to the given parameters. Results are cached to avoid re-processing.
func PrepareVectorQueries(filepath string, sampleFraction float64, seed int64) ([]vectorSelectorQuery, error) {
	// Create cache key from parameters
	cacheKey := fmt.Sprintf("%s|%f|%d", filepath, sampleFraction, seed)

	// Check cache first
	if cached, ok := vectorQueryCache.Load(cacheKey); ok {
		return cached.([]vectorSelectorQuery), nil
	}

	// Load queries from file
	queries, err := LoadQueryLogsFromFile(filepath)
	if err != nil {
		return nil, err
	}

	// Extract label matchers from each query (each query may produce multiple vector selectors)
	var vectorQueries []vectorSelectorQuery
	for i := range queries {
		matchersList, err := extractLabelMatchers(queries[i].Query)
		if err != nil {
			continue
		}

		for _, matchers := range matchersList {
			vectorQueries = append(vectorQueries, vectorSelectorQuery{
				originalQuery: &queries[i],
				matchers:      matchers,
			})
		}
	}

	// Sample queries
	sampledQueries := sampleQueries(vectorQueries, sampleFraction, seed)

	// Store in cache before returning
	vectorQueryCache.Store(cacheKey, sampledQueries)

	return sampledQueries, nil
}

// extractLabelMatchers extracts label matchers from a PromQL query string.
// It parses the PromQL expression and returns a separate set of matchers for each vector selector.
// Returns a slice of slices, where each inner slice represents one vector selector's matchers.
func extractLabelMatchers(query string) ([][]*labels.Matcher, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PromQL query: %w", err)
	}

	// Collect matchers from each vector selector separately
	var allMatcherSets [][]*labels.Matcher

	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if n, ok := node.(*parser.VectorSelector); ok {
			var matchers []*labels.Matcher

			// Add the metric name matcher if present
			if n.Name != "" {
				matcher, err := labels.NewMatcher(labels.MatchEqual, labels.MetricName, n.Name)
				if err == nil {
					matchers = append(matchers, matcher)
				}
			}

			// Add all label matchers
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

// sampleQueries samples queries by splitting them into segments and taking a continuous
// sample from each segment. This ensures representative coverage across the query set.
func sampleQueries(queries []vectorSelectorQuery, sampleFraction float64, seed int64) []vectorSelectorQuery {
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

	var sampledQueries []vectorSelectorQuery

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
