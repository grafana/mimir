// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/common/model"
)

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

// ParseQueriesFromFile parses queries from a Loki log file in JSON format.
// The file should be a JSON array of query objects.
func ParseQueriesFromFile(filepath string) ([]Query, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open query file: %w", err)
	}
	defer f.Close()

	// Decode as an array of Query objects
	dec := json.NewDecoder(f)
	var allQueries []Query
	if err := dec.Decode(&allQueries); err != nil {
		return nil, fmt.Errorf("failed to decode query file: %w", err)
	}

	// Filter to only include valid queries (POST requests with success status)
	var queries []Query
	for _, q := range allQueries {
		if q.valid {
			queries = append(queries, q)
		}
	}

	return queries, nil
}

// UnmarshalJSON implements custom JSON unmarshaling for Query.
func (q *Query) UnmarshalJSON(b []byte) error {
	var d struct {
		Fields struct {
			Query  string `json:"param_query"`
			Start  string `json:"param_start"`
			Step   string `json:"param_step"`
			End    string `json:"param_end"`
			OrgID  string `json:"user"`
			Method string `json:"method"`
			Status string `json:"status"`
		} `json:"fields"`
	}

	if err := json.Unmarshal(b, &d); err != nil {
		return err
	}

	// Only include POST requests with success status
	if d.Fields.Method != "POST" || d.Fields.Status != "success" {
		// Not a valid query to benchmark, but don't return error
		return nil
	}

	q.valid = true
	q.Query = d.Fields.Query
	q.OrgID = d.Fields.OrgID

	// Parse start time if present
	if d.Fields.Start != "" {
		start, err := parseTime(d.Fields.Start)
		if err != nil {
			return fmt.Errorf("failed to parse start time: %w", err)
		}
		q.Start = start
	}

	// Parse end time if present
	if d.Fields.End != "" {
		end, err := parseTime(d.Fields.End)
		if err != nil {
			return fmt.Errorf("failed to parse end time: %w", err)
		}
		q.End = end
	}

	// Parse step if present
	if d.Fields.Step != "" {
		step, err := parseDuration(d.Fields.Step)
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
