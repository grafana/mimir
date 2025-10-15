// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks

import (
	"bufio"
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

// ParseQueriesFromFile parses queries from a Loki log file in newline-delimited JSON format.
// Each line should be a JSON object with query information in the labels field.
func ParseQueriesFromFile(filepath string) ([]Query, error) {
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
