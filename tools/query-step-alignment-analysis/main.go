// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/common/model"
)

type QueryStat struct {
	Start     time.Time
	End       time.Time
	Timestamp time.Time
	Step      time.Duration
	Query     string
	OrgID     string
}

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

// Queries which are step aligned
func (qs *QueryStat) IsStepAligned() bool {
	start := qs.Start.Truncate(qs.Step)
	end := qs.End.Truncate(qs.Step)
	return qs.Start == start && qs.End == end
}

// Queries which are involving only recent data are not cached
func (qs *QueryStat) IsInLast10Minutes() bool {
	return qs.Timestamp.Add(-10 * time.Minute).Before(qs.Start)
}

func (qs *QueryStat) UnmarshalJSON(b []byte) error {

	var d struct {
		Labels struct {
			Query string `json:"param_query"`
			Start string `json:"param_start"`
			Step  string `json:"param_step"`
			End   string `json:"param_end"`
			OrgID string `json:"org_id"`
		} `json:"labels"`
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(b, &d); err != nil {
		return err
	}

	qs.Query = d.Labels.Query
	qs.OrgID = d.Labels.OrgID

	var err error
	qs.Start, err = parseTime(d.Labels.Start)
	if err != nil {
		return err
	}

	qs.End, err = parseTime(d.Labels.End)
	if err != nil {
		return err
	}

	timestamp, err := time.Parse(time.RFC3339Nano, d.Timestamp)
	if err != nil {
		return err
	}
	qs.Timestamp = timestamp

	step, err := parseDuration(d.Labels.Step)
	if err != nil {
		return err
	}
	qs.Step = step

	return nil
}

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

func main() {
	dec := json.NewDecoder(os.Stdin)

	var (
		total                          uint64
		countStepAligned               uint64
		countCacheableWithStepAlign    uint64
		countCacheableWithoutStepAlign uint64
		startTimestamp                 time.Time
		endTimestamp                   time.Time
	)

	var qs QueryStat

	for {
		if err := dec.Decode(&qs); err == io.EOF {
			break
		} else if err != nil {
			log.Print("warning: ", err)
			continue
		}
		total++

		if !qs.IsInLast10Minutes() {
			countCacheableWithStepAlign++
		}

		if qs.IsStepAligned() {
			countStepAligned++
		}

		if !qs.IsInLast10Minutes() && qs.IsStepAligned() {
			countCacheableWithoutStepAlign++
		}

		if startTimestamp.IsZero() || startTimestamp.After(qs.Timestamp) {
			startTimestamp = qs.Timestamp
		}
		if endTimestamp.Before(qs.Timestamp) {
			endTimestamp = qs.Timestamp
		}
	}

	fmt.Printf("time-frame:                  %s to %s\n", startTimestamp, endTimestamp)
	fmt.Printf("total queries:               % 9d\n", total)
	fmt.Printf("cachable-with-step-align:    % 9d % 2.2f%%\n", countCacheableWithStepAlign, float64(countCacheableWithStepAlign)/float64(total)*100.0)
	fmt.Printf("cachable-without-step-align: % 9d % 2.2f%%\n", countCacheableWithoutStepAlign, float64(countCacheableWithoutStepAlign)/float64(total)*100.0)
	fmt.Printf("step-aligned:                % 9d % 2.2f%%\n", countStepAligned, float64(countStepAligned)/float64(total)*100.0)
}
