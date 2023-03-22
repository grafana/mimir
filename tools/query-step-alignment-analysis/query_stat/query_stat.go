package query_stat

import (
	"encoding/json"
	"math"
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

	RequestPath      string
	Match            string
	InstantQueryTime time.Time
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
			Query       string `json:"param_query"`
			Start       string `json:"param_start"`
			Step        string `json:"param_step"`
			End         string `json:"param_end"`
			Match       string `json:"param_match"`
			MatchSeries string `json:"param_match__"`
			Time        string `json:"param_time"`
			OrgID       string `json:"org_id"`
			Path        string `json:"path"`
		} `json:"labels"`
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(b, &d); err != nil {
		return err
	}

	qs.Query = d.Labels.Query
	qs.OrgID = d.Labels.OrgID

	var err error
	if d.Labels.Start != "" {
		qs.Start, err = parseTime(d.Labels.Start)
		if err != nil {
			return err
		}
	}

	if d.Labels.End != "" {
		qs.End, err = parseTime(d.Labels.End)
		if err != nil {
			return err
		}
	}

	if d.Labels.Time != "" {
		qs.InstantQueryTime, err = parseTime(d.Labels.Time)
		if err != nil {
			return err
		}
	}

	timestamp, err := time.Parse(time.RFC3339Nano, d.Timestamp)
	if err != nil {
		return err
	}
	qs.Timestamp = timestamp

	if d.Labels.Step != "" {
		step, err := parseDuration(d.Labels.Step)
		if err != nil {
			return err
		}
		qs.Step = step
	}

	qs.Match = d.Labels.Match
	if d.Labels.MatchSeries != "" {
		qs.Match = d.Labels.MatchSeries
	}
	qs.RequestPath = d.Labels.Path

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
