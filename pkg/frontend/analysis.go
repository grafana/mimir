// SPDX-License-Identifier: AGPL-3.0-only

package frontend

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func AnalysisHandler(engine *streamingpromql.Engine) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, status, err := handleAnalysis(w, r, engine)
		w.WriteHeader(status)

		if err != nil {
			// TODO: return a Prometheus-style JSON error payload
			_, _ = w.Write([]byte(err.Error()))
			return
		}

		_, _ = w.Write(body)
	})
}

func handleAnalysis(w http.ResponseWriter, r *http.Request, engine *streamingpromql.Engine) ([]byte, int, error) {
	if err := r.ParseForm(); err != nil {
		return nil, http.StatusBadRequest, fmt.Errorf("could not parse request: %w", err)
	}

	qs := r.Form.Get("query")
	if qs == "" {
		return nil, http.StatusBadRequest, errors.New("missing 'query' parameter")
	}

	var timeRange types.QueryTimeRange

	if r.Form.Has("time") {
		t, err := parseTime(r.Form.Get("time"))
		if err != nil {
			return nil, http.StatusBadRequest, fmt.Errorf("could not parse 'time' parameter: %w", err)
		}

		timeRange = types.NewInstantQueryTimeRange(t)
	} else if r.Form.Has("start") && r.Form.Has("end") && r.Form.Has("step") {
		start, err := parseTime(r.Form.Get("start"))
		if err != nil {
			return nil, http.StatusBadRequest, fmt.Errorf("could not parse 'start' parameter: %w", err)
		}

		end, err := parseTime(r.Form.Get("end"))
		if err != nil {
			return nil, http.StatusBadRequest, fmt.Errorf("could not parse 'end' parameter: %w", err)
		}

		step, err := parseDuration(r.Form.Get("step"))
		if err != nil {
			return nil, http.StatusBadRequest, fmt.Errorf("could not parse 'step' parameter: %w", err)
		}

		if end.Before(start) {
			return nil, http.StatusBadRequest, errors.New("end time must be greater than start time")
		}

		if step <= 0 {
			return nil, http.StatusBadRequest, errors.New("step must be greater than 0")
		}

		timeRange = types.NewRangeQueryTimeRange(start, end, step)
	} else {
		return nil, http.StatusBadRequest, errors.New("missing 'time' parameter for instant query or 'start', 'end' and 'step' parameters for range query")
	}

	result, err := engine.Analyze(r.Context(), qs, timeRange)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("analysis failed: %w", err)
	}

	b, err := jsoniter.Marshal(result)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("could not marshal response: %w", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	return b, http.StatusOK, nil
}

// Based on Prometheus' web/api/v1/api.go
func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}
