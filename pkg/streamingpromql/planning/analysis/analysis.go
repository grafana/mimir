// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func Handler(planner *streamingpromql.QueryPlanner, limitsProvider streamingpromql.QueryLimitsProvider) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, status, err := handleAnalysis(w, r, planner, limitsProvider)

		if err != nil {
			body = []byte(err.Error())
			w.Header().Set("Content-Type", "text/plain")
		}

		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
}

func handleAnalysis(w http.ResponseWriter, r *http.Request, planner *streamingpromql.QueryPlanner, limitsProvider streamingpromql.QueryLimitsProvider) ([]byte, int, error) {
	if planner == nil {
		// Handle the case where query planning is disabled.
		return nil, http.StatusNotFound, errors.New("query planning is disabled, analysis is not available")
	}

	if limitsProvider == nil {
		return nil, http.StatusInternalServerError, errors.New("limits provider is not configured")
	}

	enableDelayedNameRemoval, err := limitsProvider.GetEnableDelayedNameRemoval(r.Context())
	if err != nil {
		return nil, http.StatusBadRequest, fmt.Errorf("could not get enable delayed name removal setting: %w", err)
	}

	if err := r.ParseForm(); err != nil {
		return nil, http.StatusBadRequest, fmt.Errorf("could not parse request: %w", err)
	}

	qs := r.Form.Get("query")
	if qs == "" {
		return nil, http.StatusBadRequest, errors.New("missing 'query' parameter")
	}

	var timeRange types.QueryTimeRange

	if r.Form.Has("time") && (r.Form.Has("start") || r.Form.Has("end") || r.Form.Has("step")) {
		return nil, http.StatusBadRequest, errors.New("cannot provide a mixture of parameters for instant query ('time') and range query ('start', 'end' and 'step')")
	}

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
			return nil, http.StatusBadRequest, errors.New("end time must be not be before start time")
		}

		if step <= 0 {
			return nil, http.StatusBadRequest, errors.New("step must be greater than 0")
		}

		timeRange = types.NewRangeQueryTimeRange(start, end, step)
	} else {
		return nil, http.StatusBadRequest, errors.New("missing 'time' parameter for instant query or 'start', 'end' and 'step' parameters for range query")
	}

	ctx := r.Context()
	var options querymiddleware.Options
	querymiddleware.DecodeOptions(r, &options)
	ctx = querymiddleware.ContextWithRequestHintsAndOptions(ctx, nil, options) // FIXME: populate hints as well (need cardinality estimation middleware for this)

	result, err := Analyze(ctx, planner, qs, timeRange, enableDelayedNameRemoval)
	if err != nil {
		var perr parser.ParseErrors
		if errors.As(err, &perr) {
			return nil, http.StatusBadRequest, fmt.Errorf("parsing expression failed: %w", perr)
		}

		return nil, http.StatusInternalServerError, fmt.Errorf("analysis failed: %w", err)
	}

	b, err := jsoniter.Marshal(result)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("could not marshal response: %w", err)
	}

	w.Header().Set("Content-Type", "application/json")
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

type Result struct {
	OriginalExpression string               `json:"originalExpression"`
	TimeRange          types.QueryTimeRange `json:"timeRange"`

	ASTStages      []ASTStage      `json:"astStages"`
	PlanningStages []PlanningStage `json:"planningStages"`

	PlanVersion int64 `json:"planVersion"`
}

type ASTStage struct {
	Name             string         `json:"name"`
	Duration         *time.Duration `json:"duration"` // nil if this stage has no associated duration (eg. represents final AST)
	OutputExpression string         `json:"outputExpression"`
}

type PlanningStage struct {
	Name       string              `json:"name"`
	Duration   *time.Duration      `json:"duration"`   // nil if this stage has no associated duration (eg. represents final plan)
	OutputPlan jsoniter.RawMessage `json:"outputPlan"` // Store the encoded JSON so we don't have to deal with cloning the entire query plan each time.
}

// Analyze performs query planning and produces a report on the query planning process.
func Analyze(ctx context.Context, planner *streamingpromql.QueryPlanner, qs string, timeRange types.QueryTimeRange, enableDelayedNameRemoval bool) (*Result, error) {
	observer := NewAnalysisPlanningObserver(qs, timeRange)
	_, err := planner.NewQueryPlan(ctx, qs, timeRange, enableDelayedNameRemoval, observer)
	if err != nil {
		return nil, err
	}

	return observer.Result, nil
}

type PlanningObserver struct {
	Result *Result
}

func NewAnalysisPlanningObserver(expr string, timeRange types.QueryTimeRange) *PlanningObserver {
	return &PlanningObserver{
		Result: &Result{
			OriginalExpression: expr,
			TimeRange:          timeRange,
		},
	}
}

func (o *PlanningObserver) OnASTStageComplete(stageName string, updatedExpr parser.Expr, duration time.Duration) error {
	o.Result.ASTStages = append(o.Result.ASTStages, ASTStage{
		Name:             stageName,
		Duration:         &duration,
		OutputExpression: updatedExpr.Pretty(0),
	})

	return nil
}

func (o *PlanningObserver) OnAllASTStagesComplete(finalExpr parser.Expr) error {
	o.Result.ASTStages = append(o.Result.ASTStages, ASTStage{
		Name:             "Final expression",
		OutputExpression: finalExpr.Pretty(0),
	})

	return nil
}

func (o *PlanningObserver) OnPlanningStageComplete(stageName string, updatedPlan *planning.QueryPlan, duration time.Duration) error {
	// Populate the plan version, so we can see the effect of different stages on the plan version.
	if err := updatedPlan.DeterminePlanVersion(); err != nil {
		return err
	}

	plan, _, err := updatedPlan.ToEncodedPlan(true, false)
	if err != nil {
		return err
	}

	planBytes, err := jsoniter.Marshal(plan)
	if err != nil {
		return err
	}

	o.Result.PlanningStages = append(o.Result.PlanningStages, PlanningStage{
		Name:       stageName,
		Duration:   &duration,
		OutputPlan: planBytes,
	})

	return nil
}

func (o *PlanningObserver) OnAllPlanningStagesComplete(finalPlan *planning.QueryPlan) error {
	plan, _, err := finalPlan.ToEncodedPlan(true, false)
	if err != nil {
		return err
	}

	planBytes, err := jsoniter.Marshal(plan)
	if err != nil {
		return err
	}

	o.Result.PlanningStages = append(o.Result.PlanningStages, PlanningStage{
		Name:       "Final plan",
		OutputPlan: planBytes,
	})

	return nil
}
