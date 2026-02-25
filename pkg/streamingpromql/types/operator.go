// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"context"
	"errors"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser/posrange"
)

type PrepareParams struct {
	// This struct used to contain values, but they are now passed by other means.
	// We kept it here to avoid making a big disruptive change.
}

// Operator represents all operators.
type Operator interface {
	// ExpressionPosition returns the position of the PromQL expression that this operator represents.
	ExpressionPosition() posrange.PositionRange

	// Close frees all resources associated with this operator and any nested operators.
	// Calling SeriesMetadata, NextSeries, NextStepSamples or Finalize after calling Close may result in unpredictable behaviour, corruption or crashes.
	// It must be safe to call Close at any time, including if SeriesMetadata or NextSeries have returned an error.
	// It must be safe to call Close multiple times.
	// Calling Close must not modify query results, annotations or stats.
	Close()

	// Prepare prepares the operator for execution. It must be called before calling SeriesMetadata, NextSeries, NextStepSamples or Finalize.
	// Prepare must not call SeriesMetadata, NextSeries, NextStepSamples or Finalize on another operator, and is expected to call Prepare on
	// any nested operators.
	// Prepare must only be called once.
	Prepare(ctx context.Context, params *PrepareParams) error

	// AfterPrepare is called after Prepare has returned successfully for all operators in an evaluation.
	//
	// It must be called before calling SeriesMetadata, NextSeries, NextStepSamples or Finalize.
	// AfterPrepare must not call SeriesMetadata, NextSeries, NextStepSamples or Finalize on another operator, and is expected to call AfterPrepare on
	// any nested operators.
	// AfterPrepare must only be called once.
	//
	// Favour putting logic in Prepare over AfterPrepare where possible, AfterPrepare should generally only be used for logic that relies on
	// Prepare having already been called on all operators (eg. operators that collect requests from other operators).
	AfterPrepare(ctx context.Context) error

	// Finalize performs any outstanding work required before the query result is considered complete.
	// For example, any outstanding annotations should be emitted and query stats should be updated.
	// It must be safe to call Finalize even if Prepare, SeriesMetadata, NextSeries, NextStepSamples or Finalize have not been called.
	// It must be safe to call Finalize multiple times.
	// Finalize must not call SeriesMetadata, NextSeries, NextStepSamples or Prepare on another operator, and is expected to call Finalize on
	// any nested operators.
	// Calling Finalize after Prepare, SeriesMetadata, NextSeries or NextStepSamples have returned an error may result in unpredictable
	// behaviour, corruption or crashes.
	Finalize(ctx context.Context) error
}

// SeriesOperator represents all operators that return one or more series.
type SeriesOperator interface {
	Operator

	// SeriesMetadata returns a list of all series that will be returned by this operator.
	// Additional matchers determined during query evaluation may be passed to further limit
	// the series produced by this operator. Implementations may ignore these extra matchers.
	// The returned []SeriesMetadata can be modified by the caller or returned to a pool.
	// SeriesMetadata may return series in any order, but the same order must be used by both SeriesMetadata and NextSeries.
	// SeriesMetadata should be called no more than once.
	SeriesMetadata(ctx context.Context, matchers Matchers) ([]SeriesMetadata, error)
}

// InstantVectorOperator represents all operators that produce instant vectors.
type InstantVectorOperator interface {
	SeriesOperator

	// NextSeries returns the next series from this operator, or EOS if no more series are available.
	// SeriesMetadata must be called exactly once before calling NextSeries.
	// The returned InstantVectorSeriesData can be modified by the caller or returned to a pool.
	// The returned InstantVectorSeriesData can contain no points.
	NextSeries(ctx context.Context) (InstantVectorSeriesData, error)
}

// RangeVectorOperator represents all operators that produce range vectors.
type RangeVectorOperator interface {
	SeriesOperator

	// NextSeries advances to the next series produced by this operator, or EOS if no more series are available.
	// SeriesMetadata must be called exactly once before calling NextSeries.
	NextSeries(ctx context.Context) error

	// NextStepSamples returns populated RingBuffers with the samples for the next time step for the
	// current series and the timestamps of the next time step, or returns EOS if no more time
	// steps are available.
	NextStepSamples(ctx context.Context) (*RangeVectorStepData, error)
}

// ScalarOperator represents all operators that produce scalars.
type ScalarOperator interface {
	Operator

	// GetValues returns the samples for this scalar.
	GetValues(ctx context.Context) (ScalarData, error)
}

// StringOperator represents all operators that produce strings.
type StringOperator interface {
	Operator

	// GetValue returns the string
	GetValue() string
}

// Matcher is a value type version of the Prometheus labels.Matcher type.
// It exists so that we can use matchers as map keys and compare them with
// equals operators (this is not possible with labels.Matcher types because
// they include a pointer to a regular expression).
type Matcher struct {
	Type  labels.MatchType
	Name  string
	Value string
}

func NewMatcherFromPrometheusType(m *labels.Matcher) Matcher {
	return Matcher{
		Type:  m.Type,
		Name:  m.Name,
		Value: m.Value,
	}
}

func (m Matcher) ToPrometheusType() (*labels.Matcher, error) {
	return labels.NewMatcher(m.Type, m.Name, m.Value)
}

type Matchers []Matcher

func (s Matchers) ToPrometheusType() ([]*labels.Matcher, error) {
	if len(s) == 0 {
		return []*labels.Matcher{}, nil
	}

	out := make([]*labels.Matcher, 0, len(s))
	for _, m := range s {
		prom, err := m.ToPrometheusType()
		if err != nil {
			return nil, err
		}

		out = append(out, prom)
	}

	return out, nil
}

// With returns a new Matchers that only contains matchers targeting labels
// with the given names.
func (s Matchers) With(names ...string) Matchers {
	out := make([]Matcher, 0, len(s))

	for _, m := range s {
		for _, name := range names {
			if m.Name == name {
				out = append(out, m)
			}
		}
	}

	return out
}

var EOS = errors.New("operator stream exhausted") //nolint:revive,staticcheck
