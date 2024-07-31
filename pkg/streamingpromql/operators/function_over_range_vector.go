// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operators

import (
	"context"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// FunctionOverRangeVector performs a rate calculation over a range vector.
type FunctionOverRangeVector struct {
	Inner types.RangeVectorOperator
	Pool  *pooling.LimitingPool

	MetadataFunc functions.SeriesMetadataFunction
	StepFunc     functions.RangeVectorStepFunction

	Annotations *annotations.Annotations

	numSteps        int
	rangeSeconds    float64
	floatBuffer     *types.FPointRingBuffer
	histogramBuffer *types.HPointRingBuffer

	expressionPosition posrange.PositionRange
}

var _ types.InstantVectorOperator = &FunctionOverRangeVector{}

func NewFunctionOverRangeVector(
	inner types.RangeVectorOperator,
	pool *pooling.LimitingPool,
	metadataFunc functions.SeriesMetadataFunction,
	stepFunc functions.RangeVectorStepFunction,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
) *FunctionOverRangeVector {
	return &FunctionOverRangeVector{
		Inner:              inner,
		Pool:               pool,
		MetadataFunc:       metadataFunc,
		StepFunc:           stepFunc,
		Annotations:        annotations,
		expressionPosition: expressionPosition,
	}
}

func (m *FunctionOverRangeVector) ExpressionPosition() posrange.PositionRange {
	return m.expressionPosition
}

func (m *FunctionOverRangeVector) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	metadata, err := m.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	m.numSteps = m.Inner.StepCount()
	m.rangeSeconds = m.Inner.Range().Seconds()

	return m.MetadataFunc(metadata, m.Pool)
}

func (m *FunctionOverRangeVector) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if err := m.Inner.NextSeries(ctx); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if m.floatBuffer == nil {
		m.floatBuffer = types.NewFPointRingBuffer(m.Pool)
	}

	if m.histogramBuffer == nil {
		m.histogramBuffer = types.NewHPointRingBuffer(m.Pool)
	}

	m.floatBuffer.Reset()
	m.histogramBuffer.Reset()

	data := types.InstantVectorSeriesData{}

	for {
		step, err := m.Inner.NextStepSamples(m.floatBuffer, m.histogramBuffer)

		// nolint:errorlint // errors.Is introduces a performance overhead, and NextStepSamples is guaranteed to return exactly EOS, never a wrapped error.
		if err == types.EOS {
			return data, nil
		} else if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		f, hasFloat, h, err := m.StepFunc(step, m.rangeSeconds, m.floatBuffer, m.histogramBuffer)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}
		if hasFloat {
			if data.Floats == nil {
				// Only get fPoint slice once we are sure we have float points.
				// This potentially over-allocates as some points in the steps may be histograms,
				// but this is expected to be rare.
				data.Floats, err = m.Pool.GetFPointSlice(m.numSteps)
				if err != nil {
					return types.InstantVectorSeriesData{}, err
				}
			}
			data.Floats = append(data.Floats, promql.FPoint{T: step.StepT, F: f})
		}
		if h != nil {
			if data.Histograms == nil {
				// Only get hPoint slice once we are sure we have histogram points.
				// This potentially over-allocates as some points in the steps may be floats,
				// but this is expected to be rare.
				data.Histograms, err = m.Pool.GetHPointSlice(m.numSteps)
				if err != nil {
					return types.InstantVectorSeriesData{}, err
				}
			}
			data.Histograms = append(data.Histograms, promql.HPoint{T: step.StepT, H: h})
		}
	}
}

func (m *FunctionOverRangeVector) Close() {
	m.Inner.Close()

	if m.floatBuffer != nil {
		m.floatBuffer.Close()
	}
	if m.histogramBuffer != nil {
		m.histogramBuffer.Close()
	}
}
