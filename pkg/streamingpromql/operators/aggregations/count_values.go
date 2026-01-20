// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package aggregations

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type CountValues struct {
	Inner                    types.InstantVectorOperator
	LabelName                types.StringOperator
	TimeRange                types.QueryTimeRange
	Grouping                 []string // If this is a 'without' aggregation, NewCountValues will ensure that this slice contains __name__.
	Without                  bool
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange

	resolvedLabelName string

	series [][]promql.FPoint

	// Reuse instances used to generate series labels rather than recreating them every time.
	labelsBuilder     *labels.Builder
	labelsBytesBuffer []byte
	valueBuffer       []byte
}

var _ types.InstantVectorOperator = &CountValues{}

func NewCountValues(
	inner types.InstantVectorOperator,
	labelName types.StringOperator,
	timeRange types.QueryTimeRange,
	grouping []string,
	without bool,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
) *CountValues {
	if without {
		grouping = append(grouping, model.MetricNameLabel)
	}

	slices.Sort(grouping)

	return &CountValues{
		Inner:                    inner,
		LabelName:                labelName,
		TimeRange:                timeRange,
		Grouping:                 grouping,
		Without:                  without,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

type countValuesSeries struct {
	labels           labels.Labels
	outputPointCount int
	count            []int // One entry per timestamp.
}

var countValuesSeriesPool = sync.Pool{
	New: func() interface{} {
		return &countValuesSeries{}
	},
}

func (c *CountValues) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	if err := c.loadLabelName(); err != nil {
		return nil, err
	}

	innerMetadata, err := c.Inner.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	defer types.SeriesMetadataSlicePool.Put(&innerMetadata, c.MemoryConsumptionTracker)

	c.labelsBuilder = labels.NewBuilder(labels.EmptyLabels())
	c.labelsBytesBuffer = make([]byte, 0, 1024) // Why 1024 bytes? It's what labels.Labels.String() uses as a buffer size, so we use that as a sensible starting point too.
	defer func() {
		// Don't hold onto the instances used to generate series labels for longer than necessary.
		c.labelsBuilder = nil
		c.labelsBytesBuffer = nil
		c.valueBuffer = nil
	}()

	accumulator := map[string]*countValuesSeries{}

	for _, s := range innerMetadata {
		data, err := c.Inner.NextSeries(ctx)
		if err != nil {
			return nil, err
		}

		for _, p := range data.Floats {
			if err := c.incrementCount(s.Labels, p.T, c.formatFloatValue(p.F), accumulator); err != nil {
				return nil, err
			}
		}

		for _, p := range data.Histograms {
			if err := c.incrementCount(s.Labels, p.T, p.H.String(), accumulator); err != nil {
				return nil, err
			}
		}

		types.PutInstantVectorSeriesData(data, c.MemoryConsumptionTracker)
	}

	outputMetadata, err := types.SeriesMetadataSlicePool.Get(len(accumulator), c.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	c.series = make([][]promql.FPoint, 0, len(accumulator))

	for _, s := range accumulator {
		outputMetadata, err = types.AppendSeriesMetadata(c.MemoryConsumptionTracker, outputMetadata, types.SeriesMetadata{Labels: s.labels})
		if err != nil {
			return nil, err
		}

		points, err := s.toPoints(c.MemoryConsumptionTracker, c.TimeRange)
		if err != nil {
			return nil, err
		}

		c.series = append(c.series, points)

		types.IntSlicePool.Put(&s.count, c.MemoryConsumptionTracker)
		countValuesSeriesPool.Put(s)
	}

	return outputMetadata, nil
}

func (c *CountValues) loadLabelName() error {
	c.resolvedLabelName = c.LabelName.GetValue()
	if !model.UTF8Validation.IsValidLabelName(c.resolvedLabelName) {
		return fmt.Errorf("invalid label name %q", c.resolvedLabelName)
	}

	return nil
}

func (c *CountValues) formatFloatValue(f float64) string {
	// Using AppendFloat like this (rather than FormatFloat) allows us to reuse the buffer used for formatting the string,
	// rather than creating a new one for every value.
	c.valueBuffer = c.valueBuffer[:0]
	c.valueBuffer = strconv.AppendFloat(c.valueBuffer, f, 'f', -1, 64)

	return string(c.valueBuffer)
}

func (c *CountValues) incrementCount(seriesLabels labels.Labels, t int64, value string, accumulator map[string]*countValuesSeries) error {
	l := c.computeOutputLabels(seriesLabels, value)
	c.labelsBytesBuffer = l.Bytes(c.labelsBytesBuffer)
	series, exists := accumulator[string(c.labelsBytesBuffer)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

	if !exists {
		series = countValuesSeriesPool.Get().(*countValuesSeries)
		series.labels = l
		series.outputPointCount = 0

		var err error
		series.count, err = types.IntSlicePool.Get(c.TimeRange.StepCount, c.MemoryConsumptionTracker)
		if err != nil {
			return err
		}

		series.count = series.count[:c.TimeRange.StepCount]
		accumulator[string(c.labelsBytesBuffer)] = series
	}

	idx := c.TimeRange.PointIndex(t)

	if series.count[idx] == 0 {
		series.outputPointCount++
	}

	series.count[idx]++

	return nil
}

func (c *CountValues) computeOutputLabels(seriesLabels labels.Labels, value string) labels.Labels {
	c.labelsBuilder.Reset(seriesLabels)

	if c.Without {
		c.labelsBuilder.Del(c.Grouping...)
	} else {
		c.labelsBuilder.Keep(c.Grouping...)
	}

	c.labelsBuilder.Set(c.resolvedLabelName, value)

	return c.labelsBuilder.Labels()
}

func (s *countValuesSeries) toPoints(memoryConsumptionTracker *limiter.MemoryConsumptionTracker, timeRange types.QueryTimeRange) ([]promql.FPoint, error) {
	p, err := types.FPointSlicePool.Get(s.outputPointCount, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	for idx, count := range s.count {
		if count == 0 {
			continue
		}

		t := timeRange.IndexTime(int64(idx))
		p = append(p, promql.FPoint{T: t, F: float64(count)})
	}

	return p, nil
}

func (c *CountValues) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	if len(c.series) == 0 {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	p := c.series[0]
	c.series = c.series[1:]

	return types.InstantVectorSeriesData{Floats: p}, nil
}

func (c *CountValues) ExpressionPosition() posrange.PositionRange {
	return c.expressionPosition
}

func (c *CountValues) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if err := c.Inner.Prepare(ctx, params); err != nil {
		return err
	}
	return c.LabelName.Prepare(ctx, params)
}

func (c *CountValues) AfterPrepare(ctx context.Context) error {
	if err := c.Inner.AfterPrepare(ctx); err != nil {
		return err
	}
	return c.LabelName.AfterPrepare(ctx)
}

func (c *CountValues) Finalize(ctx context.Context) error {
	if err := c.Inner.Finalize(ctx); err != nil {
		return err
	}
	return c.LabelName.Finalize(ctx)
}

func (c *CountValues) Close() {
	c.Inner.Close()
	c.LabelName.Close()

	for _, d := range c.series {
		types.FPointSlicePool.Put(&d, c.MemoryConsumptionTracker)
	}

	c.series = nil
}
