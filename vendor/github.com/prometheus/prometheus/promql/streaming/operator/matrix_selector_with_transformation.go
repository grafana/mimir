package operator

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/streaming/util"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// TODO: support transformations other than 'rate'
type MatrixSelectorWithTransformationOverRange struct {
	Queryable     storage.Queryable
	Start         time.Time
	End           time.Time
	Interval      time.Duration
	Range         time.Duration
	LookbackDelta time.Duration
	Matchers      []*labels.Matcher
	Pool          *Pool

	querier                 storage.Querier
	currentSeriesBatch      *SeriesBatch
	currentSeriesBatchIndex int
	chunkIterator           chunkenc.Iterator
	buffer                  *util.RingBuffer

	// TODO: is it cheaper to just recompute these every time we need them rather than holding them?
	startTimestamp       int64
	endTimestamp         int64
	intervalMilliseconds int64
	rangeMilliseconds    int64
}

var _ Operator = &MatrixSelectorWithTransformationOverRange{}

func (m *MatrixSelectorWithTransformationOverRange) Series(ctx context.Context) ([]SeriesMetadata, error) {
	if m.currentSeriesBatch != nil {
		panic("should not call Series() multiple times")
	}

	m.startTimestamp = timestamp.FromTime(m.Start)
	m.endTimestamp = timestamp.FromTime(m.End)
	m.intervalMilliseconds = durationMilliseconds(m.Interval)
	m.rangeMilliseconds = durationMilliseconds(m.Range)

	start := m.startTimestamp - m.rangeMilliseconds

	hints := &storage.SelectHints{
		Start: start,
		End:   m.endTimestamp,
		Step:  m.intervalMilliseconds,
		Range: durationMilliseconds(m.Range),
		Func:  "rate",
		// TODO: do we need to include other hints like By, Grouping?
	}

	var err error
	m.querier, err = m.Queryable.Querier(ctx, start, m.endTimestamp)
	if err != nil {
		return nil, err
	}

	ss := m.querier.Select(true, hints, m.Matchers...)
	m.currentSeriesBatch = m.Pool.GetSeriesBatch()
	incompleteBatch := m.currentSeriesBatch
	totalSeries := 0

	for ss.Next() {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if len(incompleteBatch.series) == cap(incompleteBatch.series) {
			nextBatch := m.Pool.GetSeriesBatch()
			incompleteBatch.next = nextBatch
			incompleteBatch = nextBatch
		}

		incompleteBatch.series = append(incompleteBatch.series, ss.At())
		totalSeries++
	}

	metadata := m.Pool.GetSeriesMetadataSlice(totalSeries)
	batch := m.currentSeriesBatch
	lb := labels.NewBuilder(labels.EmptyLabels()) // TODO: pool this?
	for batch != nil {
		for _, s := range batch.series {
			metadata = append(metadata, SeriesMetadata{Labels: dropMetricName(s.Labels(), lb)})
		}

		batch = batch.next
	}

	return metadata, ss.Err()
}

func dropMetricName(l labels.Labels, lb *labels.Builder) labels.Labels {
	lb.Reset(l)
	lb.Del(labels.MetricName)
	return lb.Labels()
}

func (m *MatrixSelectorWithTransformationOverRange) Next(ctx context.Context) (bool, SeriesData, error) {
	if m.currentSeriesBatch == nil || len(m.currentSeriesBatch.series) == 0 {
		return false, SeriesData{}, nil
	}

	if ctx.Err() != nil {
		return false, SeriesData{}, ctx.Err()
	}

	if m.buffer == nil {
		m.buffer = &util.RingBuffer{} // TODO: pool?
	}

	m.chunkIterator = m.currentSeriesBatch.series[m.currentSeriesBatchIndex].Iterator(m.chunkIterator)
	m.buffer.Reset()
	m.currentSeriesBatchIndex++

	if m.currentSeriesBatchIndex == len(m.currentSeriesBatch.series) {
		b := m.currentSeriesBatch
		m.currentSeriesBatch = m.currentSeriesBatch.next
		m.Pool.PutSeriesBatch(b)
		m.currentSeriesBatchIndex = 0
	}

	numSteps := stepCount(m.startTimestamp, m.endTimestamp, m.intervalMilliseconds)
	data := SeriesData{
		Floats: m.Pool.GetFPointSlice(numSteps),
	}

	// TODO: adjust time sought for offset
	// TODO: test behaviour with resets, missing points, extrapolation, stale markers
	// TODO: handle native histograms
	for ts := m.startTimestamp; ts <= m.endTimestamp; ts += m.intervalMilliseconds {
		rangeStart := ts - m.rangeMilliseconds
		rangeEnd := ts

		m.buffer.DiscardPointsBefore(rangeStart)

		if err := m.fillBuffer(rangeStart, rangeEnd); err != nil {
			return false, SeriesData{}, err
		}

		head, tail := m.buffer.Points()
		count := len(head) + len(tail)

		if count < 2 {
			// Not enough points, skip.
			continue
		}

		firstPoint := m.buffer.First()
		lastPoint := m.buffer.Last()
		delta := lastPoint.F - firstPoint.F
		previousValue := firstPoint.F

		accumulate := func(points []promql.FPoint) {
			for _, p := range points {
				if p.T > rangeEnd { // The buffer is already guaranteed to only contain points >= rangeStart.
					return
				}

				if p.F < previousValue {
					// Counter reset.
					delta += previousValue
				}

				previousValue = p.F
			}
		}

		accumulate(head)
		accumulate(tail)

		val := m.calculateRate(rangeStart, rangeEnd, firstPoint, lastPoint, delta, count)

		data.Floats = append(data.Floats, promql.FPoint{T: ts, F: val})
	}

	return true, data, nil
}

// TODO: move to RingBuffer type?
func (m *MatrixSelectorWithTransformationOverRange) fillBuffer(rangeStart, rangeEnd int64) error {
	// Keep filling the buffer until we reach the end of the range or the end of the iterator.
	for {
		valueType := m.chunkIterator.Next()

		switch valueType {
		case chunkenc.ValNone:
			// No more data. We are done.
			return m.chunkIterator.Err()
		case chunkenc.ValFloat:
			t, f := m.chunkIterator.At()
			if value.IsStaleNaN(f) || t < rangeStart {
				continue
			}

			m.buffer.Append(promql.FPoint{T: t, F: f})

			if t >= rangeEnd {
				return nil
			}
		default:
			// TODO: handle native histograms
			return fmt.Errorf("unknown value type %s", valueType.String())
		}
	}
}

// This is based on extrapolatedRate from promql/functions.go.
func (m *MatrixSelectorWithTransformationOverRange) calculateRate(rangeStart, rangeEnd int64, firstPoint, lastPoint promql.FPoint, delta float64, count int) float64 {
	durationToStart := float64(firstPoint.T-rangeStart) / 1000
	durationToEnd := float64(rangeEnd-lastPoint.T) / 1000

	sampledInterval := float64(lastPoint.T-firstPoint.T) / 1000
	averageDurationBetweenSamples := sampledInterval / float64(count-1)

	if delta > 0 && firstPoint.F >= 0 {
		durationToZero := sampledInterval * (firstPoint.F / delta)
		if durationToZero < durationToStart {
			durationToStart = durationToZero
		}
	}

	extrapolationThreshold := averageDurationBetweenSamples * 1.1
	extrapolateToInterval := sampledInterval

	if durationToStart < extrapolationThreshold {
		extrapolateToInterval += durationToStart
	} else {
		extrapolateToInterval += averageDurationBetweenSamples / 2
	}
	if durationToEnd < extrapolationThreshold {
		extrapolateToInterval += durationToEnd
	} else {
		extrapolateToInterval += averageDurationBetweenSamples / 2
	}
	factor := extrapolateToInterval / sampledInterval
	factor /= m.Range.Seconds()
	return delta * factor
}

func (m *MatrixSelectorWithTransformationOverRange) Close() {
	for m.currentSeriesBatch != nil {
		b := m.currentSeriesBatch
		m.currentSeriesBatch = m.currentSeriesBatch.next
		m.Pool.PutSeriesBatch(b)
	}

	if m.querier != nil {
		_ = m.querier.Close()
		m.querier = nil
	}
}
