// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operator

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type InstantVectorSelector struct {
	Selector *Selector

	startTimestamp       int64
	endTimestamp         int64
	intervalMilliseconds int64
	numSteps             int

	chunkIterator    chunkenc.Iterator
	memoizedIterator *storage.MemoizedSeriesIterator
}

var _ InstantVectorOperator = &InstantVectorSelector{}

func (v *InstantVectorSelector) Series(ctx context.Context) ([]SeriesMetadata, error) {
	// Compute values we need on every call to Next() once, here.
	v.startTimestamp = timestamp.FromTime(v.Selector.Start)
	v.endTimestamp = timestamp.FromTime(v.Selector.End)
	v.intervalMilliseconds = durationMilliseconds(v.Selector.Interval)
	v.numSteps = stepCount(v.startTimestamp, v.endTimestamp, v.intervalMilliseconds)

	return v.Selector.Series(ctx)
}

func (v *InstantVectorSelector) Next(ctx context.Context) (InstantVectorSeriesData, error) {
	if ctx.Err() != nil {
		return InstantVectorSeriesData{}, ctx.Err()
	}

	if v.memoizedIterator == nil {
		v.memoizedIterator = storage.NewMemoizedEmptyIterator(durationMilliseconds(v.Selector.LookbackDelta))
	}

	var err error
	v.chunkIterator, err = v.Selector.Next(v.chunkIterator)
	if err != nil {
		return InstantVectorSeriesData{}, err
	}

	v.memoizedIterator.Reset(v.chunkIterator)

	data := InstantVectorSeriesData{
		Floats: GetFPointSlice(v.numSteps), // TODO: only allocate this if we have any floats
	}

	for ts := v.startTimestamp; ts <= v.endTimestamp; ts += v.intervalMilliseconds {
		var t int64
		var val float64
		var h *histogram.FloatHistogram

		valueType := v.memoizedIterator.Seek(ts)

		switch valueType {
		case chunkenc.ValNone:
			if v.memoizedIterator.Err() != nil {
				return InstantVectorSeriesData{}, v.memoizedIterator.Err()
			}
		case chunkenc.ValFloat:
			t, val = v.memoizedIterator.At()
		default:
			// TODO: handle native histograms
			return InstantVectorSeriesData{}, fmt.Errorf("unknown value type %s", valueType.String())
		}

		if valueType == chunkenc.ValNone || t > ts {
			var ok bool
			t, val, h, ok = v.memoizedIterator.PeekPrev()
			if h != nil {
				panic("don't support histograms")
			}
			if !ok || t < ts-durationMilliseconds(v.Selector.LookbackDelta) {
				continue
			}
		}
		if value.IsStaleNaN(val) || (h != nil && value.IsStaleNaN(h.Sum)) {
			continue
		}

		data.Floats = append(data.Floats, promql.FPoint{T: ts, F: val})
	}

	if v.memoizedIterator.Err() != nil {
		return InstantVectorSeriesData{}, v.memoizedIterator.Err()
	}

	return data, nil
}

func (v *InstantVectorSelector) Close() {
	if v.Selector != nil {
		v.Selector.Close()
	}
}
