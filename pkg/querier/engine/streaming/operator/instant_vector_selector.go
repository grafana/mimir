// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operator

import (
	"context"
	"errors"
	"fmt"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type InstantVectorSelector struct {
	Selector *Selector

	numSteps int

	chunkIterator    chunkenc.Iterator
	memoizedIterator *storage.MemoizedSeriesIterator
}

var _ InstantVectorOperator = &InstantVectorSelector{}

func (v *InstantVectorSelector) SeriesMetadata(ctx context.Context) ([]SeriesMetadata, error) {
	// Compute value we need on every call to Next() once, here.
	v.numSteps = stepCount(v.Selector.Start, v.Selector.End, v.Selector.Interval)

	return v.Selector.SeriesMetadata(ctx)
}

func (v *InstantVectorSelector) Next(_ context.Context) (InstantVectorSeriesData, error) {
	if v.memoizedIterator == nil {
		v.memoizedIterator = storage.NewMemoizedEmptyIterator(v.Selector.LookbackDelta.Milliseconds())
	}

	var err error
	v.chunkIterator, err = v.Selector.Next(v.chunkIterator)
	if err != nil {
		return InstantVectorSeriesData{}, err
	}

	v.memoizedIterator.Reset(v.chunkIterator)

	data := InstantVectorSeriesData{
		Floats: GetFPointSlice(v.numSteps), // TODO: only allocate this if we have any floats (once we support native histograms)
	}

	for stepT := v.Selector.Start; stepT <= v.Selector.End; stepT += v.Selector.Interval {
		var t int64
		var val float64
		var h *histogram.FloatHistogram

		ts := stepT
		if v.Selector.Timestamp != nil {
			ts = *v.Selector.Timestamp
		}

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
			return InstantVectorSeriesData{}, fmt.Errorf("streaming PromQL engine: unknown value type %s", valueType.String())
		}

		if valueType == chunkenc.ValNone || t > ts {
			var ok bool
			t, val, h, ok = v.memoizedIterator.PeekPrev()
			if h != nil {
				return InstantVectorSeriesData{}, errors.New("streaming PromQL engine doesn't support histograms yet")
			}
			if !ok || t < ts-v.Selector.LookbackDelta.Milliseconds() {
				continue
			}
		}
		if value.IsStaleNaN(val) || (h != nil && value.IsStaleNaN(h.Sum)) {
			continue
		}

		data.Floats = append(data.Floats, promql.FPoint{T: stepT, F: val})
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
