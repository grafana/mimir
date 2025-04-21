// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

type Series struct {
	lbls    labels.Labels
	samples []Sample
}

func NewSeries(lbls labels.Labels, samples []Sample) Series {
	return Series{lbls, samples}
}

type Sample struct {
	t  int64
	v  float64
	h  *histogram.Histogram
	fh *histogram.FloatHistogram
}

func NewSample(t int64, v float64, h *histogram.Histogram, fh *histogram.FloatHistogram) Sample {
	return Sample{t, v, h, fh}
}

func (s Sample) T() int64                      { return s.t }
func (s Sample) F() float64                    { return s.v }
func (s Sample) H() *histogram.Histogram       { return s.h }
func (s Sample) FH() *histogram.FloatHistogram { return s.fh }

func (s Sample) Type() chunkenc.ValueType {
	switch {
	case s.h != nil:
		return chunkenc.ValHistogram
	case s.fh != nil:
		return chunkenc.ValFloatHistogram
	default:
		return chunkenc.ValFloat
	}
}

func (s Sample) Copy() chunks.Sample {
	c := Sample{t: s.t, v: s.v}
	if s.h != nil {
		c.h = s.h.Copy()
	}
	if s.fh != nil {
		c.fh = s.fh.Copy()
	}
	return c
}
