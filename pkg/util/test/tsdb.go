// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

type Series struct {
	Labels  labels.Labels
	Samples []Sample
}

type Sample struct {
	TS        int64
	Val       float64
	Hist      *histogram.Histogram
	FloatHist *histogram.FloatHistogram
}

func (s Sample) T() int64                      { return s.TS }
func (s Sample) F() float64                    { return s.Val }
func (s Sample) H() *histogram.Histogram       { return s.Hist }
func (s Sample) FH() *histogram.FloatHistogram { return s.FloatHist }

func (s Sample) Type() chunkenc.ValueType {
	switch {
	case s.Hist != nil:
		return chunkenc.ValHistogram
	case s.FloatHist != nil:
		return chunkenc.ValFloatHistogram
	default:
		return chunkenc.ValFloat
	}
}

func (s Sample) Copy() chunks.Sample {
	c := Sample{TS: s.TS, Val: s.Val}
	if s.Hist != nil {
		c.Hist = s.Hist.Copy()
	}
	if s.FloatHist != nil {
		c.FloatHist = s.FloatHist.Copy()
	}
	return c
}
