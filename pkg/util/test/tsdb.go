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

// Sample is a test implementation of the prometheus chunks.Sample interface.
// The possible sample value types are mutually exclusive -
// a sample can only be a float, histogram, or float histogram.
// This is not enforced in a constructor for convenience in test code.
type Sample struct {
	STS, TS   int64 // Start Timestamp and Timestamp
	Val       float64
	Hist      *histogram.Histogram
	FloatHist *histogram.FloatHistogram
}

func (s Sample) T() int64                      { return s.TS }
func (s Sample) ST() int64                     { return s.STS }
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
	c := Sample{STS: s.STS, TS: s.TS, Val: s.Val}
	if s.Hist != nil {
		c.Hist = s.Hist.Copy()
	}
	if s.FloatHist != nil {
		c.FloatHist = s.FloatHist.Copy()
	}
	return c
}
