// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pmetric

import (
	"go.opentelemetry.io/collector/pdata"
	"go.opentelemetry.io/collector/pdata/internal"
)

// ResourceMetricsIterator allows streaming iteration over ResourceMetrics
// in an ExportMetricsServiceRequest without allocating all of them upfront.
// This reduces peak memory by parsing and releasing ResourceMetrics one at a time.
type ResourceMetricsIterator struct {
	iter *internal.ResourceMetricsIterator
}

// NewResourceMetricsIterator creates an iterator over raw protobuf bytes
// of an ExportMetricsServiceRequest. Call Next() to advance and Current()
// to get the current ResourceMetrics.
//
// The iterator parses ResourceMetrics one at a time from the raw bytes,
// releasing each one back to the pool when Next() is called.
func NewResourceMetricsIterator(buf []byte, opts *pdata.UnmarshalOptions) *ResourceMetricsIterator {
	if opts == nil {
		opts = &pdata.DefaultUnmarshalOptions
	}
	return &ResourceMetricsIterator{
		iter: internal.NewResourceMetricsIterator(buf, opts),
	}
}

// Next advances to the next ResourceMetrics. Returns false when done or on error.
// After Next returns true, call Current() to get the ResourceMetrics.
// The previously returned ResourceMetrics is released back to the pool when
// Next is called, so do not retain references to it.
func (it *ResourceMetricsIterator) Next() bool {
	return it.iter.Next()
}

// Current returns the current ResourceMetrics. Valid until Next() is called.
// Do not retain references after calling Next() or Release().
func (it *ResourceMetricsIterator) Current() ResourceMetrics {
	rm := it.iter.Current()
	if rm == nil {
		return ResourceMetrics{}
	}
	return WrapResourceMetrics(rm)
}

// Err returns any error encountered during iteration.
func (it *ResourceMetricsIterator) Err() error {
	return it.iter.Err()
}

// Release releases any remaining resources. Call when done iterating.
// Safe to call multiple times.
func (it *ResourceMetricsIterator) Release() {
	it.iter.Release()
}
