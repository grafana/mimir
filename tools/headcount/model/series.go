// SPDX-License-Identifier: AGPL-3.0-only

package model

import "github.com/prometheus/prometheus/model/labels"

// Interval is a half-open span of time, in Unix milliseconds, during which a
// series has a sample. End is exclusive.
type Interval struct {
	Start, End int64
}

// Overlaps reports whether iv shares any time with [start, end).
func (iv Interval) Overlaps(start, end int64) bool {
	return iv.Start < end && start < iv.End
}

// Series is one label set together with every interval in which it has a
// sample. A series with a gap has two or more disjoint intervals; a series
// that ended before the model's End has a final interval that stops early.
type Series struct {
	Labels    labels.Labels
	Intervals []Interval
}

// Live reports whether s has a sample overlapping [start, end).
func (s Series) Live(start, end int64) bool {
	for _, iv := range s.Intervals {
		if iv.Overlaps(start, end) {
			return true
		}
	}
	return false
}

// Model is a fully materialized synthetic population: every series that
// exists, with the exact intervals it is live. It is the ground truth that
// generated blocks and index-derived counts are checked against.
type Model struct {
	cfg    Config
	Series []Series
}

// Config returns the configuration the Model was generated from.
func (m *Model) Config() Config { return m.cfg }
