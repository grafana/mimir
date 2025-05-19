// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package search

import (
	"slices"
	"testing"
)

func TestIntersect(t *testing.T) {
	for _, tt := range []struct{ lhs, rhs, expect []RowRange }{
		{
			lhs:    []RowRange{{from: 0, count: 4}},
			rhs:    []RowRange{{from: 2, count: 6}},
			expect: []RowRange{{from: 2, count: 2}},
		},
		{
			lhs:    []RowRange{{from: 0, count: 4}},
			rhs:    []RowRange{{from: 6, count: 8}},
			expect: []RowRange{},
		},
		{
			lhs:    []RowRange{{from: 0, count: 4}},
			rhs:    []RowRange{{from: 0, count: 4}},
			expect: []RowRange{{from: 0, count: 4}},
		},
		{
			lhs:    []RowRange{{from: 0, count: 4}, {from: 8, count: 2}},
			rhs:    []RowRange{{from: 2, count: 9}},
			expect: []RowRange{{from: 2, count: 2}, {from: 8, count: 2}},
		},
		{
			lhs:    []RowRange{{from: 0, count: 1}, {from: 4, count: 1}},
			rhs:    []RowRange{{from: 2, count: 1}, {from: 6, count: 1}},
			expect: []RowRange{},
		},
		{
			lhs:    []RowRange{{from: 0, count: 2}, {from: 2, count: 2}},
			rhs:    []RowRange{{from: 1, count: 2}, {from: 3, count: 2}},
			expect: []RowRange{{from: 1, count: 3}},
		},
		{
			lhs:    []RowRange{{from: 0, count: 2}, {from: 5, count: 2}},
			rhs:    []RowRange{{from: 0, count: 10}},
			expect: []RowRange{{from: 0, count: 2}, {from: 5, count: 2}},
		},
		{
			lhs:    []RowRange{{from: 0, count: 2}, {from: 3, count: 1}, {from: 5, count: 2}, {from: 12, count: 10}},
			rhs:    []RowRange{{from: 0, count: 10}, {from: 15, count: 32}},
			expect: []RowRange{{from: 0, count: 2}, {from: 3, count: 1}, {from: 5, count: 2}, {from: 15, count: 7}},
		},
		{
			lhs:    []RowRange{},
			rhs:    []RowRange{{from: 0, count: 10}},
			expect: []RowRange{},
		},
		{
			lhs:    []RowRange{{from: 0, count: 10}},
			rhs:    []RowRange{},
			expect: []RowRange{},
		},
		{
			lhs:    []RowRange{},
			rhs:    []RowRange{},
			expect: []RowRange{},
		},
		{
			lhs:    []RowRange{{from: 0, count: 2}},
			rhs:    []RowRange{{from: 0, count: 1}, {from: 1, count: 1}, {from: 2, count: 1}},
			expect: []RowRange{{from: 0, count: 2}},
		},
	} {
		t.Run("", func(t *testing.T) {
			if res := intersectRowRanges(tt.lhs, tt.rhs); !slices.Equal(res, tt.expect) {
				t.Fatalf("Expected %v to match %v", res, tt.expect)
			}
		})
	}
}

func TestComplement(t *testing.T) {
	for _, tt := range []struct{ lhs, rhs, expect []RowRange }{
		{
			lhs:    []RowRange{{from: 4, count: 3}},
			rhs:    []RowRange{{from: 2, count: 1}, {from: 5, count: 2}},
			expect: []RowRange{{from: 2, count: 1}},
		},
		{
			lhs:    []RowRange{{from: 2, count: 4}},
			rhs:    []RowRange{{from: 0, count: 7}},
			expect: []RowRange{{from: 0, count: 2}, {from: 6, count: 1}},
		},
		{
			lhs:    []RowRange{{from: 2, count: 4}},
			rhs:    []RowRange{{from: 3, count: 7}},
			expect: []RowRange{{from: 6, count: 4}},
		},
		{
			lhs:    []RowRange{{from: 8, count: 10}},
			rhs:    []RowRange{{from: 3, count: 7}},
			expect: []RowRange{{from: 3, count: 5}},
		},
		{
			lhs:    []RowRange{{from: 16, count: 10}},
			rhs:    []RowRange{{from: 3, count: 7}},
			expect: []RowRange{{from: 3, count: 7}},
		},
		{
			lhs:    []RowRange{{from: 1, count: 2}, {from: 4, count: 2}},
			rhs:    []RowRange{{from: 2, count: 2}, {from: 5, count: 8}},
			expect: []RowRange{{from: 3, count: 1}, {from: 6, count: 7}},
		},
		// Empty input cases
		{
			lhs:    []RowRange{},
			rhs:    []RowRange{{from: 1, count: 5}},
			expect: []RowRange{{from: 1, count: 5}},
		},
		{
			lhs:    []RowRange{{from: 1, count: 5}},
			rhs:    []RowRange{},
			expect: []RowRange{},
		},
		{
			lhs:    []RowRange{},
			rhs:    []RowRange{},
			expect: []RowRange{},
		},
		// Adjacent ranges
		{
			lhs:    []RowRange{{from: 1, count: 3}},
			rhs:    []RowRange{{from: 1, count: 3}, {from: 4, count: 2}},
			expect: []RowRange{{from: 4, count: 2}},
		},
		// Ranges with gaps
		{
			lhs:    []RowRange{{from: 1, count: 2}, {from: 5, count: 2}},
			rhs:    []RowRange{{from: 0, count: 8}},
			expect: []RowRange{{from: 0, count: 1}, {from: 3, count: 2}, {from: 7, count: 1}},
		},
		// Zero-count ranges
		{
			lhs:    []RowRange{{from: 1, count: 0}},
			rhs:    []RowRange{{from: 1, count: 5}},
			expect: []RowRange{{from: 1, count: 5}},
		},
		// Completely disjoint ranges
		{
			lhs:    []RowRange{{from: 1, count: 2}},
			rhs:    []RowRange{{from: 5, count: 2}},
			expect: []RowRange{{from: 5, count: 2}},
		},
		// Multiple overlapping ranges
		{
			lhs:    []RowRange{{from: 1, count: 3}, {from: 4, count: 3}, {from: 8, count: 2}},
			rhs:    []RowRange{{from: 0, count: 11}},
			expect: []RowRange{{from: 0, count: 1}, {from: 7, count: 1}, {from: 10, count: 1}},
		},
	} {
		t.Run("", func(t *testing.T) {
			if res := complementRowRanges(tt.lhs, tt.rhs); !slices.Equal(res, tt.expect) {
				t.Fatalf("Expected %v to match %v", res, tt.expect)
			}
		})
	}
}

func TestSimplify(t *testing.T) {
	for _, tt := range []struct{ in, expect []RowRange }{
		{
			in: []RowRange{
				{from: 0, count: 15},
				{from: 4, count: 4},
			},
			expect: []RowRange{
				{from: 0, count: 15},
			},
		},
		{
			in: []RowRange{
				{from: 4, count: 4},
				{from: 4, count: 2},
			},
			expect: []RowRange{
				{from: 4, count: 4},
			},
		},
		{
			in: []RowRange{
				{from: 0, count: 4},
				{from: 1, count: 5},
				{from: 8, count: 10},
			},
			expect: []RowRange{
				{from: 0, count: 6},
				{from: 8, count: 10},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			if res := simplify(tt.in); !slices.Equal(res, tt.expect) {
				t.Fatalf("Expected %v to match %v", res, tt.expect)
			}
		})
	}
}

func TestOverlaps(t *testing.T) {
	for _, tt := range []struct {
		a, b   RowRange
		expect bool
	}{
		// Identical ranges
		{
			a:      RowRange{from: 0, count: 5},
			b:      RowRange{from: 0, count: 5},
			expect: true,
		},
		// Completely disjoint ranges
		{
			a:      RowRange{from: 0, count: 3},
			b:      RowRange{from: 5, count: 3},
			expect: false,
		},
		// Adjacent ranges (should not overlap as ranges are half-open)
		{
			a:      RowRange{from: 0, count: 3},
			b:      RowRange{from: 3, count: 3},
			expect: false,
		},
		// One range completely contains the other
		{
			a:      RowRange{from: 0, count: 10},
			b:      RowRange{from: 2, count: 5},
			expect: true,
		},
		// Partial overlap from left
		{
			a:      RowRange{from: 0, count: 5},
			b:      RowRange{from: 3, count: 5},
			expect: true,
		},
		// Partial overlap from right
		{
			a:      RowRange{from: 3, count: 5},
			b:      RowRange{from: 0, count: 5},
			expect: true,
		},
		// Zero-count ranges
		{
			a:      RowRange{from: 0, count: 0},
			b:      RowRange{from: 0, count: 5},
			expect: false,
		},
		{
			a:      RowRange{from: 0, count: 5},
			b:      RowRange{from: 0, count: 0},
			expect: false,
		},
		// Negative ranges (edge case)
		{
			a:      RowRange{from: -5, count: 5},
			b:      RowRange{from: -3, count: 5},
			expect: true,
		},
	} {
		t.Run("", func(t *testing.T) {
			if res := tt.a.Overlaps(tt.b); res != tt.expect {
				t.Fatalf("Expected %v.Overlaps(%v) to be %v, got %v", tt.a, tt.b, tt.expect, res)
			}
			// Test symmetry
			if res := tt.b.Overlaps(tt.a); res != tt.expect {
				t.Fatalf("Expected %v.Overlaps(%v) to be %v, got %v", tt.b, tt.a, tt.expect, res)
			}
		})
	}
}
