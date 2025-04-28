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
	for _, tt := range []struct{ lhs, rhs, expect []rowRange }{
		{
			lhs:    []rowRange{{from: 0, count: 4}},
			rhs:    []rowRange{{from: 2, count: 6}},
			expect: []rowRange{{from: 2, count: 2}},
		},
		{
			lhs:    []rowRange{{from: 0, count: 4}},
			rhs:    []rowRange{{from: 6, count: 8}},
			expect: []rowRange{},
		},
		{
			lhs:    []rowRange{{from: 0, count: 4}},
			rhs:    []rowRange{{from: 0, count: 4}},
			expect: []rowRange{{from: 0, count: 4}},
		},
		{
			lhs:    []rowRange{{from: 0, count: 4}, {from: 8, count: 2}},
			rhs:    []rowRange{{from: 2, count: 9}},
			expect: []rowRange{{from: 2, count: 2}, {from: 8, count: 2}},
		},
		{
			lhs:    []rowRange{{from: 0, count: 1}, {from: 4, count: 1}},
			rhs:    []rowRange{{from: 2, count: 1}, {from: 6, count: 1}},
			expect: []rowRange{},
		},
		{
			lhs:    []rowRange{{from: 0, count: 2}, {from: 2, count: 2}},
			rhs:    []rowRange{{from: 1, count: 2}, {from: 3, count: 2}},
			expect: []rowRange{{from: 1, count: 3}},
		},
		{
			lhs:    []rowRange{{from: 0, count: 2}, {from: 5, count: 2}},
			rhs:    []rowRange{{from: 0, count: 10}},
			expect: []rowRange{{from: 0, count: 2}, {from: 5, count: 2}},
		},
		{
			lhs:    []rowRange{{from: 0, count: 2}, {from: 3, count: 1}, {from: 5, count: 2}, {from: 12, count: 10}},
			rhs:    []rowRange{{from: 0, count: 10}, {from: 15, count: 32}},
			expect: []rowRange{{from: 0, count: 2}, {from: 3, count: 1}, {from: 5, count: 2}, {from: 15, count: 7}},
		},
		{
			lhs:    []rowRange{},
			rhs:    []rowRange{{from: 0, count: 10}},
			expect: []rowRange{},
		},
		{
			lhs:    []rowRange{{from: 0, count: 10}},
			rhs:    []rowRange{},
			expect: []rowRange{},
		},
		{
			lhs:    []rowRange{},
			rhs:    []rowRange{},
			expect: []rowRange{},
		},
		{
			lhs:    []rowRange{{from: 0, count: 2}},
			rhs:    []rowRange{{from: 0, count: 1}, {from: 1, count: 1}, {from: 2, count: 1}},
			expect: []rowRange{{from: 0, count: 2}},
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
	for _, tt := range []struct{ lhs, rhs, expect []rowRange }{
		{
			lhs:    []rowRange{{from: 4, count: 3}},
			rhs:    []rowRange{{from: 2, count: 1}, {from: 5, count: 2}},
			expect: []rowRange{{from: 2, count: 1}},
		},
		{
			lhs:    []rowRange{{from: 2, count: 4}},
			rhs:    []rowRange{{from: 0, count: 7}},
			expect: []rowRange{{from: 0, count: 2}, {from: 6, count: 1}},
		},
		{
			lhs:    []rowRange{{from: 2, count: 4}},
			rhs:    []rowRange{{from: 3, count: 7}},
			expect: []rowRange{{from: 6, count: 4}},
		},
		{
			lhs:    []rowRange{{from: 8, count: 10}},
			rhs:    []rowRange{{from: 3, count: 7}},
			expect: []rowRange{{from: 3, count: 5}},
		},
		{
			lhs:    []rowRange{{from: 16, count: 10}},
			rhs:    []rowRange{{from: 3, count: 7}},
			expect: []rowRange{{from: 3, count: 7}},
		},
		{
			lhs:    []rowRange{{from: 1, count: 2}, {from: 4, count: 2}},
			rhs:    []rowRange{{from: 2, count: 2}, {from: 5, count: 8}},
			expect: []rowRange{{from: 3, count: 1}, {from: 6, count: 7}},
		},
		// Empty input cases
		{
			lhs:    []rowRange{},
			rhs:    []rowRange{{from: 1, count: 5}},
			expect: []rowRange{{from: 1, count: 5}},
		},
		{
			lhs:    []rowRange{{from: 1, count: 5}},
			rhs:    []rowRange{},
			expect: []rowRange{},
		},
		{
			lhs:    []rowRange{},
			rhs:    []rowRange{},
			expect: []rowRange{},
		},
		// Adjacent ranges
		{
			lhs:    []rowRange{{from: 1, count: 3}},
			rhs:    []rowRange{{from: 1, count: 3}, {from: 4, count: 2}},
			expect: []rowRange{{from: 4, count: 2}},
		},
		// Ranges with gaps
		{
			lhs:    []rowRange{{from: 1, count: 2}, {from: 5, count: 2}},
			rhs:    []rowRange{{from: 0, count: 8}},
			expect: []rowRange{{from: 0, count: 1}, {from: 3, count: 2}, {from: 7, count: 1}},
		},
		// Zero-count ranges
		{
			lhs:    []rowRange{{from: 1, count: 0}},
			rhs:    []rowRange{{from: 1, count: 5}},
			expect: []rowRange{{from: 1, count: 5}},
		},
		// Completely disjoint ranges
		{
			lhs:    []rowRange{{from: 1, count: 2}},
			rhs:    []rowRange{{from: 5, count: 2}},
			expect: []rowRange{{from: 5, count: 2}},
		},
		// Multiple overlapping ranges
		{
			lhs:    []rowRange{{from: 1, count: 3}, {from: 4, count: 3}, {from: 8, count: 2}},
			rhs:    []rowRange{{from: 0, count: 11}},
			expect: []rowRange{{from: 0, count: 1}, {from: 7, count: 1}, {from: 10, count: 1}},
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
	for _, tt := range []struct{ in, expect []rowRange }{
		{
			in: []rowRange{
				{from: 0, count: 15},
				{from: 4, count: 4},
			},
			expect: []rowRange{
				{from: 0, count: 15},
			},
		},
		{
			in: []rowRange{
				{from: 4, count: 4},
				{from: 4, count: 2},
			},
			expect: []rowRange{
				{from: 4, count: 4},
			},
		},
		{
			in: []rowRange{
				{from: 0, count: 4},
				{from: 1, count: 5},
				{from: 8, count: 10},
			},
			expect: []rowRange{
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
