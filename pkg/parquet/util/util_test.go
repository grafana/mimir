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

package util

import (
	"reflect"
	"testing"
)

func TestGapBasedPartitioner(t *testing.T) {
	tests := []struct {
		name        string
		maxGapSize  uint64
		inputRanges [][2]uint64
		expected    []Part
	}{
		{
			name:       "no gaps",
			maxGapSize: 5,
			inputRanges: [][2]uint64{
				{0, 10}, {10, 20}, {20, 30},
			},
			expected: []Part{
				{Start: 0, End: 30, ElemRng: [2]int{0, 3}},
			},
		},
		{
			name:       "small gaps",
			maxGapSize: 5,
			inputRanges: [][2]uint64{
				{0, 10}, {12, 22}, {27, 35},
			},
			expected: []Part{
				{Start: 0, End: 35, ElemRng: [2]int{0, 3}},
			},
		},
		{
			name:       "large gap splits range",
			maxGapSize: 3,
			inputRanges: [][2]uint64{
				{0, 10}, {20, 30}, {31, 40},
			},
			expected: []Part{
				{Start: 0, End: 10, ElemRng: [2]int{0, 1}},
				{Start: 20, End: 40, ElemRng: [2]int{1, 3}},
			},
		},
		{
			name:       "overlapping ranges",
			maxGapSize: 1,
			inputRanges: [][2]uint64{
				{0, 10}, {5, 15}, {14, 20},
			},
			expected: []Part{
				{Start: 0, End: 20, ElemRng: [2]int{0, 3}},
			},
		},
		{
			name:       "single input",
			maxGapSize: 100,
			inputRanges: [][2]uint64{
				{10, 50},
			},
			expected: []Part{
				{Start: 10, End: 50, ElemRng: [2]int{0, 1}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewGapBasedPartitioner(tt.maxGapSize)
			result := p.Partition(len(tt.inputRanges), func(i int) (uint64, uint64) {
				return tt.inputRanges[i][0], tt.inputRanges[i][1]
			})

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("unexpected result:\ngot  %#v\nwant %#v", result, tt.expected)
			}
		})
	}
}
