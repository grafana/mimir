// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/merger_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestMergeSampleSets(t *testing.T) {
	now := model.Now()
	sample1 := model.SamplePair{Timestamp: now, Value: 1}
	sample2 := model.SamplePair{Timestamp: now.Add(1 * time.Second), Value: 2}
	sample3 := model.SamplePair{Timestamp: now.Add(4 * time.Second), Value: 3}
	sample4 := model.SamplePair{Timestamp: now.Add(8 * time.Second), Value: 7}

	for _, c := range []struct {
		samplesA []model.SamplePair
		samplesB []model.SamplePair
		expected []model.SamplePair
	}{
		{
			samplesA: []model.SamplePair{},
			samplesB: []model.SamplePair{},
			expected: []model.SamplePair{},
		},
		{
			samplesA: []model.SamplePair{sample1},
			samplesB: []model.SamplePair{},
			expected: []model.SamplePair{sample1},
		},
		{
			samplesA: []model.SamplePair{},
			samplesB: []model.SamplePair{sample1},
			expected: []model.SamplePair{sample1},
		},
		{
			samplesA: []model.SamplePair{sample1},
			samplesB: []model.SamplePair{sample1},
			expected: []model.SamplePair{sample1},
		},
		{
			samplesA: []model.SamplePair{sample1, sample2, sample3},
			samplesB: []model.SamplePair{sample1, sample3, sample4},
			expected: []model.SamplePair{sample1, sample2, sample3, sample4},
		},
	} {
		samples := MergeSampleSets(c.samplesA, c.samplesB)
		require.Equal(t, c.expected, samples)
	}
}

func TestMergeNSampleSets(t *testing.T) {
	now := model.Now()
	sample1 := model.SamplePair{Timestamp: now, Value: 1}
	sample2 := model.SamplePair{Timestamp: now.Add(1 * time.Second), Value: 2}
	sample3 := model.SamplePair{Timestamp: now.Add(4 * time.Second), Value: 3}
	sample4 := model.SamplePair{Timestamp: now.Add(8 * time.Second), Value: 7}

	for _, c := range []struct {
		sampleSets [][]model.SamplePair
		expected   []model.SamplePair
	}{
		{
			sampleSets: [][]model.SamplePair{{}, {}, {}},
			expected:   []model.SamplePair{},
		},
		{
			sampleSets: [][]model.SamplePair{
				{sample1, sample2},
				{sample2},
				{sample1, sample3, sample4},
			},
			expected: []model.SamplePair{sample1, sample2, sample3, sample4},
		},
	} {
		samples := MergeNSampleSets(c.sampleSets...)
		require.Equal(t, c.expected, samples)
	}
}

func TestMergeSlices(t *testing.T) {
	tests := []struct {
		name string
		args [][]string
		want []string
	}{
		{
			name: "empty input",
			want: nil,
		},
		{
			name: "single input",
			args: [][]string{{"a", "b", "c"}},
			want: []string{"a", "b", "c"},
		},
		{
			name: "two inputs same",
			args: [][]string{{"a", "b", "c"}, {"a", "b", "c"}},
			want: []string{"a", "b", "c"},
		},
		{
			name: "two inputs interleaved",
			args: [][]string{{"a", "c", "e"}, {"b", "d", "f"}},
			want: []string{"a", "b", "c", "d", "e", "f"},
		},
		{
			name: "first input short",
			args: [][]string{{"a"}, {"b", "d", "f"}},
			want: []string{"a", "b", "d", "f"},
		},
		{
			name: "second input short",
			args: [][]string{{"a", "c", "e"}, {"b"}},
			want: []string{"a", "b", "c", "e"},
		},
		{
			name: "some duplicates",
			args: [][]string{{"a", "c", "e"}, {"b", "c"}},
			want: []string{"a", "b", "c", "e"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MergeSlices(tt.args...)
			require.Equal(t, tt.want, got)
		})
	}
}
