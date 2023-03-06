// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/merger_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package modelutil

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
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

func TestMergeHistogramSets(t *testing.T) {
	now := model.Now()
	sample1 := mimirpb.FromFloatHistogramToHistogramProto(int64(now), test.GenerateTestFloatHistogram(1))
	sample2 := mimirpb.FromHistogramToHistogramProto(int64(now.Add(1*time.Second)), test.GenerateTestHistogram(2))
	sample3 := mimirpb.FromFloatHistogramToHistogramProto(int64(now.Add(4*time.Second)), test.GenerateTestFloatHistogram(3))
	sample4 := mimirpb.FromHistogramToHistogramProto(int64(now.Add(8*time.Second)), test.GenerateTestHistogram(7))

	for _, c := range []struct {
		samplesA []mimirpb.Histogram
		samplesB []mimirpb.Histogram
		expected []mimirpb.Histogram
	}{
		{
			samplesA: []mimirpb.Histogram{},
			samplesB: []mimirpb.Histogram{},
			expected: []mimirpb.Histogram{},
		},
		{
			samplesA: []mimirpb.Histogram{sample1},
			samplesB: []mimirpb.Histogram{},
			expected: []mimirpb.Histogram{sample1},
		},
		{
			samplesA: []mimirpb.Histogram{},
			samplesB: []mimirpb.Histogram{sample1},
			expected: []mimirpb.Histogram{sample1},
		},
		{
			samplesA: []mimirpb.Histogram{sample1},
			samplesB: []mimirpb.Histogram{sample1},
			expected: []mimirpb.Histogram{sample1},
		},
		{
			samplesA: []mimirpb.Histogram{sample1, sample2, sample3},
			samplesB: []mimirpb.Histogram{sample1, sample3, sample4},
			expected: []mimirpb.Histogram{sample1, sample2, sample3, sample4},
		},
	} {
		samples := MergeHistogramSets(c.samplesA, c.samplesB)
		require.Equal(t, c.expected, samples)
	}
}

func TestMergeNHistogramSets(t *testing.T) {
	now := model.Now()
	sample1 := mimirpb.FromFloatHistogramToHistogramProto(int64(now), test.GenerateTestFloatHistogram(1))
	sample2 := mimirpb.FromHistogramToHistogramProto(int64(now.Add(1*time.Second)), test.GenerateTestHistogram(2))
	sample3 := mimirpb.FromFloatHistogramToHistogramProto(int64(now.Add(4*time.Second)), test.GenerateTestFloatHistogram(3))
	sample4 := mimirpb.FromHistogramToHistogramProto(int64(now.Add(8*time.Second)), test.GenerateTestHistogram(7))

	for _, c := range []struct {
		sampleSets [][]mimirpb.Histogram
		expected   []mimirpb.Histogram
	}{
		{
			sampleSets: [][]mimirpb.Histogram{{}, {}, {}},
			expected:   []mimirpb.Histogram{},
		},
		{
			sampleSets: [][]mimirpb.Histogram{
				{sample1, sample2},
				{sample2},
				{sample1, sample3, sample4},
			},
			expected: []mimirpb.Histogram{sample1, sample2, sample3, sample4},
		},
	} {
		samples := MergeNHistogramSets(c.sampleSets...)
		require.Equal(t, c.expected, samples)
	}
}
