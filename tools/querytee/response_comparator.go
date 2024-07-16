// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/tools/querytee/response_comparator.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querytee

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"

	util_log "github.com/grafana/mimir/pkg/util/log"
)

// SamplesComparatorFunc helps with comparing different types of samples coming from /api/v1/query and /api/v1/query_range routes.
type SamplesComparatorFunc func(expected, actual json.RawMessage, opts SampleComparisonOptions) error

type SamplesResponse struct {
	Status    string
	ErrorType string
	Error     string
	Data      struct {
		ResultType string
		Result     json.RawMessage
	}
}

type SampleComparisonOptions struct {
	Tolerance         float64
	UseRelativeError  bool
	SkipRecentSamples time.Duration
}

func NewSamplesComparator(opts SampleComparisonOptions) *SamplesComparator {
	return &SamplesComparator{
		opts: opts,
		sampleTypesComparator: map[string]SamplesComparatorFunc{
			"matrix": compareMatrix,
			"vector": compareVector,
			"scalar": compareScalar,
		},
	}
}

type SamplesComparator struct {
	opts                  SampleComparisonOptions
	sampleTypesComparator map[string]SamplesComparatorFunc
}

// RegisterSamplesComparator helps with registering custom sample types
func (s *SamplesComparator) RegisterSamplesType(samplesType string, comparator SamplesComparatorFunc) {
	s.sampleTypesComparator[samplesType] = comparator
}

func (s *SamplesComparator) Compare(expectedResponse, actualResponse []byte) (ComparisonResult, error) {
	var expected, actual SamplesResponse

	err := json.Unmarshal(expectedResponse, &expected)
	if err != nil {
		return ComparisonFailed, fmt.Errorf("unable to unmarshal expected response: %w", err)
	}

	err = json.Unmarshal(actualResponse, &actual)
	if err != nil {
		return ComparisonFailed, fmt.Errorf("unable to unmarshal actual response: %w", err)
	}

	if expected.Status != actual.Status {
		return ComparisonFailed, fmt.Errorf("expected status %s but got %s", expected.Status, actual.Status)
	}

	if expected.ErrorType != actual.ErrorType {
		return ComparisonFailed, fmt.Errorf("expected error type '%s' but got '%s'", expected.ErrorType, actual.ErrorType)
	}

	if expected.Error != actual.Error {
		return ComparisonFailed, fmt.Errorf("expected error '%s' but got '%s'", expected.Error, actual.Error)
	}

	if expected.Data.ResultType != actual.Data.ResultType {
		return ComparisonFailed, fmt.Errorf("expected resultType %s but got %s", expected.Data.ResultType, actual.Data.ResultType)
	}

	if expected.Data.ResultType == "" && actual.Data.ResultType == "" && expected.Data.Result == nil && actual.Data.Result == nil {
		return ComparisonSuccess, nil
	}

	comparator, ok := s.sampleTypesComparator[expected.Data.ResultType]
	if !ok {
		return ComparisonFailed, fmt.Errorf("resultType %s not registered for comparison", expected.Data.ResultType)
	}

	if err := comparator(expected.Data.Result, actual.Data.Result, s.opts); err != nil {
		return ComparisonFailed, err
	}

	return ComparisonSuccess, nil
}

func compareMatrix(expectedRaw, actualRaw json.RawMessage, opts SampleComparisonOptions) error {
	var expected, actual model.Matrix

	err := json.Unmarshal(expectedRaw, &expected)
	if err != nil {
		return err
	}
	err = json.Unmarshal(actualRaw, &actual)
	if err != nil {
		return err
	}

	if len(expected) != len(actual) {
		return fmt.Errorf("expected %d metrics but got %d", len(expected),
			len(actual))
	}

	metricFingerprintToIndexMap := make(map[model.Fingerprint]int, len(expected))
	for i, actualMetric := range actual {
		metricFingerprintToIndexMap[actualMetric.Metric.Fingerprint()] = i
	}

	for _, expectedMetric := range expected {
		actualMetricIndex, ok := metricFingerprintToIndexMap[expectedMetric.Metric.Fingerprint()]
		if !ok {
			return fmt.Errorf("expected metric %s missing from actual response", expectedMetric.Metric)
		}

		actualMetric := actual[actualMetricIndex]
		expectedFloatSamplesCount := len(expectedMetric.Values)
		actualFloatSamplesCount := len(actualMetric.Values)
		expectedHistogramSamplesCount := len(expectedMetric.Histograms)
		actualHistogramSamplesCount := len(actualMetric.Histograms)

		if expectedFloatSamplesCount != actualFloatSamplesCount || expectedHistogramSamplesCount != actualHistogramSamplesCount {
			err := fmt.Errorf(
				"expected %d float sample(s) and %d histogram sample(s) for metric %s but got %d float sample(s) and %d histogram sample(s)",
				expectedFloatSamplesCount,
				expectedHistogramSamplesCount,
				expectedMetric.Metric,
				actualFloatSamplesCount,
				actualHistogramSamplesCount,
			)

			shouldLog := false
			logger := util_log.Logger

			if expectedFloatSamplesCount > 0 && actualFloatSamplesCount > 0 {
				logger = log.With(logger,
					"oldest-expected-float-ts", expectedMetric.Values[0].Timestamp,
					"newest-expected-float-ts", expectedMetric.Values[expectedFloatSamplesCount-1].Timestamp,
					"oldest-actual-float-ts", actualMetric.Values[0].Timestamp,
					"newest-actual-float-ts", actualMetric.Values[actualFloatSamplesCount-1].Timestamp,
				)
				shouldLog = true
			}

			if expectedHistogramSamplesCount > 0 && actualHistogramSamplesCount > 0 {
				logger = log.With(logger,
					"oldest-expected-histogram-ts", expectedMetric.Histograms[0].Timestamp,
					"newest-expected-histogram-ts", expectedMetric.Histograms[expectedHistogramSamplesCount-1].Timestamp,
					"oldest-actual-histogram-ts", actualMetric.Histograms[0].Timestamp,
					"newest-actual-histogram-ts", actualMetric.Histograms[actualHistogramSamplesCount-1].Timestamp,
				)
				shouldLog = true
			}

			if shouldLog {
				level.Error(logger).Log("msg", err.Error())
			}

			return err
		}

		for i, expectedSamplePair := range expectedMetric.Values {
			actualSamplePair := actualMetric.Values[i]
			err := compareSamplePair(expectedSamplePair, actualSamplePair, opts)
			if err != nil {
				return fmt.Errorf("float sample pair does not match for metric %s: %w", expectedMetric.Metric, err)
			}
		}

		for i, expectedHistogramPair := range expectedMetric.Histograms {
			actualHistogramPair := actualMetric.Histograms[i]
			if err := compareSampleHistogramPair(expectedHistogramPair, actualHistogramPair, opts); err != nil {
				return fmt.Errorf("histogram sample pair does not match for metric %s: %w", expectedMetric.Metric, err)
			}
		}
	}

	return nil
}

func compareVector(expectedRaw, actualRaw json.RawMessage, opts SampleComparisonOptions) error {
	var expected, actual model.Vector

	err := json.Unmarshal(expectedRaw, &expected)
	if err != nil {
		return err
	}

	err = json.Unmarshal(actualRaw, &actual)
	if err != nil {
		return err
	}

	if len(expected) != len(actual) {
		return fmt.Errorf("expected %d metrics but got %d", len(expected),
			len(actual))
	}

	metricFingerprintToIndexMap := make(map[model.Fingerprint]int, len(expected))
	for i, actualMetric := range actual {
		metricFingerprintToIndexMap[actualMetric.Metric.Fingerprint()] = i
	}

	for _, expectedMetric := range expected {
		actualMetricIndex, ok := metricFingerprintToIndexMap[expectedMetric.Metric.Fingerprint()]
		if !ok {
			return fmt.Errorf("expected metric %s missing from actual response", expectedMetric.Metric)
		}

		actualMetric := actual[actualMetricIndex]

		if expectedMetric.Histogram == nil && actualMetric.Histogram == nil {
			err := compareSamplePair(model.SamplePair{
				Timestamp: expectedMetric.Timestamp,
				Value:     expectedMetric.Value,
			}, model.SamplePair{
				Timestamp: actualMetric.Timestamp,
				Value:     actualMetric.Value,
			}, opts)
			if err != nil {
				return fmt.Errorf("float sample pair does not match for metric %s: %w", expectedMetric.Metric, err)
			}
		} else if expectedMetric.Histogram != nil && actualMetric.Histogram == nil {
			return fmt.Errorf("sample pair does not match for metric %s: expected histogram but got float value", expectedMetric.Metric)
		} else if expectedMetric.Histogram == nil && actualMetric.Histogram != nil {
			return fmt.Errorf("sample pair does not match for metric %s: expected float value but got histogram", expectedMetric.Metric)
		} else { // Expected value is a histogram and the actual value is a histogram.
			err := compareSampleHistogramPair(model.SampleHistogramPair{
				Timestamp: expectedMetric.Timestamp,
				Histogram: expectedMetric.Histogram,
			}, model.SampleHistogramPair{
				Timestamp: actualMetric.Timestamp,
				Histogram: actualMetric.Histogram,
			}, opts)
			if err != nil {
				return fmt.Errorf("histogram sample pair does not match for metric %s: %w", expectedMetric.Metric, err)
			}
		}
	}

	return nil
}

func compareScalar(expectedRaw, actualRaw json.RawMessage, opts SampleComparisonOptions) error {
	var expected, actual model.Scalar
	err := json.Unmarshal(expectedRaw, &expected)
	if err != nil {
		return err
	}

	err = json.Unmarshal(actualRaw, &actual)
	if err != nil {
		return err
	}

	return compareSamplePair(model.SamplePair{
		Timestamp: expected.Timestamp,
		Value:     expected.Value,
	}, model.SamplePair{
		Timestamp: actual.Timestamp,
		Value:     actual.Value,
	}, opts)
}

func compareSamplePair(expected, actual model.SamplePair, opts SampleComparisonOptions) error {
	if expected.Timestamp != actual.Timestamp {
		return fmt.Errorf("expected timestamp %v but got %v", expected.Timestamp, actual.Timestamp)
	}
	if opts.SkipRecentSamples > 0 && time.Since(expected.Timestamp.Time()) < opts.SkipRecentSamples {
		return nil
	}
	if !compareSampleValue(float64(expected.Value), float64(actual.Value), opts) {
		return fmt.Errorf("expected value %s for timestamp %v but got %s", expected.Value, expected.Timestamp, actual.Value)
	}

	return nil
}

func compareSampleValue(first, second float64, opts SampleComparisonOptions) bool {
	if (math.IsNaN(first) && math.IsNaN(second)) ||
		(math.IsInf(first, 1) && math.IsInf(second, 1)) ||
		(math.IsInf(first, -1) && math.IsInf(second, -1)) {
		return true
	} else if opts.Tolerance <= 0 {
		return math.Float64bits(first) == math.Float64bits(second)
	}
	if opts.UseRelativeError && second != 0 {
		return math.Abs(first-second)/math.Abs(second) <= opts.Tolerance
	}
	return math.Abs(first-second) <= opts.Tolerance
}

func compareSampleHistogramPair(expected, actual model.SampleHistogramPair, opts SampleComparisonOptions) error {
	if expected.Timestamp != actual.Timestamp {
		return fmt.Errorf("expected timestamp %v but got %v", expected.Timestamp, actual.Timestamp)
	}

	if opts.SkipRecentSamples > 0 && time.Since(expected.Timestamp.Time()) < opts.SkipRecentSamples {
		return nil
	}

	if !compareSampleValue(float64(expected.Histogram.Sum), float64(actual.Histogram.Sum), opts) {
		return fmt.Errorf("expected sum %s for timestamp %v but got %s", expected.Histogram.Sum, expected.Timestamp, actual.Histogram.Sum)
	}

	if !compareSampleValue(float64(expected.Histogram.Count), float64(actual.Histogram.Count), opts) {
		return fmt.Errorf("expected count %s for timestamp %v but got %s", expected.Histogram.Count, expected.Timestamp, actual.Histogram.Count)
	}

	if !slices.EqualFunc(expected.Histogram.Buckets, actual.Histogram.Buckets, compareSampleHistogramBuckets(opts)) {
		return fmt.Errorf("expected buckets %s for timestamp %v but got %s", expected.Histogram.Buckets, expected.Timestamp, actual.Histogram.Buckets)
	}

	return nil
}

func compareSampleHistogramBuckets(opts SampleComparisonOptions) func(expected, actual *model.HistogramBucket) bool {
	return func(first, second *model.HistogramBucket) bool {
		if !compareSampleValue(float64(first.Lower), float64(second.Lower), opts) {
			return false
		}

		if !compareSampleValue(float64(first.Upper), float64(second.Upper), opts) {
			return false
		}

		if !compareSampleValue(float64(first.Count), float64(second.Count), opts) {
			return false
		}

		return first.Boundaries == second.Boundaries
	}
}
