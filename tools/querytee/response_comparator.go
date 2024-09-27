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
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/regexp"
	"github.com/prometheus/common/model"

	util_log "github.com/grafana/mimir/pkg/util/log"
)

// SamplesComparatorFunc helps with comparing different types of samples coming from /api/v1/query and /api/v1/query_range routes.
type SamplesComparatorFunc func(expected, actual json.RawMessage, queryEvaluationTime time.Time, opts SampleComparisonOptions) error

type SamplesResponse struct {
	Status    string
	ErrorType string
	Error     string
	Warnings  []string
	Infos     []string
	Data      struct {
		ResultType string
		Result     json.RawMessage
	}
}

type SampleComparisonOptions struct {
	Tolerance              float64
	UseRelativeError       bool
	SkipRecentSamples      time.Duration
	RequireExactErrorMatch bool
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

// RegisterSamplesType registers custom sample types.
func (s *SamplesComparator) RegisterSamplesType(samplesType string, comparator SamplesComparatorFunc) {
	s.sampleTypesComparator[samplesType] = comparator
}

func (s *SamplesComparator) Compare(expectedResponse, actualResponse []byte, queryEvaluationTime time.Time) (ComparisonResult, error) {
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

	if !s.errorsMatch(expected.Error, actual.Error) {
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

	if err := comparator(expected.Data.Result, actual.Data.Result, queryEvaluationTime, s.opts); err != nil {
		return ComparisonFailed, err
	}

	// Check annotations last: they're less important compared to the other possible differences above.
	if !slicesEqualIgnoringOrder(expected.Warnings, actual.Warnings) {
		return ComparisonFailed, fmt.Errorf("expected warning annotations %s but got %s", formatAnnotationsForErrorMessage(expected.Warnings), formatAnnotationsForErrorMessage(actual.Warnings))
	}

	if !slicesEqualIgnoringOrder(expected.Infos, actual.Infos) {
		return ComparisonFailed, fmt.Errorf("expected info annotations %s but got %s", formatAnnotationsForErrorMessage(expected.Infos), formatAnnotationsForErrorMessage(actual.Infos))
	}

	return ComparisonSuccess, nil
}

var errorEquivalenceClasses = [][]*regexp.Regexp{
	{
		// Invalid expression type for range query: MQE and Prometheus' engine return different error messages.
		// Prometheus' engine:
		regexp.MustCompile(`invalid parameter "query": invalid expression type "range vector" for range query, must be Scalar or instant Vector`),
		// MQE:
		regexp.MustCompile(`invalid parameter "query": query expression produces a range vector, but expression for range queries must produce an instant vector or scalar`),
	},
	{
		// Binary operation conflict on right (one-to-one) / many (one-to-many/many-to-one) side: MQE and Prometheus' engine return different error messages, and there's no guarantee they'll pick the same series as examples.
		// Even comparing Prometheus' engine to another instance of Prometheus' engine can produce different results: the series selected as examples are not deterministic.
		// Prometheus' engine:
		regexp.MustCompile(`found duplicate series for the match group \{.*\} on the (left|right) hand-side of the operation: \[.*\];many-to-many matching not allowed: matching labels must be unique on one side`),
		// MQE:
		regexp.MustCompile(`found duplicate series for the match group \{.*\} on the (left|right) side of the operation at timestamp \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(.\d+)?Z: \{.*\} and \{.*\}`),
	},
	{
		// Same as above, but for left (one-to-one) / one (one-to-many/many-to-one) side.
		// Prometheus' engine:
		regexp.MustCompile(`multiple matches for labels: many-to-one matching must be explicit \(group_left/group_right\)`),
		// MQE:
		regexp.MustCompile(`found duplicate series for the match group \{.*\} on the (left|right) side of the operation at timestamp \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(.\d+)?Z: \{.*\} and \{.*\}`),
	},
}

func (s *SamplesComparator) errorsMatch(expected, actual string) bool {
	if expected == actual {
		return true
	}

	if s.opts.RequireExactErrorMatch {
		// Errors didn't match exactly, and we want an exact match. We're done.
		return false
	}

	for _, equivalenceClass := range errorEquivalenceClasses {
		if anyMatch(expected, equivalenceClass) && anyMatch(actual, equivalenceClass) {
			return true
		}
	}

	return false
}

func anyMatch(s string, patterns []*regexp.Regexp) bool {
	for _, pattern := range patterns {
		if pattern.MatchString(s) {
			return true
		}
	}

	return false
}

func slicesEqualIgnoringOrder(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}

	if len(a) != len(b) {
		return false
	}

	// Make a copy before we mutate the slices.
	a = slices.Clone(a)
	b = slices.Clone(b)

	slices.Sort(a)
	slices.Sort(b)

	return slices.Equal(a, b)
}

func formatAnnotationsForErrorMessage(warnings []string) string {
	formatted := make([]string, 0, len(warnings))

	for _, warning := range warnings {
		formatted = append(formatted, fmt.Sprintf("%q", warning))
	}

	return "[" + strings.Join(formatted, ", ") + "]"
}

func compareMatrix(expectedRaw, actualRaw json.RawMessage, queryEvaluationTime time.Time, opts SampleComparisonOptions) error {
	var expected, actual model.Matrix

	err := json.Unmarshal(expectedRaw, &expected)
	if err != nil {
		return err
	}
	err = json.Unmarshal(actualRaw, &actual)
	if err != nil {
		return err
	}

	if allMatrixSamplesWithinRecentSampleWindow(expected, queryEvaluationTime, opts) && allMatrixSamplesWithinRecentSampleWindow(actual, queryEvaluationTime, opts) {
		return nil
	}

	if len(expected) != len(actual) {
		return fmt.Errorf("expected %d metrics but got %d", len(expected), len(actual))
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

		err := compareMatrixSamples(expectedMetric, actualMetric, queryEvaluationTime, opts)
		if err != nil {
			return fmt.Errorf("%w\nExpected result for series:\n%v\n\nActual result for series:\n%v", err, expectedMetric, actualMetric)
		}
	}

	return nil
}

func compareMatrixSamples(expected, actual *model.SampleStream, queryEvaluationTime time.Time, opts SampleComparisonOptions) error {
	expectedSamplesTail, actualSamplesTail, err := comparePairs(expected.Values, actual.Values, func(p1 model.SamplePair, p2 model.SamplePair) error {
		err := compareSamplePair(p1, p2, queryEvaluationTime, opts)
		if err != nil {
			return fmt.Errorf("float sample pair does not match for metric %s: %w", expected.Metric, err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	expectedHistogramSamplesTail, actualHistogramSamplesTail, err := comparePairs(expected.Histograms, actual.Histograms, func(p1 model.SampleHistogramPair, p2 model.SampleHistogramPair) error {
		err := compareSampleHistogramPair(p1, p2, queryEvaluationTime, opts)
		if err != nil {
			return fmt.Errorf("histogram sample pair does not match for metric %s: %w", expected.Metric, err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	expectedFloatSamplesCount := len(expected.Values)
	actualFloatSamplesCount := len(actual.Values)
	expectedHistogramSamplesCount := len(expected.Histograms)
	actualHistogramSamplesCount := len(actual.Histograms)

	if expectedFloatSamplesCount == actualFloatSamplesCount && expectedHistogramSamplesCount == actualHistogramSamplesCount {
		return nil
	}

	skipAllRecentFloatSamples := canSkipAllSamples(func(p model.SamplePair) bool {
		return queryEvaluationTime.Sub(p.Timestamp.Time())-opts.SkipRecentSamples < 0
	}, expectedSamplesTail, actualSamplesTail)

	skipAllRecentHistogramSamples := canSkipAllSamples(func(p model.SampleHistogramPair) bool {
		return queryEvaluationTime.Sub(p.Timestamp.Time())-opts.SkipRecentSamples < 0
	}, expectedHistogramSamplesTail, actualHistogramSamplesTail)

	if skipAllRecentFloatSamples && skipAllRecentHistogramSamples {
		return nil
	}

	err = fmt.Errorf(
		"expected %d float sample(s) and %d histogram sample(s) for metric %s but got %d float sample(s) and %d histogram sample(s)",
		len(expected.Values),
		len(expected.Histograms),
		expected.Metric,
		len(actual.Values),
		len(actual.Histograms),
	)

	shouldLog := false
	logger := util_log.Logger

	if expectedFloatSamplesCount > 0 && actualFloatSamplesCount > 0 {
		logger = log.With(logger,
			"oldest-expected-float-ts", expected.Values[0].Timestamp,
			"newest-expected-float-ts", expected.Values[expectedFloatSamplesCount-1].Timestamp,
			"oldest-actual-float-ts", actual.Values[0].Timestamp,
			"newest-actual-float-ts", actual.Values[actualFloatSamplesCount-1].Timestamp,
		)
		shouldLog = true
	}

	if expectedHistogramSamplesCount > 0 && actualHistogramSamplesCount > 0 {
		logger = log.With(logger,
			"oldest-expected-histogram-ts", expected.Histograms[0].Timestamp,
			"newest-expected-histogram-ts", expected.Histograms[expectedHistogramSamplesCount-1].Timestamp,
			"oldest-actual-histogram-ts", actual.Histograms[0].Timestamp,
			"newest-actual-histogram-ts", actual.Histograms[actualHistogramSamplesCount-1].Timestamp,
		)
		shouldLog = true
	}

	if shouldLog {
		level.Error(logger).Log("msg", err.Error())
	}
	return err
}

// comparePairs runs compare for pairs in s1 and s2. It stops on the first error the compare returns.
// If len(s1) != len(s2) it compares only elements inside the longest prefix of both. If all elements within the prefix match,
// it returns the tail of s1 and s2, and a nil error.
func comparePairs[S ~[]M, M any](s1, s2 S, compare func(M, M) error) (S, S, error) {
	var i int
	for ; i < len(s1) && i < len(s2); i++ {
		err := compare(s1[i], s2[i])
		if err != nil {
			return s1, s2, err
		}
	}
	return s1[i:], s2[i:], nil
}

func canSkipAllSamples[S ~[]M, M any](skip func(M) bool, ss ...S) bool {
	for _, s := range ss {
		for _, p := range s {
			if !skip(p) {
				return false
			}
		}
	}
	return true
}

func allMatrixSamplesWithinRecentSampleWindow(m model.Matrix, queryEvaluationTime time.Time, opts SampleComparisonOptions) bool {
	if opts.SkipRecentSamples == 0 {
		return false
	}

	for _, series := range m {
		for _, sample := range series.Values {
			if queryEvaluationTime.Sub(sample.Timestamp.Time()) > opts.SkipRecentSamples {
				return false
			}
		}

		for _, sample := range series.Histograms {
			if queryEvaluationTime.Sub(sample.Timestamp.Time()) > opts.SkipRecentSamples {
				return false
			}
		}
	}

	return true
}

func compareVector(expectedRaw, actualRaw json.RawMessage, queryEvaluationTime time.Time, opts SampleComparisonOptions) error {
	var expected, actual model.Vector

	err := json.Unmarshal(expectedRaw, &expected)
	if err != nil {
		return err
	}

	err = json.Unmarshal(actualRaw, &actual)
	if err != nil {
		return err
	}

	if allVectorSamplesWithinRecentSampleWindow(expected, queryEvaluationTime, opts) && allVectorSamplesWithinRecentSampleWindow(actual, queryEvaluationTime, opts) {
		return nil
	}

	if len(expected) != len(actual) {
		return fmt.Errorf("expected %d metrics but got %d", len(expected), len(actual))
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
			err := compareSamplePair(
				model.SamplePair{
					Timestamp: expectedMetric.Timestamp,
					Value:     expectedMetric.Value,
				},
				model.SamplePair{
					Timestamp: actualMetric.Timestamp,
					Value:     actualMetric.Value,
				},
				queryEvaluationTime,
				opts,
			)
			if err != nil {
				return fmt.Errorf("float sample pair does not match for metric %s: %w", expectedMetric.Metric, err)
			}
		} else if expectedMetric.Histogram != nil && actualMetric.Histogram == nil {
			return fmt.Errorf("sample pair does not match for metric %s: expected histogram but got float value", expectedMetric.Metric)
		} else if expectedMetric.Histogram == nil && actualMetric.Histogram != nil {
			return fmt.Errorf("sample pair does not match for metric %s: expected float value but got histogram", expectedMetric.Metric)
		} else { // Expected value is a histogram and the actual value is a histogram.
			err := compareSampleHistogramPair(
				model.SampleHistogramPair{
					Timestamp: expectedMetric.Timestamp,
					Histogram: expectedMetric.Histogram,
				},
				model.SampleHistogramPair{
					Timestamp: actualMetric.Timestamp,
					Histogram: actualMetric.Histogram,
				},
				queryEvaluationTime,
				opts,
			)
			if err != nil {
				return fmt.Errorf("histogram sample pair does not match for metric %s: %w", expectedMetric.Metric, err)
			}
		}
	}

	return nil
}

func allVectorSamplesWithinRecentSampleWindow(v model.Vector, queryEvaluationTime time.Time, opts SampleComparisonOptions) bool {
	if opts.SkipRecentSamples == 0 {
		return false
	}

	for _, sample := range v {
		if queryEvaluationTime.Sub(sample.Timestamp.Time()) > opts.SkipRecentSamples {
			return false
		}
	}

	return true
}

func compareScalar(expectedRaw, actualRaw json.RawMessage, queryEvaluationTime time.Time, opts SampleComparisonOptions) error {
	var expected, actual model.Scalar
	err := json.Unmarshal(expectedRaw, &expected)
	if err != nil {
		return err
	}

	err = json.Unmarshal(actualRaw, &actual)
	if err != nil {
		return err
	}

	return compareSamplePair(
		model.SamplePair{
			Timestamp: expected.Timestamp,
			Value:     expected.Value,
		},
		model.SamplePair{
			Timestamp: actual.Timestamp,
			Value:     actual.Value,
		},
		queryEvaluationTime,
		opts,
	)
}

func compareSamplePair(expected, actual model.SamplePair, queryEvaluationTime time.Time, opts SampleComparisonOptions) error {
	if expected.Timestamp != actual.Timestamp {
		return fmt.Errorf("expected timestamp %v but got %v", expected.Timestamp, actual.Timestamp)
	}
	if opts.SkipRecentSamples > 0 && queryEvaluationTime.Sub(expected.Timestamp.Time()) < opts.SkipRecentSamples {
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

func compareSampleHistogramPair(expected, actual model.SampleHistogramPair, queryEvaluationTime time.Time, opts SampleComparisonOptions) error {
	if expected.Timestamp != actual.Timestamp {
		return fmt.Errorf("expected timestamp %v but got %v", expected.Timestamp, actual.Timestamp)
	}

	if opts.SkipRecentSamples > 0 && queryEvaluationTime.Sub(expected.Timestamp.Time()) < opts.SkipRecentSamples {
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
