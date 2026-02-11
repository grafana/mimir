// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"math"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streamingpromql/floats"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// SplitSumOverTime configures the range vector splitting version of sum_over_time().
// In Prometheus, sum_over_time() emits a warning when processing histograms with a mix of CounterReset and
// NotCounterReset hints. This split implementation does not emit these warnings if the mix is between split ranges
// (e.g.  one split range has only histograms with CounterReset hints, the other only NotCounterReset, when combining
// the intermediate results, the implementation unable to detect there is a mix of hints across the split ranges).
// This is because of the additional complexity to do the detection. The counter reset hint is sometimes calculated
// dynamically by comparing the current histogram value to the previous histogram value, rather than just using the hint
// values from storage. In order to properly detect counter resets across split boundaries, we would need to store the
// first and last histogram in each split range within the intermediate result so we can calculate hints at the
// boundaries (similar to how we store raw samples for rate()).
// Additionally, currently counter reset detection logic changes if there's a histogram_count() function wrapping the
// sum_over_time() or not. To support this we would need to propagate this information to the split operator.
// This annotation is only expected to be emitted in rare cases (sum_over_time() is generally expected to be used for
// gauge histograms rather than counter histograms) and counter reset detection is still evolving
// (see https://github.com/prometheus/prometheus/issues/15346), so we choose to be slightly inconsistent with the
// Prometheus for now. The returned result is correct, it's just the warning annotation may occasionally be missing.
var SplitSumOverTime = NewSplitOperatorFactory[SumOverTimeIntermediate](sumOverTimeGenerate, sumOverTimeCombine, SumOverTimeCodec, SumOverTime, FUNCTION_SUM_OVER_TIME)

func sumOverTimeGenerate(
	step *types.RangeVectorStepData,
	_ []types.ScalarData,
	emitAnnotation types.EmitAnnotationFunc,
	_ *limiter.MemoryConsumptionTracker,
) (SumOverTimeIntermediate, error) {
	fHead, fTail := step.Floats.UnsafePoints()
	hHead, hTail := step.Histograms.UnsafePoints()

	haveFloats := len(fHead) > 0 || len(fTail) > 0
	haveHistograms := len(hHead) > 0 || len(hTail) > 0

	if !haveFloats && !haveHistograms {
		return SumOverTimeIntermediate{}, nil
	}

	// Skip checking if both floats and histograms exist in generate.
	// If we did the check now and returned an empty result for this split range, when combining the ranges we won't
	// know there was a problem with this range and could end up incorrectly adding the empty result to non-empty
	// results from the other ranges.
	// Instead add up both the float and histogram samples and rely on the combine function to handle mixed samples.

	result := SumOverTimeIntermediate{}

	if haveFloats {
		sum, c := 0.0, 0.0
		for _, p := range fHead {
			sum, c = floats.KahanSumInc(p.F, sum, c)
		}
		for _, p := range fTail {
			sum, c = floats.KahanSumInc(p.F, sum, c)
		}

		result.SumF = sum
		// Store compensation separately - compensation could be a lot smaller than sum so adding it to sum now
		// could lose precision. Instead, when combining all the splits we will add both sum and compensation using
		// Kahan summation. As floating-point arithmetic is lossy and we don't perform the exact same operations in
		// split and non-split sum_over_time(), results may not always be exactly identical.
		result.SumC = c
		result.HasFloat = true
	}

	if haveHistograms {
		h, err := sumHistograms(hHead, hTail, emitAnnotation)
		if err != nil {
			return SumOverTimeIntermediate{}, err
		}
		histProto := mimirpb.FromFloatHistogramToHistogramProto(0, h)
		result.SumH = &histProto
	}

	return result, nil
}

func sumOverTimeCombine(
	pieces []SumOverTimeIntermediate,
	_ []types.ScalarData,
	_ int64,
	_ int64,
	emitAnnotation types.EmitAnnotationFunc,
	_ *limiter.MemoryConsumptionTracker,
) (float64, bool, *histogram.FloatHistogram, error) {
	haveFloats := false
	sumF, c := 0.0, 0.0
	var sumH *histogram.FloatHistogram
	nhcbBoundsReconciledSeen := false

	for _, p := range pieces {
		if p.HasFloat {
			haveFloats = true
			sumF, c = floats.KahanSumInc(p.SumF, sumF, c)
			sumF, c = floats.KahanSumInc(p.SumC, sumF, c)
		}
		if p.SumH != nil {
			h := mimirpb.FromFloatHistogramProtoToFloatHistogram(p.SumH)

			if sumH == nil {
				// First histogram we're seeing, copy it to create the accumulator.
				sumH = h.Copy()
			} else {
				if _, _, nhcbBoundsReconciled, err := sumH.Add(h); err != nil {
					err = NativeHistogramErrorToAnnotation(err, emitAnnotation)
					return 0, false, nil, err
				} else if nhcbBoundsReconciled {
					nhcbBoundsReconciledSeen = true
				}
			}
		}
	}

	if haveFloats && sumH != nil {
		emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
		return 0, false, nil, nil
	}

	if nhcbBoundsReconciledSeen {
		emitAnnotation(newAggregationMismatchedCustomBucketsHistogramInfo)
	}

	return sumF + c, haveFloats, sumH, nil
}

type sumOverTimeCodec struct{}

func (c sumOverTimeCodec) Marshal(results []SumOverTimeIntermediate) ([]byte, error) {
	listProto := &SumOverTimeIntermediateList{Results: results}
	listBytes, err := listProto.Marshal()
	if err != nil {
		return nil, errors.Wrap(err, "marshaling sum_over_time list")
	}
	return listBytes, nil
}

func (c sumOverTimeCodec) Unmarshal(bytes []byte) ([]SumOverTimeIntermediate, error) {
	var listProto SumOverTimeIntermediateList
	if err := listProto.Unmarshal(bytes); err != nil {
		return nil, errors.Wrap(err, "unmarshaling sum_over_time list")
	}
	return listProto.Results, nil
}

var SumOverTimeCodec = sumOverTimeCodec{}

var SplitCountOverTime = NewSplitOperatorFactory[CountOverTimeIntermediate](
	countOverTimeGenerate,
	countOverTimeCombine,
	CountOverTimeCodec,
	CountOverTime,
	FUNCTION_COUNT_OVER_TIME,
)

func countOverTimeGenerate(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (CountOverTimeIntermediate, error) {
	count, hasValue, _, err := countOverTime(step, nil, types.QueryTimeRange{}, nil, nil)
	if err != nil {
		return CountOverTimeIntermediate{}, err
	}
	return CountOverTimeIntermediate{F: count, HasFloat: hasValue}, nil
}

func countOverTimeCombine(pieces []CountOverTimeIntermediate, _ []types.ScalarData, _ int64, _ int64, _ types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	totalCount := 0.0
	hasValue := false

	for _, p := range pieces {
		if p.HasFloat {
			hasValue = true
			totalCount += p.F
		}
	}

	return totalCount, hasValue, nil, nil
}

type countOverTimeCodec struct{}

func (c countOverTimeCodec) Marshal(results []CountOverTimeIntermediate) ([]byte, error) {
	listProto := &CountOverTimeIntermediateList{Results: results}
	listBytes, err := listProto.Marshal()
	if err != nil {
		return nil, errors.Wrap(err, "marshaling count_over_time list")
	}
	return listBytes, nil
}

func (c countOverTimeCodec) Unmarshal(bytes []byte) ([]CountOverTimeIntermediate, error) {
	var listProto CountOverTimeIntermediateList
	if err := listProto.Unmarshal(bytes); err != nil {
		return nil, errors.Wrap(err, "unmarshaling count_over_time list")
	}
	return listProto.Results, nil
}

var CountOverTimeCodec = countOverTimeCodec{}

var SplitMinOverTime = NewSplitOperatorFactory[MinMaxOverTimeIntermediate](
	minOverTimeGenerate,
	minOverTimeCombine,
	MinMaxOverTimeCodec,
	MinOverTime,
	FUNCTION_MIN_OVER_TIME,
)

func minOverTimeGenerate(
	step *types.RangeVectorStepData,
	_ []types.ScalarData,
	_ types.EmitAnnotationFunc,
	_ *limiter.MemoryConsumptionTracker,
) (MinMaxOverTimeIntermediate, error) {
	// Pass a no-op emitAnnotation: the combine step will handle emitting the mixed float+histogram annotations so we
	// can ignore the annotations emitted in minOverTime().
	f, hasFloat, _, err := minOverTime(step, nil, types.QueryTimeRange{}, emitAnnotationNoop, nil)
	if err != nil {
		return MinMaxOverTimeIntermediate{}, err
	}

	result := MinMaxOverTimeIntermediate{
		F:        f,
		HasFloat: hasFloat,
		// minOverTime() always returns nil histogram, but we need to track if histograms were present so we can emit
		// the proper annotation during combine. If this range only had histograms and the other ranges only had floats,
		// if we didn't track the presence of histograms we can't emit the histogram ignored annotation.
		HasHistogram: step.Histograms.Any(),
	}

	return result, nil
}

func minOverTimeCombine(pieces []MinMaxOverTimeIntermediate, _ []types.ScalarData, _ int64, _ int64, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	minF := math.NaN()
	hasFloat := false
	hasHistogram := false

	for _, p := range pieces {
		if p.HasFloat {
			hasFloat = true
			if p.F < minF || math.IsNaN(minF) {
				minF = p.F
			}
		}
		if p.HasHistogram {
			hasHistogram = true
		}
	}

	if hasFloat && hasHistogram {
		emitAnnotation(annotations.NewHistogramIgnoredInMixedRangeInfo)
	}

	return minF, hasFloat, nil, nil
}

var SplitMaxOverTime = NewSplitOperatorFactory[MinMaxOverTimeIntermediate](
	maxOverTimeGenerate,
	maxOverTimeCombine,
	MinMaxOverTimeCodec,
	MaxOverTime,
	FUNCTION_MAX_OVER_TIME,
)

func maxOverTimeGenerate(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (MinMaxOverTimeIntermediate, error) {
	// Pass a no-op emitAnnotation: the combine step will handle emitting the mixed float+histogram annotations so we
	// can ignore the annotations emitted in maxOverTime().
	f, hasFloat, _, err := maxOverTime(step, nil, types.QueryTimeRange{}, emitAnnotationNoop, nil)
	if err != nil {
		return MinMaxOverTimeIntermediate{}, err
	}

	result := MinMaxOverTimeIntermediate{
		F:        f,
		HasFloat: hasFloat,
		// maxOverTime() always returns nil histogram, but we need to track if histograms were present so we can emit
		// the proper annotation during combine. If this range only had histograms and the other ranges only had floats,
		// if we didn't track the presence of histograms we can't emit the histogram ignored annotation.
		HasHistogram: step.Histograms.Any(),
	}

	return result, nil
}

func maxOverTimeCombine(pieces []MinMaxOverTimeIntermediate, _ []types.ScalarData, _ int64, _ int64, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	hasFloat := false
	hasHistogram := false
	maxF := math.NaN()

	for _, p := range pieces {
		if p.HasFloat {
			hasFloat = true
			if p.F > maxF || math.IsNaN(maxF) {
				maxF = p.F
			}
		}
		if p.HasHistogram {
			hasHistogram = true
		}
	}

	// Emit the same annotation that the non-split version emits when there are both floats and histograms
	if hasFloat && hasHistogram {
		emitAnnotation(annotations.NewHistogramIgnoredInMixedRangeInfo)
	}

	return maxF, hasFloat, nil, nil
}

type minMaxOverTimeCodec struct{}

func (c minMaxOverTimeCodec) Marshal(results []MinMaxOverTimeIntermediate) ([]byte, error) {
	listProto := &MinMaxOverTimeIntermediateList{Results: results}
	listBytes, err := listProto.Marshal()
	if err != nil {
		return nil, errors.Wrap(err, "marshaling min/max_over_time list")
	}
	return listBytes, nil
}

func (c minMaxOverTimeCodec) Unmarshal(bytes []byte) ([]MinMaxOverTimeIntermediate, error) {
	var listProto MinMaxOverTimeIntermediateList
	if err := listProto.Unmarshal(bytes); err != nil {
		return nil, errors.Wrap(err, "unmarshaling min/max_over_time list")
	}
	return listProto.Results, nil
}

var MinMaxOverTimeCodec = minMaxOverTimeCodec{}

func emitAnnotationNoop(_ types.AnnotationGenerator) {}
