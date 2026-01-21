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

var SplitSumOverTime = &RangeVectorSplittingMetadata{
	OperatorFactory:       NewSplitOperatorFactory[SumOverTimeIntermediate](sumOverTimeGenerate, sumOverTimeCombine, SumOverTimeCodec, SumOverTime, FUNCTION_SUM_OVER_TIME),
	RangeVectorChildIndex: 0,
}

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

	if haveFloats && haveHistograms {
		emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
		return SumOverTimeIntermediate{}, nil
	}

	if haveFloats {
		sum, c := 0.0, 0.0
		for _, p := range fHead {
			sum, c = floats.KahanSumInc(p.F, sum, c)
		}
		for _, p := range fTail {
			sum, c = floats.KahanSumInc(p.F, sum, c)
		}

		return SumOverTimeIntermediate{
			SumF: sum,
			// Store compensation separately - compensation could be a lot smaller than sum so adding it to sum now
			// could lose precision. Instead, when combining all the splits we will add both sum and compensation using
			// Kahan summation. As floating-point arithmetic is lossy and we don't perform the exact same operations in
			// split and non-split sum_over_time() results may not always be exactly identical.
			SumC:     c,
			HasFloat: true,
		}, nil
	}

	h, err := sumHistograms(hHead, hTail, emitAnnotation)
	if err != nil {
		return SumOverTimeIntermediate{}, err
	}

	if h != nil {
		histProto := mimirpb.FromFloatHistogramToHistogramProto(0, h)
		return SumOverTimeIntermediate{
			SumH: &histProto,
		}, nil
	}

	return SumOverTimeIntermediate{}, nil
}

// TODO: handle counter reset collision warning
func sumOverTimeCombine(
	pieces []SumOverTimeIntermediate,
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
		return nil, errors.Wrap(err, "marshaling sum over time list")
	}
	return listBytes, nil
}

func (c sumOverTimeCodec) Unmarshal(bytes []byte) ([]SumOverTimeIntermediate, error) {
	var listProto SumOverTimeIntermediateList
	if err := listProto.Unmarshal(bytes); err != nil {
		return nil, errors.Wrap(err, "unmarshaling sum over time list")
	}
	return listProto.Results, nil
}

var SumOverTimeCodec = sumOverTimeCodec{}

type singleSampleCodec struct{}

func (c singleSampleCodec) Marshal(results []SingleSampleIntermediate) ([]byte, error) {
	listProto := &SingleSampleIntermediateList{Results: results}
	listBytes, err := listProto.Marshal()
	if err != nil {
		return nil, errors.Wrap(err, "marshaling single sample list")
	}
	return listBytes, nil
}

func (c singleSampleCodec) Unmarshal(bytes []byte) ([]SingleSampleIntermediate, error) {
	var listProto SingleSampleIntermediateList
	if err := listProto.Unmarshal(bytes); err != nil {
		return nil, errors.Wrap(err, "unmarshaling single sample list")
	}
	return listProto.Results, nil
}

var SingleSampleCodec = singleSampleCodec{}

var SplitCountOverTime = &RangeVectorSplittingMetadata{
	OperatorFactory: NewSplitOperatorFactory[SingleSampleIntermediate](
		countOverTimeGenerate,
		countOverTimeCombine,
		SingleSampleCodec,
		CountOverTime,
		FUNCTION_COUNT_OVER_TIME,
	),
	RangeVectorChildIndex: 0,
}

func countOverTimeGenerate(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (SingleSampleIntermediate, error) {
	count, hasValue, _, err := countOverTime(step, nil, types.QueryTimeRange{}, nil, nil)
	if err != nil {
		return SingleSampleIntermediate{}, err
	}
	return SingleSampleIntermediate{F: count, HasFloat: hasValue}, nil
}

func countOverTimeCombine(pieces []SingleSampleIntermediate, _ int64, _ int64, _ types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
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

var SplitMinOverTime = &RangeVectorSplittingMetadata{
	OperatorFactory: NewSplitOperatorFactory[SingleSampleIntermediate](
		minOverTimeGenerate,
		minOverTimeCombine,
		SingleSampleCodec,
		MinOverTime,
		FUNCTION_MIN_OVER_TIME,
	),
	RangeVectorChildIndex: 0,
}

func minOverTimeGenerate(
	step *types.RangeVectorStepData,
	_ []types.ScalarData,
	emitAnnotation types.EmitAnnotationFunc,
	_ *limiter.MemoryConsumptionTracker,
) (SingleSampleIntermediate, error) {
	f, hasFloat, h, err := minOverTime(step, nil, types.QueryTimeRange{}, emitAnnotation, nil)
	if err != nil {
		return SingleSampleIntermediate{}, err
	}

	result := SingleSampleIntermediate{
		F:        f,
		HasFloat: hasFloat,
	}

	if h != nil {
		histProto := mimirpb.FromFloatHistogramToHistogramProto(0, h)
		result.H = &histProto
	}

	return result, nil
}

func minOverTimeCombine(pieces []SingleSampleIntermediate, _ int64, _ int64, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	hasFloat := false
	hasHistogram := false
	minF := math.NaN()

	for _, p := range pieces {
		if p.HasFloat {
			hasFloat = true
			if p.F < minF || math.IsNaN(minF) {
				minF = p.F
			}
		}
		if p.H != nil {
			hasHistogram = true
		}
	}

	if hasFloat && hasHistogram {
		emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
		return 0, false, nil, nil
	}

	return minF, hasFloat, nil, nil
}

var SplitMaxOverTime = &RangeVectorSplittingMetadata{
	OperatorFactory: NewSplitOperatorFactory[SingleSampleIntermediate](
		maxOverTimeGenerate,
		maxOverTimeCombine,
		SingleSampleCodec,
		MaxOverTime,
		FUNCTION_MAX_OVER_TIME,
	),
	RangeVectorChildIndex: 0,
}

func maxOverTimeGenerate(step *types.RangeVectorStepData, _ []types.ScalarData, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (SingleSampleIntermediate, error) {
	f, hasFloat, h, err := maxOverTime(step, nil, types.QueryTimeRange{}, emitAnnotation, nil)
	if err != nil {
		return SingleSampleIntermediate{}, err
	}

	result := SingleSampleIntermediate{
		F:        f,
		HasFloat: hasFloat,
	}

	if h != nil {
		histProto := mimirpb.FromFloatHistogramToHistogramProto(0, h)
		result.H = &histProto
	}

	return result, nil
}

func maxOverTimeCombine(pieces []SingleSampleIntermediate, _ int64, _ int64, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
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
		if p.H != nil {
			hasHistogram = true
		}
	}

	if hasFloat && hasHistogram {
		emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
		return 0, false, nil, nil
	}

	return maxF, hasFloat, nil, nil
}
