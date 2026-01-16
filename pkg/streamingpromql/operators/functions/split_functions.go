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

// TODO: investigate Kahan summation more, are we using the compensation correctly?
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
	// Query time range isn't used for sum over time
	f, hasFloat, h, err := sumOverTime(step, nil, types.QueryTimeRange{}, emitAnnotation, nil)
	if err != nil {
		return SumOverTimeIntermediate{}, err
	}

	result := SumOverTimeIntermediate{
		SumF:     f,
		HasFloat: hasFloat,
	}

	if h != nil {
		histProto := mimirpb.FromFloatHistogramToHistogramProto(0, h)
		result.SumH = &histProto
	}

	return result, nil
}

func sumOverTimeCombine(
	pieces []SumOverTimeIntermediate,
	emitAnnotation types.EmitAnnotationFunc,
	_ *limiter.MemoryConsumptionTracker,
) (float64, bool, *histogram.FloatHistogram, error) {
	haveFloats := false
	sumF, c := 0.0, 0.0
	var sumH *histogram.FloatHistogram

	for _, p := range pieces {
		if p.HasFloat {
			haveFloats = true
			sumF, c = floats.KahanSumInc(p.SumF, sumF, c)
		}
		if p.SumH != nil {
			h := mimirpb.FromHistogramProtoToFloatHistogram(p.SumH)
			if sumH == nil {
				sumH = h
			} else {
				if _, _, _, err := sumH.Add(h); err != nil {
					err = NativeHistogramErrorToAnnotation(err, emitAnnotation)
					return 0, false, nil, err
				}
			}
		}
	}

	if haveFloats && sumH != nil {
		emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
		return 0, false, nil, nil
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

func countOverTimeCombine(pieces []SingleSampleIntermediate, _ types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
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

func minOverTimeCombine(pieces []SingleSampleIntermediate, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
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

func maxOverTimeCombine(pieces []SingleSampleIntermediate, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
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

