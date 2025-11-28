// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/querysplitting/cache"
	"io"
	"math"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streamingpromql/floats"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

var SplitSumOverTime = NewSplitOperatorFactory[SumOverTimeIntermediate](sumOverTimeGenerate, sumOverTimeCombine, SumOverTimeCodec, SumOverTime, FUNCTION_SUM_OVER_TIME)

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

	return sumF, haveFloats, sumH, nil
}

var SumOverTimeCodec = newProtoListCodec(
	func(results []SumOverTimeIntermediate) ([]byte, error) {
		listProto := &SumOverTimeIntermediateList{Results: results}
		listBytes, err := listProto.Marshal()
		if err != nil {
			return nil, errors.Wrap(err, "marshaling sum over time list")
		}
		return listBytes, nil
	},
	func(bytes []byte) ([]SumOverTimeIntermediate, error) {
		var listProto SumOverTimeIntermediateList
		if err := listProto.Unmarshal(bytes); err != nil {
			return nil, errors.Wrap(err, "unmarshaling sum over time list")
		}
		return listProto.Results, nil
	},
)

var SingleSampleCodec = newProtoListCodec(
	func(results []SingleSampleIntermediate) ([]byte, error) {
		listProto := &SingleSampleIntermediateList{Results: results}
		listBytes, err := listProto.Marshal()
		if err != nil {
			return nil, errors.Wrap(err, "marshaling single sample list")
		}
		return listBytes, nil
	},
	func(bytes []byte) ([]SingleSampleIntermediate, error) {
		var listProto SingleSampleIntermediateList
		if err := listProto.Unmarshal(bytes); err != nil {
			return nil, errors.Wrap(err, "unmarshaling single sample list")
		}
		return listProto.Results, nil
	},
)

var SplitCountOverTime = NewSplitOperatorFactory[SingleSampleIntermediate](
	countOverTimeGenerate,
	countOverTimeCombine,
	SingleSampleCodec,
	CountOverTime,
	FUNCTION_COUNT_OVER_TIME,
)

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

var SplitMinOverTime = NewSplitOperatorFactory[SingleSampleIntermediate](
	minOverTimeGenerate,
	minOverTimeCombine,
	SingleSampleCodec,
	MinOverTime,
	FUNCTION_MIN_OVER_TIME,
)

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

func minOverTimeCombine(pieces []SingleSampleIntermediate, _ types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	hasFloat := false
	minF := math.NaN()

	for _, p := range pieces {
		if !p.HasFloat {
			continue
		}
		hasFloat = true

		if p.F < minF || math.IsNaN(minF) {
			minF = p.F
		}
	}

	return minF, hasFloat, nil, nil
}

var SplitMaxOverTime = NewSplitOperatorFactory[SingleSampleIntermediate](
	maxOverTimeGenerate,
	maxOverTimeCombine,
	SingleSampleCodec,
	MaxOverTime,
	FUNCTION_MAX_OVER_TIME,
)

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

func maxOverTimeCombine(pieces []SingleSampleIntermediate, _ types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	hasFloat := false
	maxF := math.NaN()

	for _, p := range pieces {
		if !p.HasFloat {
			continue
		}
		hasFloat = true

		if p.F > maxF || math.IsNaN(maxF) {
			maxF = p.F
		}
	}

	return maxF, hasFloat, nil, nil
}

// Generic codec implementation for protobuf list-based intermediate results.
type protoListCodec[ItemProto any] struct {
	marshalList   func([]ItemProto) ([]byte, error)
	unmarshalList func([]byte) ([]ItemProto, error)
}

func newProtoListCodec[ItemProto any](
	marshalList func([]ItemProto) ([]byte, error),
	unmarshalList func([]byte) ([]ItemProto, error),
) cache.SplitCodec[ItemProto] {
	return &protoListCodec[ItemProto]{
		marshalList:   marshalList,
		unmarshalList: unmarshalList,
	}
}

func (c *protoListCodec[ItemProto]) NewWriter(setResultBytes func([]byte)) (cache.SplitWriter[ItemProto], error) {
	return &protoListWriter[ItemProto]{
		setResultBytes: setResultBytes,
		marshalList:    c.marshalList,
	}, nil
}

func (c *protoListCodec[ItemProto]) NewReader(bytes []byte) (cache.SplitReader[ItemProto], error) {
	results, err := c.unmarshalList(bytes)
	if err != nil {
		return nil, err
	}
	return &protoListReader[ItemProto]{results: results}, nil
}

type protoListWriter[ItemProto any] struct {
	setResultBytes func([]byte)
	marshalList    func([]ItemProto) ([]byte, error)
	results        []ItemProto
}

func (w *protoListWriter[ItemProto]) WriteNextResult(result ItemProto) error {
	w.results = append(w.results, result)
	return nil
}

func (w *protoListWriter[ItemProto]) Finalize() error {
	listBytes, err := w.marshalList(w.results)
	if err != nil {
		return err
	}
	w.setResultBytes(listBytes)
	return nil
}

type protoListReader[ItemProto any] struct {
	results []ItemProto
}

func (r *protoListReader[ItemProto]) ReadResultAt(idx int) (ItemProto, error) {
	if idx >= len(r.results) {
		var zero ItemProto
		return zero, io.EOF
	}
	return r.results[idx], nil
}
