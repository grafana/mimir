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

type SumOverTimeIntermediate struct {
	SumF float64
	// FIXME: use SumC properly
	SumC     float64 // Kahan compensation
	SumH     *histogram.FloatHistogram
	HasFloat bool
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
	return SumOverTimeIntermediate{SumF: f, HasFloat: hasFloat, SumH: h}, nil
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
			if sumH == nil {
				sumH = p.SumH.Copy()
			} else {
				if _, _, _, err := sumH.Add(p.SumH); err != nil {
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

// sumOverTimeWriter currently buffers all the results and only writes to the underlying writer when Finalize() is called.
type sumOverTimeWriter struct {
	setResultBytes func([]byte)
	results        []*SumOverTimeIntermediateProto
}

func (w *sumOverTimeWriter) WriteNextResult(result SumOverTimeIntermediate) error {
	proto := &SumOverTimeIntermediateProto{
		SumF:     result.SumF,
		HasFloat: result.HasFloat,
		SumC:     result.SumC,
	}

	if result.SumH != nil {
		histProto := mimirpb.FromFloatHistogramToHistogramProto(0, result.SumH)
		proto.SumH = &histProto
	}

	w.results = append(w.results, proto)
	return nil
}

func (w *sumOverTimeWriter) Finalize() error {
	// TODO: this might not be the final serialization format, we might want something more streamable rather than a
	//  single list proto
	listProto := &SumOverTimeIntermediateListProto{
		Results: w.results,
	}

	listBytes, err := listProto.Marshal()
	if err != nil {
		return errors.Wrap(err, "marshaling list proto")
	}

	w.setResultBytes(listBytes)
	return nil
}

type sumOverTimeReader struct {
	results []SumOverTimeIntermediate
}

func (r *sumOverTimeReader) ReadResultAt(idx int) (SumOverTimeIntermediate, error) {
	if idx >= len(r.results) {
		return SumOverTimeIntermediate{}, io.EOF
	}
	return r.results[idx], nil
}

type sumOverTimeCodec struct{}

func (s *sumOverTimeCodec) NewWriter(setResultBytes func([]byte)) (cache.SplitWriter[SumOverTimeIntermediate], error) {
	return &sumOverTimeWriter{setResultBytes: setResultBytes}, nil
}

func (s *sumOverTimeCodec) NewReader(bytes []byte) (cache.SplitReader[SumOverTimeIntermediate], error) {
	var listProto SumOverTimeIntermediateListProto
	if err := listProto.Unmarshal(bytes); err != nil {
		return nil, errors.Wrap(err, "unmarshaling sum over time list proto")
	}

	results := make([]SumOverTimeIntermediate, len(listProto.Results))
	for i, proto := range listProto.Results {
		results[i] = SumOverTimeIntermediate{
			SumF:     proto.SumF,
			HasFloat: proto.HasFloat,
			SumC:     proto.SumC,
		}
		if proto.SumH != nil {
			results[i].SumH = mimirpb.FromHistogramProtoToFloatHistogram(proto.SumH)
		}
	}

	return &sumOverTimeReader{results: results}, nil
}

var SumOverTimeCodec = &sumOverTimeCodec{}

type SingleSampleIntermediate struct {
	F        float64
	H        *histogram.FloatHistogram
	HasFloat bool
}

// singleSampleWriter currently buffers all the results and only writes to the underlying writer when Finalize() is called.
type singleSampleWriter struct {
	setResultBytes func([]byte)
	results        []*SingleSampleIntermediateProto
}

func (w *singleSampleWriter) WriteNextResult(result SingleSampleIntermediate) error {
	proto := &SingleSampleIntermediateProto{
		F:        result.F,
		HasFloat: result.HasFloat,
	}

	if result.H != nil {
		histProto := mimirpb.FromFloatHistogramToHistogramProto(0, result.H)
		proto.H = &histProto
	}

	w.results = append(w.results, proto)
	return nil
}

func (w *singleSampleWriter) Finalize() error {
	// TODO: this might not be the final serialization format, we might want something more streamable rather than a
	//  single list proto
	listProto := &SingleSampleIntermediateListProto{
		Results: w.results,
	}

	listBytes, err := listProto.Marshal()
	if err != nil {
		return errors.Wrap(err, "marshaling list proto")
	}

	w.setResultBytes(listBytes)

	return nil
}

type singleSampleReader struct {
	results []SingleSampleIntermediate
}

func (r *singleSampleReader) ReadResultAt(idx int) (SingleSampleIntermediate, error) {
	if idx >= len(r.results) {
		return SingleSampleIntermediate{}, io.EOF
	}
	return r.results[idx], nil
}

type singleSampleCodec struct{}

func (s *singleSampleCodec) NewWriter(setResultBytes func([]byte)) (cache.SplitWriter[SingleSampleIntermediate], error) {
	return &singleSampleWriter{setResultBytes: setResultBytes}, nil
}

func (s *singleSampleCodec) NewReader(bytes []byte) (cache.SplitReader[SingleSampleIntermediate], error) {
	var listProto SingleSampleIntermediateListProto
	if err := listProto.Unmarshal(bytes); err != nil {
		return nil, errors.Wrap(err, "unmarshaling list proto")
	}

	results := make([]SingleSampleIntermediate, len(listProto.Results))
	for i, proto := range listProto.Results {
		results[i] = SingleSampleIntermediate{
			F:        proto.F,
			HasFloat: proto.HasFloat,
		}

		if proto.H != nil {
			results[i].H = mimirpb.FromHistogramProtoToFloatHistogram(proto.H)
		}
	}

	return &singleSampleReader{results: results}, nil
}

var SingleSampleCodec = &singleSampleCodec{}

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
	return SingleSampleIntermediate{F: f, HasFloat: hasFloat, H: h}, nil
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
	return SingleSampleIntermediate{F: f, HasFloat: hasFloat, H: h}, nil
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
