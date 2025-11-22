// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"io"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streamingpromql/floats"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// sum_over_time query splitting

// SumOverTimeIntermediate stores intermediate results for sum_over_time query splitting.
type SumOverTimeIntermediate struct {
	SumF     float64
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

// sumOverTimeWriter implements SplittableWriter[SumOverTimeIntermediate].
// It buffers all results as protobufs and writes them using protobuf list encoding.
type sumOverTimeWriter struct {
	writer  io.Writer
	results []*SumOverTimeIntermediateProto
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

// Flush writes all buffered results to the underlying writer using protobuf list encoding.
func (w *sumOverTimeWriter) Flush() error {
	listProto := &SumOverTimeIntermediateListProto{
		Results: w.results,
	}

	// Marshal list to bytes
	listBytes, err := listProto.Marshal()
	if err != nil {
		return errors.Wrap(err, "marshaling list proto")
	}

	// Write bytes
	if _, err := w.writer.Write(listBytes); err != nil {
		return errors.Wrap(err, "writing list proto bytes")
	}

	return nil
}

// sumOverTimeWriterFactory creates a new sumOverTimeWriter.
func sumOverTimeWriterFactory(writer io.Writer) (SplittableWriter[SumOverTimeIntermediate], error) {
	return &sumOverTimeWriter{writer: writer}, nil
}

// sumOverTimeReader implements SplittableReader[SumOverTimeIntermediate].
// It reads all results from the underlying reader into memory, then serves them one at a time.
type sumOverTimeReader struct {
	results []SumOverTimeIntermediate
	idx     int
}

func (r *sumOverTimeReader) ReadNextResult() (SumOverTimeIntermediate, error) {
	if r.idx >= len(r.results) {
		return SumOverTimeIntermediate{}, io.EOF
	}
	result := r.results[r.idx]
	r.idx++
	return result, nil
}

// sumOverTimeReaderFactory creates a new sumOverTimeReader by reading all results from the reader.
func sumOverTimeReaderFactory(reader io.Reader) (SplittableReader[SumOverTimeIntermediate], error) {
	// Read all bytes from reader
	allBytes, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrap(err, "reading all bytes")
	}

	// Unmarshal list proto
	var listProto SumOverTimeIntermediateListProto
	if err := listProto.Unmarshal(allBytes); err != nil {
		return nil, errors.Wrap(err, "unmarshaling list proto")
	}

	// Convert all proto results to intermediate results
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

// count_over_time query splitting

// CountOverTimeIntermediate stores intermediate results for count_over_time query splitting.
// We reuse the sum structure since count is just a sum of 1s.
type CountOverTimeIntermediate struct {
	Count    float64
	HasValue bool
}

func countOverTimeGenerate(
	step *types.RangeVectorStepData,
	_ []types.ScalarData,
	_ types.EmitAnnotationFunc,
	_ *limiter.MemoryConsumptionTracker,
) (CountOverTimeIntermediate, error) {
	count, hasValue, _, err := countOverTime(step, nil, types.QueryTimeRange{}, nil, nil)
	if err != nil {
		return CountOverTimeIntermediate{}, err
	}
	return CountOverTimeIntermediate{Count: count, HasValue: hasValue}, nil
}

func countOverTimeCombine(
	pieces []CountOverTimeIntermediate,
	_ types.EmitAnnotationFunc,
	_ *limiter.MemoryConsumptionTracker,
) (float64, bool, *histogram.FloatHistogram, error) {
	totalCount := 0.0
	hasValue := false

	for _, p := range pieces {
		if p.HasValue {
			hasValue = true
			totalCount += p.Count
		}
	}

	return totalCount, hasValue, nil, nil
}

// countOverTimeWriter implements SplittableWriter[CountOverTimeIntermediate].
// It buffers all results as protobufs and writes them using protobuf list encoding.
type countOverTimeWriter struct {
	writer  io.Writer
	results []*CountOverTimeIntermediateProto
}

func (w *countOverTimeWriter) WriteNextResult(result CountOverTimeIntermediate) error {
	w.results = append(w.results, &CountOverTimeIntermediateProto{
		Count:    result.Count,
		HasValue: result.HasValue,
	})
	return nil
}

// Flush writes all buffered results to the underlying writer using protobuf list encoding.
func (w *countOverTimeWriter) Flush() error {
	listProto := &CountOverTimeIntermediateListProto{
		Results: w.results,
	}

	// Marshal list to bytes
	listBytes, err := listProto.Marshal()
	if err != nil {
		return errors.Wrap(err, "marshaling list proto")
	}

	// Write bytes
	if _, err := w.writer.Write(listBytes); err != nil {
		return errors.Wrap(err, "writing list proto bytes")
	}

	return nil
}

// countOverTimeWriterFactory creates a new countOverTimeWriter.
func countOverTimeWriterFactory(writer io.Writer) (SplittableWriter[CountOverTimeIntermediate], error) {
	return &countOverTimeWriter{writer: writer}, nil
}

// countOverTimeReader implements SplittableReader[CountOverTimeIntermediate].
// It reads all results from the underlying reader into memory, then serves them one at a time.
type countOverTimeReader struct {
	results []CountOverTimeIntermediate
	idx     int
}

func (r *countOverTimeReader) ReadNextResult() (CountOverTimeIntermediate, error) {
	if r.idx >= len(r.results) {
		return CountOverTimeIntermediate{}, io.EOF
	}
	result := r.results[r.idx]
	r.idx++
	return result, nil
}

// countOverTimeReaderFactory creates a new countOverTimeReader by reading all results from the reader.
func countOverTimeReaderFactory(reader io.Reader) (SplittableReader[CountOverTimeIntermediate], error) {
	// Read all bytes from reader
	allBytes, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrap(err, "reading all bytes")
	}

	// Unmarshal list proto
	var listProto CountOverTimeIntermediateListProto
	if err := listProto.Unmarshal(allBytes); err != nil {
		return nil, errors.Wrap(err, "unmarshaling list proto")
	}

	// Convert all proto results to intermediate results
	results := make([]CountOverTimeIntermediate, len(listProto.Results))
	for i, proto := range listProto.Results {
		results[i] = CountOverTimeIntermediate{
			Count:    proto.Count,
			HasValue: proto.HasValue,
		}
	}

	return &countOverTimeReader{results: results}, nil
}

func init() {
	// Update SumOverTime definition to use split operator factory
	SumOverTime.SplitOperatorFactory = NewSplitOperatorDefinition(
		sumOverTimeGenerate,
		sumOverTimeCombine,
		sumOverTimeWriterFactory,
		sumOverTimeReaderFactory,
	)

	// Update CountOverTime definition to use split operator factory
	CountOverTime.SplitOperatorFactory = NewSplitOperatorDefinition(
		countOverTimeGenerate,
		countOverTimeCombine,
		countOverTimeWriterFactory,
		countOverTimeReaderFactory,
	)
}
