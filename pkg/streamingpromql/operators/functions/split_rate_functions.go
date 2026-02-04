// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

var SplitRate = NewSplitOperatorFactory[RateIntermediate](rateGenerate, rateCombine(true), RateCodec, Rate, FUNCTION_RATE)

var SplitIncrease = NewSplitOperatorFactory[RateIntermediate](rateGenerate, rateCombine(false), RateCodec, Increase, FUNCTION_INCREASE)

func rateGenerate(step *types.RangeVectorStepData, _ []types.ScalarData, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (RateIntermediate, error) {
	fHead, fTail := step.Floats.UnsafePoints()
	fCount := len(fHead) + len(fTail)

	hHead, hTail := step.Histograms.UnsafePoints()
	hCount := len(hHead) + len(hTail)

	if fCount > 0 && hCount > 0 {
		emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
		return RateIntermediate{
			SampleCount:      0,
			ForceEmptyResult: true,
		}, nil
	}

	if fCount > 0 {
		return rateGenerateFloat(fHead, fTail, fCount)
	}

	if hCount > 0 {
		return rateGenerateHistogram(hHead, hTail, hCount, emitAnnotation)
	}

	return RateIntermediate{
		SampleCount: 0,
	}, nil
}

func rateGenerateFloat(fHead, fTail []promql.FPoint, fCount int) (RateIntermediate, error) {
	firstPoint := fHead[0]

	if fCount == 1 {
		return RateIntermediate{
			FirstSample: &mimirpb.Sample{
				TimestampMs: firstPoint.T,
				Value:       firstPoint.F,
			},
			LastSample: &mimirpb.Sample{
				TimestampMs: firstPoint.T,
				Value:       firstPoint.F,
			},
			Delta:       0,
			SampleCount: 1,
			IsHistogram: false,
		}, nil
	}

	firstPoint, lastPoint, delta := calculateFloatDelta(fHead, fTail)

	return RateIntermediate{
		FirstSample: &mimirpb.Sample{
			TimestampMs: firstPoint.T,
			Value:       firstPoint.F,
		},
		LastSample: &mimirpb.Sample{
			TimestampMs: lastPoint.T,
			Value:       lastPoint.F,
		},
		Delta:       delta,
		SampleCount: int64(fCount),
		IsHistogram: false,
	}, nil
}

func rateGenerateHistogram(hHead, hTail []promql.HPoint, hCount int, emitAnnotation types.EmitAnnotationFunc) (RateIntermediate, error) {
	firstPoint := hHead[0]

	if firstPoint.H.CounterResetHint == histogram.GaugeType {
		emitAnnotation(annotations.NewNativeHistogramNotCounterWarning)
	}

	if hCount == 1 {
		// Copy to avoid sharing memory with ring buffer that may be reused across series
		firstHistProto := mimirpb.FromFloatHistogramToHistogramProto(0, firstPoint.H.Copy())
		return RateIntermediate{
			FirstHistogram:                 &firstHistProto,
			LastHistogram:                  &firstHistProto,
			FirstHistogramTimestamp:        firstPoint.T,
			LastHistogramTimestamp:         firstPoint.T,
			FirstHistogramCountBeforeReset: firstPoint.H.Count,
			SampleCount:                    1,
			IsHistogram:                    true,
		}, nil
	}

	firstPoint, lastPoint, delta, fpHistCount, err := calculateHistogramDelta(hHead, hTail, emitAnnotation)
	if err != nil {
		// Convert histogram errors to annotations and force empty result
		err = NativeHistogramErrorToAnnotation(err, emitAnnotation)
		if err == nil {
			// Error was converted to annotation (e.g., mixed exponential/custom buckets)
			return RateIntermediate{
				SampleCount:      0,
				ForceEmptyResult: true,
			}, nil
		}
		// Other errors still need to be returned
		return RateIntermediate{}, err
	}

	// Copy to avoid sharing memory with ring buffer that may be reused across series
	firstHistProto := mimirpb.FromFloatHistogramToHistogramProto(0, firstPoint.H.Copy())
	lastHistProto := mimirpb.FromFloatHistogramToHistogramProto(0, lastPoint.H.Copy())
	deltaHistProto := mimirpb.FromFloatHistogramToHistogramProto(0, delta)

	return RateIntermediate{
		FirstHistogram:                 &firstHistProto,
		LastHistogram:                  &lastHistProto,
		FirstHistogramTimestamp:        firstPoint.T,
		LastHistogramTimestamp:         lastPoint.T,
		DeltaHistogram:                 &deltaHistProto,
		FirstHistogramCountBeforeReset: fpHistCount,
		SampleCount:                    int64(hCount),
		IsHistogram:                    true,
	}, nil
}

func rateCombine(isRate bool) SplitCombineFunc[RateIntermediate] {
	return func(
		pieces []RateIntermediate,
		_ []types.ScalarData,
		rangeStart int64,
		rangeEnd int64,
		emitAnnotation types.EmitAnnotationFunc,
		_ *limiter.MemoryConsumptionTracker,
	) (float64, bool, *histogram.FloatHistogram, error) {
		if len(pieces) == 0 {
			return 0, false, nil, nil
		}

		// Check if any split encountered an error condition that should force an empty result
		for _, p := range pieces {
			if p.ForceEmptyResult {
				return 0, false, nil, nil
			}
		}

		hasFloat := false
		hasHistogram := false
		for _, p := range pieces {
			if p.SampleCount > 0 {
				if p.IsHistogram {
					hasHistogram = true
				} else {
					hasFloat = true
				}
			}
		}

		if hasFloat && hasHistogram {
			emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
			return 0, false, nil, nil
		}

		if hasHistogram {
			h, err := rateCombineHistogram(pieces, rangeStart, rangeEnd, isRate, emitAnnotation)
			if err != nil {
				// Convert histogram errors to annotations
				err = NativeHistogramErrorToAnnotation(err, emitAnnotation)
				if err == nil {
					// Error was converted to annotation, return empty result
					return 0, false, nil, nil
				}
			}
			return 0, false, h, err
		}

		if hasFloat {
			f, hasFloat, err := rateCombineFloat(pieces, rangeStart, rangeEnd, isRate)
			return f, hasFloat, nil, err
		}

		return 0, false, nil, nil
	}
}

func rateCombineFloat(splits []RateIntermediate, rangeStart int64, rangeEnd int64, isRate bool) (float64, bool, error) {
	startIdx := -1

	for i, split := range splits {
		if split.SampleCount > 0 {
			startIdx = i
			break
		}
	}

	if startIdx == -1 {
		return 0, false, nil // No data
	}

	firstPiece := splits[startIdx]
	firstPoint := promql.FPoint{
		T: firstPiece.FirstSample.TimestampMs,
		F: firstPiece.FirstSample.Value,
	}
	lastPoint := promql.FPoint{
		T: firstPiece.LastSample.TimestampMs,
		F: firstPiece.LastSample.Value,
	}

	totalDelta := firstPiece.Delta
	totalCount := int(firstPiece.SampleCount)

	lastSplitIdx := startIdx
	for i := startIdx + 1; i < len(splits); i++ {
		split := splits[i]
		if split.SampleCount == 0 {
			continue
		}

		if split.FirstSample.Value < splits[lastSplitIdx].LastSample.Value {
			totalDelta += split.FirstSample.Value
		} else {
			interSplitDelta := split.FirstSample.Value - splits[lastSplitIdx].LastSample.Value
			totalDelta += interSplitDelta
		}

		totalDelta += split.Delta
		totalCount += int(split.SampleCount)

		lastPoint = promql.FPoint{
			T: split.LastSample.TimestampMs,
			F: split.LastSample.Value,
		}
		lastSplitIdx = i
	}

	// Need at least 2 samples total to calculate rate
	if totalCount < 2 {
		return 0, false, nil
	}

	rangeSeconds := float64(rangeEnd-rangeStart) / 1000

	result := calculateFloatRate(
		true,
		isRate,
		rangeStart,
		rangeEnd,
		rangeSeconds,
		firstPoint,
		lastPoint,
		totalDelta,
		totalCount,
		false,
	)

	return result, true, nil
}

func rateCombineHistogram(splits []RateIntermediate, rangeStart int64, rangeEnd int64, isRate bool, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, error) {
	startIdx := -1

	for i, split := range splits {
		if split.SampleCount > 0 && split.IsHistogram {
			startIdx = i
			break
		}
	}

	if startIdx == -1 {
		return nil, nil
	}

	firstPiece := splits[startIdx]

	fpHistCountBeforeReset := firstPiece.FirstHistogramCountBeforeReset
	totalCount := int(firstPiece.SampleCount)

	var totalDelta *histogram.FloatHistogram
	firstDelta := mimirpb.FromFloatHistogramProtoToFloatHistogram(firstPiece.DeltaHistogram)
	if firstDelta != nil {
		totalDelta = firstDelta.Copy()
	}

	firstPoint := promql.HPoint{
		T: firstPiece.FirstHistogramTimestamp,
		H: mimirpb.FromFloatHistogramProtoToFloatHistogram(firstPiece.FirstHistogram),
	}

	lastPoint := promql.HPoint{
		T: firstPiece.LastHistogramTimestamp,
		H: mimirpb.FromFloatHistogramProtoToFloatHistogram(firstPiece.LastHistogram),
	}

	for i := startIdx + 1; i < len(splits); i++ {
		split := splits[i]
		if split.SampleCount == 0 || !split.IsHistogram {
			continue
		}

		// Calculate and add delta between previous and current split
		newFirstHist := mimirpb.FromFloatHistogramProtoToFloatHistogram(split.FirstHistogram)
		if newFirstHist.DetectReset(lastPoint.H) {
			if totalDelta == nil {
				// First histogram we're seeing, copy it to create the accumulator
				totalDelta = newFirstHist.Copy()
			} else {
				_, _, nhcbBoundsReconciled, err := totalDelta.Add(newFirstHist)
				if err != nil {
					return nil, err
				}
				if nhcbBoundsReconciled {
					emitAnnotation(newAddMismatchedCustomBucketsHistogramInfo)
				}
			}
		} else {
			interSplitDelta := newFirstHist.Copy()
			_, _, nhcbBoundsReconciled, err := interSplitDelta.Sub(lastPoint.H)
			if err != nil {
				return nil, err
			}
			if nhcbBoundsReconciled {
				emitAnnotation(newSubMismatchedCustomBucketsHistogramInfo)
			}

			if totalDelta == nil {
				totalDelta = interSplitDelta
			} else {
				_, _, nhcbBoundsReconciled, err = totalDelta.Add(interSplitDelta)
				if err != nil {
					return nil, err
				}
				if nhcbBoundsReconciled {
					emitAnnotation(newAddMismatchedCustomBucketsHistogramInfo)
				}
			}
		}

		// Calculate and add delta within current split
		intraSplitDelta := mimirpb.FromFloatHistogramProtoToFloatHistogram(split.DeltaHistogram)
		if intraSplitDelta != nil {
			if totalDelta == nil {
				totalDelta = intraSplitDelta.Copy()
			} else {
				_, _, nhcbBoundsReconciled, err := totalDelta.Add(intraSplitDelta)
				if err != nil {
					return nil, err
				}
				if nhcbBoundsReconciled {
					emitAnnotation(newAddMismatchedCustomBucketsHistogramInfo)
				}
			}
		}
		totalCount += int(split.SampleCount)

		lastPoint = promql.HPoint{
			T: split.LastHistogramTimestamp,
			H: mimirpb.FromFloatHistogramProtoToFloatHistogram(split.LastHistogram),
		}
	}

	if totalCount < 2 {
		return nil, nil
	}

	rangeSeconds := float64(rangeEnd-rangeStart) / 1000
	result := calculateHistogramRate(
		true,
		isRate,
		rangeStart,
		rangeEnd,
		rangeSeconds,
		firstPoint,
		lastPoint,
		totalDelta,
		totalCount,
		fpHistCountBeforeReset,
	)

	return result, nil
}

type rateCodec struct{}

func (c rateCodec) Marshal(results []RateIntermediate) ([]byte, error) {
	listProto := &RateIntermediateList{Results: results}
	listBytes, err := listProto.Marshal()
	if err != nil {
		return nil, errors.Wrap(err, "marshaling rate list")
	}
	return listBytes, nil
}

func (c rateCodec) Unmarshal(bytes []byte) ([]RateIntermediate, error) {
	var listProto RateIntermediateList
	if err := listProto.Unmarshal(bytes); err != nil {
		return nil, errors.Wrap(err, "unmarshaling rate list")
	}
	return listProto.Results, nil
}

var RateCodec = rateCodec{}
