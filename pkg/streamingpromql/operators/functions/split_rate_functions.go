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

var SplitRate = &RangeVectorSplittingMetadata{
	OperatorFactory:       NewSplitOperatorFactory[RateIntermediate](rateGenerate, rateCombine(true), RateCodec, Rate, FUNCTION_RATE),
	RangeVectorChildIndex: 0,
}

var SplitIncrease = &RangeVectorSplittingMetadata{
	OperatorFactory:       NewSplitOperatorFactory[RateIntermediate](rateGenerate, rateCombine(false), RateCodec, Increase, FUNCTION_INCREASE),
	RangeVectorChildIndex: 0,
}

func rateGenerate(step *types.RangeVectorStepData, _ []types.ScalarData, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (RateIntermediate, error) {
	fHead, fTail := step.Floats.UnsafePoints()
	fCount := len(fHead) + len(fTail)

	hHead, hTail := step.Histograms.UnsafePoints()
	hCount := len(hHead) + len(hTail)

	if fCount > 0 && hCount > 0 {
		emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
		return RateIntermediate{
			SampleCount:     0,
			SplitRangeStart: step.RangeStart,
			SplitRangeEnd:   step.RangeEnd,
		}, nil
	}

	if fCount > 0 {
		return rateGenerateFloat(fHead, fTail, fCount, step.RangeStart, step.RangeEnd)
	}

	if hCount > 0 {
		return rateGenerateHistogram(hHead, hTail, hCount, step.RangeStart, step.RangeEnd, emitAnnotation)
	}

	return RateIntermediate{
		SampleCount:     0,
		SplitRangeStart: step.RangeStart,
		SplitRangeEnd:   step.RangeEnd,
	}, nil
}

func rateGenerateFloat(fHead, fTail []promql.FPoint, fCount int, rangeStart, rangeEnd int64) (RateIntermediate, error) {
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
			Delta:           0,
			SampleCount:     1,
			IsHistogram:     false,
			SplitRangeStart: rangeStart,
			SplitRangeEnd:   rangeEnd,
		}, nil
	}

	firstPoint, lastPoint, delta := calculateFloatDelta(fHead, fTail, false, false, promql.FPoint{}, false, promql.FPoint{})

	return RateIntermediate{
		FirstSample: &mimirpb.Sample{
			TimestampMs: firstPoint.T,
			Value:       firstPoint.F,
		},
		LastSample: &mimirpb.Sample{
			TimestampMs: lastPoint.T,
			Value:       lastPoint.F,
		},
		Delta:           delta,
		SampleCount:     int64(fCount),
		IsHistogram:     false,
		SplitRangeStart: rangeStart,
		SplitRangeEnd:   rangeEnd,
	}, nil
}

func rateGenerateHistogram(hHead, hTail []promql.HPoint, hCount int, rangeStart, rangeEnd int64, emitAnnotation types.EmitAnnotationFunc) (RateIntermediate, error) {
	firstPoint := hHead[0]

	if firstPoint.H.CounterResetHint == histogram.GaugeType {
		emitAnnotation(annotations.NewNativeHistogramNotCounterWarning)
	}

	if hCount == 1 {
		firstHistProto := mimirpb.FromFloatHistogramToHistogramProto(0, firstPoint.H)
		return RateIntermediate{
			FirstHistogram:                 &firstHistProto,
			LastHistogram:                  &firstHistProto,
			FirstHistogramTimestamp:        firstPoint.T,
			LastHistogramTimestamp:         firstPoint.T,
			FirstHistogramCountBeforeReset: firstPoint.H.Count,
			SampleCount:                    1,
			IsHistogram:                    true,
			SplitRangeStart:                rangeStart,
			SplitRangeEnd:                  rangeEnd,
		}, nil
	}

	firstPoint, lastPoint, delta, fpHistCount, err := calculateHistogramDelta(hHead, hTail, emitAnnotation)
	if err != nil {
		return RateIntermediate{}, err
	}

	firstHistProto := mimirpb.FromFloatHistogramToHistogramProto(0, firstPoint.H)
	lastHistProto := mimirpb.FromFloatHistogramToHistogramProto(0, lastPoint.H)
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
		SplitRangeStart:                rangeStart,
		SplitRangeEnd:                  rangeEnd,
	}, nil
}

func rateCombine(isRate bool) SplitCombineFunc[RateIntermediate] {
	return func(
		pieces []RateIntermediate,
		emitAnnotation types.EmitAnnotationFunc,
		_ *limiter.MemoryConsumptionTracker,
	) (float64, bool, *histogram.FloatHistogram, error) {
		if len(pieces) == 0 {
			return 0, false, nil, nil
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
			h, err := rateCombineHistogram(pieces, isRate, emitAnnotation)
			return 0, false, h, err
		}

		if hasFloat {
			f, hasFloat, err := rateCombineFloat(pieces, isRate)
			return f, hasFloat, nil, err
		}

		return 0, false, nil, nil
	}
}

func rateCombineFloat(splits []RateIntermediate, isRate bool) (float64, bool, error) {
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

	rangeStart := splits[0].SplitRangeStart
	rangeEnd := splits[len(splits)-1].SplitRangeEnd
	rangeSeconds := float64(rangeEnd-rangeStart) / 1000
	result := calculateFloatRate(
		true, // isCounter
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

func rateCombineHistogram(splits []RateIntermediate, isRate bool, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, error) {
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

	firstPoint := promql.HPoint{
		T: firstPiece.FirstHistogramTimestamp,
		H: mimirpb.FromHistogramProtoToFloatHistogram(firstPiece.FirstHistogram),
	}

	lastPoint := promql.HPoint{
		T: firstPiece.LastHistogramTimestamp,
		H: mimirpb.FromHistogramProtoToFloatHistogram(firstPiece.LastHistogram),
	}

	fpHistCountBeforeReset := firstPiece.FirstHistogramCountBeforeReset
	totalDelta := mimirpb.FromHistogramProtoToFloatHistogram(firstPiece.DeltaHistogram)
	totalCount := int(firstPiece.SampleCount)

	for i := startIdx + 1; i < len(splits); i++ {
		split := splits[i]
		if split.SampleCount == 0 || !split.IsHistogram {
			continue
		}

		newFirstHist := mimirpb.FromHistogramProtoToFloatHistogram(split.FirstHistogram)

		if newFirstHist.DetectReset(lastPoint.H) {
			_, _, nhcbBoundsReconciled, err := totalDelta.Add(newFirstHist)
			if err != nil {
				return nil, err
			}
			if nhcbBoundsReconciled {
				emitAnnotation(newAddMismatchedCustomBucketsHistogramInfo)
			}
		} else {
			_, _, nhcbBoundsReconciled, err := newFirstHist.Sub(lastPoint.H)
			if err != nil {
				return nil, err
			}
			if nhcbBoundsReconciled {
				emitAnnotation(newSubMismatchedCustomBucketsHistogramInfo)
			}

			_, _, nhcbBoundsReconciled, err = totalDelta.Add(newFirstHist)
			if err != nil {
				return nil, err
			}
			if nhcbBoundsReconciled {
				emitAnnotation(newAddMismatchedCustomBucketsHistogramInfo)
			}
		}

		deltaHist := mimirpb.FromHistogramProtoToFloatHistogram(split.DeltaHistogram)
		_, _, nhcbBoundsReconciled, err := totalDelta.Add(deltaHist)
		if err != nil {
			return nil, err
		}
		if nhcbBoundsReconciled {
			emitAnnotation(newAddMismatchedCustomBucketsHistogramInfo)
		}
		totalCount += int(split.SampleCount)

		lastPoint = promql.HPoint{
			T: split.LastHistogramTimestamp,
			H: mimirpb.FromHistogramProtoToFloatHistogram(split.LastHistogram),
		}
	}

	if totalCount < 2 {
		return nil, nil
	}

	rangeStart := splits[0].SplitRangeStart
	rangeEnd := splits[len(splits)-1].SplitRangeEnd
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
