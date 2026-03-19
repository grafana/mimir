// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting

import (
	"math"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streamingpromql/floats"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// SplitAvgOverTime configures the range vector splitting version of avg_over_time().
// Like [SplitSumOverTime], this implementation may occasionally omit warning annotations when processing histograms
// with a mix of CounterReset and NotCounterReset hints across split boundaries.
// The returned result remains numerically correct, only the optional warning annotation may be missing.
// See [SplitSumOverTime] for the detailed explanation of this limitation.
var SplitAvgOverTime = NewSplitOperatorFactory[AvgOverTimeIntermediate](
	avgOverTimeGenerate,
	avgOverTimeCombine,
	AvgOverTimeCodec,
	functions.AvgOverTime,
	functions.FUNCTION_AVG_OVER_TIME,
)

func avgOverTimeGenerate(step *types.RangeVectorStepData, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (AvgOverTimeIntermediate, error) {
	fHead, fTail := step.Floats.UnsafePoints()
	hHead, hTail := step.Histograms.UnsafePoints()

	haveFloats := len(fHead) > 0 || len(fTail) > 0
	haveHistograms := len(hHead) > 0 || len(hTail) > 0

	if !haveFloats && !haveHistograms {
		return AvgOverTimeIntermediate{}, nil
	}

	result := AvgOverTimeIntermediate{}

	if haveFloats {
		// To minimize floating-point precision loss, track the sum, count, and compensation.
		// If a standard sum-based average overflows, switches to incremental calculation.
		sum, compensation, incrementalAvg, count, useIncrementalAvg := avgFloats(fHead, fTail)

		result.SumF = sum
		result.CompF = compensation
		result.IncrementalAvg = incrementalAvg
		result.CountF = count
		result.UseIncrementalCalc = useIncrementalAvg
	}

	if haveHistograms {
		h, err := functions.AvgHistograms(hHead, hTail, emitAnnotation)
		if err != nil {
			err = functions.NativeHistogramErrorToAnnotation(err, emitAnnotation)
			// In case of schema incompatibility, the error was converted to annotation and empty result should be returned
			// Save the flag to preserve the behavior on combine stage
			if err == nil {
				result.ForceEmptyResult = true
			} else {
				return AvgOverTimeIntermediate{}, err
			}
		}

		if h != nil {
			protoH := mimirpb.FromFloatHistogramToHistogramProto(0, h)
			result.AvgH = &protoH
			result.CountH = int64(len(hHead)) + int64(len(hTail))
		}
	}

	return result, nil
}

func avgFloats(head, tail []promql.FPoint) (float64, float64, float64, float64, bool) {
	sum, c, count := 0.0, 0.0, 0.0
	incrementalAvg := 0.0 // Only used for incremental calculation method.
	useIncrementalCalculation := false

	accumulate := func(points []promql.FPoint) {
		for _, p := range points {
			count++

			if !useIncrementalCalculation {
				newSum, newC := floats.KahanSumInc(p.F, sum, c)

				if count == 1 || !math.IsInf(newSum, 0) {
					// Continue using simple average calculation provided we haven't overflowed,
					// and also for first point to avoid dividing by zero below.
					sum, c = newSum, newC
					continue
				}

				// We've just hit overflow, switch to incremental calculation.
				useIncrementalCalculation = true
				incrementalAvg = sum / (count - 1)
				c = c / (count - 1)
			}

			if shouldUpdateIncrementalAvg(incrementalAvg, p.F) {
				incrementalAvg, c = floats.KahanSumInc(p.F/count-(incrementalAvg+c)/count, incrementalAvg, c)
			}
		}
	}

	accumulate(head)
	accumulate(tail)

	return sum, c, incrementalAvg, count, useIncrementalCalculation
}

func avgOverTimeCombine(pieces []AvgOverTimeIntermediate, _ int64, _ int64, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	sumF, compensationF, countF, incrementalAvgF := 0.0, 0.0, 0.0, 0.0
	useIncrementalCalculation := false

	var incrementalAvgH, compensationH *histogram.FloatHistogram
	countH := 0.0
	nhcbBoundsReconciledSeen := false

	for _, p := range pieces {
		// Check if any piece encountered schema mix condition, return empty result
		if p.ForceEmptyResult {
			return 0, false, nil, nil
		}

		// Histograms are always combined incrementally using the weighted-average algorithm
		if p.AvgH != nil {
			h := mimirpb.FromFloatHistogramProtoToFloatHistogram(p.AvgH)

			if incrementalAvgH == nil {
				incrementalAvgH = h.Copy()
				countH = float64(p.CountH)
			} else {
				pieceCnt := float64(p.CountH)
				totalCnt := countH + pieceCnt

				q := pieceCnt / totalCnt
				pieceAvgPart := h.Copy().Mul(q)
				prevAvgPart := incrementalAvgH.Copy().Mul(-q)
				// Mul(-q) sets CounterResetHint to GaugeType due to negative factor,
				// which would override incrementalAvgH's hint via adjustCounterReset inside KahanAdd.
				// Restore the original hint.
				prevAvgPart.CounterResetHint = incrementalAvgH.CounterResetHint

				if compensationH != nil {
					compensationH.Mul(countH / totalCnt)
				}

				var err error
				var nhcbBoundsReconciled bool
				if compensationH, _, nhcbBoundsReconciled, err = incrementalAvgH.KahanAdd(pieceAvgPart, compensationH); err != nil {
					err = functions.NativeHistogramErrorToAnnotation(err, emitAnnotation)
					return 0, false, nil, err
				} else if nhcbBoundsReconciled {
					nhcbBoundsReconciledSeen = true
				}

				if compensationH, _, nhcbBoundsReconciled, err = incrementalAvgH.KahanAdd(prevAvgPart, compensationH); err != nil {
					err = functions.NativeHistogramErrorToAnnotation(err, emitAnnotation)
					return 0, false, nil, err
				} else if nhcbBoundsReconciled {
					nhcbBoundsReconciledSeen = true
				}

				countH = totalCnt
			}
		}

		// There are two modes used to combine intermediate pieces depending on whether overflow has been encountered.
		// In a simple mode, it accumulates sums and counts across all pieces using Kahan compensated summation to reduce floating-point error,
		// then computes the final average at the end.
		// Incremental mode is used when a partial sum overflows, maintains a running average without forming a combined sum.
		if p.CountF > 0 {
			if countF > 0 {
				if p.UseIncrementalCalc && !useIncrementalCalculation {
					// Encountered a piece that requires incremental calculation.
					// Switching to incremental mode for all remaining pieces.
					useIncrementalCalculation = true
					incrementalAvgF = sumF / countF
					compensationF = compensationF / countF
				}

				if !useIncrementalCalculation {
					newSum, newC := floats.KahanSumInc(p.SumF, sumF, compensationF)
					newSum, newC = floats.KahanSumInc(p.CompF, newSum, newC)

					// We've just hit overflow, switch to incremental calculation.
					if math.IsInf(newSum, 0) {
						useIncrementalCalculation = true
						incrementalAvgF = sumF / countF
						compensationF = compensationF / countF
					} else {
						sumF = newSum
						compensationF = newC

						countF += p.CountF
						continue
					}
				}

				pieceIncrementalAvg := p.IncrementalAvg
				pieceCompensation := p.CompF
				if !p.UseIncrementalCalc {
					pieceIncrementalAvg = p.SumF / p.CountF
					pieceCompensation = p.CompF / p.CountF
				}

				if shouldUpdateIncrementalAvg(incrementalAvgF, pieceIncrementalAvg) {
					q := p.CountF / (countF + p.CountF)
					incrementalAvgF, compensationF = floats.KahanSumInc(pieceIncrementalAvg*q-(incrementalAvgF+compensationF)*q, incrementalAvgF, compensationF)
					incrementalAvgF, compensationF = floats.KahanSumInc(pieceCompensation*q, incrementalAvgF, compensationF)
				}

				countF += p.CountF
			} else {
				sumF = p.SumF
				compensationF = p.CompF
				countF = p.CountF
				incrementalAvgF = p.IncrementalAvg
				useIncrementalCalculation = p.UseIncrementalCalc
			}
		}
	}

	if countF > 0 && incrementalAvgH != nil {
		emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
		return 0, false, nil, nil
	}

	if nhcbBoundsReconciledSeen {
		emitAnnotation(functions.NewAggregationMismatchedCustomBucketsHistogramInfo)
	}

	if countF > 0 {
		if useIncrementalCalculation {
			return incrementalAvgF + compensationF, true, nil, nil
		}

		return (sumF + compensationF) / countF, true, nil, nil
	}

	if incrementalAvgH != nil {
		if compensationH != nil {
			_, _, _, err := incrementalAvgH.Add(compensationH)

			if err != nil {
				return 0, false, nil, err
			}
		}

		return 0, false, incrementalAvgH, nil
	}

	return 0, false, nil, nil
}

func shouldUpdateIncrementalAvg(runningAvg, nextPoint float64) bool {
	// If we get here, we've hit overflow at some point in the range.
	if math.IsInf(runningAvg, 0) {
		if math.IsInf(nextPoint, 0) && (runningAvg > 0) == (nextPoint > 0) {
			// Running average is infinite and the next point is also the same infinite.
			return false
		}
		if !math.IsInf(nextPoint, 0) && !math.IsNaN(nextPoint) {
			// Running average is infinite, and the next point is neither infinite nor NaN.
			// The running average will still be infinite after considering this point, so the point should be skipped
			// to avoid incorrectly introducing NaN.
			return false
		}
	}

	return true
}

type avgOverTimeCodec struct{}

func (c avgOverTimeCodec) Marshal(results []AvgOverTimeIntermediate) ([]byte, error) {
	listProto := &AvgOverTimeIntermediateList{Results: results}
	listBytes, err := listProto.Marshal()
	if err != nil {
		return nil, errors.Wrap(err, "marshaling avg_over_time list")
	}
	return listBytes, nil
}

func (c avgOverTimeCodec) Unmarshal(bytes []byte) ([]AvgOverTimeIntermediate, error) {
	var listProto AvgOverTimeIntermediateList
	if err := listProto.Unmarshal(bytes); err != nil {
		return nil, errors.Wrap(err, "unmarshaling avg_over_time list")
	}
	return listProto.Results, nil
}

var AvgOverTimeCodec = avgOverTimeCodec{}
