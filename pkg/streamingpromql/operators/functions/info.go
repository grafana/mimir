// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// identifyingLabels are the labels we consider as identifying for info metrics.
// Currently hard coded, so we don't need knowledge of individual info metrics.
var identifyingLabels = []string{"instance", "job"}

type InfoFunction struct {
	Inner                    types.InstantVectorOperator
	Info                     types.InstantVectorOperator
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	timeRange                types.QueryTimeRange
	expressionPosition       posrange.PositionRange
	enableDelayedNameRemoval bool

	skipInnerMetadata    map[int]struct{}
	nextInnerSeriesIndex int
}

func NewInfoFunction(
	inner types.InstantVectorOperator,
	info types.InstantVectorOperator,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	timeRange types.QueryTimeRange,
	expressionPosition posrange.PositionRange,
	enableDelayedNameRemoval bool,
) *InfoFunction {
	return &InfoFunction{
		Inner:                    inner,
		Info:                     info,
		MemoryConsumptionTracker: memoryConsumptionTracker,

		timeRange:                timeRange,
		expressionPosition:       expressionPosition,
		enableDelayedNameRemoval: enableDelayedNameRemoval,
	}
}

func (f *InfoFunction) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	ivs, ok := f.Info.(*selectors.InstantVectorSelector)
	if !ok {
		return nil, fmt.Errorf("info function 2nd argument is not an instant vector selector")
	}

	innerMetadata, err := f.Inner.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}
	defer types.SeriesMetadataSlicePool.Put(&innerMetadata, f.MemoryConsumptionTracker)

	infoMetadata, err := f.Info.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}
	defer types.SeriesMetadataSlicePool.Put(&infoMetadata, f.MemoryConsumptionTracker)

	for _, metadata := range infoMetadata {
		d, err := f.Info.NextSeries(ctx)
		if err != nil {
			return nil, err
		}

		if len(d.Histograms) > 0 {
			return nil, fmt.Errorf("this should be an info metric, with float samples: %s", metadata.Labels)
		}

		types.HPointSlicePool.Put(&d.Histograms, f.MemoryConsumptionTracker)
		types.FPointSlicePool.Put(&d.Floats, f.MemoryConsumptionTracker)
	}

	ignoreSeries := f.identifyIgnoreSeries(innerMetadata, ivs.Selector.Matchers)
	innerSigs, infoSigs := f.generateSignatures(innerMetadata, infoMetadata)
	return f.combineSeriesMetadata(innerMetadata, infoMetadata, innerSigs, infoSigs, ignoreSeries, ivs.Selector.Matchers)
}

// identifyIgnoreSeries marks inner series that are info metrics.
func (f *InfoFunction) identifyIgnoreSeries(innerMetadata []types.SeriesMetadata, dataLabelMatchers types.Matchers) map[int]struct{} {
	ignoreSeries := make(map[int]struct{})

	var infoMatcher *labels.Matcher
	for _, m := range dataLabelMatchers {
		if m.Name == labels.MetricName {
			infoMatcher, _ = m.ToPrometheusType()
			break
		}
	}
	if infoMatcher == nil {
		return nil
	}

	for i, s := range innerMetadata {
		name := s.Labels.Get(labels.MetricName)
		if infoMatcher.Matches(name) {
			ignoreSeries[i] = struct{}{}
		}
	}

	return ignoreSeries
}

// generateSignatures creates signature functions and computes signatures for matching.
func (f *InfoFunction) generateSignatures(innerMetadata, infoMetadata []types.SeriesMetadata) ([]map[string]string, map[uint64]string) {
	buf := make([]byte, 0, 1024)
	lb := labels.NewScratchBuilder(0)

	sigFunction := func(name string) func(labels.Labels) string {
		return func(lset labels.Labels) string {
			lb.Reset()
			lb.Add(labels.MetricName, name)
			lset.MatchLabels(true, identifyingLabels...).Range(func(l labels.Label) {
				lb.Add(l.Name, l.Value)
			})
			lb.Sort()
			return string(lb.Labels().Bytes(buf))
		}
	}

	infoMetrics := make(map[string]struct{})
	for _, s := range infoMetadata {
		name := s.Labels.Get(labels.MetricName)
		infoMetrics[name] = struct{}{}
	}

	sigFunctions := make(map[string]func(labels.Labels) string)
	for name := range infoMetrics {
		sigFunctions[name] = sigFunction(name)
	}

	innerSigs := make([]map[string]string, len(innerMetadata))
	for i, s := range innerMetadata {
		sigs := make(map[string]string, len(infoMetrics))
		for infoName := range infoMetrics {
			sigs[infoName] = sigFunctions[infoName](s.Labels)
		}
		innerSigs[i] = sigs
	}

	infoSigs := make(map[uint64]string)
	for _, s := range infoMetadata {
		name := s.Labels.Get(labels.MetricName)
		if sigFunc, exists := sigFunctions[name]; exists {
			infoSigs[s.Labels.Hash()] = sigFunc(s.Labels)
		}
	}

	return innerSigs, infoSigs
}

// combineSeriesMetadata combines inner series metadata with info series labels.
func (f *InfoFunction) combineSeriesMetadata(innerMetadata, infoMetadata []types.SeriesMetadata, innerSigs []map[string]string, infoSigs map[uint64]string, ignoreSeries map[int]struct{}, dataLabelMatchers types.Matchers) ([]types.SeriesMetadata, error) {
	infoBySignature := make(map[string]labels.Labels)
	for _, s := range infoMetadata {
		if sig, exists := infoSigs[s.Labels.Hash()]; exists {
			infoBySignature[sig] = s.Labels
		}
	}

	result, err := types.SeriesMetadataSlicePool.Get(len(innerMetadata), f.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}
	result = result[:0]

	dataLabelMatchersMap := make(map[string]*labels.Matcher)
	for _, m := range dataLabelMatchers {
		if m.Name == labels.MetricName {
			continue
		}
		matcher, err := m.ToPrometheusType()
		if err != nil {
			return nil, err
		}
		dataLabelMatchersMap[m.Name] = matcher
	}

	lb := labels.NewBuilder(labels.EmptyLabels())

	f.skipInnerMetadata = make(map[int]struct{})
	for i, innerSeries := range innerMetadata {
		if _, shouldIgnore := ignoreSeries[i]; shouldIgnore {
			err := f.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(innerSeries.Labels)
			if err != nil {
				return nil, err
			}
			result = append(result, innerSeries)
			continue
		}

		innerLabels := innerSeries.Labels.Map()

		lb.Reset(innerSeries.Labels)

		savedLabels := make(map[string]struct{})

		seenInfoMetrics := make(map[string]struct{})
		for infoName, sig := range innerSigs[i] {
			if _, seen := seenInfoMetrics[infoName]; seen {
				continue
			}

			// Find matching info series.
			infoLabels, exists := infoBySignature[sig]
			if !exists {
				continue
			}

			// Add requested labels to inner series, skipping conflicts and metric name.
			infoLabels.Range(func(l labels.Label) {
				if l.Name == labels.MetricName {
					return
				}

				// Skip labels already on the inner metric.
				if _, exists := innerLabels[l.Name]; exists {
					savedLabels[l.Name] = struct{}{}
					return
				}

				if len(dataLabelMatchersMap) > 0 {
					if matcher, ok := dataLabelMatchersMap[l.Name]; !ok || !matcher.Matches(l.Value) {
						return
					}
				}

				lb.Set(l.Name, l.Value)
				savedLabels[l.Name] = struct{}{}
			})

			seenInfoMetrics[infoName] = struct{}{}
		}

		shouldSkip := false
		for _, m := range dataLabelMatchersMap {
			if _, saved := savedLabels[m.Name]; !saved && !m.Matches("") {
				shouldSkip = true
				break
			}
		}
		if shouldSkip {
			f.skipInnerMetadata[i] = struct{}{}
			continue
		}

		newLabels := lb.Labels()

		err := f.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(newLabels)
		if err != nil {
			return nil, err
		}

		result = append(result, types.SeriesMetadata{
			Labels:   newLabels,
			DropName: innerSeries.DropName,
		})
	}

	return result, nil
}

func (f *InfoFunction) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	for {
		result, err := f.Inner.NextSeries(ctx)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		// If this series was skipped in metadata, skip it here as well.
		if _, shouldSkip := f.skipInnerMetadata[f.nextInnerSeriesIndex]; shouldSkip {
			types.HPointSlicePool.Put(&result.Histograms, f.MemoryConsumptionTracker)
			types.FPointSlicePool.Put(&result.Floats, f.MemoryConsumptionTracker)
			f.nextInnerSeriesIndex++
			continue
		}
		f.nextInnerSeriesIndex++

		return result, nil
	}
}

func (f *InfoFunction) ExpressionPosition() posrange.PositionRange {
	return f.expressionPosition
}

func (f *InfoFunction) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if err := f.Inner.Prepare(ctx, params); err != nil {
		return err
	}

	return f.Info.Prepare(ctx, params)
}

func (f *InfoFunction) Finalize(ctx context.Context) error {
	if err := f.Inner.Finalize(ctx); err != nil {
		return err
	}

	return f.Info.Finalize(ctx)
}

func (f *InfoFunction) Close() {
	if f.Inner != nil {
		f.Inner.Close()
	}
	if f.Info != nil {
		f.Info.Close()
	}
}
