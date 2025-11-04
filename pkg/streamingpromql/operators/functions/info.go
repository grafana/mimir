// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
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

	sigFunctionLabelsOnly func(labels.Labels) string
	// labels hash:function to generate signature from labels
	sigFunctions map[string]func(labels.Labels) string
	infoSigs     map[uint64]string
	// timestamp:(signature:labels)
	sigTimestamps map[int64]map[string]labels.Labels
	// timestamp:(labels only signature:array of labels)
	sigLabelsOnlyTimestamps map[int64]map[string][]labels.Labels
	// labels only signature:(label sets hash:array of labels)
	labelSets map[string]map[string][]labels.Labels
	// inner series index - (info series label sets hash: index for ordering)
	labelSetsOrder []map[string]int
	// inner series index - inner series labels only signature
	innerSigLabelsOnly []string
	// stored series results for current inner series
	storedSeriesResults []types.InstantVectorSeriesData

	nextInnerSeriesIndex  int
	nextStoredSeriesIndex int
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

	infoMetadata, err := f.Info.SeriesMetadata(ctx, matchers)
	if err != nil {
		types.SeriesMetadataSlicePool.Put(&innerMetadata, f.MemoryConsumptionTracker)
		return nil, err
	}
	defer types.SeriesMetadataSlicePool.Put(&infoMetadata, f.MemoryConsumptionTracker)

	infoSigs, err := f.processSamplesFromInfoSeries(ctx, infoMetadata)
	if err != nil {
		types.SeriesMetadataSlicePool.Put(&innerMetadata, f.MemoryConsumptionTracker)
		return nil, err
	}
	ignoreSeries := f.identifyIgnoreSeries(innerMetadata, ivs.Selector.Matchers)
	innerSigs := f.generateInnerSignatures(innerMetadata, infoMetadata)
	return f.combineSeriesMetadata(innerMetadata, infoMetadata, innerSigs, infoSigs, ignoreSeries, ivs.Selector.Matchers)
}

func (f *InfoFunction) processSamplesFromInfoSeries(ctx context.Context, infoMetadata []types.SeriesMetadata) (map[uint64]string, error) {
	buf := make([]byte, 0, 1024)
	lb := labels.NewScratchBuilder(0)

	sigFunction := func(name string) func(labels.Labels) string {
		// Signature is the info metric name + identifying labels.
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

	f.sigFunctionLabelsOnly = func(lset labels.Labels) string {
		lb.Reset()
		lset.MatchLabels(true, identifyingLabels...).Range(func(l labels.Label) {
			lb.Add(l.Name, l.Value)
		})
		lb.Sort()
		return string(lb.Labels().Bytes(buf))
	}

	f.sigFunctions = make(map[string]func(labels.Labels) string)
	// hash:signature
	infoSigs := make(map[uint64]string)
	f.sigTimestamps = make(map[int64]map[string]labels.Labels)
	f.sigLabelsOnlyTimestamps = make(map[int64]map[string][]labels.Labels)
	f.labelSets = make(map[string]map[string][]labels.Labels)

	for _, metadata := range infoMetadata {
		metricName := metadata.Labels.Get(labels.MetricName)
		sigFunc, exists := f.sigFunctions[metricName]
		if !exists {
			sigFunc = sigFunction(metricName)
			f.sigFunctions[metricName] = sigFunc
		}
		hash := metadata.Labels.Hash()
		sig := sigFunc(metadata.Labels)
		infoSigs[hash] = sig
		sigLabelsOnly := f.sigFunctionLabelsOnly(metadata.Labels)

		d, err := f.Info.NextSeries(ctx)
		if err != nil {
			return nil, err
		}

		if len(d.Histograms) > 0 {
			return nil, fmt.Errorf("this should be an info metric, with float samples: %s", metadata.Labels)
		}

		timestamps := make(map[int64]struct{})
		for _, sample := range d.Floats {
			timestamps[sample.T] = struct{}{}

			sigsAtTimestamp, exists := f.sigTimestamps[sample.T]
			if !exists {
				sigsAtTimestamp = make(map[string]labels.Labels)
			}
			if metricLabels, exists := sigsAtTimestamp[sig]; exists {
				return nil, fmt.Errorf("found duplicate series for info metric: existing %s @ %d, new %s @ %d", metricLabels.String(), sample.T, metadata.Labels.String(), sample.T)
			}
			sigsAtTimestamp[sig] = metadata.Labels
			f.sigTimestamps[sample.T] = sigsAtTimestamp

			sigLabelsOnlyAtTimestamp, exists := f.sigLabelsOnlyTimestamps[sample.T]
			if !exists {
				sigLabelsOnlyAtTimestamp = make(map[string][]labels.Labels)
			}
			sigLabelsOnlyAtTimestamp[sigLabelsOnly] = append(sigLabelsOnlyAtTimestamp[sigLabelsOnly], metadata.Labels)
			f.sigLabelsOnlyTimestamps[sample.T] = sigLabelsOnlyAtTimestamp
		}

		types.PutInstantVectorSeriesData(d, f.MemoryConsumptionTracker)
	}

	for _, sigLabelsOnlyAtTimestamp := range f.sigLabelsOnlyTimestamps {
		for sigLabelsOnly, labelSets := range sigLabelsOnlyAtTimestamp {
			labelsArr := make([]labels.Labels, 0, len(labelSets))
			for _, labels := range labelSets {
				labelsArr = append(labelsArr, labels)
			}
			if _, exists := f.labelSets[sigLabelsOnly]; !exists {
				f.labelSets[sigLabelsOnly] = make(map[string][]labels.Labels)
			}
			f.labelSets[sigLabelsOnly][makeLabelSetsHash(labelSets)] = labelsArr
		}
	}

	return infoSigs, nil
}

func makeLabelSetsHash(labelSets []labels.Labels) string {
	if len(labelSets) == 0 {
		return "inner"
	}

	hashArr := make([]uint64, 0, len(labelSets))
	for _, labels := range labelSets {
		hashArr = append(hashArr, labels.Hash())
	}

	sort.Slice(hashArr, func(i, j int) bool { return hashArr[i] < hashArr[j] })

	hashStrArr := make([]string, 0, len(hashArr))
	for _, h := range hashArr {
		hashStrArr = append(hashStrArr, fmt.Sprintf("%d", h))
	}

	return strings.Join(hashStrArr, ",")
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

// generateInnerSignatures computes signatures of the inner series for matching with each info series.
func (f *InfoFunction) generateInnerSignatures(innerMetadata, infoMetadata []types.SeriesMetadata) []map[string]string {
	innerSigs := make([]map[string]string, len(innerMetadata))
	for i, s := range innerMetadata {
		sigs := make(map[string]string, len(f.sigFunctions))
		for infoName, sigFunc := range f.sigFunctions {
			sigs[infoName] = sigFunc(s.Labels)
		}
		innerSigs[i] = sigs
	}

	return innerSigs
}

// combineSeriesMetadata combines inner series metadata with info series labels.
func (f *InfoFunction) combineSeriesMetadata(innerMetadata, infoMetadata []types.SeriesMetadata, innerSigs []map[string]string, infoSigs map[uint64]string, ignoreSeries map[int]struct{}, dataLabelMatchers types.Matchers) ([]types.SeriesMetadata, error) {
	result, err := types.SeriesMetadataSlicePool.Get(len(innerMetadata), f.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

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

	f.labelSetsOrder = make([]map[string]int, len(innerMetadata))
	f.innerSigLabelsOnly = make([]string, len(innerMetadata))

	for i, innerSeries := range innerMetadata {
		if _, shouldIgnore := ignoreSeries[i]; shouldIgnore {
			result = append(result, innerSeries)
			f.labelSetsOrder[i] = map[string]int{"inner": 0}
			continue
		}

		sigLabelsOnly := f.sigFunctionLabelsOnly(innerSeries.Labels)
		f.innerSigLabelsOnly[i] = sigLabelsOnly
		labelSetsMap, exists := f.labelSets[sigLabelsOnly]
		if !exists {
			if len(dataLabelMatchersMap) > 0 {
				types.SeriesMetadataSlicePool.Put(&[]types.SeriesMetadata{innerSeries}, f.MemoryConsumptionTracker)
				continue
			}

			result = append(result, innerSeries)
			f.labelSetsOrder[i] = map[string]int{"inner": 0}
			continue
		}

		result = append(result, innerSeries)
		f.labelSetsOrder[i] = map[string]int{"inner": 0}

		newLabelSets, labelSetsOrder := combineLabels(lb, innerSeries, labelSetsMap, dataLabelMatchersMap)
		for _, newLabels := range newLabelSets {
			result, err = types.AppendSeriesMetadata(f.MemoryConsumptionTracker, result, types.SeriesMetadata{
				Labels:   newLabels,
				DropName: innerSeries.DropName,
			})
			if err != nil {
				return nil, err
			}
		}
		for j, labelSetsHash := range labelSetsOrder {
			f.labelSetsOrder[i][labelSetsHash] = j + 1
		}
	}

	return result, nil
}

func combineLabels(lb *labels.Builder, innerSeries types.SeriesMetadata, labelSetsMap map[string][]labels.Labels, dataLabelMatchersMap map[string]*labels.Matcher) ([]labels.Labels, []string) {
	innerLabels := innerSeries.Labels.Map()

	lb.Reset(innerSeries.Labels)

	newLabelSets := make([]labels.Labels, 0, len(labelSetsMap))
	labelSetsOrder := make([]string, 0, len(labelSetsMap))
	for _, labelSets := range labelSetsMap {
		savedLabels := make(map[string]struct{})

		for _, infoLabels := range labelSets {
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
		}

		shouldSkip := false
		for _, m := range dataLabelMatchersMap {
			if _, saved := savedLabels[m.Name]; !saved && !m.Matches("") {
				shouldSkip = true
				break
			}
		}
		if shouldSkip {
			continue
		}

		newLabelSets = append(newLabelSets, lb.Labels())
		labelSetsOrder = append(labelSetsOrder, makeLabelSetsHash(labelSets))
	}

	return newLabelSets, labelSetsOrder
}

func (f *InfoFunction) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if f.nextStoredSeriesIndex < len(f.storedSeriesResults) {
		result := f.storedSeriesResults[f.nextStoredSeriesIndex]
		f.nextStoredSeriesIndex++
		return result, nil
	}

	for {
		result, err := f.Inner.NextSeries(ctx)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}
		defer types.PutInstantVectorSeriesData(result, f.MemoryConsumptionTracker)

		labelSetsOrder := f.labelSetsOrder[f.nextInnerSeriesIndex]
		sigLabelsOnly := f.innerSigLabelsOnly[f.nextInnerSeriesIndex]
		storedSeriesResults := make(map[string]types.InstantVectorSeriesData)

		lenFloats := len(result.Floats)
		lenHistograms := len(result.Histograms)

		for _, point := range result.Floats {
			splitResult, labelSetsHash, skip, err := f.getSplitResult(point.T, sigLabelsOnly, storedSeriesResults, labelSetsOrder, lenFloats, lenHistograms)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
			if skip {
				continue
			}
			splitResult.Floats = append(splitResult.Floats, promql.FPoint{T: point.T, F: point.F})
			storedSeriesResults[labelSetsHash] = splitResult
		}

		for _, point := range result.Histograms {
			splitResult, labelSetsHash, skip, err := f.getSplitResult(point.T, sigLabelsOnly, storedSeriesResults, labelSetsOrder, lenFloats, lenHistograms)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
			if skip {
				continue
			}
			splitResult.Histograms = append(splitResult.Histograms, promql.HPoint{T: point.T, H: point.H.Copy()})
			storedSeriesResults[labelSetsHash] = splitResult
		}

		f.storedSeriesResults = make([]types.InstantVectorSeriesData, len(labelSetsOrder))
		for labelSetsHash, i := range labelSetsOrder {
			storedResults, exists := storedSeriesResults[labelSetsHash]
			if !exists {
				storedResults = types.InstantVectorSeriesData{
					Floats:     make([]promql.FPoint, 0),
					Histograms: make([]promql.HPoint, 0),
				}
			}
			f.storedSeriesResults[i] = storedResults
		}

		f.nextInnerSeriesIndex++

		if len(labelSetsOrder) == 0 {
			continue
		}

		f.nextStoredSeriesIndex = 1

		return f.storedSeriesResults[0], nil
	}
}

func (f *InfoFunction) getSplitResult(ts int64, sigLabelsOnly string, storedSeriesResults map[string]types.InstantVectorSeriesData, labelSetsOrder map[string]int, lenFloats, lenHistograms int) (types.InstantVectorSeriesData, string, bool, error) {
	var labelSetsHash string
	labelSetsBySig, exists := f.sigLabelsOnlyTimestamps[ts]
	if exists {
		labelSets, exists := labelSetsBySig[sigLabelsOnly]
		if exists {
			labelSetsHash = makeLabelSetsHash(labelSets)
		} else {
			labelSetsHash = "inner"
		}
	} else {
		labelSetsHash = "inner"
	}

	if _, exists := labelSetsOrder[labelSetsHash]; !exists {
		return types.InstantVectorSeriesData{}, "", true, nil
	}

	splitResult, exists := storedSeriesResults[labelSetsHash]
	if !exists {
		floats, err := types.FPointSlicePool.Get(lenFloats, f.MemoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, "", false, err
		}
		hists, err := types.HPointSlicePool.Get(lenHistograms, f.MemoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, "", false, err
		}
		splitResult = types.InstantVectorSeriesData{
			Floats:     floats,
			Histograms: hists,
		}
	}
	return splitResult, labelSetsHash, false, nil
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
