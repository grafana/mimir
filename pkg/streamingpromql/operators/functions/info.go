// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/grafana/regexp"
	"github.com/prometheus/common/model"
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

type labelsTime struct {
	labels labels.Labels
	time   int64
}

type InfoFunction struct {
	Inner                    types.InstantVectorOperator
	Info                     *selectors.InstantVectorSelector
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	timeRange          types.QueryTimeRange
	expressionPosition posrange.PositionRange

	// function to generate signature from labels without metric name
	sigFunctionLabelsOnly func(labels.Labels) []byte
	// dedicated buffer and scratch builder for sigFunctionLabelsOnly
	sigLabelsOnlyBuf []byte
	sigLabelsOnlyLb  labels.ScratchBuilder
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
	info *selectors.InstantVectorSelector,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	timeRange types.QueryTimeRange,
	expressionPosition posrange.PositionRange,
) *InfoFunction {
	return &InfoFunction{
		Inner:                    inner,
		Info:                     info,
		MemoryConsumptionTracker: memoryConsumptionTracker,

		timeRange:          timeRange,
		expressionPosition: expressionPosition,
	}
}

func (f *InfoFunction) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	innerMetadata, err := f.Inner.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}
	defer types.SeriesMetadataSlicePool.Put(&innerMetadata, f.MemoryConsumptionTracker)

	infoMatchers, skipQueryingInfo := f.generateInfoMatchers(innerMetadata)
	var infoMetadata []types.SeriesMetadata
	if skipQueryingInfo {
		infoMetadata = []types.SeriesMetadata{}
	} else {
		infoMetadata, err = f.Info.SeriesMetadata(ctx, infoMatchers)
		if err != nil {
			return nil, err
		}
		defer types.SeriesMetadataSlicePool.Put(&infoMetadata, f.MemoryConsumptionTracker)
	}

	if err := f.processSamplesFromInfoSeries(ctx, infoMetadata); err != nil {
		return nil, err
	}
	ignoreSeries := f.identifyIgnoreSeries(innerMetadata, f.Info.Selector.Matchers)
	return f.combineSeriesMetadata(innerMetadata, ignoreSeries, f.Info.Selector.Matchers)
}

// generateInfoMatchers creates matchers based on job and instance labels from inner series
// to avoid selecting all info series unnecessarily.
func (f *InfoFunction) generateInfoMatchers(innerMetadata []types.SeriesMetadata) (types.Matchers, bool) {
	if len(innerMetadata) == 0 {
		return nil, true
	}

	identifyingLabelValues := make(map[string]map[string]struct{})
	for _, labelName := range identifyingLabels {
		identifyingLabelValues[labelName] = make(map[string]struct{})
	}

	for _, metadata := range innerMetadata {
		for _, labelName := range identifyingLabels {
			if value := metadata.Labels.Get(labelName); value != "" {
				identifyingLabelValues[labelName][value] = struct{}{}
			}
		}
	}

	var matchers types.Matchers
	skipQueryingInfo := false

	createMatcher := func(labelName string, values map[string]struct{}) {
		switch len(values) {
		case 0:
			skipQueryingInfo = true
			return
		case 1:
			for value := range values {
				matchers = append(matchers, types.Matcher{
					Type:  labels.MatchEqual,
					Name:  labelName,
					Value: value,
				})
				break
			}
		default:
			valueSlice := make([]string, 0, len(values))
			for value := range values {
				valueSlice = append(valueSlice, regexp.QuoteMeta(value))
			}
			regexPattern := "(" + strings.Join(valueSlice, "|") + ")"
			matchers = append(matchers, types.Matcher{
				Type:  labels.MatchRegexp,
				Name:  labelName,
				Value: regexPattern,
			})
		}
	}

	for _, labelName := range identifyingLabels {
		createMatcher(labelName, identifyingLabelValues[labelName])
		if skipQueryingInfo {
			return nil, true
		}
	}

	return matchers, false
}

func (f *InfoFunction) processSamplesFromInfoSeries(ctx context.Context, infoMetadata []types.SeriesMetadata) error {
	buf := make([]byte, 0, 1024)
	lb := labels.NewScratchBuilder(0)

	sigFunction := func(name string) func(labels.Labels) []byte {
		// Signature is the info metric name + identifying labels.
		return func(lset labels.Labels) []byte {
			lb.Reset()
			lb.Add(model.MetricNameLabel, name)
			lset.MatchLabels(true, identifyingLabels...).Range(func(l labels.Label) {
				lb.Add(l.Name, l.Value)
			})
			lb.Sort()
			return lb.Labels().Bytes(buf)
		}
	}

	// Initialize dedicated buffer and scratch builder for sigFunctionLabelsOnly,
	// since this closure is stored on the struct and called later when the local
	// buf and lb would be out of scope.
	f.sigLabelsOnlyBuf = make([]byte, 0, 1024)
	f.sigLabelsOnlyLb = labels.NewScratchBuilder(0)

	f.sigFunctionLabelsOnly = func(lset labels.Labels) []byte {
		// Signature is only the identifying labels without metric names.
		f.sigLabelsOnlyLb.Reset()
		lset.MatchLabels(true, identifyingLabels...).Range(func(l labels.Label) {
			f.sigLabelsOnlyLb.Add(l.Name, l.Value)
		})
		f.sigLabelsOnlyLb.Sort()
		return f.sigLabelsOnlyLb.Labels().Bytes(f.sigLabelsOnlyBuf)
	}

	// labels hash:function to generate signature from labels
	sigFunctions := make(map[string]func(labels.Labels) []byte)
	// timestamp:(signature:labels + timestamp)
	sigTimestamps := make(map[int64]map[string]labelsTime)
	f.sigLabelsOnlyTimestamps = make(map[int64]map[string][]labels.Labels)
	f.labelSets = make(map[string]map[string][]labels.Labels)

	for _, metadata := range infoMetadata {
		metricName := metadata.Labels.Get(model.MetricNameLabel)
		sigFunc, exists := sigFunctions[metricName]
		if !exists {
			sigFunc = sigFunction(metricName)
			sigFunctions[metricName] = sigFunc
		}
		sig := sigFunc(metadata.Labels)
		sigLabelsOnly := f.sigFunctionLabelsOnly(metadata.Labels)

		// Read all samples for this info series.
		d, err := f.Info.NextSeries(ctx)
		if err != nil {
			return err
		}

		// Error out if we get histograms for an info metric.
		if len(d.Histograms) > 0 {
			return fmt.Errorf("this should be an info metric, with float samples: %s", metadata.Labels)
		}

		for _, sample := range d.Floats {
			origTs := int64(sample.F * 1000)

			// Check for duplicate series for the same timestamp and signature.
			// If a duplicate is found, only error out if the original timestamp is the same.
			// Otherwise, keep the one with the latest original timestamp.
			sigsAtTimestamp, exists := sigTimestamps[sample.T]
			if !exists {
				sigsAtTimestamp = make(map[string]labelsTime)
			}
			if metricLabels, exists := sigsAtTimestamp[string(sig)]; exists {
				if metricLabels.time == origTs {
					humanTime := time.UnixMilli(sample.T).UTC().Format("2006-01-02 15:04:05 UTC")
					return fmt.Errorf("found duplicate series for info metric: existing %s, new %s, @ %d (%s)", metricLabels.labels.String(), metadata.Labels.String(), sample.T, humanTime)
				} else if metricLabels.time > origTs {
					continue
				}
			}
			sigsAtTimestamp[string(sig)] = labelsTime{
				labels: metadata.Labels,
				time:   origTs,
			}
			sigTimestamps[sample.T] = sigsAtTimestamp

			// We summarise the info series by recording per timestamp and labels-only signature
			// the series labels we've seen.
			sigLabelsOnlyAtTimestamp, exists := f.sigLabelsOnlyTimestamps[sample.T]
			if !exists {
				sigLabelsOnlyAtTimestamp = make(map[string][]labels.Labels)
			}
			sigLabelsOnlyAtTimestamp[string(sigLabelsOnly)] = append(sigLabelsOnlyAtTimestamp[string(sigLabelsOnly)], metadata.Labels)
			f.sigLabelsOnlyTimestamps[sample.T] = sigLabelsOnlyAtTimestamp
		}

		// Return the info series data to the pool as we no longer need the raw samples
		// now that we've saved the processed summary.
		types.PutInstantVectorSeriesData(d, f.MemoryConsumptionTracker)
	}

	// Now that we've seen all info series, summarise them overall across all timestamps.
	// This will be used to generate all label sets for each inner series that can actually
	// be used, instead of generating all theoretically possible combinations which grows
	// exponentially.
	for _, sigLabelsOnlyAtTimestamp := range f.sigLabelsOnlyTimestamps {
		for sigLabelsOnly, labelSets := range sigLabelsOnlyAtTimestamp {
			labelsArr := append([]labels.Labels(nil), labelSets...)
			if _, exists := f.labelSets[sigLabelsOnly]; !exists {
				f.labelSets[sigLabelsOnly] = make(map[string][]labels.Labels)
			}
			f.labelSets[sigLabelsOnly][makeLabelSetsHash(labelSets)] = labelsArr
		}
	}

	return nil
}

// makeLabelSetsHash creates a hash string to identify a unique set of label sets.
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

// identifyIgnoreSeries marks inner series that are info metrics and are to be ignored.
func (f *InfoFunction) identifyIgnoreSeries(innerMetadata []types.SeriesMetadata, dataLabelMatchers types.Matchers) map[int]struct{} {
	ignoreSeries := make(map[int]struct{})

	var infoMatcher *labels.Matcher
	for _, m := range dataLabelMatchers {
		if m.Name == model.MetricNameLabel {
			infoMatcher, _ = m.ToPrometheusType()
			break
		}
	}
	if infoMatcher == nil {
		return nil
	}

	for i, s := range innerMetadata {
		name := s.Labels.Get(model.MetricNameLabel)
		if infoMatcher.Matches(name) {
			ignoreSeries[i] = struct{}{}
		}
	}

	return ignoreSeries
}

// combineSeriesMetadata combines inner series metadata with info series labels.
func (f *InfoFunction) combineSeriesMetadata(innerMetadata []types.SeriesMetadata, ignoreSeries map[int]struct{}, dataLabelMatchers types.Matchers) ([]types.SeriesMetadata, error) {
	// Store user-specified label matchers in a map for easy retrieval.
	dataLabelMatchersMap := make(map[string]*labels.Matcher)
	for _, m := range dataLabelMatchers {
		if m.Name == model.MetricNameLabel {
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

	extraLabelSets := make(map[int][]labels.Labels)
	totalLabelSetsCount := 0

	// Do a first pass to calculate the combined label sets for each inner series.
	for i, innerSeries := range innerMetadata {
		// If this inner series is an info series, pass the original series metadata along unchanged.
		if _, shouldIgnore := ignoreSeries[i]; shouldIgnore {
			f.labelSetsOrder[i] = map[string]int{"inner": 0}
			totalLabelSetsCount++
			continue
		}

		sigLabelsOnly := f.sigFunctionLabelsOnly(innerSeries.Labels)
		f.innerSigLabelsOnly[i] = string(sigLabelsOnly)
		labelSetsMap, exists := f.labelSets[string(sigLabelsOnly)]
		// If this inner series doesn't match the identifying labels of any info series, pass
		// the original series metadata along unchanged, unless user specified label matchers.
		if !exists {
			if len(dataLabelMatchersMap) > 0 {
				continue
			}
			f.labelSetsOrder[i] = map[string]int{"inner": 0}
			totalLabelSetsCount++
			continue
		}

		// Pass the original series metadata along unchanged.
		f.labelSetsOrder[i] = map[string]int{"inner": 0}
		totalLabelSetsCount++

		// Get all possible combinations of info series labels with this inner series,
		// and track them properly so we know exactly how many to pull from the pool later.
		newLabelSets, labelSetsOrder := combineLabels(lb, innerSeries, labelSetsMap, dataLabelMatchersMap)
		extraLabelSets[i] = newLabelSets
		totalLabelSetsCount += len(newLabelSets)
		for j, labelSetsHash := range labelSetsOrder {
			f.labelSetsOrder[i][labelSetsHash] = j + 1
		}
	}

	result, err := types.SeriesMetadataSlicePool.Get(totalLabelSetsCount, f.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	// Do a second pass to actually produce final series metadata using exact numbers from the pool.
	for i, innerSeries := range innerMetadata {
		if _, shouldPassInner := f.labelSetsOrder[i]["inner"]; shouldPassInner {
			result, err = types.AppendSeriesMetadata(f.MemoryConsumptionTracker, result, innerSeries)
			if err != nil {
				return nil, err
			}
		}

		for _, newLabels := range extraLabelSets[i] {
			result, err = types.AppendSeriesMetadata(f.MemoryConsumptionTracker, result, types.SeriesMetadata{
				Labels:   newLabels,
				DropName: innerSeries.DropName,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

// combineLabels combines inner series labels with info series label sets.
func combineLabels(lb *labels.Builder, innerSeries types.SeriesMetadata, labelSetsMap map[string][]labels.Labels, dataLabelMatchersMap map[string]*labels.Matcher) ([]labels.Labels, []string) {
	newLabelSets := make([]labels.Labels, 0, len(labelSetsMap))
	labelSetsOrder := make([]string, 0, len(labelSetsMap))
	savedLabels := make(map[string]struct{})
	for _, labelSets := range labelSetsMap {
		// Reset the builder at the start of each iteration to avoid labels bleeding over.
		lb.Reset(innerSeries.Labels)
		clear(savedLabels)

		for _, infoLabels := range labelSets {
			// Add requested labels to inner series.
			infoLabels.Range(func(l labels.Label) {
				// Ignore metric name.
				if l.Name == model.MetricNameLabel {
					return
				}

				// Ignore labels already on the inner metric.
				if innerSeries.Labels.Has(l.Name) {
					savedLabels[l.Name] = struct{}{}
					return
				}

				// If user specified certain label matchers, ignore labels that don't match.
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
		// If user specified certain label matchers but no labels matched, skip this series.
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
	// If we still have stored series results for the current inner series, return them first.
	// Don't load the next inner series until all stored split series have been returned.
	if f.nextStoredSeriesIndex < len(f.storedSeriesResults) {
		result := f.storedSeriesResults[f.nextStoredSeriesIndex]
		f.nextStoredSeriesIndex++
		return result, nil
	}

	for {
		// Retrieve the next inner series.
		result, err := f.Inner.NextSeries(ctx)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		labelSetsOrder := f.labelSetsOrder[f.nextInnerSeriesIndex]
		sigLabelsOnly := f.innerSigLabelsOnly[f.nextInnerSeriesIndex]
		storedSeriesResults := make(map[string]types.InstantVectorSeriesData)

		lenFloats := len(result.Floats)
		lenHistograms := len(result.Histograms)

		// Go timestamp by timestamp and sort samples into the correct split series by copying.
		for _, point := range result.Floats {
			splitResult, labelSetsHash, skip, err := f.getSplitResult(point.T, sigLabelsOnly, storedSeriesResults, labelSetsOrder, lenFloats, lenHistograms)
			if err != nil {
				types.PutInstantVectorSeriesData(result, f.MemoryConsumptionTracker)
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
				types.PutInstantVectorSeriesData(result, f.MemoryConsumptionTracker)
				return types.InstantVectorSeriesData{}, err
			}
			if skip {
				continue
			}
			splitResult.Histograms = append(splitResult.Histograms, promql.HPoint{T: point.T, H: point.H.Copy()})
			storedSeriesResults[labelSetsHash] = splitResult
		}

		// Arrange stored series results in the correct order to match SeriesMetadata.
		// Cache the results for subsequent calls to NextSeries for this inner series.
		f.storedSeriesResults = make([]types.InstantVectorSeriesData, len(labelSetsOrder))
		for labelSetsHash, i := range labelSetsOrder {
			storedResults, exists := storedSeriesResults[labelSetsHash]
			if !exists {
				storedResults = types.InstantVectorSeriesData{
					Floats:     nil,
					Histograms: nil,
				}
			}
			f.storedSeriesResults[i] = storedResults
		}

		// Return the inner series data to the pool now that we've copied all needed data.
		types.PutInstantVectorSeriesData(result, f.MemoryConsumptionTracker)

		// Go to the next inner series when we're ready.
		f.nextInnerSeriesIndex++

		if len(labelSetsOrder) == 0 {
			continue
		}

		// Queue the next series result, and return the first one now.
		f.nextStoredSeriesIndex = 1
		return f.storedSeriesResults[0], nil
	}
}

func (f *InfoFunction) getSplitResult(ts int64, sigLabelsOnly string, storedSeriesResults map[string]types.InstantVectorSeriesData, labelSetsOrder map[string]int, lenFloats, lenHistograms int) (types.InstantVectorSeriesData, string, bool, error) {
	// Get the label sets seen for this timestamp and labels-only signature and create a hash.
	var labelSetsHash string
	labelSetsBySig, exists := f.sigLabelsOnlyTimestamps[ts]
	if exists {
		labelSets, exists := labelSetsBySig[sigLabelsOnly]
		if exists {
			labelSetsHash = makeLabelSetsHash(labelSets)
		} else {
			// Use the original inner series labels unchanged.
			labelSetsHash = "inner"
		}
	} else {
		// Use the original inner series labels unchanged.
		labelSetsHash = "inner"
	}

	// If this label sets hash is not in the order map, it means we shouldn't create a series for it.
	if _, exists := labelSetsOrder[labelSetsHash]; !exists {
		return types.InstantVectorSeriesData{}, "", true, nil
	}

	splitResult, exists := storedSeriesResults[labelSetsHash]
	if !exists {
		// If this hasn't been created yet, create new slices from the pool.
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
