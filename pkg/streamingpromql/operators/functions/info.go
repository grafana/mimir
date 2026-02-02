// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/regexp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	model_timestamp "github.com/prometheus/prometheus/model/timestamp"
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

	// dedicated buffer and scratch builder for signature
	sigBuf []byte
	sigLb  labels.ScratchBuilder
	// timestamp:(signature:array of info series labels)
	sigTimestamps map[int64]map[string][]labels.Labels
	// signature:(label sets hash:array of info series labels)
	labelSets map[string]map[string][]labels.Labels
	// inner series index - (info series label sets hash: index for ordering)
	labelSetsOrder []map[string]int
	// inner series index - inner series signature
	innerSig []string
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
	ignoreSeries, err := f.identifyIgnoreSeries(innerMetadata, f.Info.Selector.Matchers)
	if err != nil {
		return nil, err
	}
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

	for _, labelName := range identifyingLabels {
		values := identifyingLabelValues[labelName]
		switch len(values) {
		case 0:
			return nil, true
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

	return matchers, false
}

// signature generates signature from labels without metric name
// Ensure this is only called after initializing f.sigBuf and f.sigLb
func (f *InfoFunction) signature(lset labels.Labels) []byte {
	// Signature is only the identifying labels without metric names.
	f.sigLb.Reset()
	lset.MatchLabels(true, identifyingLabels...).Range(func(l labels.Label) {
		f.sigLb.Add(l.Name, l.Value)
	})
	f.sigLb.Sort()
	return f.sigLb.Labels().Bytes(f.sigBuf)
}

func (f *InfoFunction) processSamplesFromInfoSeries(ctx context.Context, infoMetadata []types.SeriesMetadata) error {
	// Initialize dedicated buffer and scratch builder for signature,
	// since this is also called later when the local buf and lb would be out of scope.
	f.sigBuf = make([]byte, 0, 1024)
	f.sigLb = labels.NewScratchBuilder(0)

	// metric name:(timestamp:(labels-only signature:labels + timestamp))
	sigTimestampsByMetric := make(map[string]map[int64]map[string]labelsTime)
	f.sigTimestamps = make(map[int64]map[string][]labels.Labels, f.timeRange.StepCount)
	f.labelSets = make(map[string]map[string][]labels.Labels)

	for _, metadata := range infoMetadata {
		metricName := metadata.Labels.Get(model.MetricNameLabel)
		sig := f.signature(metadata.Labels)

		// Read all samples for this info series.
		d, err := f.Info.NextSeries(ctx)
		if err != nil {
			return err
		}

		// Error out if we get histograms for an info metric.
		if len(d.Histograms) > 0 {
			types.PutInstantVectorSeriesData(d, f.MemoryConsumptionTracker)
			return fmt.Errorf("info(): expected an info metric with float samples, but got %d float samples and %d histogram samples in series %s", len(d.Floats), len(d.Histograms), metadata.Labels)
		}

		for _, sample := range d.Floats {
			origTs := int64(sample.F * 1000)

			// Check for duplicate series for the same timestamp and signature.
			// If a duplicate is found, only error out if the original timestamp is the same.
			// Otherwise, keep the one with the latest original timestamp.
			sigTimestamps, exists := sigTimestampsByMetric[metricName]
			if !exists {
				sigTimestamps = make(map[int64]map[string]labelsTime)
				sigTimestampsByMetric[metricName] = sigTimestamps
			}
			sigsAtTimestamp, exists := sigTimestamps[sample.T]
			if !exists {
				sigsAtTimestamp = make(map[string]labelsTime)
			}
			if metricLabels, exists := sigsAtTimestamp[string(sig)]; exists {
				if metricLabels.time == origTs {
					types.PutInstantVectorSeriesData(d, f.MemoryConsumptionTracker)
					return fmt.Errorf("found duplicate series for info metric: existing %s, new %s, @ %d (%s)", metricLabels.labels.String(), metadata.Labels.String(), sample.T, model_timestamp.Time(sample.T).Format(time.RFC3339Nano))
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
			sigAtTimestamp, exists := f.sigTimestamps[sample.T]
			if !exists {
				sigAtTimestamp = make(map[string][]labels.Labels)
			}
			sigAtTimestamp[string(sig)] = append(sigAtTimestamp[string(sig)], metadata.Labels)
			f.sigTimestamps[sample.T] = sigAtTimestamp
		}

		// Return the info series data to the pool as we no longer need the raw samples
		// now that we've saved the processed summary.
		types.PutInstantVectorSeriesData(d, f.MemoryConsumptionTracker)
	}

	// Now that we've seen all info series, summarise them overall across all timestamps.
	// This will be used to generate all label sets for each inner series that can actually
	// be used, instead of generating all theoretically possible combinations which grows
	// exponentially.
	for _, sigAtTimestamp := range f.sigTimestamps {
		for sig, labelSets := range sigAtTimestamp {
			labelsArr := append([]labels.Labels(nil), labelSets...)
			if _, exists := f.labelSets[sig]; !exists {
				f.labelSets[sig] = make(map[string][]labels.Labels)
			}
			f.labelSets[sig][makeLabelSetsHash(labelSets)] = labelsArr
		}
	}

	return nil
}

// makeLabelSetsHash creates a hash string to identify a unique set of label sets.
func makeLabelSetsHash(labelSets []labels.Labels) string {
	length := len(labelSets)
	if length == 0 {
		return "inner"
	}

	hashArr := make([]uint64, length)
	for i, labels := range labelSets {
		hashArr[i] = labels.Hash()
	}

	slices.Sort(hashArr)

	b := make([]byte, 0, length*17-1)
	for idx, h := range hashArr {
		if idx > 0 {
			b = append(b, ',')
		}
		b = strconv.AppendUint(b, h, 16)
	}

	return string(b)
}

// identifyIgnoreSeries marks inner series that are info metrics and are to be ignored.
func (f *InfoFunction) identifyIgnoreSeries(innerMetadata []types.SeriesMetadata, dataLabelMatchers types.Matchers) (map[int]struct{}, error) {
	ignoreSeries := make(map[int]struct{})

	var infoNameMatchers []*labels.Matcher
	for _, m := range dataLabelMatchers {
		if m.Name == model.MetricNameLabel {
			matcher, err := m.ToPrometheusType()
			if err != nil {
				return nil, err
			}
			infoNameMatchers = append(infoNameMatchers, matcher)
		}
	}
	if len(infoNameMatchers) == 0 {
		return nil, nil
	}

	for i, s := range innerMetadata {
		name := s.Labels.Get(model.MetricNameLabel)
		if matchersMatch(infoNameMatchers, name) {
			ignoreSeries[i] = struct{}{}
		}
	}

	return ignoreSeries, nil
}

func matchersMatch(matchers []*labels.Matcher, value string) bool {
	for _, m := range matchers {
		if !m.Matches(value) {
			return false
		}
	}
	return true
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
	f.innerSig = make([]string, len(innerMetadata))

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

		sig := f.signature(innerSeries.Labels)
		f.innerSig[i] = string(sig)
		labelSetsMap, exists := f.labelSets[string(sig)]
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

		// Get all possible combinations of info series labels with this inner series,
		// and track them properly so we know exactly how many to pull from the pool later.
		newLabelSets, labelSetsOrder := combineLabels(lb, innerSeries, labelSetsMap, dataLabelMatchersMap)

		// If user specified label matchers but no labels from info series matched, skip this series.
		if len(dataLabelMatchersMap) > 0 && len(newLabelSets) == 0 {
			continue
		}

		// Pass the original series metadata along unchanged.
		f.labelSetsOrder[i] = map[string]int{"inner": 0}
		totalLabelSetsCount++

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

	labelSetsHashes := make(map[uint64]int) // hash -> input series index

	// Do a second pass to actually produce final series metadata using exact numbers from the pool,
	// while checking for any clashes in final label sets that come from different input series.
	for i, innerSeries := range innerMetadata {
		if _, shouldPassInner := f.labelSetsOrder[i]["inner"]; shouldPassInner {
			hash := innerSeries.Labels.Hash()
			if existingSeriesIndex, exists := labelSetsHashes[hash]; exists && existingSeriesIndex != i {
				return nil, fmt.Errorf("vector cannot contain metrics with the same labelset")
			}
			labelSetsHashes[hash] = i
			result, err = types.AppendSeriesMetadata(f.MemoryConsumptionTracker, result, innerSeries)
			if err != nil {
				return nil, err
			}
		}

		for _, newLabels := range extraLabelSets[i] {
			hash := newLabels.Hash()
			if existingSeriesIndex, exists := labelSetsHashes[hash]; exists && existingSeriesIndex != i {
				return nil, fmt.Errorf("vector cannot contain metrics with the same labelset")
			}
			labelSetsHashes[hash] = i
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
		sig := f.innerSig[f.nextInnerSeriesIndex]
		storedSeriesResults := make(map[string]types.InstantVectorSeriesData)

		lenFloats := len(result.Floats)
		lenHistograms := len(result.Histograms)

		// Go timestamp by timestamp and sort samples into the correct split series by copying.
		for _, point := range result.Floats {
			splitResult, labelSetsHash, skip, err := f.getSplitResult(point.T, sig, storedSeriesResults, labelSetsOrder, lenFloats, lenHistograms)
			if err != nil {
				for _, data := range storedSeriesResults {
					types.PutInstantVectorSeriesData(data, f.MemoryConsumptionTracker)
				}
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
			splitResult, labelSetsHash, skip, err := f.getSplitResult(point.T, sig, storedSeriesResults, labelSetsOrder, lenFloats, lenHistograms)
			if err != nil {
				for _, data := range storedSeriesResults {
					types.PutInstantVectorSeriesData(data, f.MemoryConsumptionTracker)
				}
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

func (f *InfoFunction) getSplitResult(ts int64, sig string, storedSeriesResults map[string]types.InstantVectorSeriesData, labelSetsOrder map[string]int, lenFloats, lenHistograms int) (types.InstantVectorSeriesData, string, bool, error) {
	// Get the label sets seen for this timestamp and labels-only signature and create a hash.
	var labelSetsHash string
	labelSetsBySig, exists := f.sigTimestamps[ts]
	if exists {
		labelSets, exists := labelSetsBySig[sig]
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
			types.FPointSlicePool.Put(&floats, f.MemoryConsumptionTracker)
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

func (f *InfoFunction) AfterPrepare(ctx context.Context) error {
	if err := f.Inner.AfterPrepare(ctx); err != nil {
		return err
	}

	return f.Info.AfterPrepare(ctx)
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
