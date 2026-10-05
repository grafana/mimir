// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"fmt"
	"math"
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
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type DataLabelSelector struct {
	*selectors.InstantVectorSelector
}

// identifyingLabels are the labels we consider as identifying for info metrics.
// Currently hard coded, so we don't need knowledge of individual info metrics.
var identifyingLabels = []string{"instance", "job"}

// innerSeriesKey is the sentinel label sets hash used to represent the original, un-enriched
// inner series (i.e. no matching info series contributes any labels).
const innerSeriesKey = "inner"

// labelSetsHashID indexes labelSetsHashesByID, i.e. it identifies an interned group hash.
type labelSetsHashID uint32

const innerSeriesHashID labelSetsHashID = 0

// infoSeries is an info series whose samples are kept until all info series with its signature have been read.
type infoSeries struct {
	labels labels.Labels
	// metricIndex identifies the info metric name. At each timestamp, only one series per info metric is used.
	metricIndex int
	floats      []promql.FPoint
}

// infoSignature holds what is known about the info series that have the same labels-only signature.
type infoSignature struct {
	// series holds the info series read so far, in the order that the info selector returned them. The
	// samples are only needed until the last of them has been read, when the transitions can be found.
	series []infoSeries
	// remainingSeries is the number of info series with this signature that have not been read yet.
	remainingSeries int

	// labelSetsByHash holds the distinct groups of info series: label sets hash:info series labels.
	// It is nil if no info series with this signature has a sample, and once SeriesMetadata has returned.
	labelSetsByHash map[string][]labels.Labels
	// transitions records the timestamps where the group of info series changes. From each transition's
	// timestamp until the next transition, inner series with this signature are enriched with that group.
	// This is much smaller than the samples, as the group usually only changes a few times in a query.
	transitions []infoGroupTransition
}

func (s *infoSignature) returnSamplesToPool(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	for i := range s.series {
		types.FPointSlicePool.Put(&s.series[i].floats, memoryConsumptionTracker)
	}
}

// infoGroupTransition records that, from timestamp t until the next transition, inner series with a signature are
// enriched with the group of info series identified by hashID. innerSeriesHashID means they are not enriched.
type infoGroupTransition struct {
	t      int64
	hashID labelSetsHashID
}

// infoGroupWalker walks the samples of the info series of one signature in timestamp order. At each timestamp where
// at least one of the series has a sample, it finds the group of info series that enriches inner series at that
// timestamp: for each info metric, the series with the newest original sample timestamp.
type infoGroupWalker struct {
	signature       *infoSignature
	cursors         []int
	winners         []int // For each info metric, the index in signature.series of the series in the group, or -1.
	winnerTimes     []int64
	previousWinners []int
	started         bool

	// labelSets and hash describe the group at the timestamp that next last returned.
	labelSets []labels.Labels
	hash      string
}

func (w *infoGroupWalker) reset(signature *infoSignature, metricCount int) {
	w.signature = signature
	w.cursors = resizeAndClear(w.cursors, len(signature.series))
	w.winners = resizeAndClear(w.winners, metricCount)
	w.winnerTimes = resizeAndClear(w.winnerTimes, metricCount)
	w.previousWinners = resizeAndClear(w.previousWinners, metricCount)
	w.started = false
}

func resizeAndClear[T any](s []T, size int) []T {
	if cap(s) < size {
		return make([]T, size)
	}

	s = s[:size]
	clear(s)
	return s
}

// next advances to the next timestamp at which at least one of the info series has a sample, and returns it.
// ok is false once all samples have been read. changed reports whether the group at the timestamp is different
// to the group at the previous timestamp, in which case labelSets and hash have been updated.
func (w *infoGroupWalker) next() (t int64, changed bool, ok bool, err error) {
	t = math.MaxInt64
	for i, s := range w.signature.series {
		if c := w.cursors[i]; c < len(s.floats) && s.floats[c].T < t {
			t = s.floats[c].T
			ok = true
		}
	}

	if !ok {
		return 0, false, false, nil
	}

	copy(w.previousWinners, w.winners)
	for m := range w.winners {
		w.winners[m] = -1
	}

	for i := range w.signature.series {
		s := &w.signature.series[i]
		c := w.cursors[i]
		if c >= len(s.floats) || s.floats[c].T != t {
			continue
		}

		w.cursors[i]++
		// The info selector returns the original sample timestamp, in seconds, as the sample value.
		origTs := int64(s.floats[c].F * 1000)
		m := s.metricIndex

		// If a series of the same info metric has a sample at this timestamp too, keep the one with the newest
		// original timestamp. Error out if the original timestamps are the same.
		if existing := w.winners[m]; existing >= 0 {
			if w.winnerTimes[m] == origTs {
				return 0, false, false, fmt.Errorf("found duplicate series for info metric: existing %s, new %s, @ %d (%s)", w.signature.series[existing].labels.String(), s.labels.String(), t, model_timestamp.Time(t).Format(time.RFC3339Nano))
			} else if w.winnerTimes[m] > origTs {
				continue
			}
		}

		w.winners[m] = i
		w.winnerTimes[m] = origTs
	}

	changed = !w.started || !slices.Equal(w.winners, w.previousWinners)
	w.started = true

	if changed {
		w.labelSets = w.labelSets[:0]
		for _, i := range w.winners {
			if i >= 0 {
				w.labelSets = append(w.labelSets, w.signature.series[i].labels)
			}
		}
		w.hash = makeLabelSetsHash(w.labelSets)
	}

	return t, changed, true, nil
}

// infoGroupLookup returns the label sets hash ID of the group of info series of one signature, for each of an
// increasing sequence of timestamps.
type infoGroupLookup struct {
	transitions []infoGroupTransition
	next        int
	hashID      labelSetsHashID
}

// reset prepares to look up the groups of signature, which is nil if no info series can enrich the inner series.
func (l *infoGroupLookup) reset(signature *infoSignature) {
	l.transitions = nil
	if signature != nil {
		l.transitions = signature.transitions
	}
	l.next = 0
	l.hashID = innerSeriesHashID
}

// at returns the label sets hash ID of the group at t, or innerSeriesHashID if no info series has a sample at t.
// t must not be less than t in the previous call.
func (l *infoGroupLookup) at(t int64) labelSetsHashID {
	for l.next < len(l.transitions) && l.transitions[l.next].t <= t {
		l.hashID = l.transitions[l.next].hashID
		l.next++
	}

	return l.hashID
}

type InfoFunction struct {
	Inner                    types.InstantVectorOperator
	Info                     *DataLabelSelector
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	timeRange          types.QueryTimeRange
	expressionPosition posrange.PositionRange

	// dedicated buffer and scratch builder for signature
	sigBuf []byte
	sigLb  labels.ScratchBuilder
	// signature:index in signatures, for the signatures of inner series that can be enriched; nil once SeriesMetadata
	// has returned
	signatureIndexes map[string]int
	signatures       []infoSignature
	// number of distinct info metric names among the info series
	infoMetricCount int
	// looks up the group of info series for each sample of the current inner series
	groups infoGroupLookup
	// label sets hash ID:label sets hash; index zero is innerSeriesKey.
	labelSetsHashesByID []string
	// inner series index - (info series label sets hash: index for ordering)
	labelSetsOrder []map[string]int
	// inner series index - signature of the inner series, or nil if it can't be enriched
	innerSignatures []*infoSignature
	// stored series results for current inner series
	storedSeriesResults []types.InstantVectorSeriesData

	nextInnerSeriesIndex  int
	nextStoredSeriesIndex int
}

func NewInfoFunction(
	inner types.InstantVectorOperator,
	info *DataLabelSelector,
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
	innerMetadata, err := f.Inner.SeriesMetadata(ctx, filterInfoInnerMatchers(matchers, f.Info.Selector.Matchers))
	if err != nil {
		return nil, err
	}
	defer types.SeriesMetadataSlicePool.Put(&innerMetadata, f.MemoryConsumptionTracker)

	// Info series among the inner series are not enriched, so they must not contribute their
	// identifying labels to the info fetch matchers.
	ignoreSeries, err := f.identifyIgnoreSeries(innerMetadata, f.Info.Selector.Matchers)
	if err != nil {
		return nil, err
	}

	infoMatchers, skipQueryingInfo := f.generateInfoMatchers(innerMetadata, ignoreSeries)
	if !skipQueryingInfo {
		// If the info selector contains only negative __name__ matchers, add a synthetic
		// positive __name__=~".+_info" matcher to prevent non-info metrics from being fetched.
		// This mirrors upstream Prometheus' effectiveInfoNameMatchers logic applied at query time.
		if syntheticMatcher := syntheticInfoNameMatcher(f.Info.Selector.Matchers); syntheticMatcher != nil {
			infoMatchers = append(infoMatchers, *syntheticMatcher)
		}
	}
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

	if err := f.processSamplesFromInfoSeries(ctx, infoMetadata, innerMetadata, ignoreSeries); err != nil {
		return nil, err
	}
	return f.combineSeriesMetadata(innerMetadata, ignoreSeries, f.Info.Selector.Matchers)
}

// filterInfoInnerMatchers removes matchers for labels that info() can add.
func filterInfoInnerMatchers(matchers, dataLabelMatchers types.Matchers) types.Matchers {
	dataLabelNames := make(map[string]struct{}, len(dataLabelMatchers))
	for _, matcher := range dataLabelMatchers {
		if matcher.Name != model.MetricNameLabel {
			dataLabelNames[matcher.Name] = struct{}{}
		}
	}

	// An unconstrained data selector can add any label from an info series.
	if len(dataLabelNames) == 0 {
		return nil
	}

	innerMatchers := make(types.Matchers, 0, len(matchers))
	for _, matcher := range matchers {
		if _, addedByInfo := dataLabelNames[matcher.Name]; !addedByInfo {
			innerMatchers = append(innerMatchers, matcher)
		}
	}

	return innerMatchers
}

// generateInfoMatchers creates matchers based on job and instance labels from inner series
// to avoid selecting all info series unnecessarily.
func (f *InfoFunction) generateInfoMatchers(innerMetadata []types.SeriesMetadata, ignoreSeries map[int]struct{}) (types.Matchers, bool) {
	total := len(innerMetadata) - len(ignoreSeries)
	if total == 0 {
		return nil, true
	}

	identifyingLabelValues := make(map[string]map[string]struct{})
	identifyingLabelPresent := make(map[string]int)
	for _, labelName := range identifyingLabels {
		identifyingLabelValues[labelName] = make(map[string]struct{})
	}

	for i, metadata := range innerMetadata {
		if _, ignore := ignoreSeries[i]; ignore {
			continue
		}
		for _, labelName := range identifyingLabels {
			if value := metadata.Labels.Get(labelName); value != "" {
				identifyingLabelValues[labelName][value] = struct{}{}
				identifyingLabelPresent[labelName]++
			}
		}
	}

	var matchers types.Matchers

	for _, labelName := range identifyingLabels {
		values := identifyingLabelValues[labelName]
		if len(values) == 0 {
			// No inner series have this identifying label; skip generating a matcher
			// for it but continue processing other labels. Only skip querying info
			// entirely if no identifying labels have any values at all.
			continue
		}

		// When a label is present on only some inner series, info series that lack it (matching an
		// inner series on the other identifying label) must still be fetched, so the matcher also
		// accepts the empty value. Extra cross-pairs are removed by the signature join below.
		mixed := identifyingLabelPresent[labelName] < total

		if len(values) == 1 && !mixed {
			for value := range values {
				matchers = append(matchers, types.Matcher{
					Type:  labels.MatchEqual,
					Name:  labelName,
					Value: value,
				})
				break
			}
			continue
		}

		valueSlice := make([]string, 0, len(values)+1)
		for value := range values {
			valueSlice = append(valueSlice, regexp.QuoteMeta(value))
		}
		if mixed {
			// Empty alternative: also select info series without this label.
			valueSlice = append(valueSlice, "")
		}
		regexPattern := "(" + strings.Join(valueSlice, "|") + ")"
		matchers = append(matchers, types.Matcher{
			Type:  labels.MatchRegexp,
			Name:  labelName,
			Value: regexPattern,
		})
	}

	if len(matchers) == 0 {
		return nil, true
	}

	return matchers, false
}

// hasAnyIdentifyingLabel reports whether lset carries at least one identifying label.
func hasAnyIdentifyingLabel(lset labels.Labels) bool {
	return slices.ContainsFunc(identifyingLabels, lset.Has)
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

func (f *InfoFunction) processSamplesFromInfoSeries(ctx context.Context, infoMetadata, innerMetadata []types.SeriesMetadata, ignoreSeries map[int]struct{}) error {
	// Initialize dedicated buffer and scratch builder for signature,
	// since this is also called later when the local buf and lb would be out of scope.
	f.sigBuf = make([]byte, 0, types.LabelBytesBufferSize)
	f.sigLb = labels.NewScratchBuilder(0)

	// Signatures of inner series that can be enriched: not ignored, with at least one identifying
	// label. An info series whose signature is absent here enriches nothing, so it is dropped below,
	// matching Prometheus (which fetches info series per inner-series presence pattern). This avoids
	// both enriching label-less series and spurious "duplicate series for info metric" errors from
	// over-fetched cross-signature series.
	f.signatureIndexes = make(map[string]int, len(innerMetadata))
	for i, metadata := range innerMetadata {
		if _, ignore := ignoreSeries[i]; ignore {
			continue
		}
		if !hasAnyIdentifyingLabel(metadata.Labels) {
			continue
		}

		sig := f.signature(metadata.Labels)
		if _, exists := f.signatureIndexes[string(sig)]; !exists {
			f.signatureIndexes[string(sig)] = len(f.signatureIndexes)
		}
	}
	f.signatures = make([]infoSignature, len(f.signatureIndexes))

	// Find the signature and info metric of each info series before reading any samples, so that the samples of
	// a signature's info series can be returned to the pool as soon as the last of them has been read.
	type infoSeriesRef struct {
		signatureIndex int // -1 if the info series can't enrich any inner series.
		metricIndex    int
	}
	refs := make([]infoSeriesRef, len(infoMetadata))
	metricIndexes := make(map[string]int)
	enrichingSeriesCount := 0

	for i, metadata := range infoMetadata {
		signatureIndex, exists := f.signatureIndexes[string(f.signature(metadata.Labels))]
		if !exists {
			refs[i].signatureIndex = -1
			continue
		}

		metricName := metadata.Labels.Get(model.MetricNameLabel)
		metricIndex, exists := metricIndexes[metricName]
		if !exists {
			metricIndex = len(metricIndexes)
			metricIndexes[metricName] = metricIndex
		}

		refs[i] = infoSeriesRef{signatureIndex: signatureIndex, metricIndex: metricIndex}
		f.signatures[signatureIndex].remainingSeries++
		enrichingSeriesCount++
	}

	f.infoMetricCount = len(metricIndexes)

	// Share one allocation between the series of all signatures.
	allSeries := make([]infoSeries, enrichingSeriesCount)
	for i := range f.signatures {
		signature := &f.signatures[i]
		signature.series = allSeries[:0:signature.remainingSeries]
		allSeries = allSeries[signature.remainingSeries:]
	}

	f.labelSetsHashesByID = []string{innerSeriesKey}
	hashIDs := map[string]labelSetsHashID{innerSeriesKey: innerSeriesHashID}
	var walker infoGroupWalker

	for i, metadata := range infoMetadata {
		// Read all samples for this info series.
		d, err := f.Info.NextSeries(ctx)
		if err != nil {
			return err
		}

		// Drop info series that match no enrichable inner series. Samples were read above to keep
		// the info series stream aligned with infoMetadata.
		ref := refs[i]
		if ref.signatureIndex < 0 {
			types.PutInstantVectorSeriesData(d, f.MemoryConsumptionTracker)
			continue
		}

		// Error out if we get histograms for an info metric.
		if len(d.Histograms) > 0 {
			types.PutInstantVectorSeriesData(d, f.MemoryConsumptionTracker)
			return fmt.Errorf("info(): expected an info metric with float samples, but got %d float samples and %d histogram samples in series %s", len(d.Floats), len(d.Histograms), metadata.Labels)
		}

		types.HPointSlicePool.Put(&d.Histograms, f.MemoryConsumptionTracker)
		signature := &f.signatures[ref.signatureIndex]
		if len(d.Floats) == 0 {
			types.FPointSlicePool.Put(&d.Floats, f.MemoryConsumptionTracker)
		} else {
			signature.series = append(signature.series, infoSeries{labels: metadata.Labels, metricIndex: ref.metricIndex, floats: d.Floats})
		}

		signature.remainingSeries--
		if signature.remainingSeries == 0 {
			if err := f.completeSignature(signature, &walker, hashIDs); err != nil {
				return err
			}
		}
	}

	return nil
}

// completeSignature finds the groups of info series of signature once all of its info series have been read,
// records where the group changes, and returns the samples of the info series to the pool.
func (f *InfoFunction) completeSignature(signature *infoSignature, walker *infoGroupWalker, hashIDs map[string]labelSetsHashID) error {
	if len(signature.series) == 0 {
		return nil
	}

	walker.reset(signature, f.infoMetricCount)
	signature.labelSetsByHash = make(map[string][]labels.Labels)
	// Most signatures have one transition where the info series start and one after they end.
	signature.transitions = make([]infoGroupTransition, 0, 2)
	interval := f.timeRange.IntervalMilliseconds
	hashID := innerSeriesHashID
	previousT := int64(0)
	started := false

	for {
		t, changed, ok, err := walker.next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		// Info series samples are at steps of the query, so there is a gap if the previous sample was not at the
		// previous step. Inner series samples in the gap are not enriched.
		gap := started && t > previousT+interval
		if gap {
			signature.transitions = append(signature.transitions, infoGroupTransition{t: previousT + interval, hashID: innerSeriesHashID})
		}

		if changed {
			var exists bool
			if hashID, exists = hashIDs[walker.hash]; !exists {
				hashID = labelSetsHashID(len(f.labelSetsHashesByID))
				hashIDs[walker.hash] = hashID
				f.labelSetsHashesByID = append(f.labelSetsHashesByID, walker.hash)
			}
			if _, exists := signature.labelSetsByHash[walker.hash]; !exists {
				signature.labelSetsByHash[walker.hash] = slices.Clone(walker.labelSets)
			}
		}

		if changed || gap {
			signature.transitions = append(signature.transitions, infoGroupTransition{t: t, hashID: hashID})
		}

		previousT = t
		started = true
	}

	if started {
		// Inner series samples after the last info series sample are not enriched.
		signature.transitions = append(signature.transitions, infoGroupTransition{t: previousT + interval, hashID: innerSeriesHashID})
	}

	signature.returnSamplesToPool(f.MemoryConsumptionTracker)
	signature.series = nil

	return nil
}

// makeLabelSetsHash creates a hash string to identify a unique set of label sets.
// The result is independent of the order of labelSets.
func makeLabelSetsHash(labelSets []labels.Labels) string {
	length := len(labelSets)
	if length == 0 {
		return innerSeriesKey
	}

	if length == 1 {
		// Common case: a single info series contributes labels. Avoid the slice allocation and
		// sort of the general path.
		return strconv.FormatUint(labelSets[0].Hash(), 16)
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

	effectiveMatchers := effectiveInfoNameMatchers(infoNameMatchers)
	for i, s := range innerMetadata {
		name := s.Labels.Get(model.MetricNameLabel)
		if matchersMatch(effectiveMatchers, name) {
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

// effectiveInfoNameMatchers mirrors upstream Prometheus logic for determining
// which __name__ matchers to use when identifying info series to ignore.
// When only negative matchers exist, a synthetic .+_info positive matcher is
// prepended, ensuring non-info metrics are never treated as info metrics.
// InsertOmittedTargetInfoSelector takes care of the case where the original
// has no __name__ matchers at all.
func effectiveInfoNameMatchers(matchers []*labels.Matcher) []*labels.Matcher {
	for _, m := range matchers {
		if m.Type == labels.MatchEqual || m.Type == labels.MatchRegexp {
			return matchers
		}
	}
	// Only negative matchers: prepend .+_info to create a contradiction for non-info names.
	return append([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+_info")}, matchers...)
}

// syntheticInfoNameMatcher returns a synthetic __name__=~".+_info" matcher if selectorMatchers
// contains only negative __name__ matchers, or nil otherwise.
// This is used to augment the info fetch query so that non-info metrics are not selected,
// mirroring upstream Prometheus' effectiveInfoNameMatchers logic applied at fetch time.
func syntheticInfoNameMatcher(selectorMatchers types.Matchers) *types.Matcher {
	var hasPositive, hasNegative bool
	for _, m := range selectorMatchers {
		if m.Name != model.MetricNameLabel {
			continue
		}
		if m.Type == labels.MatchEqual || m.Type == labels.MatchRegexp {
			hasPositive = true
			break
		}
		hasNegative = true
	}
	if !hasPositive && hasNegative {
		return &types.Matcher{
			Type:  labels.MatchRegexp,
			Name:  model.MetricNameLabel,
			Value: ".+_info",
		}
	}
	return nil
}

// combineSeriesMetadata combines inner series metadata with info series labels.
func (f *InfoFunction) combineSeriesMetadata(innerMetadata []types.SeriesMetadata, ignoreSeries map[int]struct{}, dataLabelMatchers types.Matchers) ([]types.SeriesMetadata, error) {
	// Store user-specified label matchers in a map for easy retrieval.
	// Multiple matchers may target the same label name (e.g. {data=~".+", data=~".*"}),
	// so we store all of them per name to match upstream Prometheus behaviour.
	dataLabelMatchersMap := make(map[string][]*labels.Matcher)
	for _, m := range dataLabelMatchers {
		if m.Name == model.MetricNameLabel {
			continue
		}
		matcher, err := m.ToPrometheusType()
		if err != nil {
			return nil, err
		}
		dataLabelMatchersMap[m.Name] = append(dataLabelMatchersMap[m.Name], matcher)
	}

	// Check if any data label matcher doesn't match the empty string (e.g. {cluster=~".+"}).
	// If so, inner series without a matching info series should be suppressed.
	hasNonEmptyDataLabelMatcher := false
	for _, ms := range dataLabelMatchersMap {
		for _, m := range ms {
			if !m.Matches("") {
				hasNonEmptyDataLabelMatcher = true
				break
			}
		}
		if hasNonEmptyDataLabelMatcher {
			break
		}
	}

	lb := labels.NewBuilder(labels.EmptyLabels())

	f.labelSetsOrder = make([]map[string]int, len(innerMetadata))
	f.innerSignatures = make([]*infoSignature, len(innerMetadata))

	// The output has at least one series per inner series in the common case, so seed the result
	// slice at that size and grow it from the pool as needed. This lets us produce the final
	// metadata in a single pass instead of first computing every combined label set into an
	// intermediate map to size an exact allocation.
	result, err := types.SeriesMetadataSlicePool.Get(len(innerMetadata), f.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	labelSetsHashes := make(map[uint64]int) // hash -> input series index

	// appendSeries adds one output series for inner series i, erroring if a different input
	// series has already produced the same labelset.
	appendSeries := func(i int, metadata types.SeriesMetadata) error {
		hash := metadata.Labels.Hash()
		if existingSeriesIndex, exists := labelSetsHashes[hash]; exists && existingSeriesIndex != i {
			return fmt.Errorf("vector cannot contain metrics with the same labelset")
		}
		labelSetsHashes[hash] = i
		if err := f.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(metadata.Labels); err != nil {
			return err
		}
		result, err = types.SeriesMetadataSlicePool.AppendToSlice(result, f.MemoryConsumptionTracker, metadata)
		return err
	}

	for i, innerSeries := range innerMetadata {
		// If this inner series is an info series, pass the original series metadata along unchanged.
		if _, shouldIgnore := ignoreSeries[i]; shouldIgnore {
			f.labelSetsOrder[i] = map[string]int{innerSeriesKey: 0}
			if err := appendSeries(i, innerSeries); err != nil {
				return nil, err
			}
			continue
		}

		var labelSetsMap map[string][]labels.Labels
		if signatureIndex, exists := f.signatureIndexes[string(f.signature(innerSeries.Labels))]; exists {
			f.innerSignatures[i] = &f.signatures[signatureIndex]
			labelSetsMap = f.signatures[signatureIndex].labelSetsByHash
		}
		// If this inner series doesn't match the identifying labels of any info series, pass
		// the original series metadata along unchanged, unless a data label matcher doesn't
		// match the empty string (e.g. {data=~".+"}), in which case we skip the series.
		if labelSetsMap == nil {
			if hasNonEmptyDataLabelMatcher {
				continue
			}
			f.labelSetsOrder[i] = map[string]int{innerSeriesKey: 0}
			if err := appendSeries(i, innerSeries); err != nil {
				return nil, err
			}
			continue
		}

		// Get all possible combinations of info series labels with this inner series.
		newLabelSets, labelSetsOrder, err := combineLabels(lb, innerSeries, labelSetsMap, dataLabelMatchersMap)
		if err != nil {
			return nil, err
		}

		// If user specified label matchers but no labels from info series matched, skip this series.
		if len(dataLabelMatchersMap) > 0 && len(newLabelSets) == 0 {
			continue
		}

		f.labelSetsOrder[i] = make(map[string]int, len(labelSetsOrder)+1)

		// Only emit the original (un-enriched) series for timestamps without a matching
		// info series when all data label matchers match empty string.
		offset := 0
		if !hasNonEmptyDataLabelMatcher {
			f.labelSetsOrder[i][innerSeriesKey] = 0
			offset = 1
			if err := appendSeries(i, innerSeries); err != nil {
				return nil, err
			}
		}

		for j, labelSetsHash := range labelSetsOrder {
			f.labelSetsOrder[i][labelSetsHash] = j + offset
		}
		for _, newLabels := range newLabelSets {
			if err := appendSeries(i, types.SeriesMetadata{
				Labels:   newLabels,
				DropName: innerSeries.DropName,
			}); err != nil {
				return nil, err
			}
		}
	}

	// These are only needed to build the series metadata, so don't keep them for the rest of the query.
	f.signatureIndexes = nil
	for i := range f.signatures {
		f.signatures[i].labelSetsByHash = nil
	}

	return result, nil
}

// combineLabels combines inner series labels with info series label sets.
func combineLabels(lb *labels.Builder, innerSeries types.SeriesMetadata, labelSetsMap map[string][]labels.Labels, dataLabelMatchersMap map[string][]*labels.Matcher) ([]labels.Labels, []string, error) {
	newLabelSets := make([]labels.Labels, 0, len(labelSetsMap))
	labelSetsOrder := make([]string, 0, len(labelSetsMap))
	savedLabels := make(map[string]struct{})
	for labelSetsHash, labelSets := range labelSetsMap {
		// Reset the builder at the start of each iteration to avoid labels bleeding over.
		lb.Reset(innerSeries.Labels)
		clear(savedLabels)

		var conflictErr error
		for _, infoLabels := range labelSets {
			// Add requested labels to inner series.
			infoLabels.Range(func(l labels.Label) {
				if conflictErr != nil {
					return
				}

				// Ignore metric name.
				if l.Name == model.MetricNameLabel {
					return
				}

				// Ignore labels already on the inner metric.
				if innerSeries.Labels.Has(l.Name) {
					savedLabels[l.Name] = struct{}{}
					return
				}

				// If user specified certain label matchers, only include labels
				// whose name is among the specified data label matchers.
				// Value filtering is not needed here as info series were already
				// pre-filtered by the selector during fetching.
				if len(dataLabelMatchersMap) > 0 {
					if _, ok := dataLabelMatchersMap[l.Name]; !ok {
						return
					}
				}

				// Detect conflicting labels from different info metrics.
				if v := lb.Get(l.Name); v != "" && v != l.Value {
					conflictErr = fmt.Errorf("conflicting label: %s", l.Name)
					return
				}

				lb.Set(l.Name, l.Value)
				savedLabels[l.Name] = struct{}{}
			})
			if conflictErr != nil {
				return nil, nil, conflictErr
			}
		}

		shouldSkip := false
		// If user specified certain label matchers but no labels matched, skip this series.
		for _, ms := range dataLabelMatchersMap {
			for _, m := range ms {
				if _, saved := savedLabels[m.Name]; !saved && !m.Matches("") {
					shouldSkip = true
					break
				}
			}
			if shouldSkip {
				break
			}
		}
		if shouldSkip {
			continue
		}

		newLabelSets = append(newLabelSets, lb.Labels())
		// labelSetsHash is the map key, which is exactly makeLabelSetsHash(labelSets) computed
		// when f.labelSets was built; recomputing it here would be redundant.
		labelSetsOrder = append(labelSetsOrder, labelSetsHash)
	}

	return newLabelSets, labelSetsOrder, nil
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
		signature := f.innerSignatures[f.nextInnerSeriesIndex]
		storedSeriesResults := make(map[string]types.InstantVectorSeriesData)

		lenFloats := len(result.Floats)
		lenHistograms := len(result.Histograms)

		// Go timestamp by timestamp and sort samples into the correct split series by copying.
		f.groups.reset(signature)
		for _, point := range result.Floats {
			splitResult, labelSetsHash, skip, err := f.getSplitResult(f.groups.at(point.T), storedSeriesResults, labelSetsOrder, lenFloats, lenHistograms)
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

		// Histogram samples are read separately from float samples, so start again from the earliest timestamp.
		f.groups.reset(signature)
		for _, point := range result.Histograms {
			splitResult, labelSetsHash, skip, err := f.getSplitResult(f.groups.at(point.T), storedSeriesResults, labelSetsOrder, lenFloats, lenHistograms)
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

func (f *InfoFunction) getSplitResult(hashID labelSetsHashID, storedSeriesResults map[string]types.InstantVectorSeriesData, labelSetsOrder map[string]int, lenFloats, lenHistograms int) (types.InstantVectorSeriesData, string, bool, error) {
	labelSetsHash := f.labelSetsHashesByID[hashID]

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

func (f *InfoFunction) FinishedReading(ctx context.Context) error {
	if err := f.Inner.FinishedReading(ctx); err != nil {
		return err
	}

	return f.Info.FinishedReading(ctx)
}

func (f *InfoFunction) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	return types.FinalizeAndCombine[types.Finalizer](ctx, f.Inner, f.Info)
}

func (f *InfoFunction) Close() {
	// Return the samples of info series of signatures that were not completed, e.g. if reading the info series failed.
	for i := range f.signatures {
		f.signatures[i].returnSamplesToPool(f.MemoryConsumptionTracker)
	}
	f.signatures = nil

	if f.Inner != nil {
		f.Inner.Close()
	}
	if f.Info != nil {
		f.Info.Close()
	}
}
