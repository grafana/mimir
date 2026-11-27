// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// info()'s own mode, in the native metadata prototype. Each inner series that is a stored series with a native
// resource takes its own resource's versions, which a lookup by series finds by the selector its inner expression maps
// one to one onto its own (DataLabelSelector.OwnResourcesMatchers), and every other inner series takes the stored
// target_info, by the join, with no lookup by target. An own series' versions stand in for target_info from the
// lookup's coverage start on. Before it, and at steps where none of its versions is visible, the series takes the
// stored target_info from a second select that ends before the cutover, as the native target_info querier's stitch
// does; the other info metrics come from the main select, as in the join.

const targetInfoMetricName = "target_info"

// ownMode is info()'s own mode for one query.
type ownMode struct {
	// release gives back the memory the lookup's answers were counted for.
	release func()
	// cutover is the lookup's coverage start: math.MaxInt64 without a lookup.
	cutover int64
	// isOwn and versions are indexed by inner series: whether it takes its own resource, and its versions that are
	// visible, in the data-label selector's time.
	isOwn    []bool
	versions [][]ownVersion
	ownCount int
	// joinCount is the number of inner series that can be enriched and take the join.
	joinCount int
}

// ownVersion is a version of an inner series' own resource, visible over [from, to) in the data-label selector's time.
type ownVersion struct {
	from, to int64
	labels   labels.Labels
}

// startOwnMode decides on own mode for the query, once, and if it applies, looks up the inner series' own resources
// and finds which inner series take them. It returns nil when own mode doesn't apply.
func (f *InfoFunction) startOwnMode(ctx context.Context, innerMetadata []types.SeriesMetadata, ignoreSeries map[int]struct{}) (*ownMode, error) {
	selector := f.Info.Selector
	start, end := selectors.ComputeQueriedTimeRange(selector.TimeRange, selector.Timestamp, 0, selector.Offset, selector.LookbackDelta, false, false)
	q, err := selector.Queryable.Querier(start, end)
	if err != nil {
		return nil, err
	}
	defer q.Close()
	resources, ok := q.(querierapi.NativeInfoResources)
	if !ok || !resources.NativeInfoOwnMode(ctx) {
		return nil, nil
	}

	m := &ownMode{release: func() {}, cutover: math.MaxInt64, isOwn: make([]bool, len(innerMetadata)), versions: make([][]ownVersion, len(innerMetadata))}
	var enrichable []int
	for i, metadata := range innerMetadata {
		if _, ignore := ignoreSeries[i]; !ignore && hasAnyIdentifyingLabel(metadata.Labels) {
			enrichable = append(enrichable, i)
		}
	}
	dataLabelMatchers, err := toPrometheusMatchers(selector.Matchers)
	if err != nil {
		return nil, err
	}

	reason := ""
	var res querierapi.NativeSeriesResources
	switch {
	case len(enrichable) == 0:
		return m, nil
	case f.Info.OwnResourcesMatchers == nil:
		reason = querierapi.NativeInfoSourceShape
	case !selectsTargetInfo(dataLabelMatchers):
		reason = querierapi.NativeInfoSourceNotTargetInfo
	default:
		var found bool
		res, found, err = resources.NativeSeriesResources(ctx, &storage.SelectHints{Start: start, End: end}, f.Info.OwnResourcesMatchers...)
		if err != nil {
			return nil, err
		}
		if !found {
			reason = querierapi.NativeInfoSourceUnavailable
		}
	}
	if reason != "" {
		m.joinCount = len(enrichable)
		resources.ObserveNativeInfoInnerSeries(reason, m.joinCount)
		stats.FromContext(ctx).AddNativeInfoJoinSeries(uint64(m.joinCount))
		return m, nil
	}
	// Released once the own signatures have the versions, or when the query ends before.
	if release := res.Release; release != nil {
		m.release = func() {
			if release != nil {
				release()
				release = nil
			}
		}
	}
	m.cutover = res.CoverageStart

	// An inner series that kept its name, as with delayed name removal, is a stored series by its labels; one that
	// lost it, by its labels without the name, unless more than one stored series has those, which deduplication
	// merges into one inner series.
	byLabels := make(map[string]int, len(res.Series))
	byLabelsWithoutName := make(map[string]int, len(res.Series))
	const merged = -1
	for j, s := range res.Series {
		byLabels[string(s.Labels.Bytes(nil))] = j
		key := string(s.Labels.BytesWithoutLabels(nil, model.MetricNameLabel))
		if _, exists := byLabelsWithoutName[key]; exists {
			byLabelsWithoutName[key] = merged
		} else {
			byLabelsWithoutName[key] = j
		}
	}
	notReturned, mergedCount := 0, 0
	lookback := selector.LookbackDelta.Milliseconds()
	for _, i := range enrichable {
		lbls := innerMetadata[i].Labels
		var j int
		var found bool
		if lbls.Has(model.MetricNameLabel) {
			j, found = byLabels[string(lbls.Bytes(nil))]
		} else if j, found = byLabelsWithoutName[string(lbls.Bytes(nil))]; found && j == merged {
			mergedCount++
			continue
		}
		if !found {
			notReturned++
			continue
		}
		m.isOwn[i] = true
		m.versions[i] = visibleOwnVersions(res.Series[j].Versions, m.cutover, lookback, dataLabelMatchers)
		m.ownCount++
	}
	m.joinCount = len(enrichable) - m.ownCount
	queryStats := stats.FromContext(ctx)
	queryStats.AddNativeInfoOwnSeries(uint64(m.ownCount))
	queryStats.AddNativeInfoJoinSeries(uint64(m.joinCount))
	for source, count := range map[string]int{querierapi.NativeInfoSourceOwn: m.ownCount, querierapi.NativeInfoSourceNotReturned: notReturned, querierapi.NativeInfoSourceMerged: mergedCount} {
		if count > 0 {
			resources.ObserveNativeInfoInnerSeries(source, count)
		}
	}
	return m, nil
}

// visibleOwnVersions returns the versions of a series' own resource that count, and when each is visible: a version
// that converts to labels, that the data-label selector's matchers accept, and that ends at or after the cutover, as
// today's native path skips the others, whose tails after the cutover come from the stored target_info. Each is
// visible from the later of its start and the cutover until its end plus the lookback, as its synthesized samples
// would be, and the newest visible version wins.
func visibleOwnVersions(versions []querierapi.NativeSeriesResourceVersion, cutover, lookback int64, dataLabelMatchers []*labels.Matcher) []ownVersion {
	var counting []ownVersion
	for _, v := range versions {
		if v.Labels.IsEmpty() || v.MaxTime < cutover || !matchesAllLabels(dataLabelMatchers, v.Labels) {
			continue
		}
		counting = append(counting, ownVersion{from: max(v.MinTime, cutover), to: v.MaxTime + lookback, labels: v.Labels})
	}
	out := counting[:0]
	for k := range counting {
		if k+1 < len(counting) {
			counting[k].to = min(counting[k].to, counting[k+1].from)
		}
		if counting[k].from < counting[k].to {
			out = append(out, counting[k])
		}
	}
	return out
}

// processSamplesOwnMode is processSamplesFromInfoSeries in own mode. The inner series that take the join have
// signatures over the main select's info series, as without own mode. Those that take their own resources have base
// signatures over the main select's other info metrics and the second select's target_info, ending before the cutover,
// and each a signature of its own, with its own visible versions standing in for its base's target_info. Both selects
// are read before any signature is walked, since a signature of both kinds of inner series uses the main select's
// other info metrics in both, and every info series' samples go back to the pool once, after all signatures are walked.
func (f *InfoFunction) processSamplesOwnMode(ctx context.Context, infoMetadata, secondMetadata, innerMetadata []types.SeriesMetadata, ignoreSeries map[int]struct{}) (map[string]labelSetsHashID, error) {
	f.sigBuf = make([]byte, 0, types.LabelBytesBufferSize)
	m := f.own

	f.signatureIndexes = make(map[infoSignatureKey]int)
	ownBaseIndexes := make(map[infoSignatureKey]int)
	for i, metadata := range innerMetadata {
		if _, ignore := ignoreSeries[i]; ignore || !hasAnyIdentifyingLabel(metadata.Labels) {
			continue
		}
		indexes := f.signatureIndexes
		if m.isOwn[i] {
			indexes = ownBaseIndexes
		}
		key := f.signatureKey(metadata.Labels)
		if _, exists := indexes[infoSignatureKey(key)]; !exists {
			indexes[infoSignatureKey(key)] = len(indexes)
		}
	}
	f.signatures = make([]infoSignature, len(f.signatureIndexes))
	ownBases := make([]infoSignature, len(ownBaseIndexes))
	metricIndexes := make(map[string]infoMetricIndex)
	metricIndex := func(lbls labels.Labels) infoMetricIndex {
		name := lbls.Get(model.MetricNameLabel)
		index, exists := metricIndexes[name]
		if !exists {
			index = infoMetricIndex(len(metricIndexes))
			metricIndexes[name] = index
		}
		return index
	}

	read := func(selector types.InstantVectorOperator, metadata []types.SeriesMetadata, intoJoin bool, ownMetrics func(labels.Labels) bool) error {
		for _, md := range metadata {
			d, err := selector.NextSeries(ctx)
			if err != nil {
				return err
			}
			key := infoSignatureKey(f.signatureKey(md.Labels))
			joinIndex, inJoin := f.signatureIndexes[key]
			inJoin = inJoin && intoJoin
			ownIndex, inOwn := ownBaseIndexes[key]
			inOwn = inOwn && ownMetrics(md.Labels)
			if len(d.Histograms) > 0 && (inJoin || inOwn) {
				types.PutInstantVectorSeriesData(d, f.MemoryConsumptionTracker)
				return errInfoHistograms(d, md.Labels)
			}
			types.HPointSlicePool.Put(&d.Histograms, f.MemoryConsumptionTracker)
			if (!inJoin && !inOwn) || len(d.Floats) == 0 {
				types.FPointSlicePool.Put(&d.Floats, f.MemoryConsumptionTracker)
				continue
			}
			f.heldSamples = append(f.heldSamples, d.Floats)
			series := infoSeries{labels: md.Labels, metricIndex: metricIndex(md.Labels), floats: d.Floats}
			if inJoin {
				f.signatures[joinIndex].series = append(f.signatures[joinIndex].series, series)
			}
			if inOwn {
				ownBases[ownIndex].series = append(ownBases[ownIndex].series, series)
			}
		}
		return nil
	}
	notTargetInfo := func(lbls labels.Labels) bool { return lbls.Get(model.MetricNameLabel) != targetInfoMetricName }
	if err := read(f.Info, infoMetadata, true, notTargetInfo); err != nil {
		return nil, err
	}
	if f.ownSecond != nil {
		if err := read(f.ownSecond, secondMetadata, false, func(labels.Labels) bool { return true }); err != nil {
			return nil, err
		}
	}
	f.infoMetricCount = len(metricIndexes)

	hashIDs := map[string]labelSetsHashID{innerSeriesKey: innerSeriesHashID}
	var walker infoGroupWalker
	for _, signatures := range [][]infoSignature{f.signatures, ownBases} {
		for i := range signatures {
			if err := signatures[i].complete(&walker, f.infoMetricCount, f.timeRange.IntervalMilliseconds, hashIDs, f.MemoryConsumptionTracker, false); err != nil {
				return nil, err
			}
		}
	}
	f.returnHeldSamples()

	hashByID := make([]string, len(hashIDs))
	for hash, id := range hashIDs {
		hashByID[id] = hash
	}
	f.ownSignatures = make([]*infoSignature, len(innerMetadata))
	for i, metadata := range innerMetadata {
		if !m.isOwn[i] {
			continue
		}
		var base *infoSignature
		if index, exists := ownBaseIndexes[infoSignatureKey(f.signatureKey(metadata.Labels))]; exists {
			base = &ownBases[index]
		}
		f.ownSignatures[i] = f.ownSignature(m.versions[i], base, hashIDs, &hashByID)
	}
	// The versions are in the own signatures now.
	m.release()
	m.versions = nil
	return hashIDs, nil
}

// ownSignature returns the signature of an inner series that takes its own resource: its base's groups, with its own
// visible versions, in force at the steps they're visible at, standing in for the base's target_info there.
func (f *InfoFunction) ownSignature(versions []ownVersion, base *infoSignature, hashIDs map[string]labelSetsHashID, hashByID *[]string) *infoSignature {
	type change struct {
		t int64
		// The base's group from t on, if base, or the own labels from t on, nil when none is visible.
		base   bool
		hashID labelSetsHashID
		own    *labels.Labels
	}
	var changes []change
	if base != nil {
		for _, transition := range base.transitions {
			changes = append(changes, change{t: transition.t, base: true, hashID: transition.hashID})
		}
	}
	for k := range versions {
		from, to := f.ownStepRange(versions[k])
		if from >= to {
			continue
		}
		changes = append(changes, change{t: from, own: &versions[k].labels}, change{t: to})
	}
	slices.SortStableFunc(changes, func(a, b change) int {
		if a.t != b.t {
			if a.t < b.t {
				return -1
			}
			return 1
		}
		return 0
	})

	s := &infoSignature{}
	baseHashID := innerSeriesHashID
	var own *labels.Labels
	current := innerSeriesHashID
	labelSetsOf := func(id labelSetsHashID) []labels.Labels {
		if id == innerSeriesHashID || base == nil {
			return nil
		}
		return base.labelSetsByHash[(*hashByID)[id]]
	}
	for k := 0; k < len(changes); {
		t := changes[k].t
		for ; k < len(changes) && changes[k].t == t; k++ {
			if changes[k].base {
				baseHashID = changes[k].hashID
			} else {
				own = changes[k].own
			}
		}
		var sets []labels.Labels
		if own == nil {
			sets = labelSetsOf(baseHashID)
		} else {
			for _, set := range labelSetsOf(baseHashID) {
				if set.Get(model.MetricNameLabel) != targetInfoMetricName {
					sets = append(sets, set)
				}
			}
			sets = append(sets, *own)
		}
		hash := makeLabelSetsHash(sets)
		id, exists := hashIDs[hash]
		if !exists {
			id = labelSetsHashID(len(hashIDs))
			hashIDs[hash] = id
			*hashByID = append(*hashByID, hash)
		}
		if id != innerSeriesHashID {
			if s.labelSetsByHash == nil {
				s.labelSetsByHash = make(map[string][]labels.Labels)
			}
			if _, exists := s.labelSetsByHash[hash]; !exists {
				s.labelSetsByHash[hash] = sets
			}
		}
		if id != current {
			s.transitions = append(s.transitions, infoGroupTransition{t: t, hashID: id})
			current = id
		}
	}
	return s
}

// ownStepRange returns the steps, [from, to), at which a version is visible: the data-label selector evaluates step T
// at T, or its pinned @, minus its offset.
func (f *InfoFunction) ownStepRange(v ownVersion) (int64, int64) {
	selector := f.Info.Selector
	tr := f.timeRange
	end := tr.EndT + tr.IntervalMilliseconds
	if selector.Timestamp != nil {
		if ts := *selector.Timestamp - selector.Offset; v.from <= ts && ts < v.to {
			return tr.StartT, end
		}
		return 0, 0
	}
	firstStepAtOrAfter := func(t int64) int64 {
		if t <= tr.StartT {
			return tr.StartT
		}
		steps := (t - tr.StartT + tr.IntervalMilliseconds - 1) / tr.IntervalMilliseconds
		return min(tr.StartT+steps*tr.IntervalMilliseconds, end)
	}
	return firstStepAtOrAfter(v.from + selector.Offset), firstStepAtOrAfter(v.to + selector.Offset)
}

// startSecondSelect makes the second select, of target_info only, which ends before the cutover, for the inner series
// that take their own resources, with matchers, the generated ones of the main select.
func (f *InfoFunction) startSecondSelect(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	selector := f.Info.Selector
	second := &selectors.Selector{
		Queryable:                cutQueryable{next: selector.Queryable, cutover: f.own.cutover},
		TimeRange:                selector.TimeRange,
		Timestamp:                selector.Timestamp,
		Offset:                   selector.Offset,
		LookbackDelta:            selector.LookbackDelta,
		Matchers:                 append(slices.Clone(selector.Matchers), types.Matcher{Type: labels.MatchEqual, Name: model.MetricNameLabel, Value: targetInfoMetricName}),
		ExpressionPosition:       selector.ExpressionPosition,
		MemoryConsumptionTracker: f.MemoryConsumptionTracker,
	}
	f.ownSecond = selectors.NewInstantVectorSelector(second, f.MemoryConsumptionTracker, false, true, 0)
	if err := f.ownSecond.Prepare(ctx, f.prepareParams); err != nil {
		return nil, err
	}
	return f.ownSecond.SeriesMetadata(ctx, matchers)
}

// returnHeldSamples returns own mode's info series' samples to the pool, once.
func (f *InfoFunction) returnHeldSamples() {
	for i := range f.heldSamples {
		types.FPointSlicePool.Put(&f.heldSamples[i], f.MemoryConsumptionTracker)
	}
	f.heldSamples = nil
}

// closeOwnMode gives back what own mode holds, if the query ends before it has.
func (f *InfoFunction) closeOwnMode() {
	f.returnHeldSamples()
	if f.own != nil {
		f.own.release()
	}
	if f.ownSecond != nil {
		f.ownSecond.Close()
	}
}

// selectsTargetInfo reports whether the data-label selector's metric name matchers accept target_info.
func selectsTargetInfo(matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if m.Name == model.MetricNameLabel && !m.Matches(targetInfoMetricName) {
			return false
		}
	}
	return true
}

// selectsOnlyTargetInfo reports whether the data-label selector selects target_info and no other info metric.
func selectsOnlyTargetInfo(matchers types.Matchers) bool {
	return slices.ContainsFunc(matchers, func(m types.Matcher) bool {
		return m.Name == model.MetricNameLabel && m.Type == labels.MatchEqual && m.Value == targetInfoMetricName
	})
}

func toPrometheusMatchers(matchers types.Matchers) ([]*labels.Matcher, error) {
	out := make([]*labels.Matcher, 0, len(matchers))
	for _, m := range matchers {
		matcher, err := m.ToPrometheusType()
		if err != nil {
			return nil, err
		}
		out = append(out, matcher)
	}
	return out, nil
}

func matchesAllLabels(matchers []*labels.Matcher, lbls labels.Labels) bool {
	for _, m := range matchers {
		if !m.Matches(lbls.Get(m.Name)) {
			return false
		}
	}
	return true
}

// cutQueryable's queriers select their series' samples before the cutover only, as the native target_info querier's
// stitch reads the stored target_info.
type cutQueryable struct {
	next    storage.Queryable
	cutover int64
}

func (q cutQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	querier, err := q.next.Querier(mint, min(maxt, q.cutover-1))
	if err != nil {
		return nil, err
	}
	return cutQuerier{Querier: querier, cutover: q.cutover}, nil
}

type cutQuerier struct {
	storage.Querier
	cutover int64
}

func (q cutQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	if hints != nil {
		cut := *hints
		cut.End = min(cut.End, q.cutover-1)
		hints = &cut
	}
	return cutSeriesSet{SeriesSet: q.Querier.Select(ctx, sortSeries, hints, matchers...), cutover: q.cutover}
}

type cutSeriesSet struct {
	storage.SeriesSet
	cutover int64
}

func (s cutSeriesSet) At() storage.Series {
	return cutSeries{Series: s.SeriesSet.At(), cutover: s.cutover}
}

type cutSeries struct {
	storage.Series
	cutover int64
}

func (s cutSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	if cut, ok := it.(*cutIterator); ok {
		cut.Iterator, cut.cutover = s.Series.Iterator(cut.Iterator), s.cutover
		return cut
	}
	return &cutIterator{Iterator: s.Series.Iterator(it), cutover: s.cutover}
}

// cutIterator ends at the first sample at or after the cutover, since storage returns samples past a select's end.
type cutIterator struct {
	chunkenc.Iterator
	cutover int64
}

func (it *cutIterator) Next() chunkenc.ValueType {
	return it.cut(it.Iterator.Next())
}

func (it *cutIterator) Seek(t int64) chunkenc.ValueType {
	return it.cut(it.Iterator.Seek(t))
}

func (it *cutIterator) cut(vt chunkenc.ValueType) chunkenc.ValueType {
	if vt != chunkenc.ValNone && it.Iterator.AtT() >= it.cutover {
		return chunkenc.ValNone
	}
	return vt
}

func errInfoHistograms(d types.InstantVectorSeriesData, lbls labels.Labels) error {
	return fmt.Errorf("info(): expected an info metric with float samples, but got %d float samples and %d histogram samples in series %s", len(d.Floats), len(d.Histograms), lbls)
}
