// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/indexheader"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/tsdb"
)

// Streaming postings selection.
//
// The batch path (toPostingGroups plus worstCaseFetchedDataStrategy) resolves every
// posting group before the strategy runs, so a matcher on a high-cardinality label
// pays for its full value list even when the strategy omits the group right after.
// The types in this file resolve groups lazily instead: the strategy drives each
// group's resolution ("measure") and parks a group as soon as its measured or
// estimated size crosses the remaining budget, before the group's keys materialize.
// A parked group degrades to its matcher, which the caller applies as a filter over
// the fetched series, exactly like the omitted groups of the batch strategies.
//
// The decision logic mirrors worstCaseFetchedDataStrategy: select posting lists
// worth at most (series count of the smallest intersecting group) times
// tsdb.EstimatedSeriesP99Size bytes. Sizes are measured incrementally, so a
// selection can only be more omitting than the exact one, never less; the extra
// omissions are bounded by the same budget that bounds the batch strategy, and
// correctness is unaffected because parked matchers still filter the fetched
// series. Known divergences from the batch strategy:
//
//   - groups resolve in an order that keeps index reads cheap (static keys first,
//     lazy groups by estimated size), not in exact-size order, so a misestimated
//     group can be parked that exact sizes would have selected, or selected that
//     exact sizes would have parked;
//   - a parked group whose scan never completed may hide the fact that it matches
//     no label value at all; the batch path proves emptiness upfront, this path
//     trades that proof for not reading the index.

// postingsSelectionStrategy decides which posting groups to resolve through the index and which matchers
// to defer to post-fetch series filtering.
type postingsSelectionStrategy[G any] interface {
	// name should be a static string which identifies this strategy.
	// The name participates in the expanded-postings cache key, so it must change
	// whenever a change alters the decisions.
	name() string

	// selectPostings drives the group streams to a decision and returns:
	//  - selected: groups whose postings the caller fetches and intersects;
	//  - pending: groups that are parked; the caller must use the matchers of pending groups to apply as filters over the series
	//    the selected postings resolve to. Those series may or may not match the pending matchers.
	//
	// A nil selected with a nil pending means the strategy proved that the request
	// selects no series: an intersecting group resolved to no keys.
	selectPostings(ctx context.Context, groups G) (selected, pending G, err error)
}

// resolvedPostingGroup is a posting group fully resolved against one block index:
// every key has a posting list in the index, and offsets matches keys 1:1.
type resolvedPostingGroup struct {
	matcher    *labels.Matcher
	isSubtract bool
	keys       []labels.Label
	offsets    []index.Range

	// totalSize is the size in bytes of all the posting lists for keys.
	totalSize int64
}

// postingListOffsetsIter pulls the label values of one label name, together with
// their posting-list ranges, in postings-offset-table order (sorted by value).
type postingListOffsetsIter interface {
	Next() bool
	At() streamindex.PostingListOffset
	Err() error
	Close() error
}

// postingOffsetsSource provides the index lookups posting-group streams need. It
// is the streaming seam towards the index-header: once indexheader.Reader grows
// a native label-values iterator, its implementation here changes and nothing
// else in this file does.
type postingOffsetsSource interface {
	// PostingsOffset returns the posting-list range of one name-value pair.
	// found is false when the block has no posting list for the pair.
	PostingsOffset(ctx context.Context, name, value string) (off index.Range, found bool, err error)

	// LabelValuesOffsetsIter returns an iterator over the values of name that
	// start with prefix and match filter, together with their posting-list ranges.
	LabelValuesOffsetsIter(ctx context.Context, name, prefix string, filter func(string) bool) (postingListOffsetsIter, error)
}

// indexHeaderPostingOffsetsSource adapts an indexheader.Reader. Until the
// index-header grows a streaming label-values API, labelValuesOffsetsIter
// materializes the full value list up front; the selection logic above still
// consumes it incrementally.
type indexHeaderPostingOffsetsSource struct {
	reader indexheader.Reader
}

func newIndexHeaderPostingOffsetsSource(r indexheader.Reader) *indexHeaderPostingOffsetsSource {
	return &indexHeaderPostingOffsetsSource{reader: r}
}

func (s *indexHeaderPostingOffsetsSource) PostingsOffset(ctx context.Context, name, value string) (index.Range, bool, error) {
	off, err := s.reader.PostingsOffset(ctx, name, value)
	if errors.Is(err, indexheader.NotFoundRangeErr) {
		return index.Range{}, false, nil
	}
	if err != nil {
		return index.Range{}, false, err
	}
	return off, true, nil
}

func (s *indexHeaderPostingOffsetsSource) LabelValuesOffsetsIter(ctx context.Context, name, prefix string, filter func(string) bool) (postingListOffsetsIter, error) {
	vals, err := s.reader.LabelValuesOffsets(ctx, name, prefix, filter)
	if err != nil {
		return nil, err
	}
	return &slicePostingListOffsetsIter{vals: vals}, nil
}

// slicePostingListOffsetsIter adapts a materialized value list to the pull
// iterator.
type slicePostingListOffsetsIter struct {
	vals []streamindex.PostingListOffset
	i    int
}

func (it *slicePostingListOffsetsIter) Next() bool {
	if it.i >= len(it.vals) {
		return false
	}
	it.i++
	return true
}

func (it *slicePostingListOffsetsIter) At() streamindex.PostingListOffset {
	return it.vals[it.i-1]
}

func (it *slicePostingListOffsetsIter) Err() error {
	return nil
}

func (it *slicePostingListOffsetsIter) Close() error {
	it.vals = nil
	return nil
}

// postingGroupSizeEstimator estimates, without reading the index, the total size
// in bytes of the posting lists a lazy group resolves to, or -1 if it cannot.
// Estimates come from per-block data like the sparse index-header. An estimate
// can only pre-park a group whose size is already over the remaining budget, so
// a wrong estimate costs plan quality (more series filtered post-fetch), never
// correctness.
type postingGroupSizeEstimator interface {
	estimatedPostingListSize(ctx context.Context, m *labels.Matcher, isSubtract bool) int64
}

// postingGroupStream resolves one matcher's posting group lazily against one
// block. Resolution happens in two passes:
//
//   - measure pulls values in batches and keeps only the running size, so a group
//     the strategy parks never materializes its keys;
//   - materialize re-walks the values of a selected group and collects keys and
//     offsets.
type postingGroupStream interface {
	matcher() *labels.Matcher
	labelName() string
	isSubtract() bool
	isAllPostings() bool

	// isLazy reports whether the group's keys are enumerated from the index
	// rather than known from the matcher.
	isLazy() bool
	// numKeys returns the number of keys known without reading the index; zero
	// for lazy groups.
	numKeys() int

	// estimatedSize returns the estimated total posting-list size in bytes, or
	// -1 when unknown.
	estimatedSize() int64

	// totalSize returns the exact total size of the group's posting lists as
	// measured so far.
	totalSize() int64
	// matchedValues returns the number of label values matched so far.
	matchedValues() int
	// measured reports whether the measure pass reached the end of the values.
	measured() bool

	// measure pulls up to ticks more values into the running size.
	measure(ctx context.Context, ticks int) error

	// materialize collects the keys and offsets of every matched value. The
	// strategy calls it only for groups it selects. It is idempotent; it is a
	// no-op for groups whose measure pass already collects their state.
	materialize(ctx context.Context) error

	// resolve returns the group's resolved state; valid after materialize.
	resolve() resolvedPostingGroup

	// close releases the underlying scan and the accumulated keys. It does not
	// invalidate groups resolved before the call.
	close()
}

// staticPostingGroupStream resolves a group whose keys are known without reading
// the index: set-match regexes, `=` and `!=` matchers, and the all-postings group.
// Every key costs one postingsOffset lookup, so the measure pass already collects
// keys and offsets and materialize is a no-op.
type staticPostingGroupStream struct {
	m           *labels.Matcher
	name        string
	subtract    bool
	allPostings bool
	src         postingOffsetsSource
	staticKeys  []labels.Label

	keys    []labels.Label
	offsets []index.Range
	total   int64
	next    int
	done    bool
}

func (g *staticPostingGroupStream) matcher() *labels.Matcher          { return g.m }
func (g *staticPostingGroupStream) labelName() string                 { return g.name }
func (g *staticPostingGroupStream) isSubtract() bool                  { return g.subtract }
func (g *staticPostingGroupStream) isAllPostings() bool               { return g.allPostings }
func (g *staticPostingGroupStream) isLazy() bool                      { return false }
func (g *staticPostingGroupStream) numKeys() int                      { return len(g.staticKeys) }
func (g *staticPostingGroupStream) estimatedSize() int64              { return -1 }
func (g *staticPostingGroupStream) totalSize() int64                  { return g.total }
func (g *staticPostingGroupStream) matchedValues() int                { return len(g.keys) }
func (g *staticPostingGroupStream) measured() bool                    { return g.done }
func (g *staticPostingGroupStream) materialize(context.Context) error { return nil }

func (g *staticPostingGroupStream) measure(ctx context.Context, ticks int) error {
	for !g.done && ticks > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		if g.next >= len(g.staticKeys) {
			g.done = true
			break
		}
		k := g.staticKeys[g.next]
		g.next++
		ticks--

		off, found, err := g.src.PostingsOffset(ctx, k.Name, k.Value)
		if err != nil {
			return err
		}
		if !found {
			// The block has no posting list for this pair; the batch path drops
			// such keys in filterNonExistingKeys.
			continue
		}
		g.keys = append(g.keys, k)
		g.offsets = append(g.offsets, off)
		g.total += off.End - off.Start
	}
	return nil
}

func (g *staticPostingGroupStream) resolve() resolvedPostingGroup {
	return resolvedPostingGroup{
		matcher:    g.m,
		isSubtract: g.subtract,
		keys:       g.keys,
		offsets:    g.offsets,
		totalSize:  g.total,
	}
}

func (g *staticPostingGroupStream) close() {}

// lazyPostingGroupStream resolves a group whose keys are enumerated from the
// index's postings offset table: generic regexes, and matchers that also select
// series without the label. The measure pass keeps only a running size;
// materialize re-walks and collects keys, so a parked group never pays for its
// keys.
type lazyPostingGroupStream struct {
	m        *labels.Matcher
	name     string
	prefix   string
	filter   func(string) bool
	subtract bool
	src      postingOffsetsSource
	estSize  int64

	iter     postingListOffsetsIter
	iterDone bool

	total int64
	n     int

	matDone bool
	keys    []labels.Label
	offsets []index.Range
}

func (g *lazyPostingGroupStream) matcher() *labels.Matcher { return g.m }
func (g *lazyPostingGroupStream) labelName() string        { return g.name }
func (g *lazyPostingGroupStream) isSubtract() bool         { return g.subtract }
func (g *lazyPostingGroupStream) isAllPostings() bool      { return false }
func (g *lazyPostingGroupStream) isLazy() bool             { return true }
func (g *lazyPostingGroupStream) numKeys() int             { return 0 }
func (g *lazyPostingGroupStream) estimatedSize() int64     { return g.estSize }
func (g *lazyPostingGroupStream) totalSize() int64         { return g.total }
func (g *lazyPostingGroupStream) matchedValues() int       { return g.n }
func (g *lazyPostingGroupStream) measured() bool           { return g.iterDone }

func (g *lazyPostingGroupStream) measure(ctx context.Context, ticks int) error {
	if g.iter == nil && !g.iterDone {
		var err error
		g.iter, err = g.src.LabelValuesOffsetsIter(ctx, g.name, g.prefix, g.filter)
		if err != nil {
			return err
		}
	}
	for ticks > 0 && !g.iterDone {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !g.iter.Next() {
			g.iterDone = true
			if err := g.iter.Err(); err != nil {
				return err
			}
			break
		}
		val := g.iter.At()
		g.total += val.Off.End - val.Off.Start
		g.n++
		ticks--
	}
	return nil
}

func (g *lazyPostingGroupStream) materialize(ctx context.Context) error {
	if g.matDone {
		return nil
	}
	// Re-walk from the start: the measure pass discarded the values.
	iter, err := g.src.LabelValuesOffsetsIter(ctx, g.name, g.prefix, g.filter)
	if err != nil {
		return err
	}
	defer iter.Close()

	g.keys = make([]labels.Label, 0, g.n)
	g.offsets = make([]index.Range, 0, g.n)
	g.total = 0
	for iter.Next() {
		val := iter.At()
		g.keys = append(g.keys, labels.Label{Name: g.name, Value: val.LabelValue})
		g.offsets = append(g.offsets, val.Off)
		g.total += val.Off.End - val.Off.Start
	}
	if err := iter.Err(); err != nil {
		return err
	}
	g.n = len(g.keys)
	g.matDone = true
	return nil
}

func (g *lazyPostingGroupStream) resolve() resolvedPostingGroup {
	return resolvedPostingGroup{
		matcher:    g.m,
		isSubtract: g.subtract,
		keys:       g.keys,
		offsets:    g.offsets,
		totalSize:  g.total,
	}
}

func (g *lazyPostingGroupStream) close() {
	if g.iter != nil {
		g.iter.Close()
		g.iter = nil
	}
	g.keys, g.offsets = nil, nil
}

// newPostingGroupStreams builds the group streams for a matcher set against one
// block, mirroring the classification of toRawPostingGroup and the group handling
// of toPostingGroups: static-key groups resolve through postingsOffset lookups,
// lazy groups stream the label's value list, and a request with only subtracting
// groups resolves the all-postings list as its anchor.
func newPostingGroupStreams(ctx context.Context, ms []*labels.Matcher, src postingOffsetsSource, est postingGroupSizeEstimator) ([]postingGroupStream, error) {
	streams := make([]postingGroupStream, 0, len(ms)+1)
	allRequested, hasAdds := false, false
	for _, m := range ms {
		raw := toRawPostingGroup(m)
		allRequested = allRequested || raw.isSubtract
		hasAdds = hasAdds || !raw.isSubtract

		if raw.isLazy {
			filter := raw.matcher.Matches
			if raw.isSubtract {
				filter = not(filter)
			}
			var estSize int64 = -1
			if est != nil {
				estSize = est.estimatedPostingListSize(ctx, m, raw.isSubtract)
			}
			streams = append(streams, &lazyPostingGroupStream{
				m:        m,
				name:     raw.labelName,
				prefix:   raw.prefix,
				filter:   filter,
				subtract: raw.isSubtract,
				src:      src,
				estSize:  estSize,
			})
			continue
		}
		streams = append(streams, &staticPostingGroupStream{
			m:          m,
			name:       raw.labelName,
			subtract:   raw.isSubtract,
			src:        src,
			staticKeys: raw.keys,
		})
	}

	if allRequested && !hasAdds {
		// Nothing to intersect with: anchor the subtractions on all postings.
		streams = append(streams, &staticPostingGroupStream{
			name:        allPostingsKey.Name,
			allPostings: true,
			src:         src,
			staticKeys:  []labels.Label{allPostingsKey},
		})
	}
	if !allRequested && !hasAdds {
		// No matchers left a group: the request selects no series.
		return nil, nil
	}
	return streams, nil
}

// defaultStreamedMeasureBatch amortizes the budget re-checks over a batch of
// values while keeping the crossing overshoot, and therefore the wasted
// measure work, a small fraction of the crossing threshold.
const defaultStreamedMeasureBatch = 1024

// worstCaseStreamedStrategy preserves the logic of worstCaseFetchedDataStrategy:
// select posting lists worth at most the worst-case series fetch, which is the
// series count of the smallest intersecting group times
// tsdb.EstimatedSeriesP99Size. Unlike the batch strategy it resolves groups
// lazily, so a group whose measured or estimated size crosses the remaining
// budget parks before its keys materialize.
type worstCaseStreamedStrategy struct {
	// postingListActualSizeFactor adjusts measured posting-list sizes, mirroring
	// worstCaseFetchedDataStrategy: lists in the bucket and in the cache differ
	// in size, and it is unknown which one the fetch will hit.
	postingListActualSizeFactor float64

	// measureBatch is how many values one measure step pulls before the strategy
	// re-checks the budget.
	measureBatch int
}

func newWorstCaseStreamedStrategy(postingListActualSizeFactor float64) worstCaseStreamedStrategy {
	return worstCaseStreamedStrategy{
		postingListActualSizeFactor: postingListActualSizeFactor,
		measureBatch:                defaultStreamedMeasureBatch,
	}
}

func (s worstCaseStreamedStrategy) name() string {
	return fmt.Sprintf("worst-case-stream%0.1f", s.postingListActualSizeFactor)
}

var _ postingsSelectionStrategy[[]postingGroupStream] = worstCaseStreamedStrategy{}

func (s worstCaseStreamedStrategy) selectPostings(ctx context.Context, groups []postingGroupStream) (selected, pending []postingGroupStream, err error) {
	if len(groups) == 0 {
		return nil, nil, nil
	}
	defer func() {
		for _, g := range groups {
			g.close()
		}
	}()

	var (
		statics, lazies []postingGroupStream
	)
	for _, g := range groups {
		if g.isLazy() {
			lazies = append(lazies, g)
		} else {
			statics = append(statics, g)
		}
	}

	// Resolve the static-key groups first, ordered like toPostingGroups resolves
	// them: one postingsOffset lookup per key, fewest keys first. This bounds
	// the resolution work to what the batch path pays, and keeps its shortcut:
	// a static group that matches no existing key empties the request before any
	// label-value scan starts.
	slices.SortFunc(statics, func(a, b postingGroupStream) int {
		if c := cmp.Compare(a.numKeys(), b.numKeys()); c != 0 {
			return c
		}
		return strings.Compare(a.labelName(), b.labelName())
	})
	for _, g := range statics {
		for !g.measured() {
			if err := g.measure(ctx, s.measureBatch); err != nil {
				return nil, nil, err
			}
		}
		if !g.isSubtract() && g.matchedValues() == 0 {
			// No posting list for any key, so the group selects no series and
			// neither does the whole request. For the all-postings group this
			// also covers a block without any series.
			return nil, nil, nil
		}
	}

	// Decision phase. Static groups come first, ordered by their resolved size
	// like the batch strategy orders all groups; lazy groups follow, ordered by
	// their estimated size so that cheap scans complete before expensive ones
	// are measured. Parking requires a resolved-and-selected intersecting group,
	// which keeps the caller with at least one group to intersect.
	slices.SortFunc(statics, func(a, b postingGroupStream) int {
		if c := cmp.Compare(a.totalSize(), b.totalSize()); c != 0 {
			return c
		}
		return strings.Compare(a.labelName(), b.labelName())
	})
	slices.SortFunc(lazies, func(a, b postingGroupStream) int {
		// Unknown estimates sort last: without an estimate the group's true size
		// is only known after measuring it to completion, so it must not be the
		// group the strategy has to resolve unconditionally.
		ae, be := a.estimatedSize(), b.estimatedSize()
		if (ae < 0) != (be < 0) {
			if be < 0 {
				return -1
			}
			return 1
		}
		if c := cmp.Compare(ae, be); c != 0 {
			return c
		}
		return strings.Compare(a.labelName(), b.labelName())
	})

	var (
		// budget is the worst-case size, in bytes, of the series the selected
		// posting lists can produce after intersection; valid once set. Like in
		// the batch strategy it comes from the smallest intersecting group seen,
		// so it only shrinks.
		budget    int64
		budgetSet bool
		// selectedSize is the adjusted size of the selected groups so far.
		selectedSize int64
	)

	for _, g := range slices.Concat(statics, lazies) {
		// An estimate over the remaining budget parks the group without reading
		// the index at all.
		if !g.measured() && g.estimatedSize() >= 0 && budgetSet && !g.isAllPostings() &&
			s.crossesBudget(g.estimatedSize(), selectedSize, budget) {
			pending = append(pending, g)
			continue
		}

		for !g.measured() {
			if err := g.measure(ctx, s.measureBatch); err != nil {
				return nil, nil, err
			}
			// The running size only grows, so once it crosses, the completed
			// group would cross too: stop measuring.
			if budgetSet && !g.isAllPostings() && s.crossesBudget(g.totalSize(), selectedSize, budget) {
				break
			}
		}

		if !g.isSubtract() && !g.isAllPostings() && g.matchedValues() == 0 {
			// The group matched no label value, so the request selects no series.
			return nil, nil, nil
		}
		if budgetSet && !g.isAllPostings() && s.crossesBudget(g.totalSize(), selectedSize, budget) {
			pending = append(pending, g)
			continue
		}

		if !g.isSubtract() && !g.isAllPostings() {
			// A resolved intersecting group (re)establishes the worst-case bound.
			selected = append(selected, g)
			selectedSize += s.adjustedSize(g.totalSize())
			if b := groupSeriesCount(g.totalSize(), g.matchedValues()) * tsdb.EstimatedSeriesP99Size; !budgetSet || b < budget {
				budget, budgetSet = b, true
				// The smaller bound can demote groups selected earlier; with all
				// sizes known upfront the batch strategy reaches the same
				// partition without ever selecting them.
				selected, selectedSize, pending = s.demoteSelected(selected, pending, budget)
			}
			continue
		}

		// Subtractive groups and the all-postings group.
		selected = append(selected, g)
		selectedSize += s.adjustedSize(g.totalSize())
	}

	// The batch strategy always selects the __name__ group: without it the
	// fetched series are sparser in the index and pricier to read.
	// Pull __name__ groups back out of the parked set, whatever their size.
	for i, g := range slices.Backward(pending) {
		if g.labelName() != model.MetricNameLabel || g.isAllPostings() {
			continue
		}
		if err := g.materialize(ctx); err != nil {
			return nil, nil, err
		}
		if !g.isSubtract() && g.matchedValues() == 0 {
			// Resolving the group proved the request selects no series.
			return nil, nil, nil
		}
		selected = append(selected, g)
		pending = append(pending[:i], pending[i+1:]...)
	}
	return selected, pending, nil
}

func resolvePostingGroupsFromStream(ctx context.Context, gs []postingGroupStream) (resolved []resolvedPostingGroup, err error) {
	for _, g := range gs {
		if err := g.materialize(ctx); err != nil {
			return nil, err
		}
		resolved = append(resolved, g.resolve())
	}
	return resolved, nil
}

// demoteSelected re-runs the batch strategy's greedy cut over the groups selected
// so far, under the new smaller budget: with every size known, the batch strategy
// selects the size-ascending prefix that fits the budget. Groups that no longer
// fit are demoted to pending. Parked groups are never re-admitted, so selections
// only become more omitting. If the cut would leave no intersecting group, the
// smallest one stays selected: the caller needs at least one group to intersect,
// which the batch strategy guarantees by never omitting the first intersecting
// group in size order.
func (s worstCaseStreamedStrategy) demoteSelected(sel []postingGroupStream, parked []postingGroupStream, budget int64) (newSel []postingGroupStream, newSelectedSize int64, newParked []postingGroupStream) {
	sorted := slices.Clone(sel)
	slices.SortFunc(sorted, func(a, b postingGroupStream) int {
		return cmp.Compare(a.totalSize(), b.totalSize())
	})

	newSel = make([]postingGroupStream, 0, len(sorted))
	for i, g := range sorted {
		if s.crossesBudget(g.totalSize(), newSelectedSize, budget) {
			// Sizes ascend, so no later group fits either.
			tail := sorted[i:]
			if !someIntersectingGroup(newSel) {
				for j, tg := range tail {
					if !tg.isSubtract() {
						newSel = append(newSel, tg)
						newSelectedSize += s.adjustedSize(tg.totalSize())
						tail = append(tail[:j], tail[j+1:]...)
						break
					}
				}
			}
			return newSel, newSelectedSize, append(parked, tail...)
		}
		newSelectedSize += s.adjustedSize(g.totalSize())
		newSel = append(newSel, g)
	}
	return newSel, newSelectedSize, parked
}

func someIntersectingGroup(groups []postingGroupStream) bool {
	for _, g := range groups {
		if !g.isSubtract() {
			return true
		}
	}
	return false
}

func (s worstCaseStreamedStrategy) adjustedSize(size int64) int64 {
	return int64(float64(size) * s.postingListActualSizeFactor)
}

func (s worstCaseStreamedStrategy) crossesBudget(size, selectedSize, budget int64) bool {
	return selectedSize+s.adjustedSize(size) > budget
}

// groupSeriesCount converts a group's measured posting-list size into its series
// count, mirroring numSeriesInSmallestIntersectingPostingGroup: each posting list
// spends 4 bytes on its number of entries, which are not series.
func groupSeriesCount(totalSize int64, matchedValues int) int64 {
	series := (totalSize - int64(matchedValues)*tsdb.BytesPerPostingInAPostingList) / tsdb.BytesPerPostingInAPostingList
	return max(series, 0)
}
