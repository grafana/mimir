// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/index.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package block

import (
	"cmp"
	"context"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	util_log "github.com/grafana/mimir/pkg/util/log"
)

var (
	SupportedIndexFormats = []int{index.FormatV2}
)

// VerifyBlock does a full run over a block index and chunk data and verifies that they fulfill the order invariants.
func VerifyBlock(ctx context.Context, logger log.Logger, blockDir string, minTime, maxTime int64, checkChunks bool) error {
	stats, err := GatherBlockHealthStats(ctx, logger, blockDir, minTime, maxTime, checkChunks)
	if err != nil {
		return err
	}

	return stats.AnyErr()
}

type HealthStats struct {
	// IndexFormat is the format version used by the TSDB index file.
	IndexFormat int

	// TotalSeries represents total number of series in block.
	TotalSeries int64
	// OutOfOrderSeries represents number of series that have out of order chunks.
	OutOfOrderSeries int

	// OutOfOrderChunks represents number of chunks that are out of order (older time range is after younger one).
	OutOfOrderChunks int
	// DuplicatedChunks represents number of chunks with same time ranges within same series, potential duplicates.
	DuplicatedChunks int
	// OutsideChunks represents number of all chunks that are before or after time range specified in block meta.
	OutsideChunks int
	// CompleteOutsideChunks is subset of OutsideChunks that will never be accessed. They are completely out of time range specified in block meta.
	CompleteOutsideChunks int
	// Issue347OutsideChunks represents subset of OutsideChunks that are outsiders caused by https://github.com/prometheus/tsdb/issues/347
	// and is something that Thanos handle.
	//
	// Specifically we mean here chunks with minTime == block.maxTime and maxTime > block.MaxTime. These are
	// segregated into separate counters. These chunks are safe to be deleted, since they are duplicated across 2 blocks.
	Issue347OutsideChunks int
	// OutOfOrderLabels represents the number of postings that contained out
	// of order labels, a bug present in Prometheus 2.8.0 and below.
	OutOfOrderLabels int
}

// UnsupportedIndexFormat returns an error if the stats indicate this TSDB block uses
// an index format that isn't supported by the read path (store-gateways use a custom
// implementation of TSDB index parsing code, to avoid mmap, which only supports v2).
func (i HealthStats) UnsupportedIndexFormat() error {
	if !slices.Contains(SupportedIndexFormats, i.IndexFormat) {
		return fmt.Errorf("index uses format %d which is not supported (%v are supported)", i.IndexFormat, SupportedIndexFormats)
	}

	return nil
}

// OutOfOrderLabelsErr returns an error if the HealthStats object indicates
// postings without of order labels.  This is corrected by Prometheus Issue
// #5372 and affects Prometheus versions 2.8.0 and below.
func (i HealthStats) OutOfOrderLabelsErr() error {
	if i.OutOfOrderLabels > 0 {
		return fmt.Errorf("index contains %d postings with out of order labels",
			i.OutOfOrderLabels)
	}
	return nil
}

// Issue347OutsideChunksErr returns error if stats indicates issue347 block issue, that is repaired explicitly before compaction (on plan block).
func (i HealthStats) Issue347OutsideChunksErr() error {
	if i.Issue347OutsideChunks > 0 {
		return fmt.Errorf("found %d chunks outside the block time range introduced by https://github.com/prometheus/tsdb/issues/347", i.Issue347OutsideChunks)
	}
	return nil
}

func (i HealthStats) OutOfOrderChunksErr() error {
	if i.OutOfOrderChunks > 0 {
		return fmt.Errorf(
			"%d/%d series have an average of %.3f out-of-order chunks: "+
				"%.3f of these are exact duplicates (in terms of data and time range)",
			i.OutOfOrderSeries,
			i.TotalSeries,
			float64(i.OutOfOrderChunks)/float64(i.OutOfOrderSeries),
			float64(i.DuplicatedChunks)/float64(i.OutOfOrderChunks),
		)
	}
	return nil
}

// CriticalErr returns error if stats indicates critical block issue, that might be solved only by manual repair procedure.
func (i HealthStats) CriticalErr() error {
	var errMsg []string

	n := i.OutsideChunks - (i.CompleteOutsideChunks + i.Issue347OutsideChunks)
	if n > 0 {
		errMsg = append(errMsg, fmt.Sprintf("found %d chunks partially outside the block time range", n))
	}

	if i.CompleteOutsideChunks > 0 {
		errMsg = append(errMsg, fmt.Sprintf("found %d chunks completely outside the block time range", i.CompleteOutsideChunks))
	}

	if len(errMsg) > 0 {
		return errors.New(strings.Join(errMsg, ", "))
	}

	return nil
}

// AnyErr returns error if stats indicates any block issue.
func (i HealthStats) AnyErr() error {
	var errMsg []string

	if err := i.CriticalErr(); err != nil {
		errMsg = append(errMsg, err.Error())
	}

	if err := i.UnsupportedIndexFormat(); err != nil {
		errMsg = append(errMsg, err.Error())
	}

	if err := i.Issue347OutsideChunksErr(); err != nil {
		errMsg = append(errMsg, err.Error())
	}

	if err := i.OutOfOrderLabelsErr(); err != nil {
		errMsg = append(errMsg, err.Error())
	}

	if err := i.OutOfOrderChunksErr(); err != nil {
		errMsg = append(errMsg, err.Error())
	}

	if len(errMsg) > 0 {
		return errors.New(strings.Join(errMsg, ", "))
	}

	return nil
}

// GatherBlockHealthStats returns useful counters as well as outsider chunks (chunks outside of block time range) that
// helps to assess index and optionally chunk health.
// It considers https://github.com/prometheus/tsdb/issues/347 as something that Thanos can handle.
// See HealthStats.Issue347OutsideChunks for details.
func GatherBlockHealthStats(ctx context.Context, logger log.Logger, blockDir string, minTime, maxTime int64, checkChunkData bool) (stats HealthStats, err error) {
	indexFn := filepath.Join(blockDir, IndexFilename)
	chunkDir := filepath.Join(blockDir, ChunksDirname)
	// index reader
	r, err := index.NewFileReader(indexFn, index.DecodePostingsRaw)
	if err != nil {
		return stats, errors.Wrap(err, "open index file")
	}
	defer runutil.CloseWithErrCapture(&err, r, "gather index issue file reader")

	stats.IndexFormat = r.Version()

	n, v := index.AllPostingsKey()
	p, err := r.Postings(ctx, n, v)
	if err != nil {
		return stats, errors.Wrap(err, "get all postings")
	}
	var (
		lastLset labels.Labels
		lset     labels.Labels
		builder  labels.ScratchBuilder
		chks     []chunks.Meta
		cr       *chunks.Reader
	)

	// chunk reader
	if checkChunkData {
		cr, err = chunks.NewDirReader(chunkDir, nil)
		if err != nil {
			return stats, errors.Wrapf(err, "failed to open chunk dir %s", chunkDir)
		}
		defer runutil.CloseWithErrCapture(&err, cr, "closing chunks reader")
	}

	// Per series.
	for p.Next() {
		lastLset = lset

		id := p.At()
		stats.TotalSeries++

		if err := r.Series(id, &builder, &chks); err != nil {
			return stats, errors.Wrap(err, "read series")
		}
		lset = builder.Labels()
		if lset.IsEmpty() {
			return stats, errors.Errorf("empty label set detected for series %d", id)
		}
		if !lastLset.IsEmpty() && labels.Compare(lastLset, lset) >= 0 {
			return stats, errors.Errorf("series %v out of order; previous %v", lset, lastLset)
		}
		first := true
		var lastName string
		lset.Range(func(l labels.Label) {
			if !first && l.Name < lastName {
				stats.OutOfOrderLabels++
				level.Warn(logger).Log("msg",
					"out-of-order label set: known bug in Prometheus 2.8.0 and below",
					"labelset", lset.String(),
					"series", fmt.Sprintf("%d", id),
				)
			}
			first = false
			lastName = l.Name
		})
		if len(chks) == 0 {
			return stats, errors.Errorf("empty chunks for series %d", id)
		}

		ooo := 0
		// Per chunk in series.
		for i, c := range chks {
			// Check if chunk data is valid.
			if checkChunkData {
				err := verifyChunks(cr, c)
				if err != nil {
					return stats, errors.Wrapf(err, "verify chunk %d of series %d", i, id)
				}
			}

			// Chunk vs the block ranges.
			if c.MinTime < minTime || c.MaxTime > maxTime {
				stats.OutsideChunks++
				if c.MinTime > maxTime || c.MaxTime < minTime {
					stats.CompleteOutsideChunks++
				} else if c.MinTime == maxTime {
					stats.Issue347OutsideChunks++
				}
			}

			if i == 0 {
				continue
			}

			c0 := chks[i-1]

			// Chunk order within block.
			if c.MinTime > c0.MaxTime {
				continue
			}

			if c.MinTime == c0.MinTime && c.MaxTime == c0.MaxTime {
				// TODO(bplotka): Calc and check checksum from chunks itself.
				// The chunks can overlap 1:1 in time, but does not have same data.
				// We assume same data for simplicity, but it can be a symptom of error.
				stats.DuplicatedChunks++
				continue
			}
			// Chunks partly overlaps or out of order.
			ooo++
		}
		if ooo > 0 {
			stats.OutOfOrderSeries++
			stats.OutOfOrderChunks += ooo
			level.Debug(logger).Log("msg", "found out of order series", "labels", lset)
		}
	}
	if p.Err() != nil {
		return stats, errors.Wrap(err, "walk postings")
	}

	return stats, nil
}

type ignoreFnType func(mint, maxt int64, prev *chunks.Meta, curr *chunks.Meta) (bool, error)

// Repair opens the block with given id in dir and creates a new one with fixed data.
// It:
// - removes out of order duplicates
// - all "complete" outsiders (they will not accessed anyway)
// - removes all near "complete" outside chunks introduced by https://github.com/prometheus/tsdb/issues/347.
// Fixable inconsistencies are resolved in the new block.
// TODO(bplotka): https://github.com/thanos-io/thanos/issues/378.
func Repair(ctx context.Context, logger log.Logger, dir string, id ulid.ULID, source SourceType, clampChunks bool, ignoreChkFns ...ignoreFnType) (resid ulid.ULID, err error) {
	if len(ignoreChkFns) == 0 {
		return resid, errors.New("no ignore chunk function specified")
	}

	bdir := filepath.Join(dir, id.String())
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	resid = ulid.MustNew(ulid.Now(), entropy)

	meta, err := ReadMetaFromDir(bdir)
	if err != nil {
		return resid, errors.Wrap(err, "read meta file")
	}
	if meta.Thanos.Downsample.Resolution > 0 {
		return resid, errors.New("cannot repair downsampled block")
	}

	b, err := tsdb.OpenBlock(util_log.SlogFromGoKit(logger), bdir, nil, nil)
	if err != nil {
		return resid, errors.Wrap(err, "open block")
	}
	defer runutil.CloseWithErrCapture(&err, b, "repair block reader")

	indexr, err := b.Index()
	if err != nil {
		return resid, errors.Wrap(err, "open index")
	}
	defer runutil.CloseWithErrCapture(&err, indexr, "repair index reader")

	chunkr, err := b.Chunks()
	if err != nil {
		return resid, errors.Wrap(err, "open chunks")
	}
	defer runutil.CloseWithErrCapture(&err, chunkr, "repair chunk reader")

	resdir := filepath.Join(dir, resid.String())

	chunkw, err := chunks.NewWriter(filepath.Join(resdir, ChunksDirname))
	if err != nil {
		return resid, errors.Wrap(err, "open chunk writer")
	}
	defer runutil.CloseWithErrCapture(&err, chunkw, "repair chunk writer")

	indexw, err := index.NewWriter(ctx, filepath.Join(resdir, IndexFilename))
	if err != nil {
		return resid, errors.Wrap(err, "open index writer")
	}
	defer runutil.CloseWithErrCapture(&err, indexw, "repair index writer")

	// TODO(fabxc): adapt so we properly handle the version once we update to an upstream
	// that has multiple.
	resmeta := *meta
	resmeta.ULID = resid
	resmeta.Stats = tsdb.BlockStats{} // Reset stats.
	resmeta.Thanos.Source = source    // Update source.

	if err := rewrite(ctx, logger, indexr, chunkr, indexw, chunkw, &resmeta, clampChunks, ignoreChkFns); err != nil {
		return resid, errors.Wrap(err, "rewrite block")
	}
	resmeta.Thanos.SegmentFiles = GetSegmentFiles(resdir)
	if err := resmeta.WriteToDir(logger, resdir); err != nil {
		return resid, err
	}
	// TSDB may rewrite metadata in bdir.
	// TODO: This is not needed in newer TSDB code. See https://github.com/prometheus/tsdb/pull/637.
	if err := meta.WriteToDir(logger, bdir); err != nil {
		return resid, err
	}
	return resid, nil
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func IgnoreCompleteOutsideChunk(mint, maxt int64, _, curr *chunks.Meta) (bool, error) {
	if curr.MinTime > maxt || curr.MaxTime < mint {
		// "Complete" outsider. Ignore.
		return true, nil
	}
	return false, nil
}

func IgnoreIssue347OutsideChunk(_, maxt int64, _, curr *chunks.Meta) (bool, error) {
	if curr.MinTime == maxt {
		// "Near" outsider from issue https://github.com/prometheus/tsdb/issues/347. Ignore.
		return true, nil
	}
	return false, nil
}

func IgnoreDuplicateOutsideChunk(_, _ int64, last, curr *chunks.Meta) (bool, error) {
	if last == nil {
		return false, nil
	}

	if curr.MinTime > last.MaxTime {
		return false, nil
	}

	// Verify that the overlapping chunks are exact copies so we can safely discard
	// the current one.
	if curr.MinTime != last.MinTime || curr.MaxTime != last.MaxTime {
		return false, errors.Errorf("non-sequential chunks not equal: [%d, %d] and [%d, %d]",
			last.MinTime, last.MaxTime, curr.MinTime, curr.MaxTime)
	}
	ca := crc32.Checksum(last.Chunk.Bytes(), castagnoli)
	cb := crc32.Checksum(curr.Chunk.Bytes(), castagnoli)

	if ca != cb {
		return false, errors.Errorf("non-sequential chunks not equal: %x and %x", ca, cb)
	}

	return true, nil
}

// sanitizeChunkSequence ensures order of the input chunks and drops any duplicates.
// It errors if the sequence contains non-dedupable overlaps.
func sanitizeChunkSequence(chks []chunks.Meta, mint, maxt int64, clampChunks bool, ignoreChkFns []ignoreFnType) ([]chunks.Meta, error) {
	if len(chks) == 0 {
		return nil, nil
	}
	// First, ensure that chunks are ordered by their start time.
	slices.SortFunc(chks, func(a, b chunks.Meta) int {
		return cmp.Compare(a.MinTime, b.MinTime)
	})

	// Remove duplicates, complete outsiders and near outsiders.
	repl := make([]chunks.Meta, 0, len(chks))
	var last *chunks.Meta

OUTER:
	// This compares the current chunk to the chunk from the last iteration
	// by pointers.  If we use "i, c := range chks" the variable c is a new
	// variable whose address doesn't change through the entire loop.
	// The current element of the chks slice is copied into it. We must take
	// the address of the indexed slice instead.
	for i := range chks {
		for _, ignoreChkFn := range ignoreChkFns {
			ignore, err := ignoreChkFn(mint, maxt, last, &chks[i])
			if err != nil {
				return nil, errors.Wrap(err, "ignore function")
			}

			if ignore {
				continue OUTER
			}
		}

		// Clamp chunk contents to be bounded by the mint and maxt
		chk := &chks[i]
		if clampChunks {
			if clampedChk, err := clampChunk(chk, mint, maxt); err != nil {
				return nil, errors.Wrap(err, "clamping chunk")
			} else if clampedChk != nil {
				chk = clampedChk
			} else {
				continue
			}
		}

		last = chk
		repl = append(repl, *chk)
	}

	return repl, nil
}

func verifyChunks(cr *chunks.Reader, cm chunks.Meta) error {
	ch, iter, err := cr.ChunkOrIterable(cm)
	if err != nil {
		return errors.Wrapf(err, "failed to read chunk %d", cm.Ref)
	}
	if iter != nil {
		return errors.Errorf("chunk %d: ChunkOrIterable shouldn't return an iterable", cm.Ref)
	}

	cb := ch.Bytes()
	if len(cb) == 0 {
		return errors.Errorf("empty chunk %d", cm.Ref)
	}

	samples := 0
	firstSample := true
	prevTs := int64(-1)

	it := ch.Iterator(nil)
	for valType := it.Next(); valType != chunkenc.ValNone; valType = it.Next() {
		samples++

		if valType != chunkenc.ValFloat && valType != chunkenc.ValHistogram && valType != chunkenc.ValFloatHistogram {
			return errors.Errorf("unsupported value type %v in chunk %d", valType, cm.Ref)
		}

		ts := it.AtT()

		if firstSample {
			firstSample = false
			if ts != cm.MinTime {
				return errors.Errorf("first sample doesn't match chunk MinTime, chunk: %d, chunk MinTime: %s, sample timestamp: %s", cm.Ref, formatTimestamp(cm.MinTime), formatTimestamp(ts))
			}
		} else if ts <= prevTs {
			return errors.Errorf("out of order sample timestamps, chunk %d, previous timestamp: %s, sample timestamp: %s", cm.Ref, formatTimestamp(prevTs), formatTimestamp(ts))
		}

		prevTs = ts
	}

	if e := it.Err(); e != nil {
		return errors.Wrapf(e, "failed to iterate over chunk samples, chunk %d", cm.Ref)
	} else if samples == 0 {
		return errors.Errorf("no samples found in chunk %d", cm.Ref)
	} else if prevTs != cm.MaxTime {
		return errors.Errorf("last sample doesn't match chunk MaxTime, chunk: %d, chunk MaxTime: %s, sample timestamp: %s", cm.Ref, formatTimestamp(cm.MaxTime), formatTimestamp(prevTs))
	}

	return nil
}

type seriesRepair struct {
	lset labels.Labels
	chks []chunks.Meta
}

// This is tsdb.IndexReader without the PostingsForMatchers method,
// which index.Reader does not implement.
type indexReader interface {
	Symbols() index.StringIter
	SortedLabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, error)
	LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, error)
	Postings(ctx context.Context, name string, values ...string) (index.Postings, error)
	SortedPostings(index.Postings) index.Postings
	ShardedPostings(p index.Postings, shardIndex, shardCount uint64) index.Postings
	Series(ref storage.SeriesRef, builder *labels.ScratchBuilder, chks *[]chunks.Meta) error
	LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, error)
	LabelNamesFor(ctx context.Context, p index.Postings) ([]string, error)
	Close() error
}

// rewrite writes all data from the readers back into the writers while cleaning
// up mis-ordered and duplicated chunks.
func rewrite(
	ctx context.Context,
	logger log.Logger,
	indexr indexReader, chunkr tsdb.ChunkReader,
	indexw tsdb.IndexWriter, chunkw tsdb.ChunkWriter,
	meta *Meta,
	clampChunks bool,
	ignoreChkFns []ignoreFnType,
) error {
	symbols := indexr.Symbols()
	for symbols.Next() {
		if err := indexw.AddSymbol(symbols.At()); err != nil {
			return errors.Wrap(err, "add symbol")
		}
	}
	if symbols.Err() != nil {
		return errors.Wrap(symbols.Err(), "next symbol")
	}

	n, v := index.AllPostingsKey()
	all, err := indexr.Postings(ctx, n, v)
	if err != nil {
		return errors.Wrap(err, "postings")
	}
	all = indexr.SortedPostings(all)

	// We fully rebuild the postings list index from merged series.
	var (
		postings = index.NewMemPostings()
		values   = map[string]stringset{}
		i        = storage.SeriesRef(0)
		series   = []seriesRepair{}
	)

	var builder labels.ScratchBuilder
	var chks []chunks.Meta
	for all.Next() {
		id := all.At()

		if err := indexr.Series(id, &builder, &chks); err != nil {
			return errors.Wrap(err, "series")
		}
		// Make sure labels are in sorted order.
		builder.Sort()

		for i, c := range chks {
			chunk, iter, err := chunkr.ChunkOrIterable(c)
			if err != nil {
				return errors.Wrap(err, "chunk read")
			}
			if iter != nil {
				return errors.New("unexpected chunk iterable returned")
			}
			chks[i].Chunk = chunk
		}

		chks, err := sanitizeChunkSequence(chks, meta.MinTime, meta.MaxTime, clampChunks, ignoreChkFns)
		if err != nil {
			return err
		}

		if len(chks) == 0 {
			continue
		}

		series = append(series, seriesRepair{
			lset: builder.Labels(),
			chks: chks,
		})
	}

	if all.Err() != nil {
		return errors.Wrap(all.Err(), "iterate series")
	}

	// Sort the series, if labels are re-ordered then the ordering of series
	// will be different.
	slices.SortFunc(series, func(i, j seriesRepair) int {
		return labels.Compare(i.lset, j.lset)
	})

	lastSet := labels.Labels{}
	// Build a new TSDB block.
	for _, s := range series {
		// The TSDB library will throw an error if we add a series with
		// identical labels as the last series. This means that we have
		// discovered a duplicate time series in the old block. We drop
		// all duplicate series preserving the first one.
		// TODO: Add metric to count dropped series if repair becomes a daemon
		// rather than a batch job.
		if labels.Compare(lastSet, s.lset) == 0 {
			level.Warn(logger).Log("msg",
				"dropping duplicate series in tsdb block found",
				"labelset", s.lset.String(),
			)
			continue
		}
		if err := chunkw.WriteChunks(s.chks...); err != nil {
			return errors.Wrap(err, "write chunks")
		}
		if err := indexw.AddSeries(i, s.lset, s.chks...); err != nil {
			return errors.Wrap(err, "add series")
		}

		updateStats(&meta.Stats, 1, s.chks)

		s.lset.Range(func(l labels.Label) {
			valset, ok := values[l.Name]
			if !ok {
				valset = stringset{}
				values[l.Name] = valset
			}
			valset.set(l.Value)
		})
		postings.Add(i, s.lset)
		i++
		lastSet = s.lset
	}
	return nil
}

func updateStats(stats *tsdb.BlockStats, series uint64, chks []chunks.Meta) {
	stats.NumSeries += series
	stats.NumChunks += uint64(len(chks))
	for _, chk := range chks {
		numSamples := uint64(chk.Chunk.NumSamples())
		stats.NumSamples += numSamples
		switch chk.Chunk.Encoding() {
		case chunkenc.EncHistogram, chunkenc.EncFloatHistogram:
			stats.NumHistogramSamples += numSamples
		case chunkenc.EncXOR:
			stats.NumFloatSamples += numSamples
		}
	}
}

// clampChunk returns a chunk that contains only samples within [mint, maxt),
// else nil if no samples were within that range.
func clampChunk(input *chunks.Meta, minT, maxT int64) (*chunks.Meta, error) {
	// Ignore chunks that are empty or all outside
	if input.Chunk.NumSamples() <= 0 || input.MinTime >= maxT || input.MaxTime < minT {
		return nil, nil
	}
	// Return chunks that are all inside
	if minT <= input.MinTime && input.MaxTime < maxT {
		return input, nil
	}

	newChunk, err := chunkenc.NewEmptyChunk(input.Chunk.Encoding())
	if err != nil {
		return nil, err
	}
	app, err := newChunk.Appender()
	if err != nil {
		return nil, err
	}

	chunkMinT := int64(math.MaxInt64)
	chunkMaxT := int64(0)
	iter := input.Chunk.Iterator(nil)
	for valType := iter.Next(); valType != chunkenc.ValNone; valType = iter.Next() {
		// When adding samples, minT is inclusive and maxT is exclusive
		if t, v := iter.At(); t >= minT && t < maxT {
			chunkMinT = min(chunkMinT, t)
			chunkMaxT = max(chunkMaxT, t)
			// TODO(krajorama): test the ST is correctly passed when chunk
			// format with ST is available.
			app.Append(iter.AtST(), t, v)
		}
	}

	if newChunk.NumSamples() > 0 {
		return &chunks.Meta{
			MinTime: chunkMinT,
			MaxTime: chunkMaxT,
			Chunk:   newChunk,
		}, nil
	}
	return nil, nil
}

type stringset map[string]struct{}

func (ss stringset) set(s string) {
	ss[s] = struct{}{}
}

func (ss stringset) String() string {
	return strings.Join(ss.slice(), ",")
}

func (ss stringset) slice() []string {
	slice := make([]string, 0, len(ss))
	for k := range ss {
		slice = append(slice, k)
	}
	slices.Sort(slice)
	return slice
}

func formatTimestamp(ts int64) string {
	return fmt.Sprintf("%d (%s)", ts, timestamp.Time(ts).UTC().Format(time.RFC3339Nano))
}
