// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/storepb/testutil/series.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package fixtures

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

const (
	// LabelLongSuffix is a label with ~50B in size, to emulate real-world high cardinality.
	LabelLongSuffix = "aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"
)

// FsInstrumentedBucket implements objstore.InstrumentedBucket with filesystem.Bucket.
// It retains a reference to the root directory for the bucket,
// which is generally formed with filepath.Join(<test root TmpDir>, "bkt").
// The directory is consumed by tests to check bucket objects via filesystem access.
type FsInstrumentedBucket struct {
	rootDir string
	fsBkt   *filesystem.Bucket
	objstore.InstrumentedBucket
}

func NewFsInstrumentedBucket(rootDir string) (*FsInstrumentedBucket, error) {
	fsBkt, err := filesystem.NewBucket(rootDir)
	if err != nil {
		return nil, err
	}

	instrBkt := objstore.WithNoopInstr(fsBkt)
	return &FsInstrumentedBucket{
		rootDir:            rootDir,
		fsBkt:              fsBkt,
		InstrumentedBucket: instrBkt,
	}, nil
}

func (b *FsInstrumentedBucket) RootDir() string {
	return b.rootDir
}

func (b *FsInstrumentedBucket) Close() error {
	_ = b.fsBkt.Close() // Close is a no-op on filesystem bucket
	return b.InstrumentedBucket.Close()
}

type BucketTestBlock struct {
	Meta   *block.Meta
	UserID string

	InstrBkt *FsInstrumentedBucket

	logger log.Logger
}

func (b *BucketTestBlock) Close() error {
	return b.InstrBkt.Close()
}

type AppendFunc = func(tb testing.TB, appenderFactory func() storage.Appender)

func AppendTestSeries(seriesCount int) AppendFunc {
	return func(t testing.TB, appenderFactory func() storage.Appender) {
		app := appenderFactory()
		b := labels.NewScratchBuilder(4)
		addSeries := func(ss ...string) {
			b.Reset()
			for i := 0; i < len(ss); i += 2 {
				b.Add(ss[i], ss[i+1])
			}
			b.Sort()
			_, err := app.Append(0, b.Labels(), 0, 0)
			assert.NoError(t, err)
		}

		seriesCount = seriesCount / 5
		for n := 0; n < 10; n++ {
			for i := 0; i < seriesCount/10; i++ {

				addSeries("i", strconv.Itoa(i)+LabelLongSuffix, "n", strconv.Itoa(n)+LabelLongSuffix, "j", "foo", "p", "foo")
				// Have some series that won't be matched, to properly test inverted matches.
				addSeries("i", strconv.Itoa(i)+LabelLongSuffix, "n", strconv.Itoa(n)+LabelLongSuffix, "j", "bar", "q", "foo")
				addSeries("i", strconv.Itoa(i)+LabelLongSuffix, "n", "0_"+strconv.Itoa(n)+LabelLongSuffix, "j", "bar", "r", "foo")
				addSeries("i", strconv.Itoa(i)+LabelLongSuffix, "n", "1_"+strconv.Itoa(n)+LabelLongSuffix, "j", "bar", "s", "foo")
				addSeries("i", strconv.Itoa(i)+LabelLongSuffix, "n", "2_"+strconv.Itoa(n)+LabelLongSuffix, "j", "foo", "t", "foo")
			}
			assert.NoError(t, app.Commit())
			app = appenderFactory()
		}
	}
}

func SetupTestBlock(tb testing.TB, appendFuncs ...AppendFunc) *BucketTestBlock {
	// Setup directories from test temp filesystem

	// Root dir from test to use for everything else
	testTmpDir := tb.TempDir()

	// Dir for writing/compacting the temp block before bucket upload,
	// analogous to ingester or block-builder storage
	localBlockBuilderDir := filepath.Join(testTmpDir, "block-builder")

	// Dir to underlie the filesystem-based test bucket
	bucketDir := filepath.Join(testTmpDir, "bkt")

	// Setup temp bucket from test filesystem & apply instrumentation
	instrBkt, err := NewFsInstrumentedBucket(bucketDir)
	assert.NoError(tb, err)

	// Initialize in-memory block
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = testTmpDir
	headOpts.ChunkRange = 1000
	headOpts.IsolationDisabled = true
	head, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	assert.NoError(tb, err)
	defer func() {
		assert.NoError(tb, head.Close())
	}()

	// Write data to block
	for _, appendFunc := range appendFuncs {
		appendFunc(tb, func() storage.Appender { return head.Appender(context.Background()) })
	}

	// Compact block to temporary location on disk
	assert.NoError(tb, os.MkdirAll(localBlockBuilderDir, os.ModePerm))
	// Put a 4 MiB limit on segment files so we can test with many segment files without creating too big blocks.
	opts := tsdb.LeveledCompactorOptions{
		MaxBlockChunkSegmentSize:    4 * 1024 * 1024,
		EnableOverlappingCompaction: true,
	}
	compactor, err := tsdb.NewLeveledCompactorWithOptions(context.Background(), nil, promslog.NewNopLogger(), []int64{1000000}, nil, opts)
	assert.NoError(tb, err)
	// Add +1 millisecond to block maxt because block intervals are half-open: [b.MinTime, b.MaxTime).
	// Because of this block intervals are always +1 than the total samples it includes.
	ulids, err := compactor.Write(localBlockBuilderDir, head, head.MinTime(), head.MaxTime()+1, nil)
	assert.NoError(tb, err)
	assert.Len(tb, ulids, 1)
	blockID := ulids[0]

	compactedBlockDir := filepath.Join(localBlockBuilderDir, blockID.String())

	// Upload block
	meta, err := block.InjectThanosMeta(log.NewNopLogger(), compactedBlockDir, block.ThanosMeta{
		Labels: labels.FromStrings("ext1", "1").Map(),
		Source: block.TestSource,
	}, nil)
	assert.NoError(tb, err)
	_, err = block.Upload(context.Background(), log.NewNopLogger(), instrBkt, compactedBlockDir, nil)
	assert.NoError(tb, err)

	testBlock := &BucketTestBlock{
		Meta:     meta,
		UserID:   "",
		InstrBkt: instrBkt,
		logger:   nil,
	}
	tb.Cleanup(func() {
		assert.NoError(tb, testBlock.Close())
	})
	return testBlock
}

type SeriesSelectionTestCase struct {
	Name          string
	Matchers      []*labels.Matcher
	ExpectedCount int
}

// SeriesSelectorTestCases expands upon the similar Prometheus benchmark cases
// https://github.com/prometheus/prometheus/blob/25aee26/tsdb/querier_bench_test.go#L102
func SeriesSelectorTestCases(
	tb testing.TB,
	series int,
) []SeriesSelectionTestCase {
	series = series / 5

	iUniqueValues := series / 10      // The amount of unique values for "i" label prefix. See AppendTestSeries.
	iUniqueValue := iUniqueValues / 2 // There will be 50 series matching: 5 per each series, 10 for each n. See AppendTestSeries.

	n1 := labels.MustNewMatcher(labels.MatchEqual, "n", "1"+LabelLongSuffix)
	nX := labels.MustNewMatcher(labels.MatchEqual, "n", "X"+LabelLongSuffix)
	nAnyPlus := labels.MustNewMatcher(labels.MatchRegexp, "n", ".+")
	nAnyStar := labels.MustNewMatcher(labels.MatchRegexp, "n", ".*")

	jFoo := labels.MustNewMatcher(labels.MatchEqual, "j", "foo")
	jNotFoo := labels.MustNewMatcher(labels.MatchNotEqual, "j", "foo")
	jAnyStar := labels.MustNewMatcher(labels.MatchRegexp, "j", ".*")
	jAnyPlus := labels.MustNewMatcher(labels.MatchRegexp, "j", ".+")

	iStar := labels.MustNewMatcher(labels.MatchRegexp, "i", "^.*$")
	iPlus := labels.MustNewMatcher(labels.MatchRegexp, "i", "^.+$")
	i1Plus := labels.MustNewMatcher(labels.MatchRegexp, "i", "^1.+$")
	iUniquePrefixPlus := labels.MustNewMatcher(labels.MatchRegexp, "i", fmt.Sprintf("%d.+", iUniqueValue))
	iNotUniquePrefixPlus := labels.MustNewMatcher(labels.MatchNotRegexp, "i", fmt.Sprintf("%d.+", iUniqueValue))
	iEmptyRe := labels.MustNewMatcher(labels.MatchRegexp, "i", "^$")
	iNotEmpty := labels.MustNewMatcher(labels.MatchNotEqual, "i", "")
	iNot2 := labels.MustNewMatcher(labels.MatchNotEqual, "n", "2"+LabelLongSuffix)
	iNot2Star := labels.MustNewMatcher(labels.MatchNotRegexp, "i", "^2.*$")
	iNotStar2Star := labels.MustNewMatcher(labels.MatchNotRegexp, "i", "^.*2.*$")
	jXXXYYY := labels.MustNewMatcher(labels.MatchRegexp, "j", "XXX|YYY")
	jXplus := labels.MustNewMatcher(labels.MatchRegexp, "j", "X.+")
	iRegexAlternate := labels.MustNewMatcher(labels.MatchRegexp, "i", "0"+LabelLongSuffix+"|1"+LabelLongSuffix+"|2"+LabelLongSuffix)
	iXYZ := labels.MustNewMatcher(labels.MatchRegexp, "i", "X|Y|Z")
	iRegexAlternateSuffix := labels.MustNewMatcher(labels.MatchRegexp, "i", "(0|1|2)"+LabelLongSuffix)
	iRegexClass := labels.MustNewMatcher(labels.MatchRegexp, "i", "[0-2]"+LabelLongSuffix)
	iRegexNotSetMatches := labels.MustNewMatcher(labels.MatchNotRegexp, "i", "(0|1|2)"+LabelLongSuffix)
	pNotEmpty := labels.MustNewMatcher(labels.MatchNotEqual, "p", "")
	pFoo := labels.MustNewMatcher(labels.MatchEqual, "p", "foo")

	// Just make sure that we're testing what we think we're testing.
	require.NotEmpty(tb, iRegexNotSetMatches.SetMatches(), "Should have non empty SetMatches to test the proper path.")

	return []SeriesSelectionTestCase{
		{`n="X"`, []*labels.Matcher{nX}, 0},
		{`n="X",j="foo"`, []*labels.Matcher{nX, jFoo}, 0},
		{`n="X",j!="foo"`, []*labels.Matcher{nX, jNotFoo}, 0},
		{`j=~"XXX|YYY"`, []*labels.Matcher{jXXXYYY}, 0},
		{`j=~"X.+"`, []*labels.Matcher{jXplus}, 0},
		{`i=~"X|Y|Z"`, []*labels.Matcher{iXYZ}, 0},
		{`n="1"`, []*labels.Matcher{n1}, int(float64(series) * 0.2)},
		{`n="1",j="foo"`, []*labels.Matcher{n1, jFoo}, int(float64(series) * 0.1)},
		{`j="foo",n="1"`, []*labels.Matcher{jFoo, n1}, int(float64(series) * 0.1)},
		{`n="1",j!="foo"`, []*labels.Matcher{n1, jNotFoo}, int(float64(series) * 0.1)},
		{`i=~".*"`, []*labels.Matcher{iStar}, 5 * series},
		{`i=~".+"`, []*labels.Matcher{iPlus}, 5 * series},
		{`i=~"^.+$",j=~"X.+"`, []*labels.Matcher{iPlus, jXplus}, 0},
		{`i=~""`, []*labels.Matcher{iEmptyRe}, 0},
		{`i!=""`, []*labels.Matcher{iNotEmpty}, 5 * series},
		{`n="1",i=~".*",j="foo"`, []*labels.Matcher{n1, iStar, jFoo}, int(float64(series) * 0.1)},
		{`n="X",i=~"^.+$",j="foo"`, []*labels.Matcher{nX, iStar, jFoo}, 0},
		{`n="1",i=~".*",i!="2",j="foo"`, []*labels.Matcher{n1, iStar, iNot2, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i!=""`, []*labels.Matcher{n1, iNotEmpty}, int(float64(series) * 0.2)},
		{`n="1",i!="",j="foo"`, []*labels.Matcher{n1, iNotEmpty, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i!="",j=~"X.+"`, []*labels.Matcher{n1, iNotEmpty, jXplus}, 0},
		{`n="1",i!="",j=~"XXX|YYY"`, []*labels.Matcher{n1, iNotEmpty, jXXXYYY}, 0},
		{`n="1",i=~"X|Y|Z",j="foo"`, []*labels.Matcher{n1, iXYZ, jFoo}, 0},
		{`n="1",i=~".+",j="foo"`, []*labels.Matcher{n1, iPlus, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i=~"1.+",j="foo"`, []*labels.Matcher{n1, i1Plus, jFoo}, int(float64(series) * 0.011111)},
		{`n="1",i=~".+",i!="2",j="foo"`, []*labels.Matcher{n1, iPlus, iNot2, jFoo}, int(float64(series) * 0.1)},
		{`n="1",i=~".+",i!~"2.*",j="foo"`, []*labels.Matcher{n1, iPlus, iNot2Star, jFoo}, int(1 + float64(series)*0.088888)},
		{`n="X",i=~"^.+$",i!~"^.*2.*$",j="foo"`, []*labels.Matcher{nX, iPlus, iNotStar2Star, jFoo}, 0},
		{`i=~"0xxx|1xxx|2xxx"`, []*labels.Matcher{iRegexAlternate}, 150},                        // 50 series for "1", 50 for "2" and 50 for "3".
		{`i=~"(0|1|2)xxx"`, []*labels.Matcher{iRegexAlternateSuffix}, 150},                      // 50 series for "1", 50 for "2" and 50 for "3".
		{`i=~"[0-2]xxx"`, []*labels.Matcher{iRegexClass}, 150},                                  // 50 series for "1", 50 for "2" and 50 for "3".
		{`i!~[0-2]xxx`, []*labels.Matcher{iRegexNotSetMatches}, 5*series - 150},                 // inverse of iRegexAlternateSuffix
		{`i=~".*", i!~[0-2]xxx`, []*labels.Matcher{iStar, iRegexNotSetMatches}, 5*series - 150}, // inverse of iRegexAlternateSuffix
		{`i=~"<unique_prefix>.+"`, []*labels.Matcher{iUniquePrefixPlus}, 50},
		{`n="1",i=~"<unique_prefix>.+"`, []*labels.Matcher{n1, iUniquePrefixPlus}, 2},
		{`n="1",i!~"<unique_prefix>.+"`, []*labels.Matcher{n1, iNotUniquePrefixPlus}, int(float64(series)*0.2) - 2},
		{`j="foo",p="foo",i=~"<unique_prefix>.+"`, []*labels.Matcher{jFoo, pFoo, iUniquePrefixPlus}, 10},
		{`j="foo",n=~".+",i=~"<unique_prefix>.+"`, []*labels.Matcher{jFoo, nAnyPlus, iUniquePrefixPlus}, 20},
		{`j="foo",n=~".*",i=~"<unique_prefix>.+"`, []*labels.Matcher{jFoo, nAnyStar, iUniquePrefixPlus}, 20},
		{`j=~".*",n=~".*",i=~"<unique_prefix>.+"`, []*labels.Matcher{jAnyStar, nAnyStar, iUniquePrefixPlus}, 50},
		{`j=~".+",n=~".+",i=~"<unique_prefix>.+"`, []*labels.Matcher{jAnyPlus, nAnyPlus, iUniquePrefixPlus}, 50},
		{`p!=""`, []*labels.Matcher{pNotEmpty}, series},
	}
}
