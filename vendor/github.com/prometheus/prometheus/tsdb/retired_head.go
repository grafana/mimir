// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/oklog/ulid/v2"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

// retiredHead is a frozen, read-only head whose inverted index has been
// written to disk and opened with the standard 1-in-32 sampled reader.
// Chunk data is still served from the original Head's stripeSeries and
// ChunkDiskMapper.
type retiredHead struct {
	head    *Head       // kept alive for chunk reads via stripeSeries + ChunkDiskMapper
	indexr  IndexReader // 1-in-32 sampled, disk-backed (wraps *index.Reader + PostingsForMatchers)
	dir     string      // directory containing the index file
	minT    int64
	maxT    int64
	blockID ulid.ULID
}

var _ BlockReader = (*retiredHead)(nil)

func (rh *retiredHead) Index() (IndexReader, error) {
	return retiredHeadIndexReader{rh.indexr}, nil
}

// retiredHeadIndexReader wraps the retired head's index reader so that
// Close() is a no-op. Ownership of the underlying reader stays with the
// retiredHead — it is closed only when retiredHead.Close() is called.
// This matches the pattern used by blockIndexReader for regular blocks.
type retiredHeadIndexReader struct {
	IndexReader
}

func (retiredHeadIndexReader) Close() error { return nil }

func (rh *retiredHead) Chunks() (ChunkReader, error) {
	return rh.head.chunksRange(math.MinInt64, math.MaxInt64, rh.head.iso.State(math.MinInt64, math.MaxInt64))
}

func (rh *retiredHead) Tombstones() (tombstones.Reader, error) {
	return tombstones.NewMemTombstones(), nil
}

func (rh *retiredHead) Meta() BlockMeta {
	return BlockMeta{
		ULID:    rh.blockID,
		MinTime: rh.minT,
		MaxTime: rh.maxT,
	}
}

func (rh *retiredHead) Size() int64 {
	fi, err := os.Stat(filepath.Join(rh.dir, indexFilename))
	if err != nil {
		return 0
	}
	return fi.Size()
}

// Close releases all resources: closes the index reader, closes the
// underlying head (which closes its ChunkDiskMapper), and removes the
// index directory from disk.
func (rh *retiredHead) Close() error {
	indexErr := rh.indexr.Close()
	headErr := rh.head.Close()
	removeErr := os.RemoveAll(rh.dir)
	if indexErr != nil {
		return indexErr
	}
	if headErr != nil {
		return headErr
	}
	return removeErr
}

// OverlapsClosedInterval reports whether the retired head's time range
// overlaps with [mint, maxt].
func (rh *retiredHead) OverlapsClosedInterval(mint, maxt int64) bool {
	return overlapsClosedInterval(mint, maxt, rh.minT, rh.maxT)
}

// buildRetiredHeadIndex writes a standard block index file from the
// head's current data and opens it with the 1-in-32 sampled reader.
//
// The index file contains symbols, series (labels + chunk metas), and
// postings. No chunk data is written — only index metadata.
func buildRetiredHeadIndex(head *Head, dir string) (IndexReader, error) {
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return nil, fmt.Errorf("create retired head dir: %w", err)
	}

	ir, err := head.Index()
	if err != nil {
		return nil, fmt.Errorf("get head index reader: %w", err)
	}
	defer ir.Close()

	ctx := context.Background()
	indexPath := filepath.Join(dir, indexFilename)

	iw, err := index.NewWriter(ctx, indexPath)
	if err != nil {
		return nil, fmt.Errorf("create index writer: %w", err)
	}

	syms := ir.Symbols()
	for syms.Next() {
		if err := iw.AddSymbol(syms.At()); err != nil {
			_ = iw.Close()
			return nil, fmt.Errorf("add symbol: %w", err)
		}
	}
	if err := syms.Err(); err != nil {
		_ = iw.Close()
		return nil, fmt.Errorf("iterate symbols: %w", err)
	}

	k, v := index.AllPostingsKey()
	allPostings, err := ir.Postings(ctx, k, v)
	if err != nil {
		_ = iw.Close()
		return nil, fmt.Errorf("get all postings: %w", err)
	}

	var (
		builder labels.ScratchBuilder
		chks    []chunks.Meta
	)
	for allPostings.Next() {
		ref := allPostings.At()
		chks = chks[:0]

		if err := ir.Series(ref, &builder, &chks); err != nil {
			_ = iw.Close()
			return nil, fmt.Errorf("read series %d: %w", ref, err)
		}
		if err := iw.AddSeries(ref, builder.Labels(), chks...); err != nil {
			_ = iw.Close()
			return nil, fmt.Errorf("write series %d: %w", ref, err)
		}
	}
	if err := allPostings.Err(); err != nil {
		_ = iw.Close()
		return nil, fmt.Errorf("iterate postings: %w", err)
	}

	if err := iw.Close(); err != nil {
		return nil, fmt.Errorf("close index writer: %w", err)
	}

	fileReader, err := index.NewFileReader(indexPath, index.DecodePostingsRaw)
	if err != nil {
		return nil, fmt.Errorf("open index reader: %w", err)
	}

	pfmc := DefaultPostingsForMatchersCacheFactory.NewPostingsForMatchersCache(nil)
	var blockID ulid.ULID
	return indexReaderWithPostingsForMatchers{blockID, fileReader, pfmc}, nil
}

// clearMemPostings replaces the head's MemPostings with an empty instance,
// reclaiming the memory used by the full inverted index.
func clearMemPostings(head *Head) {
	head.postings = index.NewMemPostings()
}
