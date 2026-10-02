// SPDX-License-Identifier: AGPL-3.0-only

// Package cardpoc runs the index-header series-count experiments against
// generated fixtures.
package cardpoc

// E1: the series count derived from a postings list's byte span in the index-header
// equals a real postings walk over the full index, for every label value in every block.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	gokitlog "github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/indexheader"
)

// E1Result is one block's outcome from RunE1.
type E1Result struct {
	Block string
	// NoMatcherMismatch is set if the all-series ("", "") postings span
	// disagreed with a full walk; empty when it agreed.
	NoMatcherMismatch string
	// ValuesChecked is every other (name, value) pair in the block that was
	// checked the same way.
	ValuesChecked int
	Mismatches    []string // "name=value: index-header N, walk M"
}

// Exact reports whether every check in res agreed.
func (res E1Result) Exact() bool {
	return res.NoMatcherMismatch == "" && len(res.Mismatches) == 0
}

// RunE1 checks the core per-block claim for every block under
// bucketDir/anonymous: a label value's index-header postings byte span
// gives (End-Start-4)/4, and that must equal a real postings walk over the
// full index for the same value. This is checked for the no-matcher
// ("", "") case specifically, since that is the whole-block count Proposal
// 1 relies on, and then for every other label value in the block.
func RunE1(bucketDir string) ([]E1Result, error) {
	anonymousDir := filepath.Join(bucketDir, "anonymous")
	entries, err := os.ReadDir(anonymousDir)
	if err != nil {
		return nil, err
	}

	ubkt, err := filesystem.NewBucket(anonymousDir)
	if err != nil {
		return nil, err
	}
	defer ubkt.Close()
	bkt := objstore.WithNoopInstr(ubkt)

	var results []E1Result
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		id, err := ulid.Parse(e.Name())
		if err != nil {
			continue // not a block directory.
		}
		res, err := checkBlockE1(bkt, anonymousDir, id)
		if err != nil {
			return nil, fmt.Errorf("block %s: %w", e.Name(), err)
		}
		results = append(results, res)
	}
	return results, nil
}

func checkBlockE1(bkt objstore.InstrumentedBucketReader, anonymousDir string, id ulid.ULID) (E1Result, error) {
	ctx := context.Background()
	res := E1Result{Block: id.String()}

	metrics := indexheader.NewStreamBinaryReaderMetrics(nil)
	hr, err := indexheader.NewStreamBinaryReader(ctx, id, bkt, anonymousDir, indexheader.Config{}, 32, gokitlog.NewNopLogger(), metrics)
	if err != nil {
		return res, fmt.Errorf("opening index-header: %w", err)
	}
	defer hr.Close()

	fr, err := index.NewFileReader(filepath.Join(anonymousDir, id.String(), "index"), index.DecodePostingsRaw)
	if err != nil {
		return res, fmt.Errorf("opening full index: %w", err)
	}
	defer fr.Close()

	// The no-matcher case: the whole-block series count.
	noMatcherSpan, err := hr.PostingsOffset(ctx, "", "")
	if err != nil {
		return res, fmt.Errorf("no-matcher postings offset: %w", err)
	}
	noMatcherWalk, err := walkCount(ctx, fr, "", "")
	if err != nil {
		return res, err
	}
	if computed := spanCount(noMatcherSpan.Start, noMatcherSpan.End); computed != noMatcherWalk {
		res.NoMatcherMismatch = fmt.Sprintf("index-header %d, walk %d", computed, noMatcherWalk)
	}

	// Every other label value in the block.
	names, err := hr.LabelNames(ctx)
	if err != nil {
		return res, err
	}
	for _, name := range names {
		offsets, err := hr.LabelValuesOffsets(ctx, name, "", nil)
		if err != nil {
			return res, fmt.Errorf("label %s: %w", name, err)
		}
		for _, off := range offsets {
			res.ValuesChecked++
			computed := spanCount(off.Off.Start, off.Off.End)

			walked, err := walkCount(ctx, fr, name, off.LabelValue)
			if err != nil {
				return res, err
			}
			if computed != walked {
				res.Mismatches = append(res.Mismatches, fmt.Sprintf("%s=%s: index-header %d, walk %d", name, off.LabelValue, computed, walked))
			}
		}
	}
	return res, nil
}

// spanCount is the series-count formula: a postings list's byte span, minus the
// 4-byte entry-count field, divided by 4 bytes per entry.
func spanCount(start, end int64) int64 {
	return (end - start - 4) / 4
}

func walkCount(ctx context.Context, r *index.Reader, name, value string) (int64, error) {
	p, err := r.Postings(ctx, name, value)
	if err != nil {
		return 0, err
	}
	var n int64
	for p.Next() {
		n++
	}
	return n, p.Err()
}
