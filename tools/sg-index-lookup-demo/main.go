// SPDX-License-Identifier: AGPL-3.0-only

// sg-index-lookup-demo runs the store-gateway's ExpandedPostings path against a
// block stored on local disk.  It uses the same code path a store-gateway replica
// would follow during a query, but without a running server, gRPC, or remote bucket.
//
// The purpose is to make the effect of the lookup planner visible: run the same
// matcher set through noopLookupPlanner (baseline) and through
// ScanEmptyMatchersLookupPlanner, then print which matchers each planner pushed
// to the scan phase and what posting count each produced.
//
// Usage:
//
//	go run ./tools/sg-index-lookup-demo <block-dir> '<{matchers}>'
//
// Example:
//
//	go run ./tools/sg-index-lookup-demo /data/01HXZ4ABCDEF... '{job="foo",i=~".+"}'
//
// The <block-dir> must be a TSDB block directory (named with its ULID) that
// contains meta.json, index, and chunks/.  The parent directory is mounted as the
// filesystem bucket, so the block path inside the bucket is <ulid>/...

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/indexheader"
	mimirtsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 3 {
		return fmt.Errorf("usage: %s <block-dir> '<{matchers}>'", os.Args[0])
	}

	blockDir := filepath.Clean(os.Args[1])
	matcherExpr := os.Args[2]

	pql := parser.NewParser(parser.Options{})
	matchers, err := pql.ParseMetricSelector(matcherExpr)
	if err != nil {
		return fmt.Errorf("parse matchers %q: %w", matcherExpr, err)
	}
	fmt.Printf("matchers : %s\n\n", formatMatchers(matchers))

	// blockDir is <root>/<ulid>; the parent is the bucket root.
	blockULIDStr := filepath.Base(blockDir)
	bucketRoot := filepath.Dir(blockDir)

	blockULID, err := ulid.Parse(blockULIDStr)
	if err != nil {
		return fmt.Errorf("block directory name %q is not a valid ULID: %w", blockULIDStr, err)
	}

	meta, err := block.ReadMetaFromDir(blockDir)
	if err != nil {
		return fmt.Errorf("read meta.json from %s: %w", blockDir, err)
	}

	fsBkt, err := filesystem.NewBucket(bucketRoot)
	if err != nil {
		return fmt.Errorf("open filesystem bucket at %s: %w", bucketRoot, err)
	}
	defer fsBkt.Close()
	instrBkt := objstore.WithNoopInstr(fsBkt)

	// Build the index-header in a temp dir; it is discarded after the run.
	indexHeaderDir, err := os.MkdirTemp("", "sg-demo-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(indexHeaderDir)

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	fmt.Fprintln(os.Stderr, "building index-header…")

	headerReader, err := indexheader.NewStreamBinaryReader(
		context.Background(),
		blockULID,
		instrBkt,
		indexHeaderDir,
		indexheader.Config{},
		mimirtsdb.DefaultPostingOffsetInMemorySampling,
		logger,
		indexheader.NewStreamBinaryReaderMetrics(nil),
	)
	if err != nil {
		return fmt.Errorf("build index-header: %w", err)
	}
	defer headerReader.Close()

	ctx := context.Background()

	fmt.Println("=== noop planner (all matchers resolved via index) ===")
	if err := runWithPlanner(ctx, meta, instrBkt, headerReader, matchers, storegateway.NoopLookupPlanner()); err != nil {
		return fmt.Errorf("noop planner: %w", err)
	}
	fmt.Println()

	fmt.Println("=== ScanEmptyMatchersLookupPlanner (defers i=\"\", i=~\".+\"; drops i=~\".*\") ===")
	if err := runWithPlanner(ctx, meta, instrBkt, headerReader, matchers, &index.ScanEmptyMatchersLookupPlanner{}); err != nil {
		return fmt.Errorf("ScanEmptyMatchersLookupPlanner: %w", err)
	}
	fmt.Println()

	fmt.Fprintln(os.Stderr, "building cost-based planner statistics…")
	costPlanner, err := storegateway.BuildCostBasedPlanner(blockDir, meta, logger)
	if err != nil {
		return fmt.Errorf("build cost-based planner: %w", err)
	}
	fmt.Println("=== CostBasedPlanner ===")
	if err := runWithPlanner(ctx, meta, instrBkt, headerReader, matchers, costPlanner); err != nil {
		return fmt.Errorf("CostBasedPlanner: %w", err)
	}

	return nil
}

func runWithPlanner(
	ctx context.Context,
	meta *block.Meta,
	bkt objstore.InstrumentedBucketReader,
	headerReader indexheader.Reader,
	matchers []*labels.Matcher,
	planner index.LookupPlanner,
) error {
	bb := storegateway.NewBucketBlockForDemo(meta, bkt, headerReader, planner, log.NewNopLogger())
	refs, scanMatchers, err := bb.ExpandedPostings(ctx, matchers)
	if err != nil {
		return err
	}

	fmt.Printf("  posting count : %d\n", len(refs))
	if len(scanMatchers) == 0 {
		fmt.Printf("  scan matchers : (none)\n")
	} else {
		fmt.Printf("  scan matchers : %s\n", formatMatchers(scanMatchers))
		fmt.Printf("  index matchers: %s\n", formatMatchers(subtract(matchers, scanMatchers)))
	}
	return nil
}

func formatMatchers(ms []*labels.Matcher) string {
	if len(ms) == 0 {
		return "{}"
	}
	parts := make([]string, len(ms))
	for i, m := range ms {
		parts[i] = m.String()
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// subtract returns the elements of all that are not present in drop (by pointer identity).
func subtract(all, drop []*labels.Matcher) []*labels.Matcher {
	dropSet := make(map[*labels.Matcher]struct{}, len(drop))
	for _, m := range drop {
		dropSet[m] = struct{}{}
	}
	out := make([]*labels.Matcher, 0, len(all)-len(drop))
	for _, m := range all {
		if _, ok := dropSet[m]; !ok {
			out = append(out, m)
		}
	}
	return out
}
