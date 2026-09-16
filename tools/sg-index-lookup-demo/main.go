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

	"github.com/grafana/mimir/pkg/ingester/lookupplan"
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

	/*
		fmt.Println("=== ScanEmptyMatchersLookupPlanner (defers i=\"\", i=~\".+\"; drops i=~\".*\") ===")
		if err := runWithPlanner(ctx, meta, instrBkt, headerReader, matchers, &index.ScanEmptyMatchersLookupPlanner{}); err != nil {
			return fmt.Errorf("ScanEmptyMatchersLookupPlanner: %w", err)
		}
		fmt.Println()

		fmt.Fprintln(os.Stderr, "building cost-based planner statistics…")
		ingestCostPlanner, err := storegateway.BuildCostBasedPlanner(blockDir, meta, ingestCostConfig(), logger)
		if err != nil {
			return fmt.Errorf("build cost-based planner (ingester config): %w", err)
		}
		fmt.Println("=== CostBasedPlanner (ingester cost config) ===")
		if err := runWithPlanner(ctx, meta, instrBkt, headerReader, matchers, ingestCostPlanner); err != nil {
			return fmt.Errorf("CostBasedPlanner (ingester): %w", err)
		}
		fmt.Println()
	*/

	sgPlanner, err := storegateway.BuildCostBasedPlanner(blockDir, meta, sgCostConfig(), logger)
	if err != nil {
		return fmt.Errorf("build cost-based planner (sg config): %w", err)
	}
	fmt.Println("=== CostBasedPlanner (store-gateway cost config) ===")
	if err := runWithPlanner(ctx, meta, instrBkt, headerReader, matchers, sgPlanner); err != nil {
		return fmt.Errorf("CostBasedPlanner (sg): %w", err)
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

// ingestCostConfig returns the default ingester cost config, for comparison.
func ingestCostConfig() lookupplan.CostConfig {
	return lookupplan.CostConfig{
		RetrievedPostingCost:              lookupplan.DefaultRetrievedPostingCost,
		RetrievedPostingListCost:          lookupplan.DefaultRetrievedPostingListCost,
		RetrievedSeriesCost:               lookupplan.DefaultRetrievedSeriesCost,
		MinSeriesPerBlockForQueryPlanning: 0,
		LabelCardinalityForLargerSketch:   lookupplan.DefaultLabelCardinalityForLargerSketch,
		LabelCardinalityForSmallerSketch:  lookupplan.DefaultLabelCardinalityForSmallerSketch,
	}
}

// sgCostConfig returns a CostConfig tuned for the store-gateway's cost profile.
//
// The ingester defaults model in-memory costs: a fixed overhead per posting list
// (RetrievedPostingListCost=10) and a cheap per-series load (RetrievedSeriesCost=15).
// Both are wrong for the store-gateway, where the bottleneck is bytes transferred
// from object storage, not per-list or per-series overhead.
//
// The two key corrections:
//
//  1. RetrievedPostingListCost — the ingester charges a flat fee per posting list
//     fetched (10 units × number of matched lists). In the store-gateway, posting lists
//     for a given label are laid out contiguously in the index file and the partitioner
//     coalesces them into a small number of range reads. The actual cost is proportional
//     to posting bytes transferred (cardinality × 4 bytes/ref), not number of lists.
//     Setting RetrievedPostingListCost to a per-byte equivalent: 4 bytes/ref ×
//     RetrievedPostingCost gives exactly cardinality × RetrievedPostingCost, so we
//     set RetrievedPostingListCost = 4 × RetrievedPostingCost.
//     But the existing formula multiplies by uniqueVals × selectivity, which
//     approximates cardinality (uniqueVals × selectivity ≈ fraction of values matched,
//     not series count). To make the per-byte model work within the existing formula
//     we set RetrievedPostingListCost low enough that per-list overhead is negligible
//     and the RetrievedPostingCost term (which does multiply by cardinality) dominates.
//
//  2. RetrievedSeriesCost — the ingester uses 15 units/series (cheap in-memory access).
//     In the store-gateway, loading a series entry from the index means a range read
//     from object storage. Series refs after intersection are scattered across the
//     512-byte-aligned series section; the partitioner uses a 17 KB window per ref,
//     so coalescing is poor compared to the dense posting section. Each series read
//     transfers ~512 bytes on average (EstimatedSeriesP99Size), with low coalescing
//     efficiency. We want this cost to dominate over posting fetch when numSelected
//     is large — and to be proportional to bytes transferred.
//
//     We express everything relative to RetrievedPostingCost=1 (one unit per posting
//     ref iterated). A posting ref is 4 bytes. A series entry is ~512 bytes and is
//     read with poor locality. The byte ratio is 512/4 = 128, with an additional
//     scatter penalty of ~4×, giving RetrievedSeriesCost ≈ 512.
//
// With these values the planner compares posting I/O bytes vs series I/O bytes:
//   - posting fetch:    cardinality × 4 bytes  (via RetrievedPostingCost, 4 bytes/ref)
//   - series fetch:     numSelected × 512 bytes (EstimatedSeriesP99Size, scattered reads)
//
// Known limitation: indexScanCost is incommensurable with the byte-denominated costs:
//
// indexScanCost = singleMatchCost × uniqueVals, where singleMatchCost is a dimensionless
// CPU complexity estimate from the regex engine (equality=1, prefix=1, .+=10, complex
// alternation=45+). This models the cost of running the matcher against label value strings
// in the postings offset table. In the store-gateway, that offset table scan is not
// necessarily cheap:
//
//   - Default config: the postings offset table is read from a local index-header file on
//     disk via a file pool. Reads are O(uniqueVals / sparseSampleFactor) random seeks into
//     a potentially large file (the 25 GB block in these experiments has a 1.7 GB postings
//     offset table). The file is not mmap'd; each sparse entry seek is a real file read.
//
//   - Experimental BucketReader config (cfg.BucketReader.Enabled=true): the postings offset
//     table is read directly from object storage via GetRange calls. LabelValuesOffsets
//     issues one GetRange per sparse section it needs to scan. This makes the offset table
//     scan a genuine remote I/O operation.
//
// So "offset-table scanning is just CPU" is not accurate for the store-gateway. The scan
// is disk I/O in the common case, and remote I/O in the experimental bucket-reader case.
// indexScanCost is therefore not categorically wrong as a cost term — but it is still
// incommensurable with the byte-denominated I/O costs, because singleMatchCost is a regex
// engine complexity estimate with no byte or latency unit, making plan comparisons
// sensitive to regex structure in ways that don't correspond to actual I/O.
//
// Observed effect: for job=~"((cortex|mimir)-.+)/((ingester.*|cortex|mimir))"
// (singleMatchCost=45, 13892 unique job values), indexScanCost=625140 dominates the plan
// comparison. The planner avoids resolving job via the index, which happens to be the
// right call, but for a reason that doesn't generalise: a simpler regexp with the same
// actual posting list size would have a lower singleMatchCost and potentially be chosen
// for index lookup even when it shouldn't be.
//
// The correct fix requires a cost parameter that reflects actual offset-table I/O bytes
// (bytes scanned = uniqueVals / sparseSampleFactor × avg entry size) rather than regex
// CPU complexity. That is not yet available in CostConfig.
func sgCostConfig() lookupplan.CostConfig {
	return lookupplan.CostConfig{
		// 1 unit = 4 bytes (one posting ref).
		RetrievedPostingCost: 4.0,
		// Per-list overhead is negligible; posting bytes are already captured by
		// RetrievedPostingCost × cardinality via the intersectionCost term.
		RetrievedPostingListCost: 0.0,
		// One series entry = ~512 bytes read from scattered locations in the index.
		// Coalescing is poor (17 KB window, non-contiguous refs after intersection).
		RetrievedSeriesCost:               512.0,
		MinSeriesPerBlockForQueryPlanning: 0,
		LabelCardinalityForLargerSketch:   lookupplan.DefaultLabelCardinalityForLargerSketch,
		LabelCardinalityForSmallerSketch:  lookupplan.DefaultLabelCardinalityForSmallerSketch,
	}
}
