// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/indexheader"
	mimirtsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway"
)

// blockDirEnv must point to the block directory; the directory name must be the block ULID.
//
//	BENCH_BLOCK_DIR=/path/to/blocks/01M1BK... go test ./tools/sg-index-lookup-demo/ -bench=.
const blockDirEnv = "BENCH_BLOCK_DIR"

var benchCases = []struct {
	name     string
	matchers []*labels.Matcher
}{
	{
		// noop correctly defers everything except __name__: __name__ alone produces
		// 794 refs, setting a budget of 794×512=406 KB. The next smallest group
		// (namespace="app-zola", 667 KB) already exceeds it.
		name: "kube_pod_container_resource_limits/selective",
		matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "kube_pod_container_resource_limits"),
			labels.MustNewMatcher(labels.MatchRegexp, "cluster", "systems-prod-.*"),
			labels.MustNewMatcher(labels.MatchEqual, "namespace", "app-zola"),
			labels.MustNewMatcher(labels.MatchEqual, "resource", "memory"),
			labels.MustNewMatcher(labels.MatchNotEqual, "container", ""),
		},
	},
	{
		// noop fetches pod!="" (7.5 MB) and container!="" (6.8 MB) even though
		// __name__ + container="kube-proxy" already returns the full 7114-series
		// result. The SG cost planner defers both to scan, saving ~14 MB of
		// posting I/O at the cost of 7114 label comparisons.
		name: "node_namespace_pod_container:container_memory_rss/wasteful_postings",
		matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "node_namespace_pod_container:container_memory_rss"),
			labels.MustNewMatcher(labels.MatchEqual, "container", "kube-proxy"),
			labels.MustNewMatcher(labels.MatchNotEqual, "pod", ""),
			labels.MustNewMatcher(labels.MatchNotEqual, "container", ""),
		},
	},
}

func BenchmarkExpandedPostings(b *testing.B) {
	blockDir := os.Getenv(blockDirEnv)
	if blockDir == "" {
		b.Skipf("%s not set", blockDirEnv)
	}
	blockDir = filepath.Clean(blockDir)

	blockULID, err := ulid.Parse(filepath.Base(blockDir))
	require.NoError(b, err)

	meta, err := block.ReadMetaFromDir(blockDir)
	require.NoError(b, err)

	fsBkt, err := filesystem.NewBucket(filepath.Dir(blockDir))
	require.NoError(b, err)
	b.Cleanup(func() { fsBkt.Close() })
	instrBkt := objstore.WithNoopInstr(fsBkt)

	indexHeaderDir := b.TempDir()
	logger := log.NewNopLogger()

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
	require.NoError(b, err)
	b.Cleanup(func() { headerReader.Close() })

	sgPlanner, err := storegateway.BuildCostBasedPlanner(blockDir, meta, sgCostConfig(), logger)
	require.NoError(b, err)

	planners := []struct {
		name    string
		planner index.LookupPlanner
	}{
		{"sg_cost", sgPlanner},
		{"noop", storegateway.NoopLookupPlanner()},
	}

	ctx := context.Background()

	for _, bc := range benchCases {
		for _, pl := range planners {
			b.Run(bc.name+"/"+pl.name, func(b *testing.B) {
				bb := storegateway.NewBucketBlockForDemo(meta, instrBkt, headerReader, pl.planner, logger)
				b.ResetTimer()
				for b.Loop() {
					_, _, err := bb.ExpandedPostings(ctx, bc.matchers)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
