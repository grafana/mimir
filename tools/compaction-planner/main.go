// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	gokitlog "github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/timestamp"

	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/util/extprom"
)

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	cfg := struct {
		bucket        bucket.Config
		userID        string
		blockRanges   mimir_tsdb.DurationList
		shardCount    int
		oooShardCount int
		splitGroups   int
		sorting       string
	}{}

	logger := gokitlog.NewNopLogger()

	// Loads bucket index, and plans compaction for all loaded meta files.
	cfg.bucket.RegisterFlags(flag.CommandLine)
	cfg.blockRanges = mimir_tsdb.DurationList{2 * time.Hour, 12 * time.Hour, 24 * time.Hour}
	flag.Var(&cfg.blockRanges, "block-ranges", "List of compaction time ranges.")
	flag.StringVar(&cfg.userID, "user", "", "User (tenant)")
	flag.IntVar(&cfg.shardCount, "shard-count", 4, "Shard count")
	flag.IntVar(&cfg.oooShardCount, "ooo-shard-count", 0, "Shard count for out-of-order blocks (0 to use shard-count)")
	flag.IntVar(&cfg.splitGroups, "split-groups", 4, "Split groups")
	flag.StringVar(&cfg.sorting, "sorting", compactor.CompactionOrderOldestFirst, "One of: "+strings.Join(compactor.CompactionOrders, ", ")+".")

	// Parse CLI arguments.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		log.Fatalln(err.Error())
	}

	if cfg.userID == "" {
		log.Fatalln("no user specified")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	bkt, err := bucket.NewClient(ctx, cfg.bucket, "bucket", logger, nil)
	if err != nil {
		log.Fatalln("failed to create bucket:", err)
	}

	idx, err := bucketindex.ReadIndex(ctx, bkt, cfg.userID, nil, logger)
	if err != nil {
		log.Fatalln("failed to load bucket index:", err)
	}

	log.Println("Using index from", time.Unix(idx.UpdatedAt, 0).UTC().Format(time.RFC3339))

	metas := compactor.ConvertBucketIndexToMetasForCompactionJobPlanning(idx)
	synced := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{Name: "synced", Help: "Number of block metadata synced"},
		[]string{"state"}, []string{block.MarkedForNoCompactionMeta},
	)

	for _, f := range []block.MetadataFilter{
		// No need to exclude blocks marked for deletion, as we did that above already.
		compactor.NewNoCompactionMarkFilter(bucket.NewUserBucketClient(cfg.userID, bkt, nil)),
	} {
		log.Printf("Filtering using %T\n", f)
		err = f.Filter(ctx, metas, synced)
		if err != nil {
			log.Fatalln("filter failed:", err)
		}
	}

	tabber := tabwriter.NewWriter(os.Stdout, 1, 4, 3, ' ', 0)
	defer tabber.Flush()

	fmt.Fprintf(tabber, "Job No.\tStart Time\tEnd Time\tBlocks\tJob Key\n")

	cfgProvider := &staticConfigProvider{
		shardCount:       cfg.shardCount,
		oooShardCount:    cfg.oooShardCount,
		splitGroupsCount: cfg.splitGroups,
	}
	grouper := compactor.NewSplitAndMergeGrouper(cfg.userID, cfg.blockRanges.ToMilliseconds(), cfgProvider, logger)
	jobs, err := grouper.Groups(metas)
	if err != nil {
		log.Fatalln("failed to plan compaction:", err)
	}

	fn := compactor.GetJobsOrderFunction(cfg.sorting)
	if fn != nil {
		jobs = fn(jobs)
	} else {
		log.Println("unknown sorting, jobs will be unsorted")
	}

	for ix, j := range jobs {
		fmt.Fprintf(tabber,
			"%d\t%s\t%s\t%d\t%s\n",
			ix+1,
			timestamp.Time(j.MinTime()).UTC().Format(time.RFC3339),
			timestamp.Time(j.MaxTime()).UTC().Format(time.RFC3339),
			len(j.IDs()),
			j.Key(),
		)
	}
}

// staticConfigProvider implements compactor.ConfigProvider
type staticConfigProvider struct {
	shardCount       int
	oooShardCount    int
	splitGroupsCount int
}

func (c *staticConfigProvider) CompactorBlocksRetentionPeriod(_ string) time.Duration { return 0 }
func (c *staticConfigProvider) CompactorSplitAndMergeShards(_ string) int             { return c.shardCount }
func (c *staticConfigProvider) CompactorOOOSplitAndMergeShards(_ string) int          { return c.oooShardCount }
func (c *staticConfigProvider) CompactorSplitGroups(_ string) int                     { return c.splitGroupsCount }
func (c *staticConfigProvider) CompactorTenantShardSize(_ string) int                 { return 0 }
func (c *staticConfigProvider) CompactorPartialBlockDeletionDelay(_ string) (time.Duration, bool) {
	return 0, true
}
func (c *staticConfigProvider) CompactorBlockUploadEnabled(_ string) bool           { return false }
func (c *staticConfigProvider) CompactorBlockUploadValidationEnabled(_ string) bool { return false }
func (c *staticConfigProvider) CompactorBlockUploadVerifyChunks(_ string) bool      { return false }
func (c *staticConfigProvider) CompactorBlockUploadMaxBlockSizeBytes(_ string) int64 {
	return 0
}
func (c *staticConfigProvider) CompactorMaxLookback(_ string) time.Duration        { return 0 }
func (c *staticConfigProvider) CompactorMaxPerBlockUploadConcurrency(_ string) int { return 0 }
func (c *staticConfigProvider) S3SSEType(_ string) string                          { return "" }
func (c *staticConfigProvider) S3SSEKMSKeyID(_ string) string                      { return "" }
func (c *staticConfigProvider) S3SSEKMSEncryptionContext(_ string) string          { return "" }
