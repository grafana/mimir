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
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/thanos-io/thanos/pkg/extprom"

	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
)

func main() {
	cfg := struct {
		bucket      bucket.Config
		userID      string
		blockRanges mimir_tsdb.DurationList
		shardCount  int
		splitGroups int
		sorting     string
	}{}

	logger := gokitlog.NewNopLogger()

	// Loads bucket index, and plans compaction for all loaded meta files.
	cfg.bucket.RegisterFlags(flag.CommandLine, logger)
	cfg.blockRanges = mimir_tsdb.DurationList{2 * time.Hour, 12 * time.Hour, 24 * time.Hour}
	flag.Var(&cfg.blockRanges, "block-ranges", "List of compaction time ranges.")
	flag.StringVar(&cfg.userID, "user", "", "User (tenant)")
	flag.IntVar(&cfg.shardCount, "shard-count", 4, "Shard count")
	flag.IntVar(&cfg.splitGroups, "split-groups", 4, "Split groups")
	flag.StringVar(&cfg.sorting, "sorting", compactor.CompactionOrderOldestFirst, "One of: "+strings.Join(compactor.CompactionOrders, ", ")+".")
	flag.Parse()

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

	// convert index to metas.
	deleted := map[ulid.ULID]bool{}
	for _, id := range idx.BlockDeletionMarks.GetULIDs() {
		deleted[id] = true
	}

	metas := map[ulid.ULID]*metadata.Meta{}
	for _, b := range idx.Blocks {
		if deleted[b.ID] {
			continue
		}
		metas[b.ID] = b.ThanosMeta()
		if metas[b.ID].Thanos.Labels == nil {
			metas[b.ID].Thanos.Labels = map[string]string{}
		}
		metas[b.ID].Thanos.Labels[mimir_tsdb.CompactorShardIDExternalLabel] = b.CompactorShardID // Needed for correct planning.
	}

	synced := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{Name: "synced", Help: "Number of block metadata synced"},
		[]string{"state"}, []string{block.MarkedForNoCompactionMeta},
	)

	for _, f := range []block.MetadataFilter{
		// No need to exclude blocks marked for deletion, as we did that above already.
		compactor.NewNoCompactionMarkFilter(bucket.NewUserBucketClient(cfg.userID, bkt, nil), true),
	} {
		log.Printf("Filtering using %T\n", f)
		err = f.Filter(ctx, metas, synced, nil)
		if err != nil {
			log.Fatalln("filter failed:", err)
		}
	}

	tabber := tabwriter.NewWriter(os.Stdout, 1, 4, 3, ' ', 0)
	defer tabber.Flush()

	fmt.Fprintf(tabber, "Job No.\tStart Time\tEnd Time\tBlocks\tJob Key\n")

	grouper := compactor.NewSplitAndMergeGrouper(cfg.userID, cfg.blockRanges.ToMilliseconds(), uint32(cfg.shardCount), uint32(cfg.splitGroups), logger)
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
