package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os/signal"
	"strings"
	"syscall"
	"time"

	gokitlog "github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	utillog "github.com/grafana/mimir/pkg/util/log"
)

func main() {
	cfg := struct {
		bucket      bucket.Config
		userID      string
		BlockRanges mimir_tsdb.DurationList
		shardCount  int
		splitGroups int
		sorting     string
		ringFile    string
	}{}

	// Loads bucket index, and plans compaction for all loaded meta files.
	cfg.bucket.RegisterFlags(flag.CommandLine)
	cfg.BlockRanges = mimir_tsdb.DurationList{2 * time.Hour, 12 * time.Hour, 24 * time.Hour}
	flag.Var(&cfg.BlockRanges, "block-ranges", "List of compaction time ranges.")
	flag.StringVar(&cfg.userID, "user", "", "User (tenant)")
	flag.IntVar(&cfg.shardCount, "shard-count", 4, "Shard count")
	flag.IntVar(&cfg.splitGroups, "split-groups", 4, "Split groups")
	flag.StringVar(&cfg.sorting, "sorting", compactor.CompactionOrderNewestFirst, "One of: "+compactor.CompactionOrderOldestFirst+", "+compactor.CompactionOrderNewestFirst+", "+compactor.CompactionOrderOldestSplitFirst+".")
	flag.StringVar(&cfg.ringFile, "ring-file", "", "JSON file with ring")
	flag.Parse()

	if cfg.userID == "" {
		log.Fatalln("no user specified")
	}

	logger := gokitlog.NewNopLogger()
	var r *ring.Ring
	if cfg.ringFile != "" {
		r = loadRing(cfg.ringFile, logger)
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
		metas[b.ID] = b.ThanosMeta(cfg.userID)
	}

	//synced := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{Name: "synced", Help: "Number of block metadata synced"},
	//	[]string{"state"}, []string{block.MarkedForNoCompactionMeta},
	//)

	//for _, f := range []block.MetadataFilter{
	//	// compactor.NewShardAwareDeduplicateFilter(), // We cannot use this because there are no compaction sources stored in bucket-index
	//	compactor.NewNoCompactionMarkFilter(logger, bucket.NewUserBucketClient(cfg.userID, bkt, nil), 16, true),
	//} {
	//	log.Printf("Filtering using %T\n", f)
	//	err = f.Filter(ctx, metas, synced)
	//	if err != nil {
	//		log.Fatalln("filter failed:", err)
	//	}
	//}

	grouper := compactor.NewSplitAndMergeGrouper(cfg.userID, cfg.BlockRanges.ToMilliseconds(), uint32(cfg.shardCount), uint32(cfg.splitGroups), logger)
	jobs, err := grouper.Groups(metas)

	switch cfg.sorting {
	case compactor.CompactionOrderOldestFirst:
		jobs = compactor.SortJobsBySmallestRangeOldestBlocksFirst(jobs)
	case compactor.CompactionOrderNewestFirst:
		jobs = compactor.SortJobsByNewestBlocksFirst(jobs)
	case compactor.CompactionOrderOldestSplitFirst:
		jobs = compactor.SortJobsBySmallestRangeOldestBlocksFirstMoveSplitToBeginning(jobs)
	default:
		log.Println("unknown sorting, not sorting:", cfg.sorting)
	}

	for ix, j := range jobs {
		detail := ""
		if j.UseSplitting() {
			detail = "splitting"
		} else {
			detail = j.Labels().Get(mimir_tsdb.CompactorShardIDExternalLabel)
		}

		instance := ""
		if r != nil {
			rs, err := r.Get(compactor.HashShardingKey(j.ShardingKey()), compactor.RingOp, nil, nil, nil)
			if err != nil {
				log.Println("invalid ring op:", err)
			} else {
				instances := []string(nil)
				for _, i := range rs.Instances {
					instances = append(instances, i.Addr)
				}
				instance = strings.Join(instances, ", ")
			}
		}

		fmt.Printf(
			"%d\t%s\t%s\t%d\t%s\t%s\n",
			ix,
			timestamp.Time(j.MinTime()).UTC().Format(time.RFC3339),
			timestamp.Time(j.MaxTime()).UTC().Format(time.RFC3339),
			len(j.IDs()),
			detail,
			instance,
		)
	}
}

func loadRing(file string, logger gokitlog.Logger) *ring.Ring {
	f, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalln("failed to load ring from", file, "due to error:", err)
	}

	log.Println("Loading ring")
	rd := &ring.Desc{}
	err = json.Unmarshal(f, rd)
	if err != nil {
		panic(err)
	}

	kvc, _ := consul.NewInMemoryClient(ring.GetCodec(), logger, nil)

	r, err := ring.New(ring.Config{
		KVStore: kv.Config{
			Mock: kvc,
		},
		HeartbeatTimeout:     1000 * time.Minute,
		ReplicationFactor:    1,
		ZoneAwarenessEnabled: false,
		ExcludedZones:        nil,
		SubringCacheDisabled: false,
	}, "compactor", "compactor", utillog.Logger, nil)
	if err != nil {
		panic(err)
	}
	r.UpdateRingState(rd)
	return r
}
