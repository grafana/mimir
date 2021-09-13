// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"sort"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	gokitlog "github.com/go-kit/kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	dstime "github.com/grafana/dskit/time"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
)

type config struct {
	bucket           bucket.Config
	userID           string
	showDeleted      bool
	showLabels       bool
	showCreationTime bool
	showSources      bool
	showParents      bool
	splitCount       int
	minTime          flagext.Time
	maxTime          flagext.Time
}

func main() {
	cfg := config{}
	cfg.bucket.RegisterFlags(flag.CommandLine)
	flag.StringVar(&cfg.userID, "user", "", "User (tenant)")
	flag.BoolVar(&cfg.showDeleted, "show-deleted", false, "Show deleted blocks")
	flag.BoolVar(&cfg.showLabels, "show-labels", false, "Show block labels")
	flag.BoolVar(&cfg.showCreationTime, "show-creation-time", false, "Show when block was created (from ULID)")
	flag.BoolVar(&cfg.showSources, "show-sources", false, "Show compaction sources")
	flag.BoolVar(&cfg.showParents, "show-parents", false, "Show parent blocks")
	flag.IntVar(&cfg.splitCount, "split-count", 0, "It not 0, shows split number that would be used for grouping blocks during split compaction")
	flag.Var(&cfg.minTime, "min-time", "If set, only blocks with MinTime >= this value are printed")
	flag.Var(&cfg.maxTime, "max-time", "If set, only blocks with MaxTime <= this value are printed")
	flag.Parse()

	if cfg.userID == "" {
		log.Fatalln("no user specified")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	logger := gokitlog.NewNopLogger()
	bkt, err := bucket.NewClient(ctx, cfg.bucket, "bucket", logger, nil)
	if err != nil {
		log.Fatalln("failed to create bucket:", err)
	}

	metas, deletedTimes, err := loadMetaFilesAndDeletionMarkers(ctx, bkt, cfg.userID, cfg.showDeleted)
	if err != nil {
		log.Fatalln("failed to read block metadata:", err)
	}

	printMetas(metas, deletedTimes, cfg)
}

func loadMetaFilesAndDeletionMarkers(ctx context.Context, bkt objstore.Bucket, user string, showDeleted bool) (map[ulid.ULID]*metadata.Meta, map[ulid.ULID]time.Time, error) {
	deletedBlocks := map[ulid.ULID]bool{}
	deletionMarkerFiles := []string(nil)

	// Find blocks marked for deletion
	err := bkt.Iter(ctx, path.Join(user, bucketindex.MarkersPathname), func(s string) error {
		if id, ok := bucketindex.IsBlockDeletionMarkFilename(path.Base(s)); ok {
			deletedBlocks[id] = true
			deletionMarkerFiles = append(deletionMarkerFiles, s)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	metaPaths := []string(nil)
	err = bkt.Iter(ctx, user, func(s string) error {
		if id, ok := block.IsBlockDir(s); ok {
			if !showDeleted && deletedBlocks[id] {
				return nil
			}
			metaPaths = append(metaPaths, path.Join(s, "meta.json"))
		}
		return nil
	})

	if err != nil {
		return nil, nil, err
	}

	var deletionTimes map[ulid.ULID]time.Time
	if showDeleted {
		deletionTimes, err = fetchDeletionTimes(ctx, bkt, deletionMarkerFiles)
		if err != nil {
			return nil, nil, err
		}
	}

	metas, err := fetchMetas(ctx, bkt, metaPaths)
	return metas, deletionTimes, err
}

const concurrencyLimit = 32

func fetchDeletionTimes(ctx context.Context, bkt objstore.Bucket, deletionMarkers []string) (map[ulid.ULID]time.Time, error) {
	mu := sync.Mutex{}
	times := map[ulid.ULID]time.Time{}

	return times, concurrency.ForEach(ctx, concurrency.CreateJobsFromStrings(deletionMarkers), concurrencyLimit, func(ctx context.Context, job interface{}) error {
		r, err := bkt.Get(ctx, job.(string))
		if err != nil {
			if bkt.IsObjNotFoundErr(err) {
				return nil
			}

			return err
		}
		defer r.Close()

		dec := json.NewDecoder(r)

		m := metadata.DeletionMark{}
		if err := dec.Decode(&m); err != nil {
			return err
		}

		mu.Lock()
		times[m.ID] = time.Unix(m.DeletionTime, 0)
		mu.Unlock()

		return nil
	})
}

func fetchMetas(ctx context.Context, bkt objstore.Bucket, metaFiles []string) (map[ulid.ULID]*metadata.Meta, error) {
	mu := sync.Mutex{}
	metas := map[ulid.ULID]*metadata.Meta{}

	return metas, concurrency.ForEach(ctx, concurrency.CreateJobsFromStrings(metaFiles), concurrencyLimit, func(ctx context.Context, job interface{}) error {
		r, err := bkt.Get(ctx, job.(string))
		if err != nil {
			if bkt.IsObjNotFoundErr(err) {
				return nil
			}

			return err
		}
		defer r.Close()

		m, err := metadata.Read(r)
		if err != nil {
			return err
		}

		mu.Lock()
		metas[m.ULID] = m
		mu.Unlock()

		return nil
	})
}

// nolint:errcheck
//goland:noinspection GoUnhandledErrorResult
func printMetas(metas map[ulid.ULID]*metadata.Meta, deletedTimes map[ulid.ULID]time.Time, cfg config) {
	var blocks []*metadata.Meta

	for _, b := range metas {
		blocks = append(blocks, b)
	}

	sort.Slice(blocks, func(i, j int) bool {
		if blocks[i].MinTime != blocks[j].MinTime {
			return blocks[i].MinTime < blocks[j].MinTime
		}
		if blocks[i].MaxTime != blocks[j].MaxTime {
			return blocks[i].MaxTime < blocks[j].MaxTime
		}

		// check block creation time instead
		return blocks[i].ULID.Time() < blocks[j].ULID.Time()
	})

	tabber := tabwriter.NewWriter(os.Stdout, 1, 4, 3, ' ', 0)
	defer tabber.Flush()

	// Header
	fmt.Fprintf(tabber, "Block ID\t")
	if cfg.splitCount > 0 {
		fmt.Fprintf(tabber, "Split ID\t")
	}
	if cfg.showCreationTime {
		fmt.Fprintf(tabber, "Created\t")
	}
	fmt.Fprintf(tabber, "Min Time\t")
	fmt.Fprintf(tabber, "Max Time\t")
	fmt.Fprintf(tabber, "Duration\t")
	if cfg.showDeleted {
		fmt.Fprintf(tabber, "Deletion Time\t")
	}
	if cfg.showLabels {
		fmt.Fprintf(tabber, "Labels (excl. "+tsdb.TenantIDExternalLabel+")\t")
	}
	if cfg.showSources {
		fmt.Fprintf(tabber, "Sources\t")
	}
	if cfg.showParents {
		fmt.Fprintf(tabber, "Parents\t")
	}
	fmt.Fprintln(tabber)

	for _, b := range blocks {
		if !cfg.showDeleted && !deletedTimes[b.ULID].IsZero() {
			continue
		}

		if !time.Time(cfg.minTime).IsZero() && dstime.FromMillis(b.MinTime).Before(time.Time(cfg.minTime)) {
			continue
		}
		if !time.Time(cfg.maxTime).IsZero() && dstime.FromMillis(b.MaxTime).After(time.Time(cfg.maxTime)) {
			continue
		}

		fmt.Fprintf(tabber, "%v\t", b.ULID)
		if cfg.splitCount > 0 {
			fmt.Fprintf(tabber, "%d\t", tsdb.HashBlockID(b.ULID)%uint32(cfg.splitCount))
		}
		if cfg.showCreationTime {
			fmt.Fprintf(tabber, "%v\t", dstime.FromMillis(int64(b.ULID.Time())).UTC().Format(time.RFC3339))
		}
		fmt.Fprintf(tabber, "%v\t", dstime.FromMillis(b.MinTime).UTC().Format(time.RFC3339))
		fmt.Fprintf(tabber, "%v\t", dstime.FromMillis(b.MaxTime).UTC().Format(time.RFC3339))
		fmt.Fprintf(tabber, "%v\t", dstime.FromMillis(b.MaxTime).Sub(dstime.FromMillis(b.MinTime)))

		if cfg.showDeleted {
			if deletedTimes[b.ULID].IsZero() {
				fmt.Fprintf(tabber, "\t") // no deletion time.
			} else {
				fmt.Fprintf(tabber, "%v\t", deletedTimes[b.ULID].UTC().Format(time.RFC3339))
			}
		}

		if cfg.showLabels {
			if m := b.Thanos.Labels; m != nil {
				fmt.Fprintf(tabber, "%s\t", labels.FromMap(b.Thanos.Labels).WithoutLabels(tsdb.TenantIDExternalLabel))
			} else {
				fmt.Fprintf(tabber, "\t")
			}
		}

		if cfg.showSources {
			// No tab at the end.
			fmt.Fprintf(tabber, "%v", b.Compaction.Sources)
		}

		if cfg.showParents {
			var p []ulid.ULID
			for _, pb := range b.Compaction.Parents {
				p = append(p, pb.ULID)
			}
			// No tab at the end.
			fmt.Fprintf(tabber, "%v", p)
		}

		fmt.Fprintln(tabber)
	}
}
