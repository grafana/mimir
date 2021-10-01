package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	gokitlog "github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/util"
)

func main() {
	cfg := struct {
		bucket           bucket.Config
		userID           string
		showDeleted      bool
		showLabels       bool
		showCreationTime bool
	}{}

	cfg.bucket.RegisterFlags(flag.CommandLine)
	flag.StringVar(&cfg.userID, "user", "", "User (tenant)")
	flag.BoolVar(&cfg.showDeleted, "show-deleted", false, "Show deleted blocks")
	flag.BoolVar(&cfg.showLabels, "show-labels", false, "Show block labels")
	flag.BoolVar(&cfg.showCreationTime, "show-creation-time", false, "Show when block was created (from ULID)")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	logger := gokitlog.NewNopLogger()
	bkt, err := bucket.NewClient(ctx, cfg.bucket, "bucket", logger, nil)
	if err != nil {
		log.Fatalln("failed to create bucket:", err)
	}

	bi, err := bucketindex.ReadIndex(ctx, bkt, cfg.userID, nil, logger)
	if err != nil {
		log.Fatalln("failed to read bucket index:", err)
	}

	deletedTimes := map[ulid.ULID]time.Time{}
	for _, dm := range bi.BlockDeletionMarks {
		deletedTimes[dm.ID] = time.Unix(dm.DeletionTime, 0)
	}

	metasSync := sync.Mutex{}
	metas := map[ulid.ULID]*metadata.Meta{}

	// We only need metas when showing labels.
	if cfg.showLabels {
		g, gctx := errgroup.WithContext(ctx)
		for _, b := range bi.Blocks {
			if !cfg.showDeleted && !deletedTimes[b.ID].IsZero() {
				continue
			}

			b := b
			g.Go(func() error {
				r, err := bkt.Get(gctx, path.Join(cfg.userID, b.ID.String(), "meta.json"))
				if err != nil {
					return err
				}

				m, err := metadata.Read(r)
				if err != nil {
					if bkt.IsObjNotFoundErr(err) {
						return nil
					}

					return err
				}

				metasSync.Lock()
				defer metasSync.Unlock()

				metas[b.ID] = m
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			log.Println("failed to fetch metas:", err)
		}
	}

	tabber := tabwriter.NewWriter(os.Stdout, 1, 4, 3, ' ', 0)
	defer tabber.Flush()

	// Header
	fmt.Fprintf(tabber, "Block ID\t")
	if cfg.showCreationTime {
		fmt.Fprintf(tabber, "Created\t")
	}
	fmt.Fprintf(tabber, "Min Time\t")
	fmt.Fprintf(tabber, "Max Time\t")
	if cfg.showDeleted {
		fmt.Fprintf(tabber, "Deletion Time\t")
	}
	if cfg.showLabels {
		fmt.Fprintf(tabber, "Labels (excl. "+tsdb.TenantIDExternalLabel+")\t")
	}
	fmt.Fprintln(tabber)

	for _, b := range bi.Blocks {
		if !cfg.showDeleted && !deletedTimes[b.ID].IsZero() {
			continue
		}

		fmt.Fprintf(tabber, "%v\t", b.ID)
		if cfg.showCreationTime {
			fmt.Fprintf(tabber, "%v\t", util.TimeFromMillis(int64(b.ID.Time())).UTC().Format(time.RFC3339))
		}
		fmt.Fprintf(tabber, "%v\t", util.TimeFromMillis(b.MinTime).UTC().Format(time.RFC3339))
		fmt.Fprintf(tabber, "%v\t", util.TimeFromMillis(b.MaxTime).UTC().Format(time.RFC3339))

		if cfg.showDeleted {
			if deletedTimes[b.ID].IsZero() {
				fmt.Fprintf(tabber, "\t") // no deletion time.
			} else {
				fmt.Fprintf(tabber, "%v\t", deletedTimes[b.ID].UTC().Format(time.RFC3339))
			}
		}

		if cfg.showLabels {
			if m := metas[b.ID]; m != nil {
				fmt.Fprintf(tabber, "%s\t", labels.FromMap(m.Thanos.Labels).WithoutLabels(tsdb.TenantIDExternalLabel))
			} else {
				fmt.Fprintf(tabber, "\t")
			}
		}

		fmt.Fprintln(tabber)
	}
}
