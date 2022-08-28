// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	gokitlog "github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/extprom"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
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

	logger := gokitlog.NewNopLogger()
	cfg.bucket.RegisterFlags(flag.CommandLine, logger)
	flag.StringVar(&cfg.userID, "user", "", "User (tenant)")
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

	log.Println("Listing metas")
	metas, err := listDebugMetas(ctx, bkt, cfg.userID)
	if err != nil {
		log.Fatalln("failed to list debug metas:", err)
	}

	log.Println("Fetching", len(metas), "metas")
	metasMap, err := fetchMetas(ctx, bkt, metas)
	if err != nil {
		log.Fatalln("failed to list fetch metas:", err)
	}

	df := compactor.NewShardAwareDeduplicateFilter()

	s := extprom.NewTxGaugeVec(
		nil,
		prometheus.GaugeOpts{
			Name: "synced",
			Help: "Number of block metadata synced",
		},
		[]string{"state"}, []string{"duplicate"})

	log.Println("Running filter")
	err = df.Filter(ctx, metasMap, s, nil)
	if err != nil {
		log.Fatalln("deduplication failed:", err)
	}

	log.Println("Deduplicated blocks")

	printBlocks(metasMap)
}

func listDebugMetas(ctx context.Context, bkt objstore.Bucket, user string) ([]string, error) {
	var paths []string

	err := bkt.Iter(ctx, path.Join(user, "debug", "metas"), func(s string) error {
		if !strings.HasSuffix(s, ".json") {
			return nil
		}

		base := path.Base(s)
		base = base[:len(base)-5] // remove ".json" suffix

		_, err := ulid.Parse(base)
		if err == nil {
			paths = append(paths, s)
		}
		return nil
	})

	return paths, err
}

func fetchMetas(ctx context.Context, bkt objstore.Bucket, metaFiles []string) (map[ulid.ULID]*metadata.Meta, error) {
	g, gctx := errgroup.WithContext(ctx)

	ch := make(chan string, len(metaFiles))
	for _, p := range metaFiles {
		ch <- p
	}
	close(ch)

	metasSync := sync.Mutex{}
	metas := map[ulid.ULID]*metadata.Meta{}

	const concurrencyLimit = 32
	for i := 0; i < concurrencyLimit; i++ {
		g.Go(func() error {
			for p := range ch {
				r, err := bkt.Get(gctx, p)
				if err != nil {
					return err
				}

				m, err := metadata.Read(r)
				if err != nil {
					if bkt.IsObjNotFoundErr(err) {
						continue
					}

					return err
				}

				metasSync.Lock()
				metas[m.ULID] = m
				metasSync.Unlock()

				fmt.Print(".")
			}

			return nil
		})
	}

	return metas, g.Wait()
}

func printBlocks(metas map[ulid.ULID]*metadata.Meta) {
	var blocks []*metadata.Meta

	for _, b := range metas {
		blocks = append(blocks, b)
	}

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].MinTime < blocks[j].MinTime
	})

	tabber := tabwriter.NewWriter(os.Stdout, 1, 4, 3, ' ', 0)
	defer tabber.Flush()

	// Header
	fmt.Fprintf(tabber, "Block ID\t")
	fmt.Fprintf(tabber, "Min Time\t")
	fmt.Fprintf(tabber, "Max Time\t")
	fmt.Fprintf(tabber, "Duration\t")
	fmt.Fprintf(tabber, "Labels\t")
	fmt.Fprintln(tabber)

	for _, b := range blocks {
		fmt.Fprintf(tabber, "%v\t", b.ULID)
		fmt.Fprintf(tabber, "%v\t", util.TimeFromMillis(b.MinTime).UTC().Format(time.RFC3339))
		fmt.Fprintf(tabber, "%v\t", util.TimeFromMillis(b.MaxTime).UTC().Format(time.RFC3339))
		fmt.Fprintf(tabber, "%v\t", util.TimeFromMillis(b.MaxTime).Sub(util.TimeFromMillis(b.MinTime)))
		fmt.Fprintf(tabber, "%s\t", labels.FromMap(b.Thanos.Labels))
		fmt.Fprintln(tabber)
	}
}
