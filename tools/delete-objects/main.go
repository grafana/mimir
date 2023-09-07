// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"context"
	"flag"
	"log"
	"os"
	"sync"

	gokitlog "github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/thanos-io/objstore"
)

type config struct {
	bucket      bucket.Config
	concurrency int
}

func main() {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	cfg := config{}
	cfg.bucket.RegisterFlags(flag.CommandLine)
	flag.IntVar(&cfg.concurrency, "concurrency", 8, "number of concurrent goroutines")

	// Parse CLI arguments.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		log.Fatalln(err.Error())
	}

	logger := gokitlog.NewNopLogger()
	bkt, err := bucket.NewClient(context.Background(), cfg.bucket, "bucket", logger, nil)
	if err != nil {
		log.Fatalln("failed to create bucket client:", err)
	}

	ch := make(chan string)

	wg := sync.WaitGroup{}
	for i := 0; i < cfg.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(context.Background(), bkt, ch)
		}()
	}

	scan := bufio.NewScanner(os.Stdin)
	for scan.Scan() {
		line := scan.Text()
		ch <- line
	}
	close(ch)

	wg.Wait()
}

func worker(ctx context.Context, bkt objstore.Bucket, ch chan string) {
	for path := range ch {
		if err := bkt.Delete(ctx, path); err != nil {
			log.Printf("Failed to delete %s: %v\n", path, err)
			continue
		}

		log.Println("Deleted", path)
	}
}
