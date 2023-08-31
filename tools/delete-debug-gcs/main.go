// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"context"
	"flag"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/grafana/dskit/flagext"
)

func main() {
	concurrency := flag.Int("concurrency", 8, "number of concurrent goroutines")

	// Parse CLI arguments.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		log.Fatalln(err.Error())
	}

	client, err := storage.NewClient(context.Background())
	if err != nil {
		log.Fatalln(err)
	}

	ch := make(chan string)

	wg := sync.WaitGroup{}
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(client, ch)
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

func worker(client *storage.Client, ch chan string) {
	var bucket *storage.BucketHandle
	var prevBucketName string

	for line := range ch {
		u, err := url.Parse(line)
		if err != nil {
			log.Println("failed to parse", u, "due to error:", err)
			continue
		}

		if u.Scheme != "gs" {
			log.Println("gs scheme expected, got", u.Scheme, "full line:", line)
			continue
		}

		bucketName := u.Hostname()
		if bucket == nil || prevBucketName != bucketName {
			bucket = client.Bucket(bucketName)
		}

		p := u.Path
		for strings.HasPrefix(p, "/") {
			p = p[1:]
		}

		objToDelete := bucket.Object(p)

		ctx := context.Background()
		if err := objToDelete.Delete(ctx); err != nil {
			log.Printf("Failed to delete %s: %v\n", line, err)
			continue
		}

		log.Println("Deleted", line)
	}
}
