// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/grafana/mimir/pkg/util"
)

func main() {
	concurrency := flag.Int("concurrency", 8, "number of concurrent goroutines")

	// Parse CLI arguments.
	if err := util.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
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

		files, err := listDeletedFiles(context.Background(), bucket, p)
		if err != nil {
			log.Println("failed to get files for prefix", line, "due to error:", err)
			continue
		}

		for _, f := range files {
			src := bucket.Object(f.Name).Generation(f.Generation)
			dst := bucket.Object(f.Name)

			copier := dst.CopierFrom(src)
			_, err = copier.Run(context.Background())
			if err != nil {
				log.Println("failed to undelete", line, "due to error:", err)
			}
			log.Println("undeleted", f.Name)
		}
	}
}

func listDeletedFiles(ctx context.Context, bucket *storage.BucketHandle, prefix string) ([]*storage.ObjectAttrs, error) {
	var files []*storage.ObjectAttrs

	log.Println("fetching files from", prefix)
	it := bucket.Objects(ctx, &storage.Query{
		// Versions true to output all generations of objects
		Versions: true,
		Prefix:   prefix,
	})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}

		if strings.HasSuffix(attrs.Name, "deletion-mark.json") {
			continue
		}

		if attrs.Deleted.IsZero() {
			continue
		}

		files = append(files, attrs)
	}
	return files, nil
}
