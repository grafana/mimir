// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	gokitlog "github.com/go-kit/kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

type config struct {
	bucket  bucket.Config
	userID  string
	blockID string
}

func main() {
	cfg := config{}
	cfg.bucket.RegisterFlags(flag.CommandLine)
	flag.StringVar(&cfg.userID, "user", "", "User (tenant)")
	flag.StringVar(&cfg.blockID, "block", "", "Block ID")
	flag.Parse()

	if cfg.userID == "" {
		log.Fatalln("no user specified")
	}

	if cfg.blockID == "" {
		log.Fatalln("no block specified")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	logger := gokitlog.NewNopLogger()
	bkt, err := bucket.NewClient(ctx, cfg.bucket, "bucket", logger, nil)
	if err != nil {
		log.Fatalln("failed to create bucket:", err)
	}

	root := &block{id: cfg.blockID}
	if err := buildBlockTree(ctx, bkt, cfg.userID, root); err != nil {
		log.Fatalln("failed to build block tree:", err)
	}

	printBlockTree(root, 0)
}

type block struct {
	id         string
	minT       int64
	maxT       int64
	labels     map[string]string
	level      int
	uploadTime time.Time

	parentsMx sync.Mutex
	parents   []*block
}

func buildBlockTree(ctx context.Context, bkt objstore.Bucket, user string, input *block) error {
	// Fetch the meta for the input block.
	inputMeta, err := fetchMeta(ctx, bkt, user, input.id)
	if err != nil {
		return err
	}

	// Check when the block was uploaded.
	uploadTime, err := getUploadTime(ctx, bkt, user, input.id)
	if err != nil {
		return err
	}

	// Enrich the input block with some information.
	input.minT = inputMeta.MinTime
	input.maxT = inputMeta.MaxTime
	input.labels = inputMeta.Thanos.Labels
	input.level = inputMeta.Compaction.Level
	input.uploadTime = uploadTime

	// Get parents block IDs.
	parentIDs := make([]string, 0, len(inputMeta.BlockMeta.Compaction.Parents))
	for _, parentMeta := range inputMeta.BlockMeta.Compaction.Parents {
		parentIDs = append(parentIDs, parentMeta.ULID.String())
	}

	// Iterate for each parent.
	return concurrency.ForEach(ctx, concurrency.CreateJobsFromStrings(parentIDs), 10, func(ctx context.Context, job interface{}) error {
		parentID := job.(string)
		parent := &block{id: parentID}

		if err := buildBlockTree(ctx, bkt, user, parent); err != nil {
			return err
		}

		input.parentsMx.Lock()
		input.parents = append(input.parents, parent)
		input.parentsMx.Unlock()
		return nil
	})
}

func printBlockTree(input *block, indent int) {
	minT := time.Unix(0, input.minT*int64(time.Millisecond)).String()
	maxT := time.Unix(0, input.maxT*int64(time.Millisecond)).String()

	fmt.Println(fmt.Sprintf("%s%s", pad(indent), input.id))
	fmt.Println(fmt.Sprintf("%s  Info:     mint: %s maxt: %s level: %d", pad(indent), minT, maxT, input.level))
	fmt.Println(fmt.Sprintf("%s  Labels:   %s", pad(indent), formatLabels(input.labels)))
	fmt.Println(fmt.Sprintf("%s  Uploaded: %s", pad(indent), input.uploadTime.String()))

	if len(input.parents) == 0 {
		return
	}

	fmt.Println(fmt.Sprintf("%s  Parents:", pad(indent)))
	for _, parent := range input.parents {
		printBlockTree(parent, indent+4)
	}
}

func formatLabels(labels map[string]string) string {
	entries := make([]string, 0, len(labels))
	for key, value := range labels {
		if key == "__org_id__" {
			continue
		}

		entries = append(entries, fmt.Sprintf("%s:%s", key, value))
	}

	return strings.Join(entries, " ")
}

func pad(length int) string {
	return strings.Repeat(" ", length)
}

func fetchMeta(ctx context.Context, bkt objstore.Bucket, userID string, blockID string) (*metadata.Meta, error) {
	r, err := bkt.Get(ctx, path.Join(userID, "debug", "metas", blockID+".json"))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return metadata.Read(r)
}

func getUploadTime(ctx context.Context, bkt objstore.Bucket, userID string, blockID string) (time.Time, error) {
	r, err := bkt.Attributes(ctx, path.Join(userID, "debug", "metas", blockID+".json"))
	if err != nil {
		return time.Time{}, err
	}

	return r.LastModified, nil
}
