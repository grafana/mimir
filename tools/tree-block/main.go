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
	"syscall"

	gokitlog "github.com/go-kit/kit/log"
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
	id      string
	parents []*block
	labels  map[string]string
	level   int
}

func buildBlockTree(ctx context.Context, bkt objstore.Bucket, user string, input *block) error {
	// TODO DEBUG
	fmt.Println("fetching", input.id)

	// Fetch the meta for the input block.
	inputMeta, err := fetchMeta(ctx, bkt, user, input.id)
	if err != nil {
		return err
	}

	// Enrich the input block with some information.
	input.labels = inputMeta.Thanos.Labels
	input.level = inputMeta.Compaction.Level

	// Iterate for each parent.
	for _, parentMeta := range inputMeta.BlockMeta.Compaction.Parents {
		parent := &block{id: parentMeta.ULID.String()}

		if err := buildBlockTree(ctx, bkt, user, parent); err != nil {
			return err
		}
		input.parents = append(input.parents, parent)
	}

	return nil
}

func printBlockTree(input *block, indent int) {
	fmt.Println(fmt.Sprintf("%s%s (level: %d, labels: %s)", pad(indent), input.id, input.level, formatLabels(input.labels)))

	for _, parent := range input.parents {
		printBlockTree(parent, indent+2)
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
