package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"

	gokitlog "github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/storage/bucket"

	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// Creates a no-compaction mark for a block.
func main() {

	cfg := struct {
		bucket  bucket.Config
		userID  string
		blockID string
		reason  string
		details string
	}{}
	cfg.bucket.RegisterFlags(flag.CommandLine)
	flag.StringVar(&cfg.blockID, "ulid", "", "The ULID of the block to mark.")
	flag.StringVar(&cfg.userID, "user", "", "User (tenant).")
	flag.StringVar(&cfg.reason, "reason", string(metadata.ManualNoCompactReason),
		fmt.Sprintf("The reason field of the marker. Valid values are %q, %q and %q.",
			metadata.ManualNoCompactReason, metadata.IndexSizeExceedingNoCompactReason, metadata.OutOfOrderChunksNoCompactReason))
	flag.StringVar(&cfg.details, "details", "", "The details field of the marker.")
	flag.Parse()

	if cfg.userID == "" {
		log.Fatal("User cannot be blank.")
	}

	if cfg.blockID == "" {
		log.Fatal("ULID cannot be blank.")
	}

	reason := metadata.NoCompactReason(cfg.reason)
	switch reason {
	case metadata.ManualNoCompactReason,
		metadata.IndexSizeExceedingNoCompactReason,
		metadata.OutOfOrderChunksNoCompactReason:
		// Valid.
	default:
		log.Fatalf("Invalid reason %q, see help for valid reason values.\n", cfg.reason)
	}

	blockID, err := ulid.Parse(cfg.blockID)
	if err != nil {
		log.Fatalf("Can't parse %q as ULID: %s.\n", cfg.blockID, err)
	}

	noCompactMark, err := json.Marshal(metadata.NoCompactMark{
		ID:            blockID,
		Version:       metadata.NoCompactMarkVersion1,
		NoCompactTime: time.Now().Unix(),
		Reason:        reason,
		Details:       cfg.details,
	})
	if err != nil {
		log.Fatalf("Can't create the mark: %s.\n", err)
	}

	logger := gokitlog.NewNopLogger()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	bkt, err := bucket.NewClient(ctx, cfg.bucket, "bucket", logger, nil)
	if err != nil {
		log.Fatalf("Can't create bucket: %s.", err)
	}

	filename := bucketindex.NoCompactMarkFilepath(blockID)
	userBucket := bucket.NewUserBucketClient(cfg.userID, bkt, nil)
	if err := userBucket.Upload(ctx, filename, bytes.NewReader(noCompactMark)); err != nil {
		log.Fatalf("Can't upload the compaction mark to the bucket: %s.", err)
	}

	log.Printf("Successfully uploaded no-compaction mark file %q.", filename)
}
