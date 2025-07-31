package main

import (
	"context"
	"fmt"

	gokitlog "github.com/go-kit/log"
	"github.com/oklog/ulid/v2"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func validateDownloadConfig(cfg config) error {
	if cfg.userID == "" {
		return fmt.Errorf("missing --user")
	}
	if cfg.blockID == "" {
		return fmt.Errorf("missing --block")
	}
	if cfg.dest == "" {
		return fmt.Errorf("missing --dest")
	}

	return nil
}

func downloadBlock(ctx context.Context, BucketCfg bucket.Config, userID, blockIDRaw, dest string, logger gokitlog.Logger) error {
	overallBktClient, err := bucket.NewClient(ctx, BucketCfg, "bucket", logger, nil)
	if err != nil {
		return fmt.Errorf("new bucket client: %w", err)
	}

	bkt := bucket.NewPrefixedBucketClient(overallBktClient, userID)

	blockID, err := ulid.Parse(blockIDRaw)
	if err != nil {
		return fmt.Errorf("parse block ID '%s': %w", blockIDRaw, err)
	}

	return block.Download(ctx, logger, bkt, blockID, dest)
}
