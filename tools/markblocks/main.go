// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
)

var validNoCompactReasons = []string{
	string(metadata.ManualNoCompactReason),
	string(metadata.IndexSizeExceedingNoCompactReason),
	string(metadata.OutOfOrderChunksNoCompactReason),
}

func main() {
	ctx := context.Background()
	logger := log.WithPrefix(log.NewLogfmtLogger(os.Stderr), "time", log.DefaultTimestampUTC)

	var cfg struct {
		bucket   bucket.Config
		tenantID string
		blockIDs flagext.StringSlice

		mark string

		details string
		reason  string // used for no-compact only

		dryRun bool
	}

	cfg.bucket.RegisterFlags(flag.CommandLine)
	flag.StringVar(&cfg.tenantID, "tenant", "", "Tenant ID of the owner of the block. Required.")
	flag.Var(&cfg.blockIDs, "block", "The ULIDs of the blocks to be marked. Required. Can be provided multiple times.")
	flag.StringVar(&cfg.mark, "mark", "", "Mark type to create, valid options: deletion, no-compact. Required.")
	flag.BoolVar(&cfg.dryRun, "dry-run", false, "Don't upload the markers generated, just print the intentions on the screen. Optional.")
	flag.StringVar(&cfg.details, "details", "", "Details field of the uploaded mark. Optional but recommended.")
	flag.StringVar(&cfg.reason, "reason", string(metadata.ManualNoCompactReason), fmt.Sprintf("Reason field of no-compact mark. Only used for no-compact. Valid reasons: %s.", strings.Join(validNoCompactReasons, ", ")))
	flag.Parse()

	marker, filename := createMarker(cfg.mark, cfg.reason, logger, cfg.details)
	ulids := validateTenantAndBlocks(logger, cfg.tenantID, cfg.blockIDs)
	userBucket := createUserBucket(ctx, logger, cfg.bucket, cfg.tenantID)
	uploadMarks(ctx, logger, ulids, marker, filename, cfg.dryRun, userBucket)
}

func validateTenantAndBlocks(logger log.Logger, tenantID string, blockIDs flagext.StringSlice) []ulid.ULID {
	if tenantID == "" {
		level.Error(logger).Log("msg", "Flag -tenant is required.")
		os.Exit(1)
	}

	if len(blockIDs) == 0 {
		level.Warn(logger).Log("msg", "No blocks were provided. Nothing was done.")
		os.Exit(0)
	}

	var ulids []ulid.ULID
	for _, b := range blockIDs {
		blockID, err := ulid.Parse(b)
		if err != nil {
			level.Error(logger).Log("msg", "Can't parse block ID.", "block_id", b, "err", err)
			os.Exit(1)
		}
		ulids = append(ulids, blockID)
	}
	return ulids
}

func createMarker(markType string, reason string, logger log.Logger, details string) (func(b ulid.ULID) ([]byte, error), string) {
	switch markType {
	case "no-compact":
		reason, validReason := isValidNoCompactReason(reason)
		if !validReason {
			level.Error(logger).Log("msg", "Invalid -reason value", "value", reason, "valid_reasons", validNoCompactReasons)
			os.Exit(1)
		}
		return func(b ulid.ULID) ([]byte, error) {
			return json.Marshal(metadata.NoCompactMark{
				ID:            b,
				Version:       metadata.NoCompactMarkVersion1,
				NoCompactTime: time.Now().Unix(),
				Reason:        reason,
				Details:       details,
			})
		}, metadata.NoCompactMarkFilename
	case "deletion":
		if reason != "" {
			level.Error(logger).Log("msg", "Flag -reason is unsupported for deletion mark")
			os.Exit(1)
		}

		return func(b ulid.ULID) ([]byte, error) {
			return json.Marshal(metadata.DeletionMark{
				ID:           b,
				Version:      metadata.NoCompactMarkVersion1,
				Details:      details,
				DeletionTime: time.Now().Unix(),
			})
		}, metadata.DeletionMarkFilename
	default:
		level.Error(logger).Log("msg", "Invalid -mark flag value. Should be no-compact or deletion.", "value", markType)
		os.Exit(1)
		panic("We never reach this.")
	}
}

func createUserBucket(ctx context.Context, logger log.Logger, cfg bucket.Config, tenantID string) objstore.Bucket {
	bkt, err := bucket.NewClient(ctx, cfg, "bucket", logger, nil)
	if err != nil {
		level.Error(logger).Log("msg", "Can't instantiate bucket.", "err", err)
		os.Exit(1)
	}
	userBucket := bucketindex.BucketWithGlobalMarkers(
		bucket.NewUserBucketClient(tenantID, bkt, nil),
	)
	return userBucket
}

func uploadMarks(ctx context.Context, logger log.Logger, ulids []ulid.ULID, mark func(b ulid.ULID) ([]byte, error), filename string, dryRun bool, userBucket objstore.Bucket) {
	for _, b := range ulids {
		blockMetaFilename := fmt.Sprintf("%s/meta.json", b)

		if exists, err := userBucket.Exists(ctx, blockMetaFilename); err != nil {
			level.Error(logger).Log("msg", "Can't check meta.json existence.", "block_id", b, "filename", blockMetaFilename, "err", err)
			os.Exit(1)
		} else if !exists {
			level.Info(logger).Log("msg", "Block does not exist, skipping.", "block_id", b)
			continue
		}

		blockMarkFilename := fmt.Sprintf("%s/%s", b, filename)
		if exists, err := userBucket.Exists(ctx, blockMarkFilename); err != nil {
			level.Error(logger).Log("msg", "Can't check mark file existence.", "block_id", b, "filename", blockMarkFilename, "err", err)
			os.Exit(1)
		} else if exists {
			level.Info(logger).Log("msg", "Mark already exists, skipping.", "block_id", b)
			continue
		}

		data, err := mark(b)
		if err != nil {
			level.Error(logger).Log("msg", "Can't create mark.", "block_id", b, "err", err)
			os.Exit(1)
		}
		if dryRun {
			logger.Log("msg", "Dry-run, so not making changes.", "block_id", b, "mark", string(data))
			continue
		}

		if err := userBucket.Upload(ctx, blockMarkFilename, bytes.NewReader(data)); err != nil {
			level.Info(logger).Log("msg", "Can't upload mark.", "block_id", b, "err", err)
			os.Exit(1)
		}

		level.Info(logger).Log("msg", "Successfully uploaded mark.", "block_id", b)
	}
}

func isValidNoCompactReason(reason string) (metadata.NoCompactReason, bool) {
	for _, r := range validNoCompactReasons {
		if r == reason {
			return metadata.NoCompactReason(r), true
		}
	}
	return "", false
}
