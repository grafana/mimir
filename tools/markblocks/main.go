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
	dskit_concurrency "github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/oklog/ulid"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
)

type config struct {
	bucket             bucket.Config
	tenantID           string
	dryRun             bool
	allowPartialBlocks bool
	concurrency        int

	mark    string
	details string
	blocks  []string

	helpAll bool
}

func main() {
	ctx := context.Background()
	logger := log.WithPrefix(log.NewLogfmtLogger(os.Stderr), "time", log.DefaultTimestampUTC)

	cfg := parseFlags(logger)
	marker, filename := createMarker(cfg.mark, logger, cfg.details)
	ulids := validateTenantAndBlocks(logger, cfg.tenantID, cfg.blocks)
	uploadMarks(ctx, logger, ulids, marker, filename, cfg.dryRun, cfg.bucket, cfg.tenantID, cfg.allowPartialBlocks, cfg.concurrency)
}

func parseFlags(logger log.Logger) config {
	var cfg config

	// We define two flag sets, one on basic straightforward flags of this cli, and the other one with all flags,
	// which includes the bucket configuration flags, as there quite a lot of them and the help output with them
	// might look a little bit overwhelming at first contact.
	fullFlagSet := flag.NewFlagSet("markblocks", flag.ExitOnError)
	fullFlagSet.SetOutput(os.Stdout)
	basicFlagSet := flag.NewFlagSet("markblocks", flag.ExitOnError)
	basicFlagSet.SetOutput(os.Stdout)

	// We register our basic flags on both basic and full flag set.
	for _, f := range []*flag.FlagSet{basicFlagSet, fullFlagSet} {
		f.StringVar(&cfg.tenantID, "tenant", "", "Tenant ID of the owner of the block. Required.")
		f.StringVar(&cfg.mark, "mark", "", "Mark type to create, valid options: deletion, no-compact. Required.")
		f.BoolVar(&cfg.dryRun, "dry-run", false, "Don't upload the markers generated, just print the intentions.")
		f.StringVar(&cfg.details, "details", "", "Details field of the uploaded mark. Recommended. (default empty).")
		f.BoolVar(&cfg.helpAll, "help-all", false, "Show help for all flags, including the bucket backend configuration.")
		f.BoolVar(&cfg.allowPartialBlocks, "allow-partial", false, "Allow upload of marks into partial blocks (ie. blocks without meta.json). Only useful for deletion mark.")
	}

	commonUsageHeader := func() {
		fmt.Println("This tool creates marks for TSDB blocks used by Mimir and uploads them to the specified backend.")
		fmt.Println("")
		fmt.Println("Usage:")
		fmt.Println("        markblocks -tenant <tenant id> -mark <deletion|no-compact> [-details <details message>] [-dry-run] blockID [blockID2 blockID3 ...]")
		fmt.Println("")
	}

	// We set the usage to fullFlagSet as that's the flag set we'll be always parsing,
	// but by default we print only the basic flag set defaults.
	fullFlagSet.Usage = func() {
		commonUsageHeader()
		if cfg.helpAll {
			fullFlagSet.PrintDefaults()
		} else {
			basicFlagSet.PrintDefaults()
		}
	}

	// We set only the `-backend` flag on the basicFlagSet, to make sure that user sees that there are more backends supported.
	// Then we register all bucket flags on the full flag set, which is the flag set we're parsing.
	basicFlagSet.StringVar(&cfg.bucket.Backend, "backend", bucket.Filesystem, fmt.Sprintf("Backend storage to use. Supported backends are: %s. Use -help-all to see help on backends configuration.", strings.Join(bucket.SupportedBackends, ", ")))
	cfg.bucket.RegisterFlags(fullFlagSet, logger)

	fullFlagSet.IntVar(&cfg.concurrency, "concurrency", 16, "How many markers to upload concurrently.")

	if err := fullFlagSet.Parse(os.Args[1:]); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// See if user did `markblocks -help-all`.
	if cfg.helpAll {
		commonUsageHeader()
		fullFlagSet.PrintDefaults()
		os.Exit(0)
	}
	cfg.blocks = fullFlagSet.Args()

	return cfg
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
			level.Error(logger).Log("msg", "Can't parse block ID.", "block", b, "err", err)
			os.Exit(1)
		}
		ulids = append(ulids, blockID)
	}
	return ulids
}

func createMarker(markType string, logger log.Logger, details string) (func(b ulid.ULID) ([]byte, error), string) {
	switch markType {
	case "no-compact":
		return func(b ulid.ULID) ([]byte, error) {
			return json.Marshal(metadata.NoCompactMark{
				ID:            b,
				Version:       metadata.NoCompactMarkVersion1,
				NoCompactTime: time.Now().Unix(),
				Reason:        metadata.ManualNoCompactReason,
				Details:       details,
			})
		}, metadata.NoCompactMarkFilename
	case "deletion":
		return func(b ulid.ULID) ([]byte, error) {
			return json.Marshal(metadata.DeletionMark{
				ID:           b,
				Version:      metadata.DeletionMarkVersion1,
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

func uploadMarks(
	ctx context.Context,
	logger log.Logger,
	ulids []ulid.ULID,
	mark func(b ulid.ULID) ([]byte, error),
	markFilename string,
	dryRun bool,
	cfg bucket.Config,
	tenantID string,
	allowPartialBlocks bool,
	concurrency int,
) {
	userBucketWithGlobalMarkers := createUserBucketWithGlobalMarkers(ctx, logger, cfg, tenantID)

	err := dskit_concurrency.ForEachJob(ctx, len(ulids), concurrency, func(ctx context.Context, idx int) error {
		b := ulids[idx]

		blockFiles := map[string]bool{}
		// List all files in the blocks directory. We don't need recursive listing: if any segment
		// files (chunks/0000xxx) are present, we will find "chunks" during iter.
		err := userBucketWithGlobalMarkers.Iter(ctx, b.String(), func(fn string) error {
			if !strings.HasPrefix(fn, b.String()+"/") {
				return nil
			}

			fn = strings.TrimPrefix(fn, b.String()+"/")
			fn = strings.TrimSuffix(fn, "/")

			blockFiles[fn] = true
			return nil
		})
		if err != nil {
			if userBucketWithGlobalMarkers.IsObjNotFoundErr(err) {
				level.Warn(logger).Log("msg", "Block does not exist", "block", b, "err", err)
				return nil
			}

			level.Error(logger).Log("msg", "Failed to list files for block.", "block", b, "err", err)
			return err
		}

		if len(blockFiles) == 0 {
			level.Warn(logger).Log("msg", "Block does not exist, skipping.", "block", b)
			return nil
		}

		if !blockFiles[metadata.MetaFilename] && !allowPartialBlocks {
			level.Warn(logger).Log("msg", "Block's meta.json file does not exist, skipping.", "block", b)
			return nil
		}

		if blockFiles[markFilename] {
			level.Warn(logger).Log("msg", "Mark already exists, skipping.", "block", b)
			return nil
		}

		data, err := mark(b)
		if err != nil {
			level.Error(logger).Log("msg", "Can't create mark.", "block", b, "err", err)
			return err
		}

		blockMarkPath := fmt.Sprintf("%s/%s", b, markFilename)
		if dryRun {
			level.Info(logger).Log("msg", "Dry-run, not uploading marker.", "block", b, "marker", blockMarkPath, "data", string(data))
			return nil
		}

		if err := userBucketWithGlobalMarkers.Upload(ctx, blockMarkPath, bytes.NewReader(data)); err != nil {
			level.Error(logger).Log("msg", "Can't upload mark.", "block", b, "err", err)
			return err
		}

		level.Info(logger).Log("msg", "Successfully uploaded mark.", "block", b)
		return nil
	})

	if err != nil {
		os.Exit(1)
	}
}

func createUserBucketWithGlobalMarkers(ctx context.Context, logger log.Logger, cfg bucket.Config, tenantID string) objstore.Bucket {
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
