// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/cmd/thanosconvert/main.go
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/tools/thanosconvert/thanosconvert.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path"
	"slices"
	"syscall"

	gklog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	dslog "github.com/grafana/dskit/log"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/log"
)

type config struct {
	LogLevel     dslog.Level
	LogFormat    string
	BucketConfig bucket.Config
	DryRun       bool
	Tenant       string
}

func main() {
	cfg := config{}

	cfg.LogLevel.RegisterFlags(flag.CommandLine)
	cfg.BucketConfig.RegisterFlags(flag.CommandLine)

	flag.StringVar(&cfg.LogFormat, "log.format", dslog.LogfmtFormat, "Output log messages in the given format. Valid formats: [logfmt, json]")
	flag.BoolVar(&cfg.DryRun, "dry-run", false, "Don't make changes; only report what needs to be done")
	flag.StringVar(&cfg.Tenant, "tenant", "", "Tenant to process")
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), os.Args[0], "is a tool to update meta.json files to conform to Mimir requirements.")
		fmt.Fprintln(flag.CommandLine.Output(), "Flags:")
		flag.PrintDefaults()
	}

	// Parse CLI arguments.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		exitWithMessage(err.Error())
	}

	logger := log.InitLogger(cfg.LogFormat, cfg.LogLevel, false, log.RateLimitedLoggerCfg{})

	if cfg.Tenant == "" {
		exitWithMessage("Use -tenant parameter to specify tenant, or -h to get list of available options.")
	}

	if err := cfg.BucketConfig.Validate(); err != nil {
		exitWithMessage("Bucket config is invalid: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	bkt, err := bucket.NewClient(ctx, cfg.BucketConfig, "metaconvert", logger, nil)
	if err != nil {
		exitWithMessage("Failed to create bucket client: %v", err)
	}

	userBucketClient := bucket.NewUserBucketClient(cfg.Tenant, bkt, nil)
	err = convertTenantBlocks(ctx, userBucketClient, cfg.Tenant, cfg.DryRun, logger)
	if err != nil {
		exitWithMessage("Update of meta.json files failed: %v", err)
	}
}

func exitWithMessage(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func convertTenantBlocks(ctx context.Context, userBucketClient objstore.Bucket, tenant string, dryRun bool, logger gklog.Logger) error {
	logger = gklog.With(logger, "tenant", tenant)

	return userBucketClient.Iter(ctx, "", func(dir string) error {
		blockID, ok := block.IsBlockDir(dir)
		if !ok {
			// not a block
			return nil
		}

		// retrieve meta.json
		meta, err := block.DownloadMeta(ctx, logger, userBucketClient, blockID)
		if err != nil {
			level.Error(logger).Log("msg", "failed to download meta.json file for block", "block", blockID.String(), "err", err.Error())
			return nil
		}

		if meta.Thanos.Labels == nil {
			meta.Thanos.Labels = map[string]string{}
		}

		updated := false

		// Sort labels before processing to have stable output for testing.
		var labels []string
		for l := range meta.Thanos.Labels {
			labels = append(labels, l)
		}
		slices.Sort(labels)

		for _, l := range labels {
			switch l {
			case mimir_tsdb.CompactorShardIDExternalLabel:
				continue
			}

			level.Warn(logger).Log("msg", "removing unknown label", "block", blockID.String(), "label", l, "value", meta.Thanos.Labels[l])
			updated = true
			delete(meta.Thanos.Labels, l)
		}

		if !updated {
			level.Info(logger).Log("msg", "no changes required", "block", blockID.String())
			return nil
		}

		if dryRun {
			level.Warn(logger).Log("msg", "changes required, not uploading back due to dry run", "block", blockID.String())
			return nil
		}

		// convert and upload if appropriate
		level.Info(logger).Log("msg", "changes required, uploading meta.json file", "block", blockID.String())

		if err := uploadMetadata(ctx, userBucketClient, meta, path.Join(blockID.String(), block.MetaFilename)); err != nil {
			return errors.Wrapf(err, "failed to upload meta.json for block %s", blockID.String())
		}

		level.Info(logger).Log("msg", "meta.json file uploaded successfully", "block", blockID.String())
		return nil
	})
}

func uploadMetadata(ctx context.Context, bkt objstore.Bucket, meta block.Meta, path string) error {
	var body bytes.Buffer
	if err := meta.Write(&body); err != nil {
		return errors.Wrap(err, "encode meta.json")
	}

	return bkt.Upload(ctx, path, &body)
}
