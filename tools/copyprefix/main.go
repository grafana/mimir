// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/util/objtools"
)

type config struct {
	copyConfig        objtools.CopyBucketConfig
	sourcePrefix      string
	destinationPrefix string
	overwrite         bool
	dryRun            bool
}

func (c *config) RegisterFlags(f *flag.FlagSet) {
	c.copyConfig.RegisterFlags(f)
	f.StringVar(&c.sourcePrefix, "source-prefix", "", "The prefix to copy from the source. If the prefix is not empty and does not end in '"+objtools.Delim+"' then it is appended.")
	f.StringVar(&c.destinationPrefix, "destination-prefix", "", "Replaces the source prefix in the object name of objects copied to the destination. If not provided the object name from the source is used.")
	f.BoolVar(&c.overwrite, "overwrite", true, "If false a listing will be performed on the destination bucket to skip copying objects already present there. The presence check is best effort and not resistant to TOCTOU (Time of Check to Time of Use).")
	f.BoolVar(&c.dryRun, "dry-run", false, "If true no copying will actually occur and instead a log message will be written.")
}

func (c *config) Validate() error {
	if err := c.copyConfig.Validate(); err != nil {
		return err
	}

	return nil
}

func main() {
	cfg := config{}
	cfg.RegisterFlags(flag.CommandLine)

	logger := slog.Default()

	// Parse CLI arguments.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := runCopy(ctx, cfg, logger); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func runCopy(ctx context.Context, cfg config, logger *slog.Logger) error {
	sourceBucket, destBucket, copyFunc, err := cfg.copyConfig.ToBuckets(ctx)
	if err != nil {
		return err
	}

	sourcePrefix := ensureDelimiterSuffix(cfg.sourcePrefix)
	destinationPrefix := ensureDelimiterSuffix(cfg.destinationPrefix)

	sourceNames, err := listNames(ctx, sourceBucket, sourcePrefix)
	if err != nil {
		return err
	}

	var exists map[string]struct{}
	if cfg.overwrite {
		destNames, err := listNames(ctx, destBucket, sourcePrefix)
		if err != nil {
			return err
		}
		exists := make(map[string]struct{}, len(destNames))
		for _, name := range destNames {
			exists[name] = struct{}{}
		}
	}

	for _, sourceName := range sourceNames {
		destinationName, err := destinationName(sourceName, sourcePrefix, destinationPrefix)
		if err != nil {
			return err
		}

		logger := slog.With("sourceObject", sourceName, "destinationObject", destinationName)

		if _, ok := exists[destinationName]; ok {
			logger.Info("Not copying an object since it exists in the destination bucket")
			continue
		}
		if cfg.dryRun {
			logger.Info("Would have copied an object, but skipping due to dry run.")
			continue
		}
		err = copyFunc(ctx, sourceName, objtools.CopyOptions{
			DestinationObjectName: destinationName,
		})
		if err != nil {
			logger.Error("Failed copying an object")
			return err
		}
		logger.Info("Successfully copied an object")
	}

	return nil
}

func ensureDelimiterSuffix(s string) string {
	if s != "" && !strings.HasSuffix(s, "/") {
		return s + objtools.Delim
	}
	return s
}

func listNames(ctx context.Context, bucket objtools.Bucket, prefix string) ([]string, error) {
	listing, err := bucket.List(ctx, objtools.ListOptions{
		Prefix:    prefix,
		Recursive: true,
	})
	if err != nil {
		return nil, err
	}
	return listing.ToNames(), nil
}

func destinationName(sourceName string, sourcePrefix string, destinationPrefix string) (string, error) {
	if destinationPrefix == "" {
		return sourceName, nil
	}
	s, ok := strings.CutPrefix(sourceName, sourcePrefix)
	if !ok {
		return "", errors.Errorf("unexpected object encountered that did not contains prefix %s: %s", sourcePrefix, sourceName)
	}
	return destinationPrefix + s, nil
}
