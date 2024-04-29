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
	skipOverwrites    bool
	dryRun            bool
}

func (c *config) RegisterFlags(f *flag.FlagSet) {
	c.copyConfig.RegisterFlags(f)
	f.StringVar(&c.sourcePrefix, "source-prefix", "", "The prefix to copy from the source. If the prefix is not empty and does not end in '"+objtools.Delim+"' then it is appended.")
	f.StringVar(&c.destinationPrefix, "destination-prefix", "", "Replaces the source prefix in the object name of objects copied to the destination. If not provided the object name from the source is used.")
	f.BoolVar(&c.skipOverwrites, "skip-overwrites", false, "If true a listing will be performed on the destination bucket to skip copying objects already present there. The presence check is best effort.")
	f.BoolVar(&c.dryRun, "dry-run", false, "If true no copying will actually occur and instead a log message will be written.")
}

func (c *config) Validate() error {
	return c.copyConfig.Validate()
}

func main() {
	cfg := config{}
	cfg.RegisterFlags(flag.CommandLine)

	logger := slog.Default()

	// Parse CLI arguments.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "failed while parsing flags"))
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "configuration validation failed"))
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
		slog.Error("failed to construct object storage clients")
		return err
	}

	sourcePrefix := cfg.sourcePrefix
	var destinationPrefix string
	if cfg.destinationPrefix == "" { // this assumption prevents up-copying to root, possibly an option could be added for that if needed
		destinationPrefix = sourcePrefix
	} else {
		destinationPrefix = cfg.destinationPrefix
	}

	sourceNames, err := listNames(ctx, sourceBucket, sourcePrefix)
	if err != nil {
		slog.Error("failed to list source bucket", "prefix", sourcePrefix)
		return err
	}

	var exists map[string]struct{}
	if cfg.skipOverwrites {
		destNames, err := listNames(ctx, destBucket, destinationPrefix)
		if err != nil {
			logger.Error("failed to list destination bucket", "prefix", destinationPrefix)
			return err
		}
		exists = make(map[string]struct{}, len(destNames))
		for _, name := range destNames {
			exists[name] = struct{}{}
		}
	}

	for _, sourceName := range sourceNames {
		destinationName, err := destinationName(sourceName, sourcePrefix, destinationPrefix)
		if err != nil {
			return err
		}

		logger := logger.With("sourceObject", sourceName, "destinationObject", destinationName)

		if _, ok := exists[destinationName]; ok {
			logger.Info("not copying an object since it exists in the destination bucket")
			continue
		}
		if cfg.dryRun {
			logger.Info("would have copied an object, but skipping due to dry run.")
			continue
		}
		err = copyFunc(ctx, sourceName, objtools.CopyOptions{
			DestinationObjectName: destinationName,
		})
		if err != nil {
			logger.Error("failed copying an object")
			return err
		}
		logger.Info("successfully copied an object")
	}

	return nil
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
	if destinationPrefix == sourcePrefix {
		return sourceName, nil
	}
	s, ok := strings.CutPrefix(sourceName, sourcePrefix)
	if !ok {
		return "", errors.Errorf("unexpected object encountered that did not contain prefix %s: %s", sourcePrefix, sourceName)
	}
	return destinationPrefix + s, nil
}
