package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/objtools"
)

type config struct {
	subsetBucketConfig              objtools.BucketConfig
	supersetBucketConfig            objtools.BucketConfig
	minBlockDuration                time.Duration
	minTime                         flagext.Time
	maxTime                         flagext.Time
	tenantConcurrency               int
	blockConcurrency                int
	enabledUsers                    flagext.StringSliceCSV
	disabledUsers                   flagext.StringSliceCSV
	dryRun                          bool
	skipNoCompactBlockDurationCheck bool
	httpListen                      string
}

func (c *config) registerFlags(f *flag.FlagSet) {
	c.subsetBucketConfig.RegisterFlags(f)
	c.supersetBucketConfig.RegisterFlags(f)
	f.DurationVar(&c.minBlockDuration, "min-block-duration", 0, "If non-zero, ignore blocks that cover block range smaller than this.")
	f.Var(&c.minTime, "min-time", fmt.Sprintf("If set, only blocks with MinTime >= this value are inspected. The supported time format is %q.", time.RFC3339))
	f.Var(&c.maxTime, "max-time", fmt.Sprintf("If set, only blocks with MaxTime <= this value are inspected. The supported time format is %q.", time.RFC3339))
	f.IntVar(&c.tenantConcurrency, "tenant-concurrency", 5, "How many tenants to process at once.")
	f.IntVar(&c.blockConcurrency, "block-concurrency", 5, "How many blocks to inspect at once per tenant.")
	f.Var(&c.enabledUsers, "enabled-users", "If not empty, only blocks for these users are inspected.")
	f.Var(&c.disabledUsers, "disabled-users", "If not empty, blocks for these users are not inspected.")
	f.BoolVar(&c.skipNoCompactBlockDurationCheck, "skip-no-compact-block-duration-check", false, "If set, blocks marked as no-compact are not checked against min-block-duration.")
	f.StringVar(&c.httpListen, "http-listen-address", ":8080", "HTTP listen address.")
}

func (c *config) validate() error {
	if err := c.subsetBucketConfig.Validate(); err != nil {
		return err
	}
	if err := c.supersetBucketConfig.Validate(); err != nil {
		return err
	}
	if c.tenantConcurrency < 1 {
		return fmt.Errorf("-tenant-concurrency must be positive")
	}
	if c.blockConcurrency < 1 {
		return fmt.Errorf("-block-concurrency must be positive")
	}
	return nil
}

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	cfg := config{}
	cfg.registerFlags(flag.CommandLine)

	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		level.Info(logger).Log("msg", "HTTP server listening on "+cfg.httpListen)
		http.Handle("/metrics", promhttp.Handler())
		server := &http.Server{
			Addr:         cfg.httpListen,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
		}
		if err := server.ListenAndServe(); err != nil {
			level.Error(logger).Log("msg", "failed to start HTTP server")
			os.Exit(1)
		}
	}()

	err := run(ctx, logger, cfg)
	if err != nil {
		level.Error(logger).Log("msg", "run failed", "err", err, "dryRun", cfg.dryRun)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger log.Logger, cfg config) error {
	subBkt, err := cfg.subsetBucketConfig.ToBucket(ctx)
	if err != nil {
		return err
	}
	supBkt, err := cfg.supersetBucketConfig.ToBucket(ctx)
	if err != nil {
		return err
	}

	tenants, err := listTenants(ctx, supBkt)
	if err != nil {
		return fmt.Errorf("list tenants in superset bucket: %w", err)
	}

	allowedTenants := util.NewAllowList(cfg.enabledUsers, cfg.disabledUsers)

	return concurrency.ForEachUser(ctx, tenants, cfg.tenantConcurrency, func(ctx context.Context, tenantID string) error {
		if !allowedTenants.IsAllowed(tenantID) {
			return nil
		}

		logger := log.With(logger, "tenantID", tenantID)

		supBlocks, err := listBlocksForTenant(ctx, supBkt, tenantID)
		if err != nil {
			return fmt.Errorf("list superset blocks for tenant %v: %w", tenantID, err)
		}

		markers, err := listBlockMarkersForTenant(ctx, supBkt, tenantID)
		if err != nil {
			return fmt.Errorf("list supeset block markers for tenant %v: %w", tenantID, err)
		}

		var blockIDs []string
		for _, b := range supBlocks {
			blockIDs = append(blockIDs, b.String())
		}

		// We use ForEachUser here to keep processing other blocks, if the block fails. We pass block IDs as "users".
		return concurrency.ForEachUser(ctx, blockIDs, cfg.blockConcurrency, func(ctx context.Context, blockIDStr string) error {
			blockID, err := ulid.Parse(blockIDStr)
			if err != nil {
				return err
			}

			logger := log.With(logger, "block", blockID)

			// Optimization: if the block has been created before the min time filter, then we're comfortable
			// assuming there will be no samples >= min time filter in the block.
			if filterMinTime, blockCreationTime := time.Time(cfg.minTime), ulid.Time(blockID.Time()); !filterMinTime.IsZero() && blockCreationTime.Before(filterMinTime) {
				level.Debug(logger).Log("msg", "skipping block, block creation time is lower than the configured min time filter", "configured_min_time", filterMinTime, "block_creation_time", blockCreationTime)
				return nil
			}

			blockMeta, err := loadMetaJSONFile(ctx, supBkt, tenantID, blockID)
			if err != nil {
				return fmt.Errorf("read meta.json file: %w", err)
			}

			// Add min/max time to each log entry. This is useful for debugging purposes.
			blockMinTime := time.Unix(0, blockMeta.MinTime*int64(time.Millisecond)).UTC()
			blockMaxTime := time.Unix(0, blockMeta.MaxTime*int64(time.Millisecond)).UTC()
			logger = log.With(logger, "block_min_time", blockMinTime, "block_max_time", blockMaxTime)

			if markers[blockID].deletion {
				level.Debug(logger).Log("msg", "skipping block because it is marked for deletion")
				return nil
			}

			// If the min time filter is set, only blocks with MinTime >= the configured value are copied.
			if filterMinTime := time.Time(cfg.minTime); !filterMinTime.IsZero() && blockMinTime.Before(filterMinTime) {
				level.Debug(logger).Log("msg", "skipping block, block min time is less than the configured min time filter", "configured_min_time", filterMinTime)
				return nil
			}

			// If the max time filter is set, only blocks with MaxTime <= the configured value are copied.
			if filterMaxTime := time.Time(cfg.maxTime); !filterMaxTime.IsZero() && blockMaxTime.After(filterMaxTime) {
				level.Debug(logger).Log("msg", "skipping block, block max time is greater than the configured max time filter", "configured_max_time", filterMaxTime)
				return nil
			}

			skipDurationCheck := cfg.skipNoCompactBlockDurationCheck && markers[blockID].noCompact
			if !skipDurationCheck && cfg.minBlockDuration > 0 {
				blockDuration := time.Millisecond * time.Duration(blockMeta.MaxTime-blockMeta.MinTime)
				if blockDuration < cfg.minBlockDuration {
					level.Debug(logger).Log("msg", "skipping block, block duration is less than the configured minimum duration", "block_duration", blockDuration, "configured_min_duration", cfg.minBlockDuration)
					return nil
				}
			}

			if cfg.dryRun {
				level.Info(logger).Log("msg", "would inspect block, but skipping due to dry-run")
				return nil
			}

			level.Info(logger).Log("msg", "copying block")

			err = copySingleBlock(ctx, tenantID, destinationTenantID, blockID, markers[blockID], sourceBucket, copyFunc)
			if err != nil {
				m.blocksCopyFailed.Inc()
				level.Error(logger).Log("msg", "failed to copy block", "err", err)
				return err
			}

			m.blocksCopied.Inc()
			level.Info(logger).Log("msg", "block copied successfully")

			// Note that only the blockID and destination bucket are considered in the copy marker.
			// If multiple tenants in the same destination bucket are copied to from the same source tenant the markers will currently clash.
			err = uploadCopiedMarkerFile(ctx, sourceBucket, tenantID, blockID, destBucket.Name())
			if err != nil {
				level.Error(logger).Log("msg", "failed to upload copied-marker file for block", "block", blockID.String(), "err", err)
				return err
			}
			return nil
		})
	})
}

// This method copies files within single TSDB block to a destination bucket.
func copySingleBlock(ctx context.Context, sourceTenantID, destinationTenantID string, blockID ulid.ULID, markers blockMarkers, srcBkt objtools.Bucket, copyFunc objtools.CopyFunc) error {
	result, err := srcBkt.List(ctx, objtools.ListOptions{
		Prefix:    sourceTenantID + objtools.Delim + blockID.String(),
		Recursive: true,
	})
	if err != nil {
		return errors.Wrapf(err, "copySingleBlock: failed to list block files for %v/%v", sourceTenantID, blockID.String())
	}
	paths := result.ToNames()

	// Reorder paths, move meta.json at the end. We want to upload meta.json as last file, because it signals to Mimir that
	// block upload has finished.
	length := len(paths)
	for i := length - 1; i >= 0; i-- {
		if strings.HasSuffix(paths[i], block.MetaFilename) {
			paths[i], paths[length-1] = paths[length-1], paths[i]
			break
		}
	}

	// Copy global markers too (skipping deletion mark because deleted blocks are not copied by this tool).
	if markers.noCompact {
		paths = append(paths, sourceTenantID+objtools.Delim+block.NoCompactMarkFilepath(blockID))
	}

	isCrossTenant := sourceTenantID != destinationTenantID

	for _, fullPath := range paths {
		options := objtools.CopyOptions{}
		if isCrossTenant {
			after, found := strings.CutPrefix(fullPath, sourceTenantID)
			if !found {
				return fmt.Errorf("unexpected object path that does not begin with sourceTenantID: path=%s, sourceTenantID=%s", fullPath, sourceTenantID)
			}
			options.DestinationObjectName = destinationTenantID + after
		}
		err := copyFunc(ctx, fullPath, options)
		if err != nil {
			return errors.Wrapf(err, "copySingleBlock: failed to copy %v", fullPath)
		}
	}

	return nil
}

func loadMetaJSONFile(ctx context.Context, bkt objtools.Bucket, tenantID string, blockID ulid.ULID) (block.Meta, error) {
	objectName := tenantID + objtools.Delim + blockID.String() + objtools.Delim + block.MetaFilename
	r, err := bkt.Get(ctx, objectName, objtools.GetOptions{})
	if err != nil {
		return block.Meta{}, fmt.Errorf("read %v: %w", objectName, err)
	}

	var m block.Meta
	err = json.NewDecoder(r).Decode(&m)
	closeErr := r.Close() // do this before any return.

	if err != nil {
		return block.Meta{}, fmt.Errorf("decode %v: %w", objectName, err)
	}
	if closeErr != nil {
		return block.Meta{}, fmt.Errorf("close reader for %v: %w", objectName, err)
	}

	return m, nil
}

func listTenants(ctx context.Context, bkt objtools.Bucket) ([]string, error) {
	result, err := bkt.List(ctx, objtools.ListOptions{})
	if err != nil {
		return nil, err
	}

	return result.ToNames(), nil
}

func listBlocksForTenant(ctx context.Context, bkt objtools.Bucket, tenantID string) ([]ulid.ULID, error) {
	result, err := bkt.List(ctx, objtools.ListOptions{
		Prefix: tenantID,
	})
	if err != nil {
		return nil, err
	}

	items := result.ToNames()
	blocks := make([]ulid.ULID, 0, len(items))

	for _, b := range items {
		if id, ok := block.IsBlockDir(b); ok {
			blocks = append(blocks, id)
		}
	}

	return blocks, nil
}

// Each block can have multiple markers. This struct combines them together into single struct.
type blockMarkers struct {
	deletion  bool
	noCompact bool
}

func listBlockMarkersForTenant(ctx context.Context, bkt objtools.Bucket, tenantID string) (map[ulid.ULID]blockMarkers, error) {
	prefix := tenantID + objtools.Delim + block.MarkersPathname
	listing, err := bkt.List(ctx, objtools.ListOptions{
		Prefix:    prefix,
		Recursive: false,
	})
	if err != nil {
		return nil, err
	}
	markers, err := listing.ToNamesWithoutPrefix(prefix)
	if err != nil {
		return nil, err
	}

	result := map[ulid.ULID]blockMarkers{}

	for _, m := range markers {
		if id, ok := block.IsDeletionMarkFilename(m); ok {
			bm := result[id]
			bm.deletion = true
			result[id] = bm
		}

		if id, ok := block.IsNoCompactMarkFilename(m); ok {
			bm := result[id]
			bm.noCompact = true
			result[id] = bm
		}
	}

	return result, nil
}
