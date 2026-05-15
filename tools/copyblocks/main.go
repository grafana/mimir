// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/cmd/blockscopy/main.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Grafana Labs.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/tenant"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/mimirtool/client"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/objtools"
)

type config struct {
	copyConfig                      objtools.CopyBucketConfig
	backfill backfillConfig
	minBlockDuration                time.Duration
	minTime                         flagext.Time
	maxTime                         flagext.Time
	tenantConcurrency               int
	blockConcurrency                int
	copyPeriod                      time.Duration
	enabledUsers                    flagext.StringSliceCSV
	disabledUsers                   flagext.StringSliceCSV
	userMapping                     flagext.StringSliceCSV
	dryRun                          bool
	skipNoCompactBlockDurationCheck bool
	httpListen                      string
}

func (c *config) registerFlags(f *flag.FlagSet) {
	c.copyConfig.RegisterFlags(f, true)
	c.backfill.RegisterFlags(f)
	f.DurationVar(&c.minBlockDuration, "min-block-duration", 0, "If non-zero, ignore blocks that cover block range smaller than this.")
	f.Var(&c.minTime, "min-time", fmt.Sprintf("If set, only blocks with MinTime >= this value are copied. The supported time format is %q.", time.RFC3339))
	f.Var(&c.maxTime, "max-time", fmt.Sprintf("If set, only blocks with MaxTime <= this value are copied. The supported time format is %q.", time.RFC3339))
	f.IntVar(&c.tenantConcurrency, "tenant-concurrency", 5, "How many tenants to process at once.")
	f.IntVar(&c.blockConcurrency, "block-concurrency", 5, "How many blocks to copy at once per tenant.")
	f.DurationVar(&c.copyPeriod, "copy-period", 0, "How often to repeat the copy. If set to 0, copy is done once, and the program stops. Otherwise, the program keeps running and copying blocks until it is terminated.")
	f.Var(&c.enabledUsers, "enabled-users", "If not empty, only blocks for these users are copied.")
	f.Var(&c.disabledUsers, "disabled-users", "If not empty, blocks for these users are not copied.")
	f.Var(&c.userMapping, "user-mapping", "A comma-separated list of (source user):(destination user). If a user is not mapped then its destination user is assumed to be identical.")
	f.BoolVar(&c.dryRun, "dry-run", false, "Don't perform any copy; only log what would happen.")
	f.BoolVar(&c.skipNoCompactBlockDurationCheck, "skip-no-compact-block-duration-check", false, "If set, blocks marked as no-compact are not checked against min-block-duration.")
	f.StringVar(&c.httpListen, "http-listen-address", ":8080", "HTTP listen address.")
}

func (c *config) validate() error {
	if c.isBackfill() {
		if err := c.copyConfig.ValidateSource(); err != nil {
			return err
		}
		if err := c.backfill.Validate(); err != nil {
			return err
		}
		if len(c.userMapping) > 0 {
			return fmt.Errorf("-user-mapping cannot be used with -destination.backend=backfill")
		}
	} else if err := c.copyConfig.Validate(); err != nil {
		return err
	}
	if c.tenantConcurrency < 1 {
		return fmt.Errorf("-tenant-concurrency must be positive")
	}
	if c.blockConcurrency < 1 {
		return fmt.Errorf("-block-concurrency must be positive")
	}
	if c.copyPeriod < 0 {
		return fmt.Errorf("-copy-period must be non-negative")
	}
	return nil
}

func (c *config) isBackfill() bool {
	return c.copyConfig.DestinationBackend() == backfillBackend
}

func (c *config) parseUserMapping() (map[string]string, error) {
	m := make(map[string]string, len(c.userMapping))
	for _, mapping := range c.userMapping {
		splitMapping := strings.Split(mapping, ":")
		if len(splitMapping) != 2 || slices.Contains(splitMapping, "") {
			return nil, fmt.Errorf("invalid user mapping: %s", mapping)
		}
		for _, id := range splitMapping {
			if err := tenant.ValidTenantID(id); err != nil {
				return nil, err
			}
		}
		source := splitMapping[0]
		if _, ok := m[source]; ok {
			return nil, fmt.Errorf("multiple user mappings for source user: %s", source)
		}
		m[source] = splitMapping[1]
	}
	return m, nil
}

const backfillBackend = "backfill"

var errBlockAlreadyExists = errors.New("block already exists")

type backfillConfig struct {
	clientConfig client.Config
	sleepTime    time.Duration
}

func (c *backfillConfig) RegisterFlags(f *flag.FlagSet) {
	c.clientConfig.RegisterConnectionFlagsWithPrefix(backfillBackend, f)
	f.DurationVar(&c.sleepTime, backfillBackend+".sleep-time", 20*time.Second, "How long to sleep between checking the state of block upload.")
}

func (c *backfillConfig) Validate() error {
	if c.clientConfig.Address == "" {
		return fmt.Errorf("-backfill.address is required when -destination.backend is set to backfill")
	}
	if c.clientConfig.ID == "" {
		return fmt.Errorf("-backfill.id is required when -destination.backend is set to backfill")
	}
	if c.clientConfig.Key != "" && c.clientConfig.AuthToken != "" {
		return fmt.Errorf("at most one of -backfill.key and -backfill.auth-token can be set")
	}
	return nil
}

type metrics struct {
	copyCyclesSucceeded prometheus.Counter
	copyCyclesFailed    prometheus.Counter
	blocksCopied        prometheus.Counter
	blocksCopyFailed    prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *metrics {
	return &metrics{
		copyCyclesSucceeded: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocks_copy_successful_cycles_total",
			Help: "Number of successful blocks copy cycles.",
		}),
		copyCyclesFailed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocks_copy_failed_cycles_total",
			Help: "Number of failed blocks copy cycles.",
		}),
		blocksCopied: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocks_copy_blocks_copied_total",
			Help: "Number of blocks copied between buckets.",
		}),
		blocksCopyFailed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blocks_copy_blocks_failed_total",
			Help: "Number of blocks that failed to copy.",
		}),
	}
}

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	cfg := config{}
	cfg.registerFlags(flag.CommandLine)

	// Parse CLI arguments.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	userMapping, err := cfg.parseUserMapping()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	m := newMetrics(prometheus.DefaultRegisterer)

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

	success := runCopy(ctx, cfg, userMapping, logger, m)
	if cfg.copyPeriod <= 0 {
		if success {
			os.Exit(0)
		}
		os.Exit(1)
	}

	t := time.NewTicker(cfg.copyPeriod)
	defer t.Stop()

	for ctx.Err() == nil {
		select {
		case <-t.C:
			_ = runCopy(ctx, cfg, userMapping, logger, m)
		case <-ctx.Done():
		}
	}
}

func runCopy(ctx context.Context, cfg config, userMapping map[string]string, logger log.Logger, m *metrics) bool {
	err := copyBlocks(ctx, cfg, userMapping, logger, m)
	if err != nil {
		m.copyCyclesFailed.Inc()
		level.Error(logger).Log("msg", "failed to copy blocks", "err", err, "dryRun", cfg.dryRun)
		return false
	}

	m.copyCyclesSucceeded.Inc()
	level.Info(logger).Log("msg", "finished copying blocks", "dryRun", cfg.dryRun)
	return true
}

type blockCopier struct {
	name      string
	copyBlock func(ctx context.Context, srcTenant, dstTenant string, blockID ulid.ULID, markers blockMarkers) error
}

func newBucketBlockCopier(ctx context.Context, cfg config) (objtools.Bucket, *blockCopier, error) {
	sourceBucket, destBucket, copyFunc, err := cfg.copyConfig.ToBuckets(ctx)
	if err != nil {
		return nil, nil, err
	}
	return sourceBucket, &blockCopier{
		name: destBucket.Name(),
		copyBlock: func(ctx context.Context, srcTenant, dstTenant string, blockID ulid.ULID, markers blockMarkers) error {
			exists, err := destBucket.Exists(ctx, metaObjectName(dstTenant, blockID))
			if err != nil {
				return fmt.Errorf("failed to check if block already exists on destination: %w", err)
			}
			if exists {
				return errBlockAlreadyExists
			}
			return copySingleBlock(ctx, srcTenant, dstTenant, blockID, markers, sourceBucket, copyFunc)
		},
	}, nil
}

func newBackfillBlockCopier(ctx context.Context, cfg config, logger log.Logger) (objtools.Bucket, *blockCopier, error) {
	sourceBucket, err := cfg.copyConfig.SourceBucket(ctx)
	if err != nil {
		return nil, nil, err
	}
	objstoreBkt, err := cfg.copyConfig.SourceObjstoreBucket(ctx, logger)
	if err != nil {
		return nil, nil, err
	}
	mimirClient, err := client.New(cfg.backfill.clientConfig, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create Mimir client for backfill: %w", err)
	}
	return sourceBucket, &blockCopier{
		name: "backfill",
		copyBlock: func(ctx context.Context, srcTenant, _ string, blockID ulid.ULID, _ blockMarkers) error {
			prefixedBkt := objstore.NewPrefixedBucket(objstoreBkt, srcTenant)
			err := mimirClient.BackfillBlock(ctx, prefixedBkt, blockID, cfg.backfill.sleepTime)
			if errors.Is(err, client.ErrConflict) {
				return errBlockAlreadyExists 
			}
			return err
		},
	}, nil
}

func copyBlocks(ctx context.Context, cfg config, userMapping map[string]string, logger log.Logger, m *metrics) error {
	var sourceBucket objtools.Bucket
	var copier *blockCopier
	var err error
	if cfg.isBackfill() {
		sourceBucket, copier, err = newBackfillBlockCopier(ctx, cfg, logger)
	} else {
		sourceBucket, copier, err = newBucketBlockCopier(ctx, cfg)
	}
	if err != nil {
		return err
	}

	tenants, err := listTenants(ctx, sourceBucket)
	if err != nil {
		return fmt.Errorf("failed to list tenants: %w", err)
	}

	enabledUsers := map[string]struct{}{}
	disabledUsers := map[string]struct{}{}
	for _, u := range cfg.enabledUsers {
		enabledUsers[u] = struct{}{}
	}
	for _, u := range cfg.disabledUsers {
		disabledUsers[u] = struct{}{}
	}

	return concurrency.ForEachUser(ctx, tenants, cfg.tenantConcurrency, func(ctx context.Context, sourceTenantID string) error {
		if !isAllowedUser(enabledUsers, disabledUsers, sourceTenantID) {
			return nil
		}

		destinationTenantID, ok := userMapping[sourceTenantID]
		if !ok {
			destinationTenantID = sourceTenantID
		}

		logger := log.With(logger, "sourceTenantID", sourceTenantID, "destinationTenantID", destinationTenantID)

		blocks, err := listBlocksForTenant(ctx, sourceBucket, sourceTenantID)
		if err != nil {
			level.Error(logger).Log("msg", "failed to list blocks for tenant", "err", err)
			return fmt.Errorf("failed to list blocks for tenant %v: %w", sourceTenantID, err)
		}

		markers, err := listBlockMarkersForTenant(ctx, sourceBucket, sourceTenantID, copier.name)
		if err != nil {
			level.Error(logger).Log("msg", "failed to list blocks markers for tenant", "err", err)
			return fmt.Errorf("failed to list block markers for tenant %v: %w", sourceTenantID, err)
		}

		var blockIDs []string
		for _, b := range blocks {
			blockIDs = append(blockIDs, b.String())
		}

		// We use ForEachUser here to keep processing other blocks, if the block fails. We pass block IDs as "users".
		return concurrency.ForEachUser(ctx, blockIDs, cfg.blockConcurrency, func(ctx context.Context, blockIDStr string) error {
			blockID, err := ulid.Parse(blockIDStr)
			if err != nil {
				return err
			}

			logger := log.With(logger, "block", blockID)

			// Skip if the block was already copied.
			if markers[blockID].copied {
				level.Debug(logger).Log("msg", "skipping block because it has been copied already")
				return nil
			}

			// Optimization: if the block has been created before the min time filter, then we're comfortable
			// assuming there will be no samples >= min time filter in the block.
			if filterMinTime, blockCreationTime := time.Time(cfg.minTime), ulid.Time(blockID.Time()); !filterMinTime.IsZero() && blockCreationTime.Before(filterMinTime) {
				level.Debug(logger).Log("msg", "skipping block, block creation time is lower than the configured min time filter", "configured_min_time", filterMinTime, "block_creation_time", blockCreationTime)
				return nil
			}

			blockMeta, err := loadMetaJSONFile(ctx, sourceBucket, sourceTenantID, blockID)
			if err != nil {
				level.Error(logger).Log("msg", "skipping block, failed to read meta.json file", "err", err)
				return err
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
				level.Info(logger).Log("msg", "would copy block, but skipping due to dry-run")
				return nil
			}

			level.Info(logger).Log("msg", "copying block")

			err = copier.copyBlock(ctx, sourceTenantID, destinationTenantID, blockID, markers[blockID])
			if errors.Is(err, errBlockAlreadyExists) {
				level.Info(logger).Log("msg", "block already exists on destination, writing copy marker")
			} else if err != nil {
				m.blocksCopyFailed.Inc()
				level.Error(logger).Log("msg", "failed to copy block", "err", err)
				return err
			} else {
				m.blocksCopied.Inc()
				level.Info(logger).Log("msg", "block copied successfully")
			}

			// Note that only the blockID and destination name are considered in the copy marker.
			// If multiple tenants in the same destination are copied to from the same source tenant the markers will currently clash.
			err = uploadCopiedMarkerFile(ctx, sourceBucket, sourceTenantID, blockID, copier.name)
			if err != nil {
				level.Error(logger).Log("msg", "failed to upload copied-marker file for block", "block", blockID.String(), "err", err)
				return err
			}
			return nil
		})
	})
}

func isAllowedUser(enabled map[string]struct{}, disabled map[string]struct{}, tenantID string) bool {
	if len(enabled) > 0 {
		if _, ok := enabled[tenantID]; !ok {
			return false
		}
	}

	if len(disabled) > 0 {
		if _, ok := disabled[tenantID]; ok {
			return false
		}
	}

	return true
}

// This method copies files within single TSDB block to a destination bucket.
func copySingleBlock(ctx context.Context, sourceTenantID, destinationTenantID string, blockID ulid.ULID, markers blockMarkers, srcBkt objtools.Bucket, copyFunc objtools.CopyFunc) error {
	result, err := srcBkt.List(ctx, objtools.ListOptions{
		Prefix:    sourceTenantID + objtools.Delim + blockID.String(),
		Recursive: true,
	})
	if err != nil {
		return fmt.Errorf("copySingleBlock: failed to list block files for %v/%v: %w", sourceTenantID, blockID.String(), err)
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
			return fmt.Errorf("copySingleBlock: failed to copy %v: %w", fullPath, err)
		}
	}

	return nil
}

func uploadCopiedMarkerFile(ctx context.Context, bkt objtools.Bucket, tenantID string, blockID ulid.ULID, targetBucketName string) error {
	objectName := tenantID + objtools.Delim + CopiedToBucketMarkFilename(blockID, targetBucketName)
	err := bkt.Upload(ctx, objectName, bytes.NewReader([]byte{}), 0)
	if err != nil {
		return fmt.Errorf("uploadCopiedMarkerFile: %w", err)
	}
	return nil
}

func metaObjectName(tenantID string, blockID ulid.ULID) string {
	return tenantID + objtools.Delim + blockID.String() + objtools.Delim + block.MetaFilename
}

func loadMetaJSONFile(ctx context.Context, bkt objtools.Bucket, tenantID string, blockID ulid.ULID) (block.Meta, error) {
	objectName := metaObjectName(tenantID, blockID)
	r, err := bkt.Get(ctx, objectName, objtools.GetOptions{})
	if err != nil {
		return block.Meta{}, fmt.Errorf("failed to read %v: %w", objectName, err)
	}

	var m block.Meta

	dec := json.NewDecoder(r)
	err = dec.Decode(&m)
	closeErr := r.Close() // do this before any return.

	if err != nil {
		return block.Meta{}, fmt.Errorf("read %v: %w", objectName, err)
	}
	if closeErr != nil {
		return block.Meta{}, fmt.Errorf("close reader for %v: %w", objectName, closeErr)
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
	copied    bool
	noCompact bool
}

func listBlockMarkersForTenant(ctx context.Context, bkt objtools.Bucket, tenantID string, destinationBucket string) (map[ulid.ULID]blockMarkers, error) {
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

		if ok, id, targetBucket := IsCopiedToBucketMarkFilename(m); ok && targetBucket == destinationBucket {
			bm := result[id]
			bm.copied = true
			result[id] = bm
		}
	}

	return result, nil
}

const CopiedMarkFilename = "copied"

// CopiedToBucketMarkFilename returns the path of marker file signalling that block was copied to given destination bucket.
// Returned path is relative to the tenant's bucket location.
func CopiedToBucketMarkFilename(blockID ulid.ULID, targetBucket string) string {
	// eg markers/01EZED0X3YZMNJ3NHGMJJKMHCR-copied-target-bucket
	return fmt.Sprintf("%s/%s-%s-%s", block.MarkersPathname, blockID.String(), CopiedMarkFilename, targetBucket)
}

// IsCopiedToBucketMarkFilename returns whether the input filename matches the expected pattern
// of copied markers stored in markers location.
// Target bucket is part of the mark filename, and is returned as 3rd return value.
func IsCopiedToBucketMarkFilename(name string) (bool, ulid.ULID, string) {
	parts := strings.SplitN(name, "-", 3)
	if len(parts) != 3 {
		return false, ulid.ULID{}, ""
	}

	// Ensure the 2nd part matches the block copy mark filename.
	if parts[1] != CopiedMarkFilename {
		return false, ulid.ULID{}, ""
	}

	// Ensure the 1st part is a valid block ID.
	id, err := ulid.Parse(filepath.Base(parts[0]))
	if err != nil {
		return false, ulid.ULID{}, ""
	}

	return true, id, parts[2]
}
