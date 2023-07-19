// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/cmd/blockscopy/main.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Grafana Labs.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

const (
	serviceGCS = "gcs" // Google Cloud Storage
	serviceABS = "abs" // Azure Blob Storage
	serviceS3  = "s3"  // Amazon Simple Storage Service
	delim      = "/"   // Used by Mimir to delimit tenants and blocks, and objects within blocks.
)

type bucket interface {
	Get(ctx context.Context, objectName string) (io.ReadCloser, error)
	ServerSideCopy(ctx context.Context, objectName string, dstBucket bucket) error
	ClientSideCopy(ctx context.Context, objectName string, dstBucket bucket) error
	ListPrefix(ctx context.Context, prefix string, recursive bool) ([]string, error)
	Upload(ctx context.Context, objectName string, reader io.Reader, contentLength int64) error
	Name() string
}

type CopyFunc func(context.Context, string) error

type config struct {
	sourceService      string
	destinationService string
	sourceBucket       string
	destBucket         string
	minBlockDuration   time.Duration
	minTime            flagext.Time
	maxTime            flagext.Time
	tenantConcurrency  int
	blocksConcurrency  int
	copyPeriod         time.Duration
	enabledUsers       flagext.StringSliceCSV
	disabledUsers      flagext.StringSliceCSV
	dryRun             bool
	clientSideCopy     bool
	httpListen         string
	azureConfig        azureConfig
	s3Config           s3Config
}

func (c *config) RegisterFlags(f *flag.FlagSet) {
	acceptedServices := fmt.Sprintf(" Accepted values: %s, %s or %s.", serviceABS, serviceGCS, serviceS3)
	f.StringVar(&c.sourceService, "source-service", "", "Service that the source bucket is on."+acceptedServices)
	f.StringVar(&c.destinationService, "destination-service", "", "Service that the destination bucket is on."+acceptedServices)
	f.StringVar(&c.sourceBucket, "source-bucket", "", "Source bucket with blocks.")
	f.StringVar(&c.destBucket, "destination-bucket", "", "Destination bucket with blocks.")
	f.DurationVar(&c.minBlockDuration, "min-block-duration", 0, "If non-zero, ignore blocks that cover block range smaller than this.")
	f.Var(&c.minTime, "min-time", fmt.Sprintf("If set, only blocks with MinTime >= this value are copied. The supported time format is %q.", time.RFC3339))
	f.Var(&c.maxTime, "max-time", fmt.Sprintf("If set, only blocks with MaxTime <= this value are copied. The supported time format is %q.", time.RFC3339))
	f.IntVar(&c.tenantConcurrency, "tenant-concurrency", 5, "How many tenants to process at once.")
	f.IntVar(&c.blocksConcurrency, "block-concurrency", 5, "How many blocks to copy at once per tenant.")
	f.DurationVar(&c.copyPeriod, "copy-period", 0, "How often to repeat the copy. If set to 0, copy is done once, and the program stops. Otherwise, the program keeps running and copying blocks until it is terminated.")
	f.Var(&c.enabledUsers, "enabled-users", "If not empty, only blocks for these users are copied.")
	f.Var(&c.disabledUsers, "disabled-users", "If not empty, blocks for these users are not copied.")
	f.BoolVar(&c.dryRun, "dry-run", false, "Don't perform copy; only log what would happen.")
	f.BoolVar(&c.clientSideCopy, "client-side-copy", false, "Use client side copying. This option is only respected if copying between two buckets of the same service. Client side copying is always used when copying between different services.")
	f.StringVar(&c.httpListen, "http-listen-address", ":8080", "HTTP listen address.")
	c.azureConfig.RegisterFlags(f)
	c.s3Config.RegisterFlags(f)
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
	cfg.RegisterFlags(flag.CommandLine)

	flag.Parse()

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	m := newMetrics(prometheus.DefaultRegisterer)

	go func() {
		level.Info(logger).Log("msg", "HTTP server listening on "+cfg.httpListen)
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(cfg.httpListen, nil)
		if err != nil {
			level.Error(logger).Log("msg", "failed to start HTTP server")
			os.Exit(1)
		}
	}()

	success := runCopy(ctx, cfg, logger, m)
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
			_ = runCopy(ctx, cfg, logger, m)
		case <-ctx.Done():
		}
	}
}

func initializeBuckets(ctx context.Context, cfg config) (sourceBucket bucket, destBucket bucket, err error) {
	if cfg.sourceService == "" || cfg.destinationService == "" {
		return nil, nil, errors.New("--source-service or --destination-service is missing")
	}
	if cfg.sourceBucket == "" || cfg.destBucket == "" {
		return nil, nil, errors.New("--source-bucket or --destination-bucket is missing")
	}
	if cfg.sourceBucket == cfg.destBucket {
		return nil, nil, errors.New("--source-bucket and --destination-bucket can not be the same")
	}
	if err := cfg.azureConfig.validate(cfg.sourceService, cfg.destinationService); err != nil {
		return nil, nil, err
	}
	if err := cfg.s3Config.validate(cfg.sourceService, cfg.destinationService); err != nil {
		return nil, nil, err
	}

	sourceBucket, err = initializeBucket(ctx, cfg, cfg.sourceService, cfg.sourceBucket, true)
	if err != nil {
		return nil, nil, err
	}
	destBucket, err = initializeBucket(ctx, cfg, cfg.destinationService, cfg.destBucket, false)
	if err != nil {
		return nil, nil, err
	}
	return sourceBucket, destBucket, nil
}

func initializeBucket(ctx context.Context, cfg config, service, bucket string, isSource bool) (bucket, error) {
	switch strings.ToLower(service) {
	case serviceABS:
		if isSource {
			return newAzureBucketClient(cfg.azureConfig.source, bucket, cfg.azureConfig.copyStatusBackoff)
		}
		return newAzureBucketClient(cfg.azureConfig.destination, bucket, cfg.azureConfig.copyStatusBackoff)
	case serviceGCS:
		client, err := storage.NewClient(ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create GCS storage client")
		}
		return newGCSBucket(client, bucket), nil
	case serviceS3:
		if isSource {
			return newS3Client(cfg.s3Config.source, bucket)
		}
		return newS3Client(cfg.s3Config.destination, bucket)
	default:
		return nil, errors.Errorf("invalid service: %v", service)
	}
}

func runCopy(ctx context.Context, cfg config, logger log.Logger, m *metrics) bool {
	err := copyBlocks(ctx, cfg, logger, m)
	if err != nil {
		m.copyCyclesFailed.Inc()
		level.Error(logger).Log("msg", "failed to copy blocks", "err", err, "dryRun", cfg.dryRun)
		return false
	}

	m.copyCyclesSucceeded.Inc()
	level.Info(logger).Log("msg", "finished copying blocks", "dryRun", cfg.dryRun)
	return true
}

func copyBlocks(ctx context.Context, cfg config, logger log.Logger, m *metrics) error {
	sourceBucket, destBucket, err := initializeBuckets(ctx, cfg)
	if err != nil {
		return err
	}

	var copyFunc CopyFunc
	if cfg.clientSideCopy || cfg.sourceService != cfg.destinationService {
		copyFunc = func(ctx context.Context, objectName string) error {
			return sourceBucket.ClientSideCopy(ctx, objectName, destBucket)
		}
	} else {
		copyFunc = func(ctx context.Context, objectName string) error {
			return sourceBucket.ServerSideCopy(ctx, objectName, destBucket)
		}
	}

	tenants, err := listTenants(ctx, sourceBucket)
	if err != nil {
		return errors.Wrapf(err, "failed to list tenants")
	}

	enabledUsers := map[string]struct{}{}
	disabledUsers := map[string]struct{}{}
	for _, u := range cfg.enabledUsers {
		enabledUsers[u] = struct{}{}
	}
	for _, u := range cfg.disabledUsers {
		disabledUsers[u] = struct{}{}
	}

	return concurrency.ForEachUser(ctx, tenants, cfg.tenantConcurrency, func(ctx context.Context, tenantID string) error {
		if !isAllowedUser(enabledUsers, disabledUsers, tenantID) {
			return nil
		}

		logger := log.With(logger, "tenantID", tenantID)

		blocks, err := listBlocksForTenant(ctx, sourceBucket, tenantID)
		if err != nil {
			level.Error(logger).Log("msg", "failed to list blocks for tenant", "err", err)
			return errors.Wrapf(err, "failed to list blocks for tenant %v", tenantID)
		}

		markers, err := listBlockMarkersForTenant(ctx, sourceBucket, tenantID, destBucket.Name())
		if err != nil {
			level.Error(logger).Log("msg", "failed to list blocks markers for tenant", "err", err)
			return errors.Wrapf(err, "failed to list block markers for tenant %v", tenantID)
		}

		var blockIDs []string
		for _, b := range blocks {
			blockIDs = append(blockIDs, b.String())
		}

		// We use ForEachUser here to keep processing other blocks, if the block fails. We pass block IDs as "users".
		return concurrency.ForEachUser(ctx, blockIDs, cfg.blocksConcurrency, func(ctx context.Context, blockIDStr string) error {
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

			blockMeta, err := loadMetaJSONFile(ctx, sourceBucket, tenantID, blockID)
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

			if cfg.minBlockDuration > 0 {
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

			err = copySingleBlock(ctx, tenantID, blockID, markers[blockID], sourceBucket, copyFunc)
			if err != nil {
				m.blocksCopyFailed.Inc()
				level.Error(logger).Log("msg", "failed to copy block", "err", err)
				return err
			}

			m.blocksCopied.Inc()
			level.Info(logger).Log("msg", "block copied successfully")

			err = uploadCopiedMarkerFile(ctx, sourceBucket, tenantID, blockID, destBucket.Name())
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
func copySingleBlock(ctx context.Context, tenantID string, blockID ulid.ULID, markers blockMarkers, srcBkt bucket, copyFunc CopyFunc) error {
	paths, err := srcBkt.ListPrefix(ctx, tenantID+delim+blockID.String(), true)
	if err != nil {
		return errors.Wrapf(err, "copySingleBlock: failed to list block files for %v/%v", tenantID, blockID.String())
	}

	// Reorder paths, move meta.json at the end. We want to upload meta.json as last file, because it signals to Mimir that
	// block upload has finished.
	for ix := 0; ix < len(paths); ix++ {
		if paths[ix] == block.MetaFilename && ix < len(paths)-1 {
			paths = append(paths[:ix], paths[ix+1:]...)
			paths = append(paths, block.MetaFilename)
		} else {
			ix++
		}
	}

	// Generate the full paths.
	for idx, p := range paths {
		paths[idx] = tenantID + delim + blockID.String() + delim + p
	}

	// Copy global markers too (skipping deletion mark because deleted blocks are not copied by this tool).
	if markers.noCompact {
		paths = append(paths, tenantID+delim+block.NoCompactMarkFilepath(blockID))
	}

	for _, fullPath := range paths {
		err := copyFunc(ctx, fullPath)
		if err != nil {
			return errors.Wrapf(err, "copySingleBlock: failed to copy %v", fullPath)
		}
	}

	return nil
}

func uploadCopiedMarkerFile(ctx context.Context, bkt bucket, tenantID string, blockID ulid.ULID, targetBucketName string) error {
	objectName := tenantID + delim + CopiedToBucketMarkFilename(blockID, targetBucketName)
	err := bkt.Upload(ctx, objectName, bytes.NewReader([]byte{}), 0)
	return errors.Wrap(err, "uploadCopiedMarkerFile")
}

func loadMetaJSONFile(ctx context.Context, bkt bucket, tenantID string, blockID ulid.ULID) (block.Meta, error) {
	objectName := tenantID + delim + blockID.String() + delim + block.MetaFilename
	r, err := bkt.Get(ctx, objectName)
	if err != nil {
		return block.Meta{}, errors.Wrapf(err, "failed to read %v", objectName)
	}

	var m block.Meta

	dec := json.NewDecoder(r)
	err = dec.Decode(&m)
	closeErr := r.Close() // do this before any return.

	if err != nil {
		return block.Meta{}, errors.Wrapf(err, "read %v", objectName)
	}
	if closeErr != nil {
		return block.Meta{}, errors.Wrapf(err, "close reader for %v", objectName)
	}

	return m, nil
}

func listTenants(ctx context.Context, bkt bucket) ([]string, error) {
	users, err := bkt.ListPrefix(ctx, "", false)
	if err != nil {
		return nil, err
	}

	trimDelimSuffix(users)

	return users, nil
}

func listBlocksForTenant(ctx context.Context, bkt bucket, tenantID string) ([]ulid.ULID, error) {
	items, err := bkt.ListPrefix(ctx, tenantID, false)
	if err != nil {
		return nil, err
	}

	trimDelimSuffix(items)

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

func listBlockMarkersForTenant(ctx context.Context, bkt bucket, tenantID string, destinationBucket string) (map[ulid.ULID]blockMarkers, error) {
	markers, err := bkt.ListPrefix(ctx, tenantID+delim+block.MarkersPathname, false)
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

func trimDelimSuffix(items []string) {
	for ix := range items {
		items[ix] = strings.TrimSuffix(items[ix], delim)
	}
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
