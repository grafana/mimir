// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/multierror"
	dskittenant "github.com/grafana/dskit/tenant"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_storage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/storage/indexheader"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/globalerror"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

type TSDBBuilder struct {
	partitionID int32

	// Map of a tenant in a partition to its TSDB.
	tsdbsMu sync.RWMutex
	tsdbs   map[tsdbTenant]*userTSDB

	cfg    Config
	limits *validation.Overrides

	logger             log.Logger
	tsdbBuilderMetrics tsdbBuilderMetrics
	tsdbMetrics        *mimir_tsdb.TSDBMetrics
}

// We use this only to identify the soft errors.
var softErrProcessor = mimir_storage.NewSoftAppendErrorProcessor(
	func() {}, func(int64, []mimirpb.LabelAdapter) {}, func(int64, []mimirpb.LabelAdapter) {},
	func(int64, []mimirpb.LabelAdapter) {}, func(int64, []mimirpb.LabelAdapter) {}, func(string, int64, []mimirpb.LabelAdapter) {},
	func([]mimirpb.LabelAdapter) {}, func([]mimirpb.LabelAdapter) {},
	func(err error, _ int64, _ []mimirpb.LabelAdapter) bool {
		_, ok := globalerror.MapNativeHistogramErr(err)
		return ok
	},
	func([]mimirpb.LabelAdapter) {},
)

type tsdbTenant struct {
	partitionID int32
	tenantID    string
}

func NewTSDBBuilder(
	partitionID int32,
	cfg Config,
	limits *validation.Overrides,
	logger log.Logger,
	tsdbBuilderMetrics tsdbBuilderMetrics,
	tsdbMetrics *mimir_tsdb.TSDBMetrics,
) *TSDBBuilder {
	return &TSDBBuilder{
		partitionID:        partitionID,
		tsdbs:              make(map[tsdbTenant]*userTSDB),
		cfg:                cfg,
		limits:             limits,
		logger:             logger,
		tsdbBuilderMetrics: tsdbBuilderMetrics,
		tsdbMetrics:        tsdbMetrics,
	}
}

// PushToStorageAndReleaseRequest implements ingest.Pusher.
// It puts the samples in the TSDB. Some parts taken from (*Ingester).pushSamplesToAppender.
func (b *TSDBBuilder) PushToStorageAndReleaseRequest(ctx context.Context, req *mimirpb.WriteRequest) error {
	defer req.FreeBuffer()
	defer mimirpb.ReuseSlice(req.Timeseries)

	tenantID, err := dskittenant.TenantID(ctx)
	if err != nil {
		return fmt.Errorf("extract tenant id: %w", err)
	}

	if len(req.Timeseries) == 0 {
		return nil
	}

	tenant := tsdbTenant{
		partitionID: b.partitionID,
		tenantID:    tenantID,
	}
	db, err := b.getOrCreateTSDB(tenant)
	if err != nil {
		return fmt.Errorf("get tsdb for tenant %s: %w", tenantID, err)
	}

	app := db.AppenderV2(ctx).(extendedAppender)
	defer func() {
		if err != nil {
			if e := app.Rollback(); e != nil && !errors.Is(e, tsdb.ErrAppenderClosed) {
				level.Warn(b.logger).Log("msg", "failed to rollback appender on error", "tenant", tenantID, "err", e)
			}
			// Always wrap the returned error with tenant.
			err = fmt.Errorf("failed to process record for tenant %s: %w", tenantID, err)
		}
	}()

	nativeHistogramsIngestionEnabled := b.limits.NativeHistogramsIngestionEnabled(tenantID)

	var (
		labelsBuilder   labels.ScratchBuilder
		nonCopiedLabels labels.Labels

		discardedSamples = 0
	)
	for _, ts := range req.Timeseries {
		mimirpb.FromLabelAdaptersOverwriteLabels(&labelsBuilder, ts.Labels, &nonCopiedLabels)
		hash := nonCopiedLabels.Hash()
		// Look up a reference for this series. The hash passed should be the output of Labels.Hash()
		// and NOT the stable hashing because that's what TSDB expects. We don't need stable hashing in block builder.
		ref, copiedLabels := app.GetRef(nonCopiedLabels, hash)

		ingestCreatedTimestamp := ts.CreatedTimestamp > 0

		metricName := nonCopiedLabels.Get(model.MetricNameLabel)

		// Build resource context once per time series.
		var resourceCtx *storage.ResourceContext
		if ts.ResourceAttributes != nil && len(ts.ResourceAttributes.Identifying) > 0 && metricName != "target_info" {
			resourceCtx = &storage.ResourceContext{
				Identifying: entriesToMap(ts.ResourceAttributes.Identifying),
				Descriptive: entriesToMap(ts.ResourceAttributes.Descriptive),
				Entities:    convertResourceEntities(ts.ResourceAttributes.Entities),
			}
		}

		// Build scope context once per time series.
		var scopeCtx *storage.ScopeContext
		if ts.ScopeAttributes != nil && metricName != "target_info" {
			if ts.ScopeAttributes.Name != "" || ts.ScopeAttributes.Version != "" || ts.ScopeAttributes.SchemaURL != "" || len(ts.ScopeAttributes.Attrs) > 0 {
				scopeCtx = &storage.ScopeContext{
					Name:      strings.Clone(ts.ScopeAttributes.Name),
					Version:   strings.Clone(ts.ScopeAttributes.Version),
					SchemaURL: strings.Clone(ts.ScopeAttributes.SchemaURL),
					Attrs:     entriesToMap(ts.ScopeAttributes.Attrs),
				}
			}
		}

		for _, s := range ts.Samples {
			// Append ST zero sample (created timestamp) before the regular sample.
			if ingestCreatedTimestamp && ts.CreatedTimestamp < s.TimestampMs &&
				(!nativeHistogramsIngestionEnabled || len(ts.Histograms) == 0 || ts.Histograms[0].Timestamp >= s.TimestampMs) {
				stOpts := storage.AppendV2Options{RejectOutOfOrder: true}
				if ref != 0 {
					_, err = app.Append(ref, copiedLabels, 0, ts.CreatedTimestamp, 0, nil, nil, stOpts)
				} else {
					copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)
					ref, err = app.Append(0, copiedLabels, 0, ts.CreatedTimestamp, 0, nil, nil, stOpts)
				}
				if err != nil && !errors.Is(err, storage.ErrDuplicateSampleForTimestamp) && !errors.Is(err, storage.ErrOutOfOrderST) && !errors.Is(err, storage.ErrOutOfOrderSample) {
					level.Warn(b.logger).Log("msg", "failed to ingest ST zero sample", "err", err)
					discardedSamples++
				}
				ingestCreatedTimestamp = false
				err = nil
			}

			opts := storage.AppendV2Options{
				Resource: resourceCtx,
				Scope:    scopeCtx,
			}

			if ref != 0 {
				// If the cached reference exists, we try to use it.
				if _, err = app.Append(ref, copiedLabels, 0, s.TimestampMs, s.Value, nil, nil, opts); err == nil {
					continue
				}
			} else {
				// Copy the label set because TSDB may retain it.
				copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)
				// Retain the reference in case there are multiple samples for the series.
				if ref, err = app.Append(0, copiedLabels, 0, s.TimestampMs, s.Value, nil, nil, opts); err == nil {
					continue
				}
			}

			if err != nil {
				// Only abort the processing on a terminal error.
				if !softErrProcessor.ProcessErr(err, 0, nil) && !errors.Is(err, errMaxInMemorySeriesReached) {
					return err
				}
				discardedSamples++
			}
		}

		if !nativeHistogramsIngestionEnabled {
			continue
		}

		for _, h := range ts.Histograms {
			var (
				ih *histogram.Histogram
				fh *histogram.FloatHistogram
			)

			if h.IsFloatHistogram() {
				fh = mimirpb.FromFloatHistogramProtoToFloatHistogram(&h)
			} else {
				ih = mimirpb.FromHistogramProtoToHistogram(&h)
			}

			// Append ST zero sample (created timestamp) before the regular histogram.
			if ingestCreatedTimestamp && ts.CreatedTimestamp < h.Timestamp {
				var zeroIH *histogram.Histogram
				var zeroFH *histogram.FloatHistogram
				if fh != nil {
					zeroFH = &histogram.FloatHistogram{
						CounterResetHint: histogram.CounterReset,
						Schema:           fh.Schema,
						ZeroThreshold:    fh.ZeroThreshold,
						CustomValues:     fh.CustomValues,
					}
				} else if ih != nil {
					zeroIH = &histogram.Histogram{
						CounterResetHint: histogram.CounterReset,
						Schema:           ih.Schema,
						ZeroThreshold:    ih.ZeroThreshold,
						CustomValues:     ih.CustomValues,
					}
				}
				stOpts := storage.AppendV2Options{RejectOutOfOrder: true}
				if ref != 0 {
					_, err = app.Append(ref, copiedLabels, 0, ts.CreatedTimestamp, 0, zeroIH, zeroFH, stOpts)
				} else {
					copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)
					ref, err = app.Append(0, copiedLabels, 0, ts.CreatedTimestamp, 0, zeroIH, zeroFH, stOpts)
				}
				if err != nil && !errors.Is(err, storage.ErrDuplicateSampleForTimestamp) && !errors.Is(err, storage.ErrOutOfOrderST) && !errors.Is(err, storage.ErrOutOfOrderSample) {
					level.Warn(b.logger).Log("msg", "failed to ingest ST zero histogram sample", "err", err)
					discardedSamples++
				}
				ingestCreatedTimestamp = false
				err = nil
			}

			opts := storage.AppendV2Options{
				Resource: resourceCtx,
				Scope:    scopeCtx,
			}

			if ref != 0 {
				// If the cached reference exists, we try to use it.
				if _, err = app.Append(ref, copiedLabels, 0, h.Timestamp, 0, ih, fh, opts); err == nil {
					continue
				}
			} else {
				// Copy the label set because both TSDB and the active series tracker may retain it.
				copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)
				// Retain the reference in case there are multiple samples for the series.
				if ref, err = app.Append(0, copiedLabels, 0, h.Timestamp, 0, ih, fh, opts); err == nil {
					continue
				}
			}

			if err != nil {
				// Only abort the processing on a terminal error.
				if !softErrProcessor.ProcessErr(err, 0, nil) && !errors.Is(err, errMaxInMemorySeriesReached) {
					return err
				}
				discardedSamples++
			}
		}

		// Exemplars and metadata are not persisted in the block. So we skip them.
	}

	if discardedSamples > 0 {
		partitionStr := fmt.Sprintf("%d", tenant.partitionID)
		b.tsdbBuilderMetrics.processSamplesDiscarded.WithLabelValues(partitionStr).Add(float64(discardedSamples))
	}

	return app.Commit()
}

func (b *TSDBBuilder) getOrCreateTSDB(tenant tsdbTenant) (*userTSDB, error) {
	b.tsdbsMu.RLock()
	db := b.tsdbs[tenant]
	b.tsdbsMu.RUnlock()
	if db != nil {
		return db, nil
	}

	b.tsdbsMu.Lock()
	defer b.tsdbsMu.Unlock()

	// Check again for DB in the event it was created in-between locks
	var ok bool
	db, ok = b.tsdbs[tenant]
	if ok {
		return db, nil
	}

	db, err := b.newTSDB(tenant)
	if err != nil {
		return nil, err
	}

	b.tsdbs[tenant] = db

	return db, nil
}

func (b *TSDBBuilder) newTSDB(tenant tsdbTenant) (*userTSDB, error) {
	tsdbPromReg := prometheus.NewRegistry()

	udir := filepath.Join(b.cfg.DataDir, strconv.Itoa(int(tenant.partitionID)), tenant.tenantID)
	// Remove any previous TSDB dir. We don't need it.
	if err := os.RemoveAll(udir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(udir, os.ModePerm); err != nil {
		return nil, err
	}

	userID := tenant.tenantID
	userLogger := util_log.WithUserID(userID, b.logger)

	udb := &userTSDB{
		userID: userID,
	}

	// Until we have a better way to enforce the same limits between ingesters and block builders,
	// as a stop gap, we apply limits when they are under a given value. Reason is that when a tenant
	// has higher limits, the higher usage and increase is expected and capacity is planned accordingly
	// and the tenant is generally more careful. It is the smaller tenants that can create problem
	// if they suddenly send millions of series when they are supposed to be limited to a few thousand.
	userLimit := b.limits.MaxGlobalSeriesPerUser(userID)
	if userLimit <= b.cfg.ApplyMaxGlobalSeriesPerUserBelow {
		udb.maxGlobalSeries = userLimit
	}

	db, err := tsdb.Open(udir, util_log.SlogFromGoKit(userLogger), tsdbPromReg, &tsdb.Options{
		RetentionDuration:                    0,
		MinBlockDuration:                     2 * time.Hour.Milliseconds(),
		MaxBlockDuration:                     2 * time.Hour.Milliseconds(),
		NoLockfile:                           true,
		StripeSize:                           b.cfg.BlocksStorage.TSDB.StripeSize,
		HeadChunksWriteBufferSize:            b.cfg.BlocksStorage.TSDB.HeadChunksWriteBufferSize,
		HeadChunksWriteQueueSize:             b.cfg.BlocksStorage.TSDB.HeadChunksWriteQueueSize,
		WALSegmentSize:                       -1,                                                                             // No WAL
		BlocksToDelete:                       func([]*tsdb.Block) map[ulid.ULID]struct{} { return map[ulid.ULID]struct{}{} }, // Always noop
		IsolationDisabled:                    true,
		EnableOverlappingCompaction:          false,                                                // Always false since Mimir only uploads lvl 1 compacted blocks
		OutOfOrderTimeWindow:                 b.limits.OutOfOrderTimeWindow(userID).Milliseconds(), // The unit must be same as our timestamps.
		OutOfOrderCapMax:                     int64(b.cfg.BlocksStorage.TSDB.OutOfOrderCapacityMax),
		SecondaryHashFunction:                nil, // TODO(codesome): May needed when applying limits. Used to determine the owned series by an ingesters
		SeriesLifecycleCallback:              udb,
		HeadPostingsForMatchersCacheMetrics:  tsdb.NewPostingsForMatchersCacheMetrics(nil), // No need for these metrics; no one queries tsdb through block-builder
		BlockPostingsForMatchersCacheMetrics: tsdb.NewPostingsForMatchersCacheMetrics(nil), // No need for these metrics; no one queries tsdb through block-builder
		PostingsClonerFactory:                tsdb.DefaultPostingsClonerFactory{},
		EnableSTAsZeroSample:                 false,
		EnableNativeMetadata:                 b.cfg.BlocksStorage.TSDB.OTelPersistResourceAttributes,
		EnableResourceAttrIndex:              b.cfg.BlocksStorage.TSDB.OTelResourceAttrIndexEnabled,
		IndexedResourceAttrs:                 stringSliceToSet(b.cfg.BlocksStorage.TSDB.OTelIndexedResourceAttributes),
	}, nil)
	if err != nil {
		return nil, err
	}

	db.DisableCompactions() // we compact on our own schedule

	udb.DB = db

	b.tsdbMetrics.SetRegistryForTenant(userID, tsdbPromReg)

	return udb, nil
}

// stringSliceToSet converts a string slice to a set (map[string]struct{}).
func stringSliceToSet(s []string) map[string]struct{} {
	if len(s) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(s))
	for _, v := range s {
		m[v] = struct{}{}
	}
	return m
}

func (b *TSDBBuilder) NotifyPreCommit(_ context.Context) error {
	return nil
}

// Function to upload the blocks.
type blockUploader func(_ context.Context, tenantID, dbDir string, metas []tsdb.BlockMeta) error

// CompactAndUpload compacts the blocks of all the TSDBs and uploads them.
// uploadBlocks is a function that uploads the blocks to the required storage.
// All the DBs are closed and directories cleared irrespective of success or failure of this function.
func (b *TSDBBuilder) CompactAndUpload(ctx context.Context, uploadBlocks blockUploader) (metas []tsdb.BlockMeta, err error) {
	var (
		closedDBsMu, metasMu sync.Mutex

		closedDBs = make(map[*userTSDB]bool)
	)

	b.tsdbsMu.Lock()
	defer func() {
		var merr multierror.MultiError
		merr.Add(err)
		// If some TSDB was not compacted or uploaded, it will be re-tried in the next cycle, so we always remove it here.
		for tenant, db := range b.tsdbs {
			if !closedDBs[db] {
				merr.Add(db.Close())
			}
			merr.Add(os.RemoveAll(db.Dir()))

			// Remove all registered per-tenant TSDB metrics. Their local DBs are wiped out from the block-builder no-matter what.
			b.tsdbMetrics.RemoveRegistryForTenant(tenant.tenantID)
		}

		// Clear the map so that it can be released from the memory. Not setting to nil in case we want to reuse the TSDBBuilder.
		clear(b.tsdbs)
		b.tsdbsMu.Unlock()

		err = merr.Err()
	}()

	level.Info(b.logger).Log("msg", "compacting and uploading blocks", "num_tsdb", len(b.tsdbs))

	if len(b.tsdbs) == 0 {
		return nil, nil
	}

	eg, ctx := errgroup.WithContext(ctx)
	if b.cfg.BlocksStorage.TSDB.ShipConcurrency > 0 {
		eg.SetLimit(b.cfg.BlocksStorage.TSDB.ShipConcurrency)
	}
	for tenant, db := range b.tsdbs {
		eg.Go(func() (err error) {
			defer func(t time.Time) {
				if errors.Is(err, context.Canceled) {
					// Don't track any metrics if context was cancelled. Otherwise, it might be misleading.
					return
				}
				partitionStr := strconv.Itoa(int(tenant.partitionID))
				if err != nil {
					b.tsdbBuilderMetrics.compactAndUploadFailed.WithLabelValues(partitionStr).Inc()
					return
				}
				b.tsdbBuilderMetrics.compactAndUploadDuration.WithLabelValues(partitionStr).Observe(time.Since(t).Seconds())
				b.tsdbBuilderMetrics.lastSuccessfulCompactAndUploadTime.WithLabelValues(partitionStr).SetToCurrentTime()
			}(time.Now())

			if err := db.compactBlocks(ctx); err != nil {
				return err
			}

			dbDir := db.Dir()
			var localMetas []tsdb.BlockMeta
			for _, b := range db.Blocks() {
				localMetas = append(localMetas, b.Meta())
			}

			if b.cfg.GenerateSparseIndexHeaders {
				if err := b.buildSparseIndexHeaders(ctx, dbDir, localMetas); err != nil {
					return err
				}
			}

			metasMu.Lock()
			metas = append(metas, localMetas...)
			metasMu.Unlock()

			if err := db.Close(); err != nil {
				return err
			}

			closedDBsMu.Lock()
			closedDBs[db] = true
			closedDBsMu.Unlock()

			if err := uploadBlocks(ctx, tenant.tenantID, dbDir, localMetas); err != nil {
				return err
			}

			// Clear the DB from the disk. Don't need it anymore.
			return os.RemoveAll(dbDir)
		})
	}
	err = eg.Wait()
	return metas, err
}

// Close closes all DBs and deletes their data directories.
// This functions is useful when block builder has faced some unrecoverable error
// and has to discard the block building for the current cycle.
func (b *TSDBBuilder) Close() error {
	b.tsdbsMu.Lock()
	defer b.tsdbsMu.Unlock()

	var merr multierror.MultiError
	for tenant, db := range b.tsdbs {
		dbDir := db.Dir()
		merr.Add(db.Close())
		merr.Add(os.RemoveAll(dbDir))

		b.tsdbMetrics.RemoveRegistryForTenant(tenant.tenantID)
	}

	// Clear the map so that it can be released from the memory. Not setting to nil in case
	// we want to reuse the TSDBBuilder.
	b.tsdbs = make(map[tsdbTenant]*userTSDB)
	return merr.Err()
}

type extendedAppender interface {
	storage.AppenderV2
	storage.GetRef
}

type userTSDB struct {
	*tsdb.DB
	userID          string
	maxGlobalSeries int
}

var (
	errMaxInMemorySeriesReached = errors.New("max series for user reached")
)

func (u *userTSDB) PreCreation(labels.Labels) error {
	// Global series limit.
	if u.maxGlobalSeries > 0 && u.Head().NumSeries() >= uint64(u.maxGlobalSeries) {
		return fmt.Errorf("limit of %d reached for user %s: %w", u.maxGlobalSeries, u.userID, errMaxInMemorySeriesReached)
	}

	return nil
}

func (u *userTSDB) PostCreation(labels.Labels) {}

func (u *userTSDB) PostDeletion(map[chunks.HeadSeriesRef]labels.Labels) {}

func (u *userTSDB) compactBlocks(ctx context.Context) error {
	blockRange := 2 * time.Hour.Milliseconds()

	// Compact the in-order data.
	mint, maxt := u.Head().MinTime(), u.Head().MaxTime()
	mint = (mint / blockRange) * blockRange
	for blockMint := mint; blockMint <= maxt; blockMint += blockRange {
		blockMaxt := blockMint + blockRange - 1
		rh := tsdb.NewRangeHead(u.Head(), blockMint, blockMaxt)
		if err := u.CompactHeadWithoutTruncation(rh); err != nil {
			return err
		}
	}

	// Compact the out-of-order data.
	if err := u.CompactOOOHead(ctx); err != nil {
		return err
	}

	return nil
}

// buildSparseIndexHeaders builds sparse index-headers for all blocks in the metas list in the directory.
func (b *TSDBBuilder) buildSparseIndexHeaders(ctx context.Context, dbDir string, metas []tsdb.BlockMeta) error {
	for _, m := range metas {
		if err := b.buildSparseIndexHeader(ctx, dbDir, m.ULID); err != nil {
			return err
		}
	}
	return nil
}

// prepareSparseIndexHeader builds a sparse index-header for a single block.
func (b *TSDBBuilder) buildSparseIndexHeader(ctx context.Context, dbDir string, id ulid.ULID) error {
	// Indexheader code uses a bucket client to read;
	// construct local-filesystem-backed bucket to read from disk.
	fsBkt, err := filesystem.NewBucket(dbDir)
	if err != nil {
		return err
	}
	fsInstrBkt := objstore.WithNoopInstr(fsBkt)

	// Calling NewStreamBinaryReader reads a block's index and writes a sparse-index-header to disk.
	metrics := indexheader.NewStreamBinaryReaderMetrics(nil)
	logger := log.With(b.logger, "id", id)
	br, err := indexheader.NewStreamBinaryReader(
		ctx,
		logger,
		fsInstrBkt,
		dbDir,
		id,
		b.cfg.BlocksStorage.BucketStore.PostingOffsetsInMemSampling,
		metrics,
		b.cfg.BlocksStorage.BucketStore.IndexHeader)
	if err != nil {
		return err
	}
	return br.Close()
}

// entriesToMap converts attribute entries to a map, cloning all strings to
// ensure safety. The input entries may contain UnsafeMutableStrings backed
// by a gRPC buffer that is returned to the pool after the request is
// processed (see CLAUDE.md "Unsafe memory tricks").
func entriesToMap(entries []mimirpb.AttributeEntry) map[string]string {
	if len(entries) == 0 {
		return nil
	}
	m := make(map[string]string, len(entries))
	for _, e := range entries {
		m[strings.Clone(e.Key)] = strings.Clone(e.Value)
	}
	return m
}

func convertResourceEntities(entities []mimirpb.ResourceEntity) []storage.EntityData {
	if len(entities) == 0 {
		return nil
	}
	result := make([]storage.EntityData, len(entities))
	for i, e := range entities {
		result[i] = storage.EntityData{
			Type:        strings.Clone(e.Type),
			ID:          entriesToMap(e.ID),
			Description: entriesToMap(e.Description),
		}
	}
	return result
}
