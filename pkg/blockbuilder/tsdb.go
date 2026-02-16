// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/multierror"
	dskittenant "github.com/grafana/dskit/tenant"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_storage "github.com/grafana/mimir/pkg/storage"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/globalerror"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

type TSDBBuilder struct {
	dataDir string

	logger             log.Logger
	limits             *validation.Overrides
	blocksStorageCfg   mimir_tsdb.BlocksStorageConfig
	tsdbBuilderMetrics tsdbBuilderMetrics
	tsdbMetrics        *mimir_tsdb.TSDBMetrics

	applyMaxGlobalSeriesPerUserBelow int // inclusive

	partitionID int32

	// Map of a tenant in a partition to its TSDB.
	tsdbsMu sync.RWMutex
	tsdbs   map[tsdbTenant]*userTSDB
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
	logger log.Logger,
	dataDir string,
	partitionID int32,
	blocksStorageCfg mimir_tsdb.BlocksStorageConfig,
	limits *validation.Overrides,
	tsdbBuilderMetrics tsdbBuilderMetrics,
	tsdbMetrics *mimir_tsdb.TSDBMetrics,
	applyMaxGlobalSeriesPerUserBelow int,
) *TSDBBuilder {
	return &TSDBBuilder{
		dataDir:                          dataDir,
		logger:                           logger,
		limits:                           limits,
		blocksStorageCfg:                 blocksStorageCfg,
		tsdbBuilderMetrics:               tsdbBuilderMetrics,
		tsdbMetrics:                      tsdbMetrics,
		applyMaxGlobalSeriesPerUserBelow: applyMaxGlobalSeriesPerUserBelow,
		partitionID:                      partitionID,
		tsdbs:                            make(map[tsdbTenant]*userTSDB),
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

	app := db.Appender(ctx).(extendedAppender)
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

		for _, s := range ts.Samples {
			if ingestCreatedTimestamp && ts.CreatedTimestamp < s.TimestampMs &&
				(!nativeHistogramsIngestionEnabled || len(ts.Histograms) == 0 || ts.Histograms[0].Timestamp >= s.TimestampMs) {
				if ref != 0 {
					// If the cached reference exists, we try to use it.
					_, err = app.AppendSTZeroSample(ref, copiedLabels, s.TimestampMs, ts.CreatedTimestamp)
				} else {
					// Copy the label set because TSDB may retain it.
					copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)
					ref, err = app.AppendSTZeroSample(0, copiedLabels, s.TimestampMs, ts.CreatedTimestamp)
				}
				if err != nil && !errors.Is(err, storage.ErrDuplicateSampleForTimestamp) && !errors.Is(err, storage.ErrOutOfOrderST) && !errors.Is(err, storage.ErrOutOfOrderSample) {
					// According to OTEL spec: https://opentelemetry.io/docs/specs/otel/metrics/data-model/#cumulative-streams-handling-unknown-start-time
					// if the start time is unknown, then it should equal to the timestamp of the first sample,
					// which will mean a created timestamp equal to the timestamp of the first sample for later
					// samples. Thus we ignore if zero sample would cause duplicate.
					// We also ignore out of order sample as created timestamp is out of order most of the time,
					// except when written before the first sample.
					level.Warn(b.logger).Log("msg", "failed to store zero float sample for created timestamp", "tenant", tenantID, "err", err)
					discardedSamples++
				}
				ingestCreatedTimestamp = false // Only try to append created timestamp once per series.
			}

			if ref != 0 {
				// If the cached reference exists, we try to use it.
				if _, err = app.Append(ref, copiedLabels, s.TimestampMs, s.Value); err == nil {
					continue
				}
			} else {
				// Copy the label set because TSDB may retain it.
				copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)
				// Retain the reference in case there are multiple samples for the series.
				if ref, err = app.Append(0, copiedLabels, s.TimestampMs, s.Value); err == nil {
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
			if ingestCreatedTimestamp && ts.CreatedTimestamp < h.Timestamp {
				var (
					ih *histogram.Histogram
					fh *histogram.FloatHistogram
				)
				// AppendHistogramCTZeroSample doesn't care about the content of the passed histograms,
				// just uses it to decide the type, so don't convert the input, use dummy histograms.
				if h.IsFloatHistogram() {
					fh = zeroFloatHistogram
				} else {
					ih = zeroHistogram
				}
				if ref != 0 {
					_, err = app.AppendHistogramSTZeroSample(ref, copiedLabels, h.Timestamp, ts.CreatedTimestamp, ih, fh)
				} else {
					// Copy the label set because both TSDB and the active series tracker may retain it.
					copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)
					ref, err = app.AppendHistogramSTZeroSample(0, copiedLabels, h.Timestamp, ts.CreatedTimestamp, ih, fh)
				}
				if err != nil && !errors.Is(err, storage.ErrDuplicateSampleForTimestamp) && !errors.Is(err, storage.ErrOutOfOrderST) && !errors.Is(err, storage.ErrOutOfOrderSample) {
					// According to OTEL spec: https://opentelemetry.io/docs/specs/otel/metrics/data-model/#cumulative-streams-handling-unknown-start-time
					// if the start time is unknown, then it should equal to the timestamp of the first sample,
					// which will mean a created timestamp equal to the timestamp of the first sample for later
					// samples. Thus we ignore if zero sample would cause duplicate.
					// We also ignore out of order sample as created timestamp is out of order most of the time,
					// except when written before the first sample.
					level.Warn(b.logger).Log("msg", "failed to store zero histogram sample for created timestamp", "tenant", tenantID, "err", err)
					discardedSamples++
				}
				ingestCreatedTimestamp = false // Only try to append created timestamp once per series.
			}
			var (
				ih *histogram.Histogram
				fh *histogram.FloatHistogram
			)

			if h.IsFloatHistogram() {
				fh = mimirpb.FromFloatHistogramProtoToFloatHistogram(&h)
			} else {
				ih = mimirpb.FromHistogramProtoToHistogram(&h)
			}

			if ref != 0 {
				// If the cached reference exists, we try to use it.
				if _, err = app.AppendHistogram(ref, copiedLabels, h.Timestamp, ih, fh); err == nil {
					continue
				}
			} else {
				// Copy the label set because both TSDB and the active series tracker may retain it.
				copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)
				// Retain the reference in case there are multiple samples for the series.
				if ref, err = app.AppendHistogram(0, copiedLabels, h.Timestamp, ih, fh); err == nil {
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

var (
	zeroHistogram      = &histogram.Histogram{}
	zeroFloatHistogram = &histogram.FloatHistogram{}
)

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

	udir := filepath.Join(b.dataDir, strconv.Itoa(int(tenant.partitionID)), tenant.tenantID)
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
	if userLimit <= b.applyMaxGlobalSeriesPerUserBelow {
		udb.maxGlobalSeries = userLimit
	}

	db, err := tsdb.Open(udir, util_log.SlogFromGoKit(userLogger), tsdbPromReg, &tsdb.Options{
		RetentionDuration:                    0,
		MinBlockDuration:                     2 * time.Hour.Milliseconds(),
		MaxBlockDuration:                     2 * time.Hour.Milliseconds(),
		NoLockfile:                           true,
		StripeSize:                           b.blocksStorageCfg.TSDB.StripeSize,
		HeadChunksWriteBufferSize:            b.blocksStorageCfg.TSDB.HeadChunksWriteBufferSize,
		HeadChunksWriteQueueSize:             b.blocksStorageCfg.TSDB.HeadChunksWriteQueueSize,
		WALSegmentSize:                       -1,                                                                             // No WAL
		BlocksToDelete:                       func([]*tsdb.Block) map[ulid.ULID]struct{} { return map[ulid.ULID]struct{}{} }, // Always noop
		IsolationDisabled:                    true,
		EnableOverlappingCompaction:          false,                                                // Always false since Mimir only uploads lvl 1 compacted blocks
		OutOfOrderTimeWindow:                 b.limits.OutOfOrderTimeWindow(userID).Milliseconds(), // The unit must be same as our timestamps.
		OutOfOrderCapMax:                     int64(b.blocksStorageCfg.TSDB.OutOfOrderCapacityMax),
		SecondaryHashFunction:                nil, // TODO(codesome): May needed when applying limits. Used to determine the owned series by an ingesters
		SeriesLifecycleCallback:              udb,
		HeadPostingsForMatchersCacheMetrics:  tsdb.NewPostingsForMatchersCacheMetrics(nil), // No need for these metrics; no one queries tsdb through block-builder
		BlockPostingsForMatchersCacheMetrics: tsdb.NewPostingsForMatchersCacheMetrics(nil), // No need for these metrics; no one queries tsdb through block-builder
		PostingsClonerFactory:                tsdb.DefaultPostingsClonerFactory{},
	}, nil)
	if err != nil {
		return nil, err
	}

	db.DisableCompactions() // we compact on our own schedule

	udb.DB = db

	b.tsdbMetrics.SetRegistryForTenant(userID, tsdbPromReg)

	return udb, nil
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
		for _, db := range b.tsdbs {
			if !closedDBs[db] {
				merr.Add(db.Close())
			}
			merr.Add(os.RemoveAll(db.Dir()))
		}

		// Remove all registered per-tenant TSDB metrics. Their local DBs are wiped out from the block-builder no-matter what.
		for tenant := range b.tsdbs {
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
	if b.blocksStorageCfg.TSDB.ShipConcurrency > 0 {
		eg.SetLimit(b.blocksStorageCfg.TSDB.ShipConcurrency)
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

			if err := db.compactEverything(ctx); err != nil {
				return err
			}

			dbDir := db.Dir()
			var localMetas []tsdb.BlockMeta
			for _, b := range db.Blocks() {
				localMetas = append(localMetas, b.Meta())
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
	storage.Appender
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

func (u *userTSDB) compactEverything(ctx context.Context) error {
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
