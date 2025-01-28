// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/multierror"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_storage "github.com/grafana/mimir/pkg/storage"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

type TSDBBuilder struct {
	dataDir string

	logger                           log.Logger
	limits                           *validation.Overrides
	blocksStorageCfg                 mimir_tsdb.BlocksStorageConfig
	metrics                          tsdbBuilderMetrics
	applyMaxGlobalSeriesPerUserBelow int // inclusive

	// Map of a tenant in a partition to its TSDB.
	tsdbsMu sync.RWMutex
	tsdbs   map[tsdbTenant]*userTSDB
}

// We use this only to identify the soft errors.
var softErrProcessor = mimir_storage.NewSoftAppendErrorProcessor(
	func() {}, func(int64, []mimirpb.LabelAdapter) {}, func(int64, []mimirpb.LabelAdapter) {},
	func(int64, []mimirpb.LabelAdapter) {}, func(int64, []mimirpb.LabelAdapter) {}, func(int64, []mimirpb.LabelAdapter) {},
	func([]mimirpb.LabelAdapter) {}, func([]mimirpb.LabelAdapter) {}, func(error, int64, []mimirpb.LabelAdapter) {},
	func(error, int64, []mimirpb.LabelAdapter) {}, func(error, int64, []mimirpb.LabelAdapter) {}, func(error, int64, []mimirpb.LabelAdapter) {},
	func(error, int64, []mimirpb.LabelAdapter) {}, func(error, int64, []mimirpb.LabelAdapter) {},
)

type tsdbTenant struct {
	partitionID int32
	tenantID    string
}

func NewTSDBBuilder(logger log.Logger, dataDir string, blocksStorageCfg mimir_tsdb.BlocksStorageConfig, limits *validation.Overrides, metrics tsdbBuilderMetrics, applyMaxGlobalSeriesPerUserBelow int) *TSDBBuilder {
	return &TSDBBuilder{
		dataDir:                          dataDir,
		logger:                           logger,
		limits:                           limits,
		blocksStorageCfg:                 blocksStorageCfg,
		metrics:                          metrics,
		applyMaxGlobalSeriesPerUserBelow: applyMaxGlobalSeriesPerUserBelow,
		tsdbs:                            make(map[tsdbTenant]*userTSDB),
	}
}

// Process puts the samples in the TSDB. Some parts taken from (*Ingester).pushSamplesToAppender.
// It returns false if at least one sample was skipped to process later, true otherwise. true also includes the cases
// where the sample was not put in the TSDB because it was discarded or was already processed before.
// lastBlockMax: max time of the block in the previous block building cycle.
// blockMax: max time of the block in the current block building cycle. This blockMax is exclusive of the last sample by design in TSDB.
// recordAlreadyProcessed: true if the record was processed in the previous cycle. (It gets processed again if some samples did not fit in the previous cycle.)
func (b *TSDBBuilder) Process(ctx context.Context, rec *kgo.Record, lastBlockMax, blockMax int64, recordAlreadyProcessed, processEverything bool) (_ bool, err error) {
	userID := string(rec.Key)

	req := mimirpb.PreallocWriteRequest{
		SkipUnmarshalingExemplars: true,
	}
	defer mimirpb.ReuseSlice(req.Timeseries)

	// TODO(codesome): see if we can skip parsing exemplars. They are not persisted in the block so we can save some parsing here.
	err = req.Unmarshal(rec.Value)
	if err != nil {
		return false, fmt.Errorf("unmarshal record key %s: %w", rec.Key, err)
	}

	if len(req.Timeseries) == 0 {
		return true, nil
	}

	tenant := tsdbTenant{
		partitionID: rec.Partition,
		tenantID:    userID,
	}
	db, err := b.getOrCreateTSDB(tenant)
	if err != nil {
		return false, fmt.Errorf("get tsdb for tenant %s: %w", userID, err)
	}

	app := db.Appender(ctx).(extendedAppender)
	defer func() {
		if err != nil {
			if e := app.Rollback(); e != nil && !errors.Is(e, tsdb.ErrAppenderClosed) {
				level.Warn(b.logger).Log("msg", "failed to rollback appender on error", "tenant", userID, "err", e)
			}
			// Always wrap the returned error with tenant.
			err = fmt.Errorf("failed to process record for tenant %s: %w", userID, err)
		}
	}()

	var (
		labelsBuilder   labels.ScratchBuilder
		nonCopiedLabels labels.Labels

		allSamplesProcessed = true
		discardedSamples    = 0

		nativeHistogramsIngestionEnabled = b.limits.NativeHistogramsIngestionEnabled(userID)
	)
	for _, ts := range req.Timeseries {
		mimirpb.FromLabelAdaptersOverwriteLabels(&labelsBuilder, ts.Labels, &nonCopiedLabels)
		hash := nonCopiedLabels.Hash()
		// Look up a reference for this series. The hash passed should be the output of Labels.Hash()
		// and NOT the stable hashing because that's what TSDB expects. We don't need stable hashing in block builder.
		ref, copiedLabels := app.GetRef(nonCopiedLabels, hash)

		for _, s := range ts.Samples {
			if !processEverything && s.TimestampMs >= blockMax {
				// We will process this sample in the next cycle.
				allSamplesProcessed = false
				continue
			}
			if !processEverything && recordAlreadyProcessed && s.TimestampMs < lastBlockMax {
				// This sample was already processed in the previous cycle.
				continue
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
					return false, err
				}
				discardedSamples++
			}
		}

		if !nativeHistogramsIngestionEnabled {
			continue
		}

		for _, h := range ts.Histograms {
			if !processEverything && h.Timestamp >= blockMax {
				// We will process this sample in the next cycle.
				allSamplesProcessed = false
				continue
			}
			if !processEverything && recordAlreadyProcessed && h.Timestamp < lastBlockMax {
				// This sample was already processed in the previous cycle.
				continue
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
					return false, err
				}
				discardedSamples++
			}
		}

		// Exemplars and metadata are not persisted in the block. So we skip them.
	}

	if discardedSamples > 0 {
		partitionStr := fmt.Sprintf("%d", tenant.partitionID)
		b.metrics.processSamplesDiscarded.WithLabelValues(partitionStr).Add(float64(discardedSamples))
	}

	return allSamplesProcessed, app.Commit()
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

	db, err := tsdb.Open(udir, util_log.SlogFromGoKit(userLogger), nil, &tsdb.Options{
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
		EnableNativeHistograms:               b.limits.NativeHistogramsIngestionEnabled(userID),
		SecondaryHashFunction:                nil, // TODO(codesome): May needed when applying limits. Used to determine the owned series by an ingesters
		SeriesLifecycleCallback:              udb,
		HeadPostingsForMatchersCacheMetrics:  tsdb.NewPostingsForMatchersCacheMetrics(nil),
		BlockPostingsForMatchersCacheMetrics: tsdb.NewPostingsForMatchersCacheMetrics(nil),
	}, nil)
	if err != nil {
		return nil, err
	}

	db.DisableCompactions()

	udb.DB = db

	return udb, nil
}

// Function to upload the blocks.
type blockUploader func(_ context.Context, tenantID, dbDir string, blockIDs []string) error

// CompactAndUpload compacts the blocks of all the TSDBs and uploads them.
// uploadBlocks is a function that uploads the blocks to the required storage.
// All the DBs are closed and directories cleared irrespective of success or failure of this function.
func (b *TSDBBuilder) CompactAndUpload(ctx context.Context, uploadBlocks blockUploader) (_ int, err error) {
	var (
		closedDBsMu sync.Mutex
		closedDBs   = make(map[*userTSDB]bool)
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
		// Clear the map so that it can be released from the memory. Not setting to nil in case we want to reuse the TSDBBuilder.
		clear(b.tsdbs)
		b.tsdbsMu.Unlock()

		err = merr.Err()
	}()

	level.Info(b.logger).Log("msg", "compacting and uploading blocks", "num_tsdb", len(b.tsdbs))

	if len(b.tsdbs) == 0 {
		return 0, nil
	}

	numBlocks := atomic.NewInt64(0)

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
				partitionStr := fmt.Sprintf("%d", tenant.partitionID)
				if err != nil {
					b.metrics.compactAndUploadFailed.WithLabelValues(partitionStr).Inc()
					return
				}
				b.metrics.compactAndUploadDuration.WithLabelValues(partitionStr).Observe(time.Since(t).Seconds())
			}(time.Now())

			if err := db.compactEverything(ctx); err != nil {
				return err
			}

			var blockIDs []string
			dbDir := db.Dir()
			for _, b := range db.Blocks() {
				blockIDs = append(blockIDs, b.Meta().ULID.String())
			}
			numBlocks.Add(int64(len(blockIDs)))

			if err := db.Close(); err != nil {
				return err
			}

			closedDBsMu.Lock()
			closedDBs[db] = true
			closedDBsMu.Unlock()

			if err := uploadBlocks(ctx, tenant.tenantID, dbDir, blockIDs); err != nil {
				return err
			}

			// Clear the DB from the disk. Don't need it anymore.
			return os.RemoveAll(dbDir)
		})
	}
	err = eg.Wait()
	return int(numBlocks.Load()), err
}

// Close closes all DBs and deletes their data directories.
// This functions is useful when block builder has faced some unrecoverable error
// and has to discard the block building for the current cycle.
func (b *TSDBBuilder) Close() error {
	b.tsdbsMu.Lock()
	defer b.tsdbsMu.Unlock()

	var merr multierror.MultiError
	for _, db := range b.tsdbs {
		dbDir := db.Dir()
		merr.Add(db.Close())
		merr.Add(os.RemoveAll(dbDir))
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
		return errors.Wrapf(errMaxInMemorySeriesReached, "limit of %d reached for user %s", u.maxGlobalSeries, u.userID)
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
