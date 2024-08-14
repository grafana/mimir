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
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

type TSDBBuilder struct {
	logger log.Logger

	// Map of a tenant in a partition to its TSDB.
	tsdbsMu sync.RWMutex
	tsdbs   map[tsdbTenant]*userTSDB

	limits *validation.Overrides
	config mimir_tsdb.BlocksStorageConfig
}

type tsdbTenant struct {
	partitionID int32
	tenantID    string
}

func NewTSDBBuilder(logger log.Logger, limits *validation.Overrides, config mimir_tsdb.BlocksStorageConfig) *TSDBBuilder {
	return &TSDBBuilder{
		tsdbs:  make(map[tsdbTenant]*userTSDB),
		logger: logger,
		limits: limits,
		config: config,
	}
}

// Process puts the samples in the TSDB. Some parts taken from (*Ingester).pushSamplesToAppender.
// It returns false if at least one sample was skipped to process later, true otherwise. true also includes the cases
// where the sample was not put in the TSDB because it was discarded or was already processed before.
// lastBlockMax: max time of the block in the previous block building cycle.
// blockMax: max time of the block in the current block building cycle. This blockMax is exclusive of the last sample by design in TSDB.
// recordProcessedBefore: true if the record was processed in the previous cycle. (It gets processed again if some samples did not fit in the previous cycle.)
func (b *TSDBBuilder) Process(ctx context.Context, rec *kgo.Record, lastBlockMax, blockMax int64, recordProcessedBefore bool) (_ bool, err error) {
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
		labelsBuilder       labels.ScratchBuilder
		nonCopiedLabels     labels.Labels
		allSamplesProcessed = true
	)
	for _, ts := range req.Timeseries {
		mimirpb.FromLabelAdaptersOverwriteLabels(&labelsBuilder, ts.Labels, &nonCopiedLabels)
		hash := nonCopiedLabels.Hash()
		// Look up a reference for this series. The hash passed should be the output of Labels.Hash()
		// and NOT the stable hashing because that's what TSDB expects. We don't need stable hashing in block builder.
		ref, copiedLabels := app.GetRef(nonCopiedLabels, hash)

		for _, s := range ts.Samples {
			if s.TimestampMs >= blockMax {
				// We will process this sample in the next cycle.
				allSamplesProcessed = false
				continue
			}
			if recordProcessedBefore && s.TimestampMs < lastBlockMax {
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
			// TODO(v): not all errors should terminate; see how it ingester handles them
			return false, err
		}

		for _, h := range ts.Histograms {
			if h.Timestamp >= blockMax {
				// We will process this sample in the next cycle.
				allSamplesProcessed = false
				continue
			}
			if recordProcessedBefore && h.Timestamp < lastBlockMax {
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
			// TODO(v): not all errors should terminate; see how it ingester handles them
			return false, err
		}

		// Exemplars and metadata are not persisted in the block. So we skip them.
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
	udir := filepath.Join(b.config.TSDB.Dir, strconv.Itoa(int(tenant.partitionID)), tenant.tenantID)
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

	db, err := tsdb.Open(udir, userLogger, nil, &tsdb.Options{
		RetentionDuration:           0,
		MinBlockDuration:            2 * time.Hour.Milliseconds(),
		MaxBlockDuration:            2 * time.Hour.Milliseconds(),
		NoLockfile:                  true,
		StripeSize:                  b.config.TSDB.StripeSize,
		HeadChunksWriteBufferSize:   b.config.TSDB.HeadChunksWriteBufferSize,
		HeadChunksWriteQueueSize:    b.config.TSDB.HeadChunksWriteQueueSize,
		WALSegmentSize:              -1, // No WAL
		SeriesLifecycleCallback:     udb,
		BlocksToDelete:              udb.blocksToDelete,
		IsolationDisabled:           true,
		EnableOverlappingCompaction: false,                                                // always false since Mimir only uploads lvl 1 compacted blocks
		OutOfOrderTimeWindow:        b.limits.OutOfOrderTimeWindow(userID).Milliseconds(), // The unit must be same as our timestamps.
		OutOfOrderCapMax:            int64(b.config.TSDB.OutOfOrderCapacityMax),
		EnableNativeHistograms:      b.limits.NativeHistogramsIngestionEnabled(userID),
		SecondaryHashFunction:       nil, // TODO(codesome): May needed when applying limits. Used to determine the owned series by an ingesters
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
// If the functions returns an error, the DBs that are not successfully compacted or
// uploaded will not be cleared from the TSDBBuilder.
func (b *TSDBBuilder) CompactAndUpload(ctx context.Context, uploadBlocks blockUploader) (int, error) {
	b.tsdbsMu.Lock()
	defer b.tsdbsMu.Unlock()

	level.Info(b.logger).Log("msg", "compacting and uploading blocks", "num_tsdb", len(b.tsdbs))

	if len(b.tsdbs) == 0 {
		return 0, nil
	}

	eg, ctx := errgroup.WithContext(ctx)
	if b.config.TSDB.ShipConcurrency > 0 {
		eg.SetLimit(b.config.TSDB.ShipConcurrency)
	}
	var numBlocks atomic.Int64
	for tenant, db := range b.tsdbs {
		// Change scope of for loop variables.
		tenant := tenant
		db := db

		eg.Go(func() error {
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

			if err := uploadBlocks(ctx, tenant.tenantID, dbDir, blockIDs); err != nil {
				return err
			}

			delete(b.tsdbs, tenant)

			// Clear the DB from the disk. Don't need it anymore.
			return os.RemoveAll(dbDir)
		})
	}
	err := eg.Wait()

	// Clear the map so that it can be released from the memory. Not setting to nil in case
	// we want to reuse the TSDBBuilder.
	// If the map is not empty, it means some tenants failed to upload the blocks. In that case we do not
	// remove the tenant in case the caller wants to retry.
	if len(b.tsdbs) == 0 {
		b.tsdbs = make(map[tsdbTenant]*userTSDB)
	}
	return int(numBlocks.Load()), err
}

// Close closes all DBs and deletes their data directories.
// This functions is useful when block builder has faced some unrecoverable error
// and has to discard the block building for the current cycle.
func (b *TSDBBuilder) Close() error {
	b.tsdbsMu.Lock()
	defer b.tsdbsMu.Unlock()

	var firstErr error
	for _, db := range b.tsdbs {
		dbDir := db.Dir()

		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}

		// Remove any artifacts of this TSDB. We don't need them anymore.
		if err := os.RemoveAll(dbDir); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Clear the map so that it can be released from the memory. Not setting to nil in case
	// we want to reuse the TSDBBuilder.
	b.tsdbs = make(map[tsdbTenant]*userTSDB)
	return firstErr
}

type extendedAppender interface {
	storage.Appender
	storage.GetRef
}

type userTSDB struct {
	*tsdb.DB
	userID string
}

// BlocksUploader interface is used to have an easy way to mock it in tests.
type BlocksUploader interface {
	Sync(ctx context.Context) (uploaded int, err error)
}

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

func (u *userTSDB) PreCreation(_ labels.Labels) error                     { return nil }
func (u *userTSDB) PostCreation(_ labels.Labels)                          {}
func (u *userTSDB) PostDeletion(_ map[chunks.HeadSeriesRef]labels.Labels) {}
func (u *userTSDB) blocksToDelete(_ []*tsdb.Block) map[ulid.ULID]struct{} {
	return map[ulid.ULID]struct{}{}
}
