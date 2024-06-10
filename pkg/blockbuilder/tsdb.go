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

type builder interface {
	process(ctx context.Context, rec *kgo.Record, lastBlockMax, blockMax int64, recordProcessedBefore bool) (_ bool, err error)
	compactAndUpload(ctx context.Context, blockUploaderForUser func(context.Context, string) blockUploader) error
	close() error
}

type tsdbTenant struct {
	part int32
	id   string
}

type tsdbBuilder struct {
	logger log.Logger

	// tenant-to-tsdb
	tsdbs   map[tsdbTenant]*userTSDB
	tsdbsMu sync.RWMutex

	limits              *validation.Overrides
	blocksStorageConfig mimir_tsdb.BlocksStorageConfig
}

type blockUploader func(blockDir string) error

func newTSDBBuilder(logger log.Logger, limits *validation.Overrides, blocksStorageConfig mimir_tsdb.BlocksStorageConfig) *tsdbBuilder {
	return &tsdbBuilder{
		tsdbs:               make(map[tsdbTenant]*userTSDB),
		logger:              logger,
		limits:              limits,
		blocksStorageConfig: blocksStorageConfig,
	}
}

// process puts the samples in the TSDB. Some parts taken from (*Ingester).pushSamplesToAppender.
// It returns false if at least one sample was skipped to process later, true otherwise. true also includes the cases
// where the sample was not put in the TSDB because it was discarded or was already processed before.
// lastEnd: "end" time of the previous block building cycle.
// currEnd: end time of the block we are looking at right now.
func (b *tsdbBuilder) process(ctx context.Context, rec *kgo.Record, lastBlockMax, blockMax int64, recordProcessedBefore bool) (_ bool, err error) {
	userID := string(rec.Key)

	req := mimirpb.WriteRequest{}
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
		part: rec.Partition,
		id:   userID,
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
		}
	}()

	//minValidTime, ok := db.AppendableMinValidTime()

	var (
		labelsBuilder       labels.ScratchBuilder
		nonCopiedLabels     labels.Labels
		allSamplesProcessed = true
	)
	for _, ts := range req.Timeseries {
		mimirpb.FromLabelAdaptersOverwriteLabels(&labelsBuilder, ts.Labels, &nonCopiedLabels)
		hash := nonCopiedLabels.Hash()
		// Look up a reference for this series. The hash passed should be the output of Labels.Hash()
		// and NOT the stable hashing because we use the stable hashing in ingesters only for query sharding.
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
				// Copy the label set because both TSDB and the active series tracker may retain it.
				copiedLabels = mimirpb.CopyLabels(nonCopiedLabels)
				// Retain the reference in case there are multiple samples for the series.
				if ref, err = app.Append(0, copiedLabels, s.TimestampMs, s.Value); err == nil {
					continue
				}
			}
			// TODO(v): not all errors should terminate; see how it ingester handles them
			// TODO(codesome): handle out of order carefully here.
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
			// TODO(codesome): handle out of order carefully here.
			return false, err
		}

		// Exemplars are not persisted in the block. So we skip it.
	}

	// TODO(codesome): wrap the error with user
	return allSamplesProcessed, app.Commit()
}

func (b *tsdbBuilder) getTSDB(tenant tsdbTenant) *userTSDB {
	b.tsdbsMu.RLock()
	defer b.tsdbsMu.RUnlock()
	return b.tsdbs[tenant]
}

func (b *tsdbBuilder) getOrCreateTSDB(tenant tsdbTenant) (*userTSDB, error) {
	db := b.getTSDB(tenant)
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

func (b *tsdbBuilder) tsdbDir(tenant tsdbTenant) string {
	return filepath.Join(b.blocksStorageConfig.TSDB.Dir, strconv.Itoa(int(tenant.part)), tenant.id)
}

func (b *tsdbBuilder) newTSDB(tenant tsdbTenant) (*userTSDB, error) {
	udir := b.tsdbDir(tenant)
	// Remove any previous TSDB dir. We don't need it.
	if err := os.RemoveAll(udir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(udir, os.ModePerm); err != nil {
		return nil, err
	}

	userID := tenant.id
	userLogger := util_log.WithUserID(userID, b.logger)

	udb := &userTSDB{
		userID: userID,
	}

	db, err := tsdb.Open(udir, userLogger, nil, &tsdb.Options{
		RetentionDuration:           0,
		MinBlockDuration:            2 * time.Hour.Milliseconds(),
		MaxBlockDuration:            2 * time.Hour.Milliseconds(),
		NoLockfile:                  true,
		StripeSize:                  b.blocksStorageConfig.TSDB.StripeSize,
		HeadChunksWriteBufferSize:   b.blocksStorageConfig.TSDB.HeadChunksWriteBufferSize,
		HeadChunksEndTimeVariance:   b.blocksStorageConfig.TSDB.HeadChunksEndTimeVariance,
		HeadChunksWriteQueueSize:    b.blocksStorageConfig.TSDB.HeadChunksWriteQueueSize,
		WALSegmentSize:              -1, // No WAL
		SeriesLifecycleCallback:     udb,
		BlocksToDelete:              udb.blocksToDelete,
		IsolationDisabled:           true,
		EnableOverlappingCompaction: false, // always false since Mimir only uploads lvl 1 compacted blocks
		// TODO(codesome): take into consideration the block builder's processing interval and set this properly.
		OutOfOrderTimeWindow:   b.limits.OutOfOrderTimeWindow(userID).Milliseconds(), // The unit must be same as our timestamps.
		OutOfOrderCapMax:       int64(b.blocksStorageConfig.TSDB.OutOfOrderCapacityMax),
		EnableNativeHistograms: b.limits.NativeHistogramsIngestionEnabled(userID),
		// TODO(codesome): this is used to determine the owned series by an ingesters. May need when applying limits.
		SecondaryHashFunction: nil, // TODO secondaryTSDBHashFunctionForUser(userID),
	}, nil)
	if err != nil {
		return nil, err
	}

	db.DisableCompactions()

	udb.db = db

	return udb, nil
}

// compactAndUpload compacts the blocks of all the TSDBs
// and uploads them.
func (b *tsdbBuilder) compactAndUpload(ctx context.Context, blockUploaderForUser func(context.Context, string) blockUploader) error {
	b.tsdbsMu.Lock()
	defer b.tsdbsMu.Unlock()

	if len(b.tsdbs) == 0 {
		return nil
	}

	eg, ctx := errgroup.WithContext(ctx)
	if b.blocksStorageConfig.TSDB.ShipConcurrency > 0 {
		eg.SetLimit(b.blocksStorageConfig.TSDB.ShipConcurrency)
	}
	for tenant, db := range b.tsdbs {
		if err := db.compactEverything(ctx); err != nil {
			return err
		}

		// TODO(codesome): the delete() on the map does not release the memory until after the map is reset.
		// So, if we choose to not truncation while compacting the in-prder head above, we should try to
		// truncate the entire Head efficiently before closing the DB since closing the DB does not release
		// the memory either. NOTE: this is unnecessary if it does not reduce the memory spikes of compaction.
		var blockNames []string
		dbDir := db.db.Dir()
		for _, b := range db.db.Blocks() {
			blockNames = append(blockNames, b.Meta().ULID.String())
		}

		if err := db.Close(); err != nil {
			return err
		}

		delete(b.tsdbs, tenant)

		eg.Go(func() error {
			uploader := blockUploaderForUser(ctx, tenant.id)
			for _, bn := range blockNames {
				if err := uploader(filepath.Join(dbDir, bn)); err != nil {
					return err
				}
			}

			// Clear the DB from the disk. Don't need it anymore.
			return os.RemoveAll(dbDir)
		})
	}
	err := eg.Wait()

	// Clear the map so that it can be released from the memory. Not setting to nil in case
	// we want to reuse the tsdbBuilder.
	b.tsdbs = make(map[tsdbTenant]*userTSDB)
	return err
}

func (b *tsdbBuilder) close() error {
	b.tsdbsMu.Lock()
	defer b.tsdbsMu.Unlock()

	for userID, db := range b.tsdbs {
		dbDir := db.db.Dir()

		if err := db.Close(); err != nil {
			return err
		}
		delete(b.tsdbs, userID)

		// Remove any artifacts of this TSDB. We don't need them anymore.
		if err := os.RemoveAll(dbDir); err != nil {
			return err
		}
	}

	// Clear the map so that it can be released from the memory. Not setting to nil in case
	// we want to reuse the tsdbBuilder.
	b.tsdbs = make(map[tsdbTenant]*userTSDB)
	return nil
}

type extendedAppender interface {
	storage.Appender
	storage.GetRef
}

type userTSDB struct {
	db      *tsdb.DB
	userID  string
	shipper BlocksUploader
}

// BlocksUploader interface is used to have an easy way to mock it in tests.
type BlocksUploader interface {
	Sync(ctx context.Context) (uploaded int, err error)
}

func (u *userTSDB) Head() *tsdb.Head {
	return u.db.Head()
}

func (u *userTSDB) Close() error {
	return u.db.Close()
}

func (u *userTSDB) Appender(ctx context.Context) storage.Appender {
	return u.db.Appender(ctx)
}

func (u *userTSDB) compactEverything(ctx context.Context) error {
	blockRange := 2 * time.Hour.Milliseconds()

	// Compact the in-order data.
	mint, maxt := u.Head().MinTime(), u.Head().MaxTime()
	mint = (mint / blockRange) * blockRange
	for blockMint := mint; blockMint <= maxt; blockMint += blockRange {
		blockMaxt := blockMint + blockRange - 1
		rh := tsdb.NewRangeHead(u.Head(), blockMint, blockMaxt)
		// TODO(codesome): this also truncates the memory here. We can skip it since we will close
		// this TSDB right after all the compactions. We will save a good chunks of computation this way.
		// See https://github.com/grafana/mimir-prometheus/pull/638
		if err := u.db.CompactHead(rh); err != nil {
			return err
		}
	}

	// Compact the out-of-order data.
	if err := u.db.CompactOOOHead(ctx); err != nil {
		return err
	}

	return nil
}

func (u *userTSDB) PreCreation(metric labels.Labels) error {
	// TODO: Implement
	return nil
}

func (u *userTSDB) PostCreation(metric labels.Labels) {
	// TODO: Implement
}

func (u *userTSDB) PostDeletion(metrics map[chunks.HeadSeriesRef]labels.Labels) {
	// TODO: Implement
}

func (u *userTSDB) blocksToDelete(blocks []*tsdb.Block) map[ulid.ULID]struct{} {
	// TODO(codesome): delete all the blocks that have been shipped.
	return map[ulid.ULID]struct{}{}
}
