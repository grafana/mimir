package blockbuilder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

// TODO(v): move to config
var bbRootDir string

func init() {
	bbRootDir, _ = os.MkdirTemp("", "blockbuilder")
}

func blocksDir(tenantID string) string {
	return filepath.Join(bbRootDir, tenantID)
}

type tsdbBuilder struct {
	logger log.Logger

	// tenant-to-tsdb
	tsdbs   map[string]*tsdb.DB // TODO: Use tsdb.DB instead
	tsdbsMu sync.RWMutex
}

func newTSDBBuilder(logger log.Logger) *tsdbBuilder {
	return &tsdbBuilder{
		tsdbs:  make(map[string]*tsdb.DB),
		logger: logger,
	}
}

// see (*Ingester).pushSamplesToAppender
// did not make it into the block in this round.
// lastEnd: "end" time of the previous block building cycle.
// end: end time of the block we are looking at right now.
// Returns true if all the samples were taken to be put in a block, false if at least one sample
func (b *tsdbBuilder) process(ctx context.Context, rec *kgo.Record, lastEnd, end int64, recordProcessedBefore bool) (bool, error) {
	tenantID := string(rec.Key)

	req := mimirpb.WriteRequest{}
	defer mimirpb.ReuseSlice(req.Timeseries)

	// TODO(codesome): see if we can skip parsing exemplars. They are not persisted in the block so we can save some parsing here.
	err := req.Unmarshal(rec.Value)
	if err != nil {
		return false, fmt.Errorf("unmarshal record key %s: %w", rec.Key, err)
	}

	if len(req.Timeseries) == 0 {
		return false, nil
	}

	db, err := b.getOrCreateTSDB(tenantID)
	if err != nil {
		return false, fmt.Errorf("get tsdb for tenant %s: %w", tenantID, err)
	}

	app := db.Appender(rec.Context).(extendedAppender)
	defer func() {
		if err := app.Rollback(); err != nil && !errors.Is(err, tsdb.ErrAppenderClosed) {
			level.Warn(b.logger).Log("msg", "failed to rollback appender on error", "tenant", tenantID, "err", err)
		}
	}()

	//minValidTime, ok := db.AppendableMinValidTime()

	var (
		labelsBuilder       labels.ScratchBuilder
		nonCopiedLabels     labels.Labels
		allSamplesProcessed bool
	)
	for _, ts := range req.Timeseries {
		mimirpb.FromLabelAdaptersOverwriteLabels(&labelsBuilder, ts.Labels, &nonCopiedLabels)
		hash := nonCopiedLabels.Hash()
		// Look up a reference for this series. The hash passed should be the output of Labels.Hash()
		// and NOT the stable hashing because we use the stable hashing in ingesters only for query sharding.
		ref, copiedLabels := app.GetRef(nonCopiedLabels, hash)

		for _, s := range ts.Samples {
			if s.TimestampMs >= end {
				// We will process this sample in the next cycle.
				allSamplesProcessed = false
				continue
			}
			if recordProcessedBefore && s.TimestampMs < lastEnd {
				// This sample was already processed in the previous cycle.
				continue
			}
			var err error
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
			if h.Timestamp >= end {
				// We will process this sample in the next cycle.
				allSamplesProcessed = false
				continue
			}
			if recordProcessedBefore && h.Timestamp < lastEnd {
				// This sample was already processed in the previous cycle.
				continue
			}
			var (
				err error
				ih  *histogram.Histogram
				fh  *histogram.FloatHistogram
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

func (b *tsdbBuilder) getOrCreateTSDB(tenantID string) (*tsdb.DB, error) {
	b.tsdbsMu.RLock()
	db, _ := b.tsdbs[tenantID]
	b.tsdbsMu.RUnlock()

	if db != nil {
		return db, nil
	}

	b.tsdbsMu.Lock()
	defer b.tsdbsMu.Unlock()

	// Check again for DB in the event it was created in-between locks
	var ok bool
	db, ok = b.tsdbs[tenantID]
	if ok {
		return db, nil
	}

	db, err := b.newTSDB(tenantID)
	if err != nil {
		return nil, err
	}

	b.tsdbs[tenantID] = db

	return db, nil
}

func (b *tsdbBuilder) newTSDB(tenantID string) (*tsdb.DB, error) {
	udir := blocksDir(tenantID)

	userLogger := util_log.WithUserID(tenantID, b.logger)

	return tsdb.Open(udir, userLogger, nil, &tsdb.Options{
		RetentionDuration:           0,
		MinBlockDuration:            2 * time.Hour.Milliseconds(),
		MaxBlockDuration:            2 * time.Hour.Milliseconds(),
		NoLockfile:                  true,
		StripeSize:                  128, // TODO i.cfg.BlocksStorageConfig.TSDB.StripeSize,
		HeadChunksWriteBufferSize:   128, // TODO i.cfg.BlocksStorageConfig.TSDB.HeadChunksWriteBufferSize,
		HeadChunksEndTimeVariance:   0,   // TODO i.cfg.BlocksStorageConfig.TSDB.HeadChunksEndTimeVariance,
		WALSegmentSize:              -1,  // No WAL
		SeriesLifecycleCallback:     nil, // TODO
		BlocksToDelete:              nil, // TODO
		SeriesHashCache:             nil, // TODO: do we need it? I dont think so
		IsolationDisabled:           true,
		HeadChunksWriteQueueSize:    128,                          // TODO i.cfg.BlocksStorageConfig.TSDB.HeadChunksWriteQueueSize,
		EnableOverlappingCompaction: false,                        // always false since Mimir only uploads lvl 1 compacted blocks
		OutOfOrderTimeWindow:        2 * time.Hour.Milliseconds(), // i.limits.OutOfOrderTimeWindow(userID)
		OutOfOrderCapMax:            30,                           // TODO int64(i.cfg.BlocksStorageConfig.TSDB.OutOfOrderCapacityMax),
		EnableNativeHistograms:      true,                         // TODO i.limits.NativeHistogramsIngestionEnabled(userID),
		SecondaryHashFunction:       nil,                          // TODO secondaryTSDBHashFunctionForUser(userID),
	}, nil)
}

// compactBlocks compacts the blocks of all the TSDBs.
func (b *tsdbBuilder) compactAndCloseDBs(ctx context.Context) error {
	blockRange := 2 * time.Hour.Milliseconds()
	b.tsdbsMu.Lock()
	defer b.tsdbsMu.Unlock()
	for userID, db := range b.tsdbs {
		// Compact the in-order data.
		mint, maxt := db.Head().MinTime(), db.Head().MaxTime()
		mint = blockRange * mint / blockRange
		for blockMint := mint; blockMint <= maxt; blockMint += blockRange {
			blockMaxt := blockMint + blockRange - 1
			rh := tsdb.NewRangeHead(db.Head(), blockMint, blockMaxt)
			// TODO(codesome): this also truncates the memory here. We can skip it since we will close
			// this TSDB right after all the compactions. We will save a good chunks of computation this way.
			if err := db.CompactHead(rh); err != nil {
				return err
			}
		}

		// Compact the out-of-order data.
		if err := db.CompactOOOHead(ctx); err != nil {
			return err
		}

		// TODO(codesome): the delete() on the map does not release the memory until after the map is reset.
		// So, if we choose to not truncation while compacting the in-prder head above, we should try to
		// truncate the entire Head efficiently before closing the DB since closing the DB does not release
		// the memory either. NOTE: this is unnecessary if it does not reduce the memory spikes of compaction.

		if err := db.Close(); err != nil {
			return err
		}

		delete(b.tsdbs, userID)
	}

	// Clear the map so that it can be released from the memory. Not setting to nil in case
	// we want to reuse the tsdbBuilder.
	b.tsdbs = make(map[string]*tsdb.DB)
	return nil
}

type extendedAppender interface {
	storage.Appender
	storage.GetRef
}
