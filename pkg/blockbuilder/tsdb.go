package blockbuilder

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
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
	tsdbs   map[string]*tsdb.Head
	tsdbsMu sync.RWMutex
}

func newTSDBBuilder(logger log.Logger) *tsdbBuilder {
	return &tsdbBuilder{
		tsdbs:  make(map[string]*tsdb.Head),
		logger: logger,
	}
}

// see (*Ingester).pushSamplesToAppender
func (b *tsdbBuilder) process(ctx context.Context, rec *kgo.Record) error {
	tenantID := string(rec.Key)

	req := &mimirpb.WriteRequest{}
	defer mimirpb.ReuseSlice(req.Timeseries)

	err := req.Unmarshal(rec.Value)
	if err != nil {
		return fmt.Errorf("unmarshal record key %s: %w", rec.Key, err)
	}

	if len(req.Timeseries) == 0 {
		return nil
	}

	db, err := b.getOrCreateTSDB(tenantID)
	if err != nil {
		return fmt.Errorf("get tsdb for tenant %s: %w", tenantID, err)
	}

	app := db.Appender(rec.Context).(appender)
	defer func() {
		if err := app.Rollback(); err != nil && !errors.Is(err, tsdb.ErrAppenderClosed) {
			level.Warn(b.logger).Log("msg", "failed to rollback appender on error", "tenant", tenantID, "err", err)
		}
	}()

	//minValidTime, ok := db.AppendableMinValidTime()

	var (
		labelsBuilder   labels.ScratchBuilder
		nonCopiedLabels labels.Labels
	)
	for _, ts := range req.Timeseries {
		mimirpb.FromLabelAdaptersOverwriteLabels(&labelsBuilder, ts.Labels, &nonCopiedLabels)
		hash := nonCopiedLabels.Hash()
		// Look up a reference for this series. The hash passed should be the output of Labels.Hash()
		// and NOT the stable hashing because we use the stable hashing in ingesters only for query sharding.
		ref, copiedLabels := app.GetRef(nonCopiedLabels, hash)

		for _, s := range ts.Samples {
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
			return err
		}

		for _, h := range ts.Histograms {
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
			return err
		}

		for _, ex := range ts.Exemplars {
			e := exemplar.Exemplar{
				Value:  ex.Value,
				Ts:     ex.TimestampMs,
				HasTs:  true,
				Labels: mimirpb.FromLabelAdaptersToLabelsWithCopy(ex.Labels),
			}

			var err error
			if _, err = app.AppendExemplar(ref, labels.EmptyLabels(), e); err == nil {
				continue
			}
			// TODO(v): not all errors should terminate; see how it ingester handles them
			return err
		}
	}

	return app.Commit()
}

func (b *tsdbBuilder) getOrCreateTSDB(tenantID string) (*tsdb.Head, error) {
	b.tsdbsMu.RLock()
	head, _ := b.tsdbs[tenantID]
	b.tsdbsMu.RUnlock()

	if head != nil {
		return head, nil
	}

	b.tsdbsMu.Lock()
	defer b.tsdbsMu.Unlock()

	// Check again for DB in the event it was created in-between locks
	var ok bool
	head, ok = b.tsdbs[tenantID]
	if ok {
		return head, nil
	}

	head, err := b.newTSDB(tenantID)
	if err != nil {
		return nil, err
	}

	b.tsdbs[tenantID] = head

	return head, nil
}

func (b *tsdbBuilder) newTSDB(tenantID string) (*tsdb.Head, error) {
	rootDir := blocksDir(tenantID)

	opts := tsdb.DefaultHeadOptions()
	opts.ChunkDirRoot = filepath.Join(rootDir, "chunks")
	opts.ChunkRange = math.MaxInt64
	opts.EnableNativeHistograms.Store(true)

	return tsdb.NewHead(nil, b.logger, nil, nil, opts, tsdb.NewHeadStats())
}

type appender interface {
	storage.Appender
	storage.GetRef
}
