// SPDX-License-Identifier: AGPL-3.0-only

package remotewrite

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promconfig "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"github.com/prometheus/prometheus/util/compression"

	utillog "github.com/grafana/mimir/pkg/util/log"
)

// Storage owns a WAL and a WriteStorage (which manages the QueueManagers) for
// a single (tenant, remote-write config) pair.
// It implements storage.Appendable so it can be used directly by the ruler.
type Storage struct {
	cfg    Config
	logger log.Logger

	wal *wlog.WL
	rws *remote.WriteStorage

	// series tracks the mapping of label-set hash → WAL series reference.
	// This avoids re-registering the same series on every rule evaluation.
	// walSeries is the subset of series whose series record has been written
	// to the WAL; the WAL watcher must see the series record before it can
	// forward samples for that ref to the remote endpoint.
	seriesMu  sync.Mutex
	series    map[uint64]chunks.HeadSeriesRef
	walSeries map[uint64]struct{}
	nextRef   atomic.Uint64

	totalSamples  prometheus.Counter
	failedSamples *prometheus.CounterVec
}

// newStorage creates a Storage. baseDir is the root directory for this storage;
// the WAL is placed at baseDir/wal and QueueManager state alongside it.
// The directory is created if it does not exist.
func newStorage(cfg Config, baseDir string, flushDeadline time.Duration, reg prometheus.Registerer, logger log.Logger) (*Storage, error) {
	// Prometheus QueueManager's WAL watcher constructs the WAL path as <dir>/wal
	// (see wlog.NewWatcher: walDir = filepath.Join(dir, "wal")).
	// We must therefore create our WAL one level down so the watcher finds it.
	walDir := filepath.Join(baseDir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating WAL directory %q: %w", walDir, err)
	}

	slogger := utillog.SlogFromGoKit(logger)

	// Create the WAL at <baseDir>/wal. The QueueManagers' Watchers will look
	// for it at <baseDir>/wal (i.e. <dir>/wal where dir=baseDir).
	wal, err := wlog.New(slogger, reg, walDir, compression.None)
	if err != nil {
		return nil, fmt.Errorf("opening WAL at %q: %w", walDir, err)
	}

	// Create the WriteStorage with baseDir so QueueManagers watch baseDir/wal.
	rws := remote.NewWriteStorage(slogger, reg, baseDir, flushDeadline, nil, false)

	// Translate our Config to a Prometheus RemoteWriteConfig and apply it,
	// which starts the QueueManager(s).
	promRWCfg, err := cfg.toPrometheusRemoteWriteConfig()
	if err != nil {
		_ = wal.Close()
		rws.Close() //nolint:errcheck
		return nil, fmt.Errorf("building remote write config: %w", err)
	}
	if err := rws.ApplyConfig(&promconfig.Config{
		RemoteWriteConfigs: []*promconfig.RemoteWriteConfig{promRWCfg},
	}); err != nil {
		_ = wal.Close()
		rws.Close() //nolint:errcheck
		return nil, fmt.Errorf("applying remote write config: %w", err)
	}

	// The registerer is already wrapped with {user, remote} labels by the Manager,
	// so no additional ConstLabels are needed here.
	totalSamples := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_ruler_remote_write_samples_total",
		Help: "Total number of samples enqueued to the ruler remote write WAL.",
	})
	failedSamples := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ruler_remote_write_samples_failed_total",
		Help: "Total number of samples that failed to be enqueued to the ruler remote write WAL.",
	}, []string{"reason"})

	s := &Storage{
		cfg:           cfg,
		logger:        logger,
		wal:           wal,
		rws:           rws,
		series:        make(map[uint64]chunks.HeadSeriesRef),
		walSeries:     make(map[uint64]struct{}),
		totalSamples:  totalSamples,
		failedSamples: failedSamples,
	}
	return s, nil
}

// URL returns the remote write endpoint URL string.
func (s *Storage) URL() string {
	if s.cfg.URL.URL == nil {
		return ""
	}
	return s.cfg.URL.URL.String()
}

// Appender returns a new storage.Appender that writes to the WAL.
func (s *Storage) Appender(ctx context.Context) storage.Appender {
	return &remoteWriteAppender{
		ctx:     ctx,
		storage: s,
	}
}

// Stop shuts down the WriteStorage (drains queues) and closes the WAL.
func (s *Storage) Stop() {
	if err := s.rws.Close(); err != nil {
		level.Error(s.logger).Log("msg", "error closing remote write storage", "remote", s.cfg.Name, "err", err)
	}
	if err := s.wal.Close(); err != nil {
		level.Error(s.logger).Log("msg", "error closing WAL", "remote", s.cfg.Name, "err", err)
	}
}

// remoteWriteAppender is a storage.Appender for a single rule evaluation cycle.
// On Commit it writes all buffered samples to the WAL and notifies the QueueManager.
type remoteWriteAppender struct {
	ctx     context.Context
	storage *Storage

	// buffered data
	labelSets []labels.Labels
	samples   []record.RefSample
	hSamples  []record.RefHistogramSample
	fhSamples []record.RefFloatHistogramSample
}

func (a *remoteWriteAppender) SetOptions(_ *storage.AppendOptions) {}

// Append buffers a float sample.
func (a *remoteWriteAppender) Append(_ storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	ref := a.storage.refForLabels(l)
	a.labelSets = append(a.labelSets, l)
	a.samples = append(a.samples, record.RefSample{Ref: ref, T: t, V: v})
	return storage.SeriesRef(ref), nil
}

// AppendHistogram buffers a native histogram sample.
func (a *remoteWriteAppender) AppendHistogram(_ storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	ref := a.storage.refForLabels(l)
	a.labelSets = append(a.labelSets, l)
	if h != nil {
		a.hSamples = append(a.hSamples, record.RefHistogramSample{Ref: ref, T: t, H: h})
	} else {
		a.fhSamples = append(a.fhSamples, record.RefFloatHistogramSample{Ref: ref, T: t, FH: fh})
	}
	return storage.SeriesRef(ref), nil
}

func (a *remoteWriteAppender) AppendExemplar(_ storage.SeriesRef, _ labels.Labels, _ exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, errors.New("exemplars are unsupported by ruler remote write")
}

func (a *remoteWriteAppender) UpdateMetadata(_ storage.SeriesRef, _ labels.Labels, _ metadata.Metadata) (storage.SeriesRef, error) {
	return 0, errors.New("metadata updates are unsupported by ruler remote write")
}

func (a *remoteWriteAppender) AppendSTZeroSample(_ storage.SeriesRef, _ labels.Labels, _, _ int64) (storage.SeriesRef, error) {
	return 0, errors.New("ST zero samples are unsupported by ruler remote write")
}

func (a *remoteWriteAppender) AppendHistogramSTZeroSample(_ storage.SeriesRef, _ labels.Labels, _, _ int64, _ *histogram.Histogram, _ *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, errors.New("ST zero samples are unsupported by ruler remote write")
}

// Commit writes buffered samples to the WAL and notifies the QueueManager.
// It checks the per-group URL filter from the context before writing.
// Remote write errors are never propagated — they are logged and metered so
// that a remote write failure cannot interrupt rule evaluation.
func (a *remoteWriteAppender) Commit() error {
	// Per-group URL filter: if the context carries a URL list and our URL is not in it, skip.
	if groupURLs := RemoteWriteURLsFromContext(a.ctx); len(groupURLs) > 0 {
		if !slices.Contains(groupURLs, a.storage.URL()) {
			a.reset()
			return nil
		}
	}

	totalSamples := len(a.samples) + len(a.hSamples) + len(a.fhSamples)
	if totalSamples == 0 {
		return nil
	}

	if err := a.flushToWAL(); err != nil {
		a.storage.failedSamples.WithLabelValues("wal_write").Add(float64(totalSamples))
		level.Error(a.storage.logger).Log("msg", "failed to write samples to remote write WAL",
			"remote", a.storage.cfg.Name, "samples", totalSamples, "err", err)
		a.reset()
		return nil // non-blocking: remote write failure must not fail rule evaluation
	}

	a.storage.totalSamples.Add(float64(totalSamples))
	a.storage.rws.Notify()
	a.reset()
	return nil
}

// Rollback discards all buffered data.
func (a *remoteWriteAppender) Rollback() error {
	a.reset()
	return nil
}

// flushToWAL writes all buffered series definitions and samples to the WAL.
func (a *remoteWriteAppender) flushToWAL() error {
	enc := record.Encoder{}

	// Collect series whose record hasn't yet been written to the WAL.
	// Note: s.series is populated eagerly by refForLabels (called from Append)
	// to assign stable refs, but a series record must also be written to the WAL
	// so the WAL watcher can associate the ref with its label set. walSeries
	// tracks the subset that has been written.
	a.storage.seriesMu.Lock()
	var newSeries []record.RefSeries
	for _, l := range a.labelSets {
		h := l.Hash()
		if _, ok := a.storage.walSeries[h]; !ok {
			ref := a.storage.series[h] // already assigned by refForLabels
			a.storage.walSeries[h] = struct{}{}
			newSeries = append(newSeries, record.RefSeries{Ref: ref, Labels: l})
		}
	}
	a.storage.seriesMu.Unlock()

	// Write new series records before the samples that reference them.
	if len(newSeries) > 0 {
		buf := enc.Series(newSeries, nil)
		if err := a.storage.wal.Log(buf); err != nil {
			return fmt.Errorf("WAL log series: %w", err)
		}
	}

	// Write float sample records.
	if len(a.samples) > 0 {
		buf := enc.Samples(a.samples, nil)
		if err := a.storage.wal.Log(buf); err != nil {
			return fmt.Errorf("WAL log samples: %w", err)
		}
	}

	// Write native histogram records.
	if len(a.hSamples) > 0 {
		buf, overflow := enc.HistogramSamples(a.hSamples, nil)
		if err := a.storage.wal.Log(buf); err != nil {
			return fmt.Errorf("WAL log histograms: %w", err)
		}
		if len(overflow) > 0 {
			buf = enc.CustomBucketsHistogramSamples(overflow, nil)
			if err := a.storage.wal.Log(buf); err != nil {
				return fmt.Errorf("WAL log custom-bucket histograms: %w", err)
			}
		}
	}

	// Write float histogram records.
	if len(a.fhSamples) > 0 {
		buf, overflow := enc.FloatHistogramSamples(a.fhSamples, nil)
		if err := a.storage.wal.Log(buf); err != nil {
			return fmt.Errorf("WAL log float histograms: %w", err)
		}
		if len(overflow) > 0 {
			buf = enc.CustomBucketsFloatHistogramSamples(overflow, nil)
			if err := a.storage.wal.Log(buf); err != nil {
				return fmt.Errorf("WAL log custom-bucket float histograms: %w", err)
			}
		}
	}

	return nil
}

// reset clears all buffers.
func (a *remoteWriteAppender) reset() {
	a.labelSets = a.labelSets[:0]
	a.samples = a.samples[:0]
	a.hSamples = a.hSamples[:0]
	a.fhSamples = a.fhSamples[:0]
}

// refForLabels returns the WAL series reference for l, creating one if needed.
func (s *Storage) refForLabels(l labels.Labels) chunks.HeadSeriesRef {
	h := l.Hash()
	s.seriesMu.Lock()
	ref, ok := s.series[h]
	if !ok {
		ref = chunks.HeadSeriesRef(s.nextRef.Add(1))
		s.series[h] = ref
	}
	s.seriesMu.Unlock()
	return ref
}

