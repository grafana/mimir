package wal

import (
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

// Files wal.go and series.go in this package are forked and modified from grafana/agent/pkg/prom/wal
// As of now, we cannot re-use upstream agent code here because it agent uses a special
// pkg/intern that lies in our fork of prometheus (grafana/prometheus). Also check comment in intern.go
// For more context: https://raintank-corp.slack.com/archives/CSN5HV0CQ/p1594985039015000

type storageMetrics struct {
	r prometheus.Registerer

	numActiveSeries      prometheus.Gauge
	numDeletedSeries     prometheus.Gauge
	totalCreatedSeries   prometheus.Counter
	totalRemovedSeries   prometheus.Counter
	totalAppendedSamples prometheus.Counter
}

func newStorageMetrics(r prometheus.Registerer) *storageMetrics {
	m := storageMetrics{r: r}
	m.numActiveSeries = promauto.With(r).NewGauge(prometheus.GaugeOpts{
		Name: "agent_wal_storage_active_series",
		Help: "Current number of active series being tracked by the WAL storage",
	})

	m.numDeletedSeries = promauto.With(r).NewGauge(prometheus.GaugeOpts{
		Name: "agent_wal_storage_deleted_series",
		Help: "Current number of series marked for deletion from memory",
	})

	m.totalCreatedSeries = promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "agent_wal_storage_created_series_total",
		Help: "Total number of created series appended to the WAL",
	})

	m.totalRemovedSeries = promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "agent_wal_storage_removed_series_total",
		Help: "Total number of created series removed from the WAL",
	})

	m.totalAppendedSamples = promauto.With(r).NewCounter(prometheus.CounterOpts{
		Name: "agent_wal_samples_appended_total",
		Help: "Total number of samples appended to the WAL",
	})

	if r != nil {
		r.MustRegister(
			m.numActiveSeries,
			m.numDeletedSeries,
			m.totalCreatedSeries,
			m.totalRemovedSeries,
			m.totalAppendedSamples,
		)
	}

	return &m
}

func (m *storageMetrics) Unregister() {
	if m.r == nil {
		return
	}
	cs := []prometheus.Collector{
		m.numActiveSeries,
		m.numDeletedSeries,
		m.totalCreatedSeries,
		m.totalRemovedSeries,
	}
	for _, c := range cs {
		m.r.Unregister(c)
	}
}

// Storage implements storage.Storage, and just writes to the WAL.
type Storage struct {
	// Embed Queryable for compatibility, but don't actually implement it.
	storage.Queryable

	path   string
	wal    *wlog.WL
	logger log.Logger

	appenderPool sync.Pool
	bufPool      sync.Pool

	mtx     sync.RWMutex
	nextRef chunks.HeadSeriesRef
	series  *stripeSeries

	deletedMtx sync.Mutex
	deleted    map[chunks.HeadSeriesRef]int // Deleted series, and what WAL segment they must be kept until.

	// this is a map from groupKey (userID + delimiter + groupname) to the respective ref id
	groupToRef    map[string]chunks.HeadSeriesRef
	groupToRefMtx sync.Mutex

	metrics *storageMetrics
}

// NewStorage makes a new Storage.
func NewStorage(logger log.Logger, registerer prometheus.Registerer, path string) (*Storage, error) {
	w, err := wlog.NewSize(logger, registerer, filepath.Join(path, "wal"), wlog.DefaultSegmentSize, true)
	if err != nil {
		return nil, err
	}

	storage := &Storage{
		path:       path,
		wal:        w,
		logger:     logger,
		deleted:    map[chunks.HeadSeriesRef]int{},
		series:     newStripeSeries(),
		groupToRef: make(map[string]chunks.HeadSeriesRef),
		metrics:    newStorageMetrics(registerer),

		// The first ref ID must be non-zero, as the scraping code treats 0 as a
		// non-existent ID and won't cache it.
		nextRef: 1,
	}

	storage.bufPool.New = func() interface{} {
		b := make([]byte, 0, 1024)
		return b
	}

	storage.appenderPool.New = func() interface{} {
		return &appender{
			w:       storage,
			series:  make([]record.RefSeries, 0, 100),
			samples: make([]record.RefSample, 0, 100),
		}
	}

	if err := storage.replayWAL(); err != nil {
		level.Warn(storage.logger).Log("msg", "encountered WAL read error, attempting repair", "err", err)
		if err := w.Repair(err); err != nil {
			return nil, errors.Wrap(err, "repair corrupted WAL")
		}
	}

	return storage, nil
}

func (w *Storage) replayWAL() error {
	level.Info(w.logger).Log("msg", "replaying WAL, this may take a while", "dir", w.wal.Dir())
	dir, startFrom, err := wlog.LastCheckpoint(w.wal.Dir())
	if err != nil && !errors.Is(err, record.ErrNotFound) {
		return errors.Wrap(err, "find last checkpoint")
	}

	if err == nil {
		sr, err := wlog.NewSegmentsReader(dir)
		if err != nil {
			return errors.Wrap(err, "open checkpoint")
		}
		defer func() {
			if err := sr.Close(); err != nil {
				level.Warn(w.logger).Log("msg", "error while closing the wal segments reader", "err", err)
			}
		}()

		// A corrupted checkpoint is a hard error for now and requires user
		// intervention. There's likely little data that can be recovered anyway.
		if err := w.loadWAL(wlog.NewReader(sr)); err != nil {
			return errors.Wrap(err, "backfill checkpoint")
		}
		startFrom++
		level.Info(w.logger).Log("msg", "WAL checkpoint loaded")
	}

	// Find the last segment.
	_, last, err := wlog.Segments(w.wal.Dir())
	if err != nil {
		return errors.Wrap(err, "finding WAL segments")
	}

	// Backfill segments from the most recent checkpoint onwards.
	for i := startFrom; i <= last; i++ {
		s, err := wlog.OpenReadSegment(wlog.SegmentName(w.wal.Dir(), i))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("open WAL segment: %d", i))
		}

		sr := wlog.NewSegmentBufReader(s)
		err = w.loadWAL(wlog.NewReader(sr))
		if err := sr.Close(); err != nil {
			level.Warn(w.logger).Log("msg", "error while closing the wal segments reader", "err", err)
		}
		if err != nil {
			return err
		}
		level.Info(w.logger).Log("msg", "WAL segment loaded", "segment", i, "maxSegment", last)
	}

	return nil
}

func (w *Storage) loadWAL(r *wlog.Reader) (err error) {
	var (
		dec record.Decoder
	)

	var (
		decoded    = make(chan interface{}, 10)
		errCh      = make(chan error, 1)
		seriesPool = sync.Pool{
			New: func() interface{} {
				return []record.RefSeries{}
			},
		}
		samplesPool = sync.Pool{
			New: func() interface{} {
				return []record.RefSample{}
			},
		}
	)

	go func() {
		defer close(decoded)
		for r.Next() {
			rec := r.Record()
			switch dec.Type(rec) {
			case record.Series:
				series := seriesPool.Get().([]record.RefSeries)[:0]
				series, err = dec.Series(rec, series)
				if err != nil {
					errCh <- &wlog.CorruptionErr{
						Err:     errors.Wrap(err, "decode series"),
						Segment: r.Segment(),
						Offset:  r.Offset(),
					}
					return
				}
				decoded <- series
			case record.Samples:
				samples := samplesPool.Get().([]record.RefSample)[:0]
				samples, err = dec.Samples(rec, samples)
				if err != nil {
					errCh <- &wlog.CorruptionErr{
						Err:     errors.Wrap(err, "decode samples"),
						Segment: r.Segment(),
						Offset:  r.Offset(),
					}
				}
				decoded <- samples
			case record.Tombstones:
				// We don't care about tombstones
				continue
			default:
				errCh <- &wlog.CorruptionErr{
					Err:     errors.Errorf("invalid record type %v", dec.Type(rec)),
					Segment: r.Segment(),
					Offset:  r.Offset(),
				}
				return
			}
		}
	}()

	for d := range decoded {
		switch v := d.(type) {
		case []record.RefSeries:
			for _, s := range v {
				// If this is a new series, create it in memory without a timestamp.
				// If we read in a sample for it, we'll use the timestamp of the latest
				// sample. Otherwise, the series is stale and will be deleted once
				// the truncation is performed.
				if w.series.getByID(s.Ref) == nil {
					series := &memSeries{ref: s.Ref, hash: s.Labels.Hash(), lastTs: 0}

					// Store both the series and the labels so the appender's Add function
					// can look up this series later. We only want to store the labels for
					// replayed series and delete the entry for the after it's read to save
					// on memory usage.
					w.series.set(series)
					w.series.saveLabels(series.hash, series, s.Labels)

					w.metrics.numActiveSeries.Inc()
					w.metrics.totalCreatedSeries.Inc()

					w.mtx.Lock()
					if w.nextRef <= s.Ref {
						w.nextRef = s.Ref + 1
					}
					w.mtx.Unlock()
				}
			}

			//nolint:staticcheck
			seriesPool.Put(v)
		case []record.RefSample:
			for _, s := range v {
				// Update the lastTs for the series based
				series := w.series.getByID(s.Ref)
				if series == nil {
					level.Warn(w.logger).Log("msg", "found sample referencing non-existing series, skipping")
					continue
				}

				series.Lock()
				if s.T > series.lastTs {
					series.lastTs = s.T
				}
				series.Unlock()
			}

			//nolint:staticcheck
			samplesPool.Put(v)
		default:
			panic(fmt.Errorf("unexpected decoded type: %T", d))
		}
	}

	select {
	case err := <-errCh:
		return err
	default:
	}

	if r.Err() != nil {
		return errors.Wrap(r.Err(), "read records")
	}

	return nil
}

// Directory returns the path where the WAL storage is held.
func (w *Storage) Directory() string {
	return w.path
}

// Appender returns a new appender against the storage.
// groupKey is passed from the manager in pkg/ruler/manager.go and is used to track the series IDs for a given groupKey
func (w *Storage) Appender(groupKey string) storage.Appender {
	if groupKey == "" {
		return w.appenderPool.Get().(storage.Appender)
	}
	return &remoteAppender{
		s:           w,
		groupKey:    groupKey,
		walAppender: w.appenderPool.Get().(storage.Appender),
	}
}

func (w *Storage) MarkSeriesStale(groupKey string) error {
	w.groupToRefMtx.Lock()
	ref, exists := w.groupToRef[groupKey]
	if !exists {
		return fmt.Errorf("no series found for groupkey")
	}
	w.groupToRefMtx.Unlock()

	ts := timestamp.FromTime(time.Now())
	app := w.Appender(groupKey)
	_, err := app.Append(storage.SeriesRef(ref), labels.Labels{}, ts, math.Float64frombits(value.StaleNaN))
	if err != nil {
		return err
	}
	return app.Commit()
}

// StartTime always returns 0, nil. It is implemented for compatibility with
// Prometheus
func (*Storage) StartTime() (int64, error) {
	return 0, nil
}

// Truncate removes all data from the WAL prior to the timestamp specified by
// mint.
func (w *Storage) Truncate(mint int64) error {
	start := time.Now()

	// Garbage collect series that haven't received an update since mint.
	w.gc(mint)
	level.Info(w.logger).Log("msg", "series GC completed", "duration", time.Since(start))

	first, last, err := wlog.Segments(w.wal.Dir())
	if err != nil {
		return errors.Wrap(err, "get segment range")
	}

	// Start a new segment, so low ingestion volume instance don't have more WAL
	// than needed.
	_, err = w.wal.NextSegment()
	if err != nil {
		return errors.Wrap(err, "next segment")
	}

	last-- // Never consider last segment for checkpoint.
	if last < 0 {
		return nil // no segments yet.
	}

	// The lower two thirds of segments should contain mostly obsolete samples.
	// If we have less than two segments, it's not worth checkpointing yet.
	last = first + (last-first)*2/3
	if last <= first {
		return nil
	}

	keep := func(id chunks.HeadSeriesRef) bool {
		if w.series.getByID(id) != nil {
			return true
		}

		w.deletedMtx.Lock()
		_, ok := w.deleted[id]
		w.deletedMtx.Unlock()
		return ok
	}
	if _, err = wlog.Checkpoint(w.logger, w.wal, first, last, keep, mint); err != nil {
		return errors.Wrap(err, "create checkpoint")
	}
	if err := w.wal.Truncate(last + 1); err != nil {
		// If truncating fails, we'll just try again at the next checkpoint.
		// Leftover segments will just be ignored in the future if there's a checkpoint
		// that supersedes them.
		level.Error(w.logger).Log("msg", "truncating segments failed", "err", err)
	}

	// The checkpoint is written and segments before it is truncated, so we no
	// longer need to track deleted series that are before it.
	w.deletedMtx.Lock()
	for ref, segment := range w.deleted {
		if segment < first {
			delete(w.deleted, ref)
			w.metrics.totalRemovedSeries.Inc()
		}
	}
	w.metrics.numDeletedSeries.Set(float64(len(w.deleted)))
	w.deletedMtx.Unlock()

	if err := wlog.DeleteCheckpoints(w.wal.Dir(), last); err != nil {
		// Leftover old checkpoints do not cause problems down the line beyond
		// occupying disk space.
		// They will just be ignored since a higher checkpoint exists.
		level.Error(w.logger).Log("msg", "delete old checkpoints", "err", err)
	}

	level.Info(w.logger).Log("msg", "WAL checkpoint complete",
		"first", first, "last", last, "duration", time.Since(start))
	return nil
}

// gc removes data before the minimum timestamp from the head.
func (w *Storage) gc(mint int64) {
	deleted := w.series.gc(mint)
	w.metrics.numActiveSeries.Sub(float64(len(deleted)))

	_, last, _ := wlog.Segments(w.wal.Dir())
	w.deletedMtx.Lock()
	defer w.deletedMtx.Unlock()

	// We want to keep series records for any newly deleted series
	// until we've passed the last recorded segment. The WAL will
	// still contain samples records with all of the ref IDs until
	// the segment's samples has been deleted from the checkpoint.
	//
	// If the series weren't kept on startup when the WAL was replied,
	// the samples wouldn't be able to be used since there wouldn't
	// be any labels for that ref ID.
	for ref := range deleted {
		w.deleted[chunks.HeadSeriesRef(ref)] = last
	}

	w.metrics.numDeletedSeries.Set(float64(len(w.deleted)))
}

// WriteStalenessMarkers appends a staleness sample for all active series.
func (w *Storage) WriteStalenessMarkers(remoteTsFunc func() int64) error {
	var lastErr error
	var lastTs int64

	app := w.Appender("")
	it := w.series.iterator()
	for series := range it.Channel() {
		var (
			ref = series.ref
		)

		ts := timestamp.FromTime(time.Now())
		_, err := app.Append(storage.SeriesRef(ref), nil, ts, math.Float64frombits(value.StaleNaN))
		if err != nil {
			lastErr = err
		}

		// Remove millisecond precision; the remote write timestamp we get
		// only has second precision.
		lastTs = (ts / 1000) * 1000
	}

	if lastErr == nil {
		if err := app.Commit(); err != nil {
			return fmt.Errorf("failed to commit staleness markers: %w", err)
		}

		// Wait for remote write to write the lastTs, but give up after 1m
		level.Info(w.logger).Log("msg", "waiting for remote write to write staleness markers...")

		stopCh := time.After(1 * time.Minute)
		start := time.Now()

	Outer:
		for {
			select {
			case <-stopCh:
				level.Error(w.logger).Log("msg", "timed out waiting for staleness markers to be written")
				break Outer
			default:
				writtenTs := remoteTsFunc()
				if writtenTs >= lastTs {
					duration := time.Since(start)
					level.Info(w.logger).Log("msg", "remote write wrote staleness markers", "duration", duration)
					break Outer
				}

				level.Info(w.logger).Log("msg",
					"remote write hasn't written staleness markers yet",
					"remoteTs",
					writtenTs,
					"lastTs",
					lastTs)

				// Wait a bit before reading again
				time.Sleep(5 * time.Second)
			}
		}
	}

	return lastErr
}

// Close closes the storage and all its underlying resources.
func (w *Storage) Close() error {
	if w.metrics != nil {
		w.metrics.Unregister()
	}
	return w.wal.Close()
}

// remoteAppender is a wrapper around appender that can mark a particular series stale
type remoteAppender struct {
	s           *Storage
	groupKey    string
	walAppender storage.Appender
}

func (a *remoteAppender) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	ref, err := a.walAppender.Append(ref, l, t, v)
	a.s.groupToRefMtx.Lock()
	a.s.groupToRef[a.groupKey] = chunks.HeadSeriesRef(ref)
	a.s.groupToRefMtx.Unlock()
	return ref, err
}

func (a *remoteAppender) AppendExemplar(_ storage.SeriesRef, _ labels.Labels, _ exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, errors.New("exemplars are unsupported")
}

func (a *remoteAppender) UpdateMetadata(_ storage.SeriesRef, _ labels.Labels, _ metadata.Metadata) (storage.SeriesRef, error) {
	return 0, errors.New("metadata updates are unsupported")
}

func (a *remoteAppender) AppendHistogram(_ storage.SeriesRef, _ labels.Labels, _ int64, _ *histogram.Histogram, _ *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, errors.New("histograms are unsupported")
}

func (a *remoteAppender) Commit() error {
	return a.walAppender.Commit()
}

func (a *remoteAppender) Rollback() error {
	return a.walAppender.Rollback()
}

type appender struct {
	w       *Storage
	series  []record.RefSeries
	samples []record.RefSample
}

// Make sure appender implements storage.Appender
var _ storage.Appender = &appender{}

func (a *appender) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	series := a.w.series.getByID(chunks.HeadSeriesRef(ref))
	if series == nil {
		hash := l.Hash()
		a.w.mtx.Lock()
		ref = storage.SeriesRef(a.w.nextRef)
		a.w.nextRef++
		a.w.mtx.Unlock()
		series = &memSeries{ref: chunks.HeadSeriesRef(ref), hash: hash}
		series.updateTs(t)
		a.series = append(a.series, record.RefSeries{
			Ref:    chunks.HeadSeriesRef(ref),
			Labels: l,
		})

		a.w.series.set(series)
		a.w.series.saveLabels(series.hash, series, l)

		a.w.metrics.numActiveSeries.Inc()
		a.w.metrics.totalCreatedSeries.Inc()
		a.w.metrics.totalAppendedSamples.Inc()
	}
	series.Lock()
	defer series.Unlock()

	// Update last recorded timestamp. Used by Storage.gc to determine if a
	// series is dead.
	series.updateTs(t)

	a.samples = append(a.samples, record.RefSample{
		Ref: chunks.HeadSeriesRef(ref),
		T:   t,
		V:   v,
	})
	a.w.metrics.totalAppendedSamples.Inc()

	return storage.SeriesRef(series.ref), nil
}

func (a *appender) AppendExemplar(_ storage.SeriesRef, _ labels.Labels, _ exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, errors.New("exemplars are unsupported")
}

func (a *appender) AppendHistogram(_ storage.SeriesRef, _ labels.Labels, _ int64, _ *histogram.Histogram, _ *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, errors.New("histograms are unsupported")
}

func (a *appender) UpdateMetadata(_ storage.SeriesRef, _ labels.Labels, _ metadata.Metadata) (storage.SeriesRef, error) {
	return 0, errors.New("metadata updates are unsupported")
}

// Commit submits the collected samples and purges the batch.
func (a *appender) Commit() error {
	var encoder record.Encoder
	buf := a.w.bufPool.Get().([]byte)

	if len(a.series) > 0 {
		buf = encoder.Series(a.series, buf)
		if err := a.w.wal.Log(buf); err != nil {
			return err
		}
		buf = buf[:0]
	}

	if len(a.samples) > 0 {
		buf = encoder.Samples(a.samples, buf)
		if err := a.w.wal.Log(buf); err != nil {
			return err
		}
		buf = buf[:0]
	}

	//nolint:staticcheck
	a.w.bufPool.Put(buf)

	for _, sample := range a.samples {
		series := a.w.series.getByID(sample.Ref)
		if series != nil {
			series.Lock()
			series.pendingCommit = false
			series.Unlock()
		}
	}

	return a.Rollback()
}

func (a *appender) Rollback() error {
	a.series = a.series[:0]
	a.samples = a.samples[:0]
	a.w.appenderPool.Put(a)
	return nil
}
