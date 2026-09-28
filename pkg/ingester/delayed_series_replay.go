// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
)

// delayedSeriesReplayer promotes delayed series back to the TSDB head. When a delayed_series rule is
// retired, the ingester re-reads its own partition over the replay window and appends the samples of the
// rule's series, which it skipped while the rule applied. Queriers keep reading the older delayed interval
// from blocks; the replay covers the recent interval that store-gateways can't serve yet.
type delayedSeriesReplayer struct {
	kafkaCfg  ingest.KafkaConfig
	partition int32
	window    time.Duration
	limits    *validation.Overrides
	push      func(ctx context.Context, req *mimirpb.WriteRequest) error
	logger    log.Logger

	mu       sync.Mutex
	replayed map[replayKey]struct{}
	jobs     chan replayJob

	replays        *prometheus.CounterVec
	replaySamples  prometheus.Counter
	replayRecords  prometheus.Counter
	replayDuration prometheus.Histogram
	inProgress     prometheus.Gauge
}

type replayKey struct {
	userID    string
	match     string
	retiredAt int64
}

type replayJob struct {
	userID string
	rule   validation.DelayedSeriesRule
}

func newDelayedSeriesReplayer(kafkaCfg ingest.KafkaConfig, partition int32, window time.Duration, limits *validation.Overrides, push func(context.Context, *mimirpb.WriteRequest) error, logger log.Logger, reg prometheus.Registerer) *delayedSeriesReplayer {
	return &delayedSeriesReplayer{
		kafkaCfg:  kafkaCfg,
		partition: partition,
		window:    window,
		limits:    limits,
		push:      push,
		logger:    log.With(logger, "component", "delayed_series_replayer"),
		replayed:  map[replayKey]struct{}{},
		// Promotions run one at a time: each one re-reads the whole partition.
		jobs: make(chan replayJob, 100),

		replays: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_delayed_series_replays_total",
			Help: "Total number of replays of promoted delayed series, by outcome.",
		}, []string{"outcome"}),
		replaySamples: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_delayed_series_replay_samples_total",
			Help: "Total number of samples of promoted delayed series appended by replays.",
		}),
		replayRecords: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_delayed_series_replay_records_total",
			Help: "Total number of Kafka records read by replays of promoted delayed series.",
		}),
		replayDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_delayed_series_replay_duration_seconds",
			Help:    "Time taken to replay promoted delayed series, from the start of the read to reaching the end offset.",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 15),
		}),
		inProgress: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_ingester_delayed_series_replay_in_progress",
			Help: "1 while a replay of promoted delayed series is running.",
		}),
	}
}

// discover queues a replay for each rule retired within the replay window that hasn't been replayed yet.
func (r *delayedSeriesReplayer) discover(userIDs []string, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for key := range r.replayed {
		if now.Sub(time.UnixMilli(key.retiredAt)) > r.window {
			delete(r.replayed, key)
		}
	}

	for _, userID := range userIDs {
		for _, rule := range r.limits.DelayedSeries(userID) {
			if rule.RetiredAt.IsZero() || now.Sub(rule.RetiredAt) > r.window {
				continue
			}
			key := replayKey{userID: userID, match: rule.Match, retiredAt: rule.RetiredAt.UnixMilli()}
			if _, ok := r.replayed[key]; ok {
				continue
			}
			select {
			case r.jobs <- replayJob{userID: userID, rule: rule}:
				r.replayed[key] = struct{}{}
				level.Info(r.logger).Log("msg", "queued replay of promoted delayed series", "user", userID, "match", rule.Match, "retired_at", rule.RetiredAt)
			default:
				level.Warn(r.logger).Log("msg", "replay queue full, promotion will be retried", "user", userID, "match", rule.Match)
			}
		}
	}
}

// run replays queued promotions until ctx is done.
func (r *delayedSeriesReplayer) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-r.jobs:
			if err := r.replay(ctx, job); err != nil {
				r.replays.WithLabelValues("failed").Inc()
				level.Error(r.logger).Log("msg", "replay of promoted delayed series failed", "user", job.userID, "match", job.rule.Match, "err", err)
				continue
			}
			r.replays.WithLabelValues("succeeded").Inc()
		}
	}
}

func (r *delayedSeriesReplayer) replay(ctx context.Context, job replayJob) error {
	r.inProgress.Set(1)
	defer r.inProgress.Set(0)
	start := time.Now()

	client, err := ingest.NewKafkaReaderClient(r.kafkaCfg, nil, r.logger)
	if err != nil {
		return errors.Wrap(err, "creating Kafka client")
	}
	defer client.Close()

	from := job.rule.RetiredAt.Add(-r.window)
	startOffset, endOffset, err := r.offsets(ctx, kadm.NewClient(client), from)
	if err != nil {
		return err
	}
	if endOffset <= startOffset {
		level.Info(r.logger).Log("msg", "nothing to replay for promoted delayed series", "user", job.userID, "match", job.rule.Match)
		return nil
	}
	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{r.kafkaCfg.Topic: {r.partition: kgo.NewOffset().At(startOffset)}})

	var records, tenantRecords, samples int64
	pushCtx := user.InjectOrgID(ctx, job.userID)
	for done := false; !done; {
		fetches := client.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("fetching records: %w", errs[0].Err)
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			if done || rec.Offset >= endOffset {
				done = true
				return
			}
			if rec.Offset == endOffset-1 {
				done = true
			}
			records++
			if string(rec.Key) != job.userID {
				return
			}
			tenantRecords++
			samples += r.pushPromoted(pushCtx, rec, &job.rule)
		})
	}

	r.replayRecords.Add(float64(records))
	r.replaySamples.Add(float64(samples))
	r.replayDuration.Observe(time.Since(start).Seconds())
	level.Info(r.logger).Log("msg", "replayed promoted delayed series", "user", job.userID, "match", job.rule.Match,
		"from", from, "start_offset", startOffset, "end_offset", endOffset, "records", records, "tenant_records", tenantRecords,
		"samples", samples, "duration", time.Since(start))
	return nil
}

// offsets returns the partition's first offset at or after from, and its current end offset.
func (r *delayedSeriesReplayer) offsets(ctx context.Context, adm *kadm.Client, from time.Time) (int64, int64, error) {
	starts, err := adm.ListOffsetsAfterMilli(ctx, from.UnixMilli(), r.kafkaCfg.Topic)
	if err != nil {
		return 0, 0, errors.Wrap(err, "listing start offsets")
	}
	ends, err := adm.ListEndOffsets(ctx, r.kafkaCfg.Topic)
	if err != nil {
		return 0, 0, errors.Wrap(err, "listing end offsets")
	}
	s, ok := starts.Lookup(r.kafkaCfg.Topic, r.partition)
	if !ok || s.Err != nil {
		return 0, 0, fmt.Errorf("no start offset for partition %d: %v", r.partition, s.Err)
	}
	e, ok := ends.Lookup(r.kafkaCfg.Topic, r.partition)
	if !ok || e.Err != nil {
		return 0, 0, fmt.Errorf("no end offset for partition %d: %v", r.partition, e.Err)
	}
	return s.Offset, e.Offset, nil
}

// pushPromoted appends the record's series selected by the rule and returns the number of samples pushed.
// Samples already in the head are rejected as duplicates, which is expected.
func (r *delayedSeriesReplayer) pushPromoted(ctx context.Context, rec *kgo.Record, rule *validation.DelayedSeriesRule) int64 {
	req := &mimirpb.PreallocWriteRequest{}
	if err := ingest.DeserializeRecordContent(rec.Value, req, ingest.ParseRecordVersion(rec)); err != nil {
		level.Warn(r.logger).Log("msg", "skipping undecodable record during replay", "offset", rec.Offset, "err", err)
		return 0
	}

	var drop []int
	var samples int64
	for idx, ts := range req.Timeseries {
		if rule.Selects(ts.Labels) {
			samples += int64(len(ts.Samples) + len(ts.Histograms))
			continue
		}
		drop = append(drop, idx)
	}
	for _, idx := range drop {
		mimirpb.ReusePreallocTimeseries(&req.Timeseries[idx])
	}
	req.Timeseries = util.RemoveSliceIndexes(req.Timeseries, drop)
	req.Metadata = nil

	if len(req.Timeseries) == 0 {
		mimirpb.ReuseSlice(req.Timeseries)
		return 0
	}
	if err := r.push(ctx, &req.WriteRequest); err != nil {
		level.Debug(r.logger).Log("msg", "replayed samples partially rejected", "offset", rec.Offset, "err", err)
	}
	return samples
}

// delayedSeriesUsers returns the tenants that may have retired delayed_series rules: those with a TSDB and
// those whose series are all delayed, which have none.
func (i *Ingester) delayedSeriesUsers() []string {
	seen := map[string]struct{}{}
	var out []string
	for _, userID := range append(i.getTSDBUsers(), i.delayedSeries.userIDs()...) {
		if _, ok := seen[userID]; !ok {
			seen[userID] = struct{}{}
			out = append(out, userID)
		}
	}
	return out
}
