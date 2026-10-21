// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

const (
	topic = "ingest"
	// Series per write request, like a remote write client's batches.
	requestSeries = 2000
	// Largest record, as the distributor's.
	maxRecordBytes = 15 << 20
)

// feed produces the world's samples to Kafka, one second of scrapes at a time, as distributors would: first the
// backfill as fast as it goes, then each second as it passes.
type feed struct {
	world   *world
	cluster *kfake.Cluster
	client  *kgo.Client
	// Last produced offset, and when each second of samples was all produced.
	lastOffset atomic.Int64
	producedMu sync.Mutex
	produced   []producedSecond
	samples    atomic.Int64
	// Seconds produced so far: everything with a timestamp before it, but the late samples, is in Kafka.
	through atomic.Int64

	// Records produced and not yet acknowledged, and the first error of one.
	pending sync.WaitGroup
	errMu   sync.Mutex
	err     error

	late lateQueue
	// Fraction of samples written late, and of the backdated tenants' also written up to their window late, as
	// thresholds of a slot's hash.
	lateThreshold      uint64
	backdatedThreshold uint64
	exemplarMod        uint64
	leValues           []string
}

type producedSecond struct {
	second int64
	offset int64
	at     time.Time
}

const producerBatchMaxBytes = 16 << 20

func newFeed(w *world, dataDir string, port int) (*feed, error) {
	opts := []kfake.Opt{kfake.NumBrokers(1), kfake.SeedTopics(1, topic),
		// As large as the client's batches, which the feed's seconds of many tenants fill: the default 1 MiB refused them.
		kfake.BrokerConfigs(map[string]string{"message.max.bytes": strconv.Itoa(producerBatchMaxBytes)})}
	if port > 0 {
		opts = append(opts, kfake.Ports(port))
	}
	if dataDir != "" {
		opts = append(opts, kfake.DataDir(dataDir))
	}
	cluster, err := kfake.NewCluster(opts...)
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerBatchMaxBytes(producerBatchMaxBytes),
		kgo.MaxBufferedRecords(100_000),
		kgo.ProducerLinger(5*time.Millisecond),
	)
	if err != nil {
		cluster.Close()
		return nil, err
	}
	f := &feed{world: w, cluster: cluster, client: client}
	f.lastOffset.Store(-1)
	f.lateThreshold = uint64(w.profile.LateShare * math.MaxUint64)
	f.backdatedThreshold = uint64(w.profile.BackdatedShare * math.MaxUint64)
	if w.profile.ExemplarShare > 0 {
		f.exemplarMod = uint64(math.Round(1 / w.profile.ExemplarShare))
	}
	for i := range max(1, w.profile.ClassicBuckets-1) {
		f.leValues = append(f.leValues, strconv.FormatFloat(0.005*math.Pow(2.5, float64(i)), 'g', 4, 64))
	}
	f.leValues = append(f.leValues, "+Inf")
	return f, nil
}

func (f *feed) address() string { return f.cluster.ListenAddrs()[0] }

func (f *feed) close() {
	f.client.Close()
	f.cluster.Close()
}

// produceSeconds produces the samples of the seconds [from, to), as fast as Kafka takes them.
func (f *feed) produceSeconds(ctx context.Context, from, to int64) error {
	// The client keeps the partition's records in order, so a second needn't wait for the previous one's: waiting for
	// each second's acknowledgements was most of the backfill's time.
	for s := from; s < to; s += 1000 {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := f.produceSecond(ctx, s, false); err != nil {
			return err
		}
	}
	return f.acknowledged(to - 1000)
}

// live produces each second once it passed, until ctx is done.
func (f *feed) live(ctx context.Context, from int64) error {
	next := from
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		now := time.Now().UnixMilli()
		for next+1000 <= now {
			if err := f.produceSecond(ctx, next, true); err != nil {
				return err
			}
			next += 1000
		}
	}
}

// produceSecond produces the second's samples, and with wait, waits for all produced so far to be acknowledged, to
// know the offset the second ends at.
func (f *feed) produceSecond(ctx context.Context, s int64, wait bool) error {
	w := f.world
	produce := func(tenant string, series []mimirpb.PreallocTimeseries) {
		req := &mimirpb.WriteRequest{Timeseries: series, Source: mimirpb.API}
		records, _, err := ingest.RecordSerializerFromVersion(2).ToRecords(topic, 0, tenant, req, maxRecordBytes)
		if err != nil {
			f.fail(err)
			return
		}
		for _, record := range records {
			f.pending.Add(1)
			f.client.Produce(ctx, record, func(r *kgo.Record, err error) {
				defer f.pending.Done()
				if err != nil {
					f.fail(err)
					return
				}
				for {
					last := f.lastOffset.Load()
					if r.Offset <= last || f.lastOffset.CompareAndSwap(last, r.Offset) {
						break
					}
				}
			})
		}
	}
	// Tenants are independent: each tenant's series are generated and produced in order by one goroutine, and the
	// second ends once all are, so no sample comes after a later one of its series.
	var count atomic.Int64
	tenants := make(chan *tenantModel)
	var workers sync.WaitGroup
	for range max(1, runtime.GOMAXPROCS(0)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for t := range tenants {
				var batch []mimirpb.PreallocTimeseries
				samples := 0
				add := func(ts *mimirpb.TimeSeries) {
					samples += len(ts.Samples) + len(ts.Histograms)
					batch = append(batch, mimirpb.PreallocTimeseries{TimeSeries: ts})
					if len(batch) >= requestSeries {
						produce(t.id, batch)
						batch = nil
					}
				}
				for _, m := range t.metrics {
					m.slotsAt(s, func(slot int, at int64) {
						f.scrape(m, slot, at, add)
					})
				}
				for _, ts := range f.late.due(t.id, s+1000) {
					add(ts)
				}
				if len(batch) > 0 {
					produce(t.id, batch)
				}
				count.Add(int64(samples))
			}
		}()
	}
	for _, t := range w.tenants {
		tenants <- t
	}
	close(tenants)
	workers.Wait()
	f.samples.Add(count.Load())
	if !wait {
		return f.failure()
	}
	return f.acknowledged(s)
}

// acknowledged waits for every record produced so far, and records that they end second s.
func (f *feed) acknowledged(s int64) error {
	f.pending.Wait()
	if err := f.failure(); err != nil {
		return err
	}
	f.through.Store(s + 1000)
	f.producedMu.Lock()
	f.produced = append(f.produced, producedSecond{second: s, offset: f.lastOffset.Load(), at: time.Now()})
	if len(f.produced) > 7200 {
		f.produced = f.produced[len(f.produced)-3600:]
	}
	f.producedMu.Unlock()
	return nil
}

func (f *feed) fail(err error) {
	f.errMu.Lock()
	defer f.errMu.Unlock()
	if f.err == nil {
		f.err = err
	}
}

func (f *feed) failure() error {
	f.errMu.Lock()
	defer f.errMu.Unlock()
	return f.err
}

// offsetProducedBefore is the last offset produced at least d ago, or -1.
func (f *feed) offsetProducedBefore(d time.Duration) int64 {
	f.producedMu.Lock()
	defer f.producedMu.Unlock()
	cutoff := time.Now().Add(-d)
	for i := len(f.produced) - 1; i >= 0; i-- {
		if f.produced[i].at.Before(cutoff) {
			return f.produced[i].offset
		}
	}
	return -1
}

// scrape adds the series of the slot's scrape at t: its samples, the stale markers of its previous generation's
// series, the late ones for later, and a backdated tenant's scrape again, days late.
func (f *feed) scrape(m *metricModel, slot int, t int64, add func(*mimirpb.TimeSeries)) {
	w := f.world
	generation := w.generation(m, slot, t)
	start := w.generationStart(m, slot, generation)
	if generation > 0 && t-start < m.intervalMs {
		// The previous series stopped: the scraper writes stale markers for them.
		for _, labels := range f.familyLabels(m, slot, generation-1) {
			add(&mimirpb.TimeSeries{Labels: labels, Samples: []mimirpb.Sample{{TimestampMs: t, Value: staleNaN}}})
		}
	}
	late := m.hash64(slot, uint64(t)) < f.lateThreshold
	emit := add
	if late {
		delay := m.intervalMs + int64(m.hash64(slot, uint64(t)^4)%uint64(max(1, int64(w.profile.LateMaxSeconds)*1000-m.intervalMs)))
		emit = func(ts *mimirpb.TimeSeries) { f.late.push(m.tenant.id, t+delay, ts) }
	}
	labels := f.familyLabels(m, slot, generation)
	f.emitScrape(m, slot, t, start, labels, true, emit)
	// The scrape again, days late at most, of a timestamp half a second off the scrapes', which the series don't
	// have yet: with the value it would have then had, as old as its series is now.
	if m.tenant.backdated && m.hash64(slot, uint64(t)^7) < f.backdatedThreshold {
		span := max(1, (w.backdatedWindowMs-2*3_600_000)/1000)
		delay := 3_600_000 + int64(m.hash64(slot, uint64(t)^8)%uint64(span))*1000 + 500
		// Labels of its own: serializing a request may give its series' back to a pool.
		f.emitScrape(m, slot, t-delay, start-delay, f.familyLabels(m, slot, generation), false, add)
	}
}

// emitScrape emits the series of the slot's scrape at t of its generation that started at start, with labels, and
// with its exemplars if any.
func (f *feed) emitScrape(m *metricModel, slot int, t, start int64, labels [][]mimirpb.LabelAdapter, exemplars bool, emit func(*mimirpb.TimeSeries)) {
	w := f.world
	value := m.sample(slot, t, start)
	switch m.kind {
	case kindNativeHistogram:
		emit(&mimirpb.TimeSeries{Labels: labels[0], Histograms: []mimirpb.Histogram{m.nativeHistogram(slot, t, start, w.profile.NativeBuckets)}})
	case kindClassicHistogram:
		for i, le := range f.leValues {
			cdf := 1 - math.Exp(-float64(i+1)/float64(len(f.leValues))*4)
			if le == "+Inf" {
				cdf = 1
			}
			emit(&mimirpb.TimeSeries{Labels: labels[i], Samples: []mimirpb.Sample{{TimestampMs: t, Value: math.Floor(value * cdf)}}})
		}
		emit(&mimirpb.TimeSeries{Labels: labels[len(f.leValues)], Samples: []mimirpb.Sample{{TimestampMs: t, Value: value * 0.25}}})
		emit(&mimirpb.TimeSeries{Labels: labels[len(f.leValues)+1], Samples: []mimirpb.Sample{{TimestampMs: t, Value: value}}})
	default:
		ts := &mimirpb.TimeSeries{Labels: labels[0], Samples: []mimirpb.Sample{{TimestampMs: t, Value: value}}}
		if exemplars && f.exemplarMod > 0 && m.kind == kindCounter && m.hash64(slot, uint64(t)^5)%f.exemplarMod == 0 {
			ts.Exemplars = []mimirpb.Exemplar{{TimestampMs: t, Value: 1, Labels: []mimirpb.LabelAdapter{{Name: "trace_id", Value: strconv.FormatUint(m.hash64(slot, uint64(t)^6), 16)}}}}
		}
		emit(ts)
	}
}

// familyLabels returns the labels of the series of the slot's generation: one, or a classic histogram's buckets, sum
// and count.
func (f *feed) familyLabels(m *metricModel, slot int, generation int64) [][]mimirpb.LabelAdapter {
	if m.kind != kindClassicHistogram {
		return [][]mimirpb.LabelAdapter{m.seriesLabels(slot, generation, "")}
	}
	out := make([][]mimirpb.LabelAdapter, 0, len(f.leValues)+2)
	for _, le := range f.leValues {
		out = append(out, m.seriesLabels(slot, generation, "_bucket", mimirpb.LabelAdapter{Name: "le", Value: le}))
	}
	return append(out, m.seriesLabels(slot, generation, "_sum"), m.seriesLabels(slot, generation, "_count"))
}

// lateQueue holds the samples written late, by when they are written.
type lateQueue struct {
	mu    sync.Mutex
	items lateHeap
}

type lateItem struct {
	tenant string
	at     int64
	ts     *mimirpb.TimeSeries
}

type lateHeap []lateItem

func (h lateHeap) Len() int           { return len(h) }
func (h lateHeap) Less(a, b int) bool { return h[a].at < h[b].at }
func (h lateHeap) Swap(a, b int)      { h[a], h[b] = h[b], h[a] }
func (h *lateHeap) Push(x any)        { *h = append(*h, x.(lateItem)) }
func (h *lateHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	*h = old[:len(old)-1]
	return item
}

func (q *lateQueue) push(tenant string, at int64, ts *mimirpb.TimeSeries) {
	q.mu.Lock()
	defer q.mu.Unlock()
	heap.Push(&q.items, lateItem{tenant, at, ts})
}

// due returns the tenant's late series to write before the time, keeping the other tenants'.
func (q *lateQueue) due(tenant string, before int64) []*mimirpb.TimeSeries {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []*mimirpb.TimeSeries
	var keep []lateItem
	for q.items.Len() > 0 && q.items[0].at < before {
		item := heap.Pop(&q.items).(lateItem)
		if item.tenant == tenant {
			out = append(out, item.ts)
		} else {
			keep = append(keep, item)
		}
	}
	for _, item := range keep {
		heap.Push(&q.items, item)
	}
	return out
}

func (f *feed) String() string {
	return fmt.Sprintf("offset=%d samples=%d", f.lastOffset.Load(), f.samples.Load())
}
