// SPDX-License-Identifier: AGPL-3.0-only

// freshness-replay-bench measures the cost of promoting delayed series: reading one ingest-storage
// partition from Kafka over a past time window, decoding every record the way ingesters do, and keeping
// only the series matching the promoted selector. With -append, the kept series are also appended to a
// throwaway TSDB head, which is what a promotion adds on top of the read.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"syscall"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

type config struct {
	kafka     string
	topic     string
	partition int
	window    time.Duration
	endAgo    time.Duration
	tenant    string
	selector  string
	decode    bool
	appendTo  bool
}

type stats struct {
	records, bytes                  int64
	tenantRecords                   int64
	seriesDecoded, samplesDecoded   int64
	seriesMatched, samplesMatched   int64
	uniqueMatched                   map[uint64]struct{}
	firstRecordTime, lastRecordTime time.Time
}

func main() {
	var cfg config
	flag.StringVar(&cfg.kafka, "kafka", "localhost:29092", "Kafka bootstrap address.")
	flag.StringVar(&cfg.topic, "topic", "mimir-ingest", "Ingest-storage topic.")
	flag.IntVar(&cfg.partition, "partition", -1, "Partition to replay. -1 picks the partition with the most records in the window.")
	flag.DurationVar(&cfg.window, "window", time.Hour, "Length of the tail to replay.")
	flag.DurationVar(&cfg.endAgo, "end-ago", 0, "Replay the window ending this long ago, instead of ending now.")
	flag.StringVar(&cfg.tenant, "tenant", "delayed", "Tenant whose series are promoted. Other tenants' records are still read and decoded.")
	flag.StringVar(&cfg.selector, "selector", `{__name__="grafana_http_request_duration_seconds_count"}`, "Promoted series selector.")
	flag.BoolVar(&cfg.decode, "decode", true, "Decode records. With false, only fetch them, to separate fetch from decode cost.")
	flag.BoolVar(&cfg.appendTo, "append", false, "Append the promoted series to a throwaway TSDB head.")
	flag.Parse()

	matchers, err := promqlext.NewPromQLParser().ParseMetricSelector(cfg.selector)
	if err != nil {
		log.Fatalf("parsing selector: %v", err)
	}

	ctx := context.Background()
	client, err := kgo.NewClient(kgo.SeedBrokers(cfg.kafka), kgo.FetchMaxBytes(100<<20), kgo.FetchMaxPartitionBytes(50<<20))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	end := time.Now().Add(-cfg.endAgo)
	start := end.Add(-cfg.window)
	partition, startOffset, endOffset := pickRange(ctx, kadm.NewClient(client), cfg, start, end)
	log.Printf("replaying partition %d offsets [%d, %d) = %s window ending %s ago", partition, startOffset, endOffset, cfg.window, cfg.endAgo)

	var head *tsdb.DB
	if cfg.appendTo {
		dir, err := os.MkdirTemp("", "replay-bench-tsdb-")
		if err != nil {
			log.Fatal(err)
		}
		defer os.RemoveAll(dir)
		opts := tsdb.DefaultOptions()
		opts.OutOfOrderTimeWindow = (48 * time.Hour).Milliseconds()
		head, err = tsdb.Open(dir, nil, nil, opts, nil)
		if err != nil {
			log.Fatal(err)
		}
		defer head.Close()
	}

	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{cfg.topic: {partition: kgo.NewOffset().At(startOffset)}})

	st := stats{uniqueMatched: map[uint64]struct{}{}}
	cpuStart := cpuTime()
	wallStart := time.Now()

	for done := false; !done; {
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Fatalf("fetch: %v", errs)
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			if rec.Offset >= endOffset {
				done = true
				return
			}
			st.records++
			st.bytes += int64(len(rec.Value))
			if st.firstRecordTime.IsZero() {
				st.firstRecordTime = rec.Timestamp
			}
			st.lastRecordTime = rec.Timestamp
			if rec.Offset == endOffset-1 {
				done = true
			}
			if cfg.decode {
				process(rec, cfg.tenant, matchers, head, &st)
			}
		})
	}

	wall := time.Since(wallStart)
	cpu := cpuTime() - cpuStart
	report(cfg, partition, st, wall, cpu)
}

// pickRange resolves the window to offsets, on the requested partition or the busiest one.
func pickRange(ctx context.Context, adm *kadm.Client, cfg config, start, end time.Time) (int32, int64, int64) {
	starts, err := adm.ListOffsetsAfterMilli(ctx, start.UnixMilli(), cfg.topic)
	if err != nil {
		log.Fatalf("listing start offsets: %v", err)
	}
	ends, err := adm.ListOffsetsAfterMilli(ctx, end.UnixMilli(), cfg.topic)
	if err != nil {
		log.Fatalf("listing end offsets: %v", err)
	}

	type candidate struct {
		partition  int32
		start, end int64
	}
	var candidates []candidate
	starts.Each(func(o kadm.ListedOffset) {
		e, ok := ends.Lookup(o.Topic, o.Partition)
		if !ok || o.Err != nil || e.Err != nil {
			return
		}
		candidates = append(candidates, candidate{o.Partition, o.Offset, e.Offset})
	})
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].end-candidates[i].start > candidates[j].end-candidates[j].start
	})

	for _, c := range candidates {
		if cfg.partition < 0 || int(c.partition) == cfg.partition {
			if c.end <= c.start {
				log.Fatalf("partition %d has no records in the window", c.partition)
			}
			return c.partition, c.start, c.end
		}
	}
	log.Fatalf("partition %d not found", cfg.partition)
	return 0, 0, 0
}

func process(rec *kgo.Record, tenant string, matchers []*labels.Matcher, head *tsdb.DB, st *stats) {
	req := &mimirpb.PreallocWriteRequest{}
	if err := ingest.DeserializeRecordContent(rec.Value, req, ingest.ParseRecordVersion(rec)); err != nil {
		log.Fatalf("decoding record at offset %d: %v", rec.Offset, err)
	}
	defer mimirpb.ReuseSlice(req.Timeseries)

	isTenant := string(rec.Key) == tenant
	if isTenant {
		st.tenantRecords++
	}

	var app interface {
		Commit() error
	}
	var appender = func(lbls labels.Labels, ts int64, v float64) {}
	if head != nil && isTenant {
		a := head.Appender(context.Background())
		app = a
		appender = func(lbls labels.Labels, ts int64, v float64) {
			_, _ = a.Append(0, lbls, ts, v)
		}
	}

	for _, ts := range req.Timeseries {
		st.seriesDecoded++
		st.samplesDecoded += int64(len(ts.Samples) + len(ts.Histograms))
		if !isTenant || !matchesAll(matchers, ts.Labels) {
			continue
		}
		st.seriesMatched++
		st.samplesMatched += int64(len(ts.Samples))
		lbls := mimirpb.FromLabelAdaptersToLabelsWithCopy(ts.Labels)
		st.uniqueMatched[lbls.Hash()] = struct{}{}
		for _, s := range ts.Samples {
			appender(lbls, s.TimestampMs, s.Value)
		}
	}
	if app != nil {
		if err := app.Commit(); err != nil {
			log.Fatalf("commit: %v", err)
		}
	}
}

func matchesAll(matchers []*labels.Matcher, lbls []mimirpb.LabelAdapter) bool {
	for _, m := range matchers {
		v := ""
		for _, l := range lbls {
			if l.Name == m.Name {
				v = l.Value
				break
			}
		}
		if !m.Matches(v) {
			return false
		}
	}
	return true
}

func cpuTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func report(cfg config, partition int32, st stats, wall, cpu time.Duration) {
	span := st.lastRecordTime.Sub(st.firstRecordTime)
	perSec := func(n int64, d time.Duration) float64 { return float64(n) / d.Seconds() }

	fmt.Printf("\npartition %d, %s of writes (%s .. %s), selector %s, decode=%v append=%v\n",
		partition, span.Round(time.Second), st.firstRecordTime.Format(time.DateTime), st.lastRecordTime.Format(time.DateTime), cfg.selector, cfg.decode, cfg.appendTo)
	fmt.Printf("  records            %12d   (%d for tenant %s)\n", st.records, st.tenantRecords, cfg.tenant)
	fmt.Printf("  bytes              %12d   (%.1f MiB)\n", st.bytes, float64(st.bytes)/(1<<20))
	fmt.Printf("  series decoded     %12d\n", st.seriesDecoded)
	fmt.Printf("  samples decoded    %12d\n", st.samplesDecoded)
	fmt.Printf("  samples matched    %12d   (%d unique series)\n", st.samplesMatched, len(st.uniqueMatched))
	fmt.Printf("  wall               %12s\n", wall.Round(time.Millisecond))
	fmt.Printf("  cpu                %12s   (%.2f cores)\n", cpu.Round(time.Millisecond), cpu.Seconds()/wall.Seconds())
	fmt.Printf("  throughput         %12.0f samples/s wall, %.0f samples per cpu-second, %.1f MiB/s\n",
		perSec(st.samplesDecoded, wall), perSec(st.samplesDecoded, cpu), perSec(st.bytes, wall)/(1<<20))
	if span > 0 {
		fmt.Printf("  speed-up           %12.1fx real time (%s of writes replayed in %s)\n", span.Seconds()/wall.Seconds(), span.Round(time.Second), wall.Round(time.Millisecond))
	}
}
