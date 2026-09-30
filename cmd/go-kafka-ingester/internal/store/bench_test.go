// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/record"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/segment"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// The Rust ingester's store benches are programs printing one line per case (`cargo bench --bench
// <name>`); these are the same programs as tests, run with GO_INGESTER_BENCH=1, printing the same
// fields. The query benches encode their responses like the ingester service and decode them like
// the Rust benches do, so their times compare.

func requireBench(t *testing.T) {
	if os.Getenv("GO_INGESTER_BENCH") == "" {
		t.Skip("set GO_INGESTER_BENCH=1 to run")
	}
}

const (
	responseTargetBytes = 1024 * 1024
	seriesBatchSize     = 1024
)

// queryStream encodes a query's series like the ingester service's QueryStream: batches of series
// labels, then batches of their chunks, and decodes each response like the Rust benches, returning
// the responses, series and response bytes.
func queryStream(views []QuerySeriesView, chunksBatch int) (responses, series, bytes int) {
	var response client.QueryStreamResponse
	send := func(encoded []byte) {
		responses++
		bytes += len(encoded)
		response.Reset()
		if err := response.Unmarshal(encoded); err != nil {
			panic(err)
		}
		series += len(response.StreamingSeries)
	}
	for start := 0; start < len(views); start += seriesBatchSize {
		end := min(start+seriesBatchSize, len(views))
		size := 2
		for _, view := range views[start:end] {
			size += len(view.EncodedLabels) + 16
		}
		encoded := make([]byte, 0, size)
		for _, view := range views[start:end] {
			size := len(view.EncodedLabels)
			if len(view.Chunks) > 0 {
				size += 1 + protowire.SizeVarint(uint64(len(view.Chunks)))
			}
			encoded = protowire.AppendTag(encoded, 3, protowire.BytesType)
			encoded = protowire.AppendVarint(encoded, uint64(size))
			encoded = append(encoded, view.EncodedLabels...)
			if len(view.Chunks) > 0 {
				encoded = protowire.AppendTag(encoded, 2, protowire.VarintType)
				encoded = protowire.AppendVarint(encoded, uint64(len(view.Chunks)))
			}
		}
		if end == len(views) {
			encoded = append(encoded, 0x20, 0x01)
		}
		send(encoded)
	}
	if len(views) == 0 {
		send([]byte{0x20, 0x01})
	}
	// Sized to what the responses hold, up to the target: a whole target zeroed for every query
	// would cost more than the query.
	total := 0
	for index, view := range views {
		for _, chunk := range view.Chunks {
			total += 2 + protowire.SizeBytes(len(chunk.Wire))
		}
		total += 12 + protowire.SizeVarint(uint64(index))
	}
	newBuffer := func() []byte { return make([]byte, 0, min(total, responseTargetBytes)) }
	encoded := newBuffer()
	items := 0
	for index, view := range views {
		if len(view.Chunks) == 0 {
			continue
		}
		size := 0
		if index > 0 {
			size += 1 + protowire.SizeVarint(uint64(index))
		}
		for _, chunk := range view.Chunks {
			size += 1 + protowire.SizeBytes(len(chunk.Wire))
		}
		itemSize := 1 + protowire.SizeVarint(uint64(size)) + size
		if items > 0 && (items >= chunksBatch || len(encoded)+itemSize > responseTargetBytes) {
			send(encoded)
			total -= len(encoded)
			encoded = newBuffer()
			items = 0
		}
		encoded = protowire.AppendTag(encoded, 5, protowire.BytesType)
		encoded = protowire.AppendVarint(encoded, uint64(size))
		if index > 0 {
			encoded = protowire.AppendTag(encoded, 1, protowire.VarintType)
			encoded = protowire.AppendVarint(encoded, uint64(index))
		}
		for _, chunk := range view.Chunks {
			encoded = protowire.AppendTag(encoded, 2, protowire.BytesType)
			encoded = protowire.AppendBytes(encoded, chunk.Wire)
		}
		items++
	}
	if items > 0 {
		send(encoded)
	}
	return responses, series, bytes
}

func rssBytes() uint64 {
	output, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		return 0
	}
	kilobytes, _ := strconv.ParseUint(strings.TrimSpace(string(output)), 10, 64)
	return kilobytes * 1024
}

const queryStreamSeries = 25_000
const shardedSamples = int64(1_200)

func queryStreamStore(t testing.TB) *Store {
	s := Default()
	for index := range queryStreamSeries {
		pairs := [][2]string{{"__name__", fmt.Sprintf("metric_%d", index)}}
		for label := range 19 {
			value := fmt.Sprintf("value_%d_%d", index, label)
			if label == 9 {
				value = fmt.Sprintf("group_%d", index/100)
			}
			pairs = append(pairs, [2]string{fmt.Sprintf("label_%d", label), value})
		}
		series := record.DecodedSeries{Labels: pairs}
		for sample := range int64(20) {
			series.Samples = append(series.Samples, mimirpb.Sample{TimestampMs: sample * 1000, Value: float64(int64(index) + sample)})
		}
		if index%10 == 0 {
			series.Histograms = []mimirpb.Histogram{{
				Timestamp: 20_000,
				Count:     &mimirpb.Histogram_CountInt{CountInt: 1},
				ZeroCount: &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 1},
			}}
		}
		require.NoError(t, s.Ingest("benchmark", record.DecodedRequest{Series: []record.DecodedSeries{series}}))
	}
	// One metric's series over five hours, each in ten chunks: queriers split a query for one
	// metric into query shards, so each request walks the metric's series for a sixteenth.
	for index := range queryStreamSeries {
		pairs := [][2]string{{"__name__", "sharded"}, {"pod", fmt.Sprintf("pod_%d", index)}}
		// Like Adaptive Metrics' aggregated series, a few have __aggregation__.
		if index%100 == 0 {
			pairs = append(pairs, [2]string{"__aggregation__", "sum"})
		}
		series := record.DecodedSeries{Labels: pairs}
		for sample := range shardedSamples {
			series.Samples = append(series.Samples, mimirpb.Sample{TimestampMs: sample * 15_000, Value: float64(sample)})
		}
		require.NoError(t, s.Ingest("benchmark", record.DecodedRequest{Series: []record.DecodedSeries{series}}))
	}
	return s
}

type queryStreamCase struct {
	name     string
	matchers []LabelMatcher
	end      int64
	expected int
}

func queryStreamCases() []queryStreamCase {
	return []queryStreamCase{
		{"selective", []LabelMatcher{matcher(0, "label_9", "group_0")}, 20_000, 100},
		{"broad", []LabelMatcher{matcher(2, "label_9", ".+")}, 20_000, queryStreamSeries},
		{"sharded", []LabelMatcher{matcher(0, "__name__", "sharded"), matcher(0, "__query_shard__", "1_of_16")}, shardedSamples * 15_000, -1},
		// What queriers ask when Adaptive Metrics excludes aggregated series from raw queries.
		{"sharded_without_aggregated", []LabelMatcher{matcher(0, "__name__", "sharded"), matcher(3, "__aggregation__", ".+"), matcher(0, "__query_shard__", "1_of_16")}, shardedSamples * 15_000, -1},
		// A label regex without a metric name.
		{"nameless_label_regex", []LabelMatcher{matcher(2, "__aggregation__", "s.*"), matcher(0, "__query_shard__", "1_of_16")}, shardedSamples * 15_000, -1},
		// Alternations of literals, like dashboard variables with several values.
		{"name_alternation", []LabelMatcher{matcher(2, "__name__", "metric_1|metric_20|metric_300|metric_4000|metric_5|metric_60|metric_700|metric_8000|metric_9|metric_10")}, 20_000, 10},
		{"label_alternation", []LabelMatcher{matcher(2, "label_9", "group_1|group_2|group_3")}, 20_000, 300},
		{"sharded_label_alternation", []LabelMatcher{matcher(0, "__name__", "sharded"), matcher(2, "pod", "pod_1|pod_2|pod_3|pod_40|pod_500|pod_6000")}, shardedSamples * 15_000, 6},
	}
}

// TestQueryStreamBench is the Rust query_stream bench.
func TestQueryStreamBench(t *testing.T) {
	requireBench(t)
	s := queryStreamStore(t)
	const runs = 20
	run := func(c queryStreamCase) (int, int) {
		views, err := s.SelectChunks("benchmark", 0, c.end, c.matchers)
		require.NoError(t, err)
		_, series, bytes := queryStream(views, 1024)
		return series, bytes
	}
	for _, c := range queryStreamCases() {
		before := rssBytes()
		started := time.Now()
		series, bytes := run(c)
		cold := time.Since(started)
		if c.expected >= 0 {
			require.Equal(t, c.expected, series, c.name)
		}
		var hot time.Duration
		for range runs {
			started := time.Now()
			again, againBytes := run(c)
			hot += time.Since(started)
			require.Equal(t, [2]int{series, bytes}, [2]int{again, againBytes})
		}
		fmt.Printf("%s: series=%d response_B=%d cold_ms=%.2f hot_ms=%.2f rss_B=%d\n", c.name, series, bytes,
			float64(cold.Microseconds())/1000, float64(hot.Microseconds())/1000/runs, max(int64(rssBytes())-int64(before), 0))
	}
}

// BenchmarkQueryStream runs the query_stream cases for allocations.
func BenchmarkQueryStream(b *testing.B) {
	s := queryStreamStore(b)
	for _, c := range queryStreamCases() {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				views, err := s.SelectChunks("benchmark", 0, c.end, c.matchers)
				if err != nil {
					b.Fatal(err)
				}
				queryStream(views, 1024)
			}
		})
	}
}

func coldQueryStore(t testing.TB, directory string) *Store {
	const cold, live = 25_000, 2_500
	s, err := New(20*60*1000, Retention{}, directory)
	require.NoError(t, err)
	start := 10 * hour
	series := func(pod string, from, to int64) record.DecodedSeries {
		out := record.DecodedSeries{Labels: [][2]string{{"__name__", "churned"}, {"job", "api"}, {"pod", pod}}}
		for timestamp := from; timestamp < to; timestamp += 15_000 {
			out.Samples = append(out.Samples, mimirpb.Sample{TimestampMs: timestamp, Value: 1})
		}
		return out
	}
	// Pods that ran for the first 2 hours, then pods that run on to 15h.
	for first := 0; first < cold; first += 1000 {
		var batch []record.DecodedSeries
		for pod := first; pod < min(first+1000, cold); pod++ {
			batch = append(batch, series(fmt.Sprintf("old-%d", pod), start, start+2*hour))
		}
		require.NoError(t, s.Ingest("bench", record.DecodedRequest{Series: batch}))
	}
	for first := 0; first < live; first += 500 {
		var batch []record.DecodedSeries
		for pod := first; pod < min(first+500, live); pod++ {
			batch = append(batch, series(fmt.Sprintf("live-%d", pod), start, start+5*hour))
		}
		require.NoError(t, s.Ingest("bench", record.DecodedRequest{Series: batch}))
	}
	s.HeadTick(true, false)
	s.HeadTick(false, false)
	require.Equal(t, uint64(live), s.NumSeries("bench"), "old pods are in cold blocks")
	return s
}

var coldQueryMatchers = []LabelMatcher{matcher(0, "__name__", "churned"), matcher(0, "__query_shard__", "1_of_16")}

// TestColdQueryBench is the Rust cold_query bench: sharded queries over series that left the head
// for cold blocks, like a metric whose pods churned.
func TestColdQueryBench(t *testing.T) {
	requireBench(t)
	s := coldQueryStore(t, t.TempDir())
	defer s.Close()
	start := 10 * hour
	query := func() []QuerySeriesView {
		views, err := s.SelectChunks("bench", start, start+5*hour, coldQueryMatchers)
		require.NoError(t, err)
		return views
	}
	started := time.Now()
	series := query()
	cold := time.Since(started)
	runs := 20
	if value, err := strconv.Atoi(os.Getenv("COLD_RUNS")); err == nil {
		runs = value
	}
	var hot time.Duration
	for range runs {
		started := time.Now()
		again := query()
		hot += time.Since(started)
		require.Len(t, again, len(series))
	}
	chunks := 0
	for _, view := range series {
		chunks += len(view.Chunks)
	}
	fmt.Printf("cold_sharded: series=%d chunks=%d cold_ms=%.2f hot_ms=%.2f\n", len(series), chunks,
		float64(cold.Microseconds())/1000, float64(hot.Microseconds())/1000/float64(runs))
}

func BenchmarkColdQuery(b *testing.B) {
	s := coldQueryStore(b, b.TempDir())
	defer s.Close()
	start := 10 * hour
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := s.SelectChunks("bench", start, start+5*hour, coldQueryMatchers); err != nil {
			b.Fatal(err)
		}
	}
}

// TestFloatWindowQueryBench is the Rust float_window_query bench.
func TestFloatWindowQueryBench(t *testing.T) {
	requireBench(t)
	const series, points = 600, 3_600
	s := Default()
	build := func(point func(index int) []mimirpb.Sample) record.DecodedRequest {
		request := record.DecodedRequest{}
		for index := range series {
			request.Series = append(request.Series, record.DecodedSeries{Labels: [][2]string{{"__name__", fmt.Sprintf("metric_%d", index)}}, Samples: point(index)})
		}
		return request
	}
	require.NoError(t, s.Ingest("benchmark", build(func(index int) []mimirpb.Sample {
		samples := make([]mimirpb.Sample, points)
		for point := range points {
			samples[point] = mimirpb.Sample{TimestampMs: int64(point) * 2_000, Value: math.Sin(float64(index+point) * 0.125)}
		}
		return samples
	})))
	query := func() [2]int {
		views, err := s.SelectChunks("benchmark", 3_600_000, 3_660_000, []LabelMatcher{matcher(2, "__name__", ".+")})
		require.NoError(t, err)
		responses, _, bytes := queryStream(views, 64)
		return [2]int{responses, bytes}
	}
	started := time.Now()
	expected := query()
	cold := time.Since(started)
	started = time.Now()
	require.Equal(t, expected, query())
	hot := time.Since(started)
	require.NoError(t, s.Ingest("benchmark", build(func(index int) []mimirpb.Sample {
		return []mimirpb.Sample{{TimestampMs: points * 2_000, Value: float64(index)}}
	})))
	started = time.Now()
	require.Equal(t, expected, query())
	afterUpdate := time.Since(started)
	fmt.Printf("series=%d samples=%d responses=%d response_bytes=%d cold_s=%.3f hot_s=%.3f after_update_s=%.3f\n",
		series, series*points, expected[0], expected[1], cold.Seconds(), hot.Seconds(), afterUpdate.Seconds())
}

// TestHistogramQueryBench is the Rust histogram_query bench.
func TestHistogramQueryBench(t *testing.T) {
	requireBench(t)
	const series, histogramsPerSeries, runs = 600, 400, 3
	histogram := func(timestamp int64) mimirpb.Histogram {
		deltas := make([]int64, 50)
		for index := range deltas {
			deltas[index] = 1
		}
		return mimirpb.Histogram{Timestamp: timestamp, Count: &mimirpb.Histogram_CountInt{CountInt: 50}, PositiveSpans: []mimirpb.BucketSpan{{Offset: 0, Length: 50}}, PositiveDeltas: deltas}
	}
	s := Default()
	request := record.DecodedRequest{}
	for index := range series {
		s := record.DecodedSeries{Labels: [][2]string{{"__name__", fmt.Sprintf("metric_%d", index)}}}
		for sample := range histogramsPerSeries {
			s.Histograms = append(s.Histograms, histogram(int64(sample)*1000))
		}
		request.Series = append(request.Series, s)
	}
	require.NoError(t, s.Ingest("benchmark", request))
	query := func() [2]int {
		views, err := s.SelectChunks("benchmark", 100_000, 160_000, []LabelMatcher{matcher(2, "__name__", ".+")})
		require.NoError(t, err)
		responses, _, bytes := queryStream(views, 64)
		return [2]int{responses, bytes}
	}
	restRSS := rssBytes()
	started := time.Now()
	expected := query()
	cold := time.Since(started)
	queryRSS := rssBytes()
	var hot []float64
	for range runs {
		started := time.Now()
		require.Equal(t, expected, query())
		hot = append(hot, time.Since(started).Seconds())
	}
	update := record.DecodedRequest{}
	for index := range series {
		update.Series = append(update.Series, record.DecodedSeries{Labels: [][2]string{{"__name__", fmt.Sprintf("metric_%d", index)}}, Histograms: []mimirpb.Histogram{histogram(histogramsPerSeries * 1000)}})
	}
	require.NoError(t, s.Ingest("benchmark", update))
	started = time.Now()
	after := query()
	updateSeconds := time.Since(started).Seconds()
	require.Equal(t, expected, after)
	fmt.Printf("series=%d histograms=%d responses=%d response_bytes=%d cold_s=%.3f hot_s=%v after_update_s=%.3f rest_rss_bytes=%d after_query_rss_bytes=%d\n",
		series, series*histogramsPerSeries, expected[0], expected[1], cold.Seconds(), hot, updateSeconds, restRSS, queryRSS)
}

func cpuSeconds() (user, system float64) {
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	seconds := func(tv syscall.Timeval) float64 { return float64(tv.Sec) + float64(tv.Usec)/1e6 }
	return seconds(usage.Utime), seconds(usage.Stime)
}

// Like mimir-dev-15: 41 samples per record, 19 labels per series.
const (
	steadySeriesPerRecord = 41
	steadyLabels          = 19
	scrapeMs              = int64(15_000)
)

func steadySeriesLabels(id int) []mimirpb.LabelAdapter {
	labels := []mimirpb.LabelAdapter{{Name: "__name__", Value: fmt.Sprintf("metric_%d", id%400)}}
	for label := 1; label < steadyLabels; label++ {
		var value string
		switch label {
		case 1:
			value = fmt.Sprintf("pod-%d", id/400%2_000)
		case 2:
			value = fmt.Sprintf("namespace-%d", id%30)
		case 3:
			value = "cluster-a"
		default:
			value = fmt.Sprintf("value_%d_%d", label, id%(label*7+3))
		}
		labels = append(labels, mimirpb.LabelAdapter{Name: fmt.Sprintf("label_%02d", label), Value: value})
	}
	return labels
}

// steadyRecords returns one Kafka record value per group of series, for the scrape at timestampMs.
func steadyRecords(from, to int, timestampMs int64) [][]byte {
	var records [][]byte
	for first := from; first < to; first += steadySeriesPerRecord {
		request := mimirpb.WriteRequest{}
		for id := first; id < min(first+steadySeriesPerRecord, to); id++ {
			request.Timeseries = append(request.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
				Labels:  steadySeriesLabels(id),
				Samples: []mimirpb.Sample{{TimestampMs: timestampMs, Value: float64(timestampMs/scrapeMs) * float64(id)}},
			}})
		}
		encoded, err := request.Marshal()
		if err != nil {
			panic(err)
		}
		records = append(records, encoded)
	}
	return records
}

type steadyPhases struct {
	decode, keys, encode, apply, write time.Duration
}

// steadyIngest is like the Kafka consumer: records are decoded and their series keys hashed first,
// then encoded for the segment log in order, applied, and written.
func steadyIngest(t testing.TB, s *Store, log *segment.Log, records [][]byte, batch int, offset *int64, phases *steadyPhases) {
	for first := 0; first < len(records); first += batch {
		group := records[first:min(first+batch, len(records))]
		now := nowMs()
		require.NoError(t, log.BeginBatch(now))
		frames := make([]*segment.CompressedFrame, 0, len(group))
		prepared := make([]IngestRecord, 0, len(group))
		for _, bytes := range group {
			*offset++
			started := time.Now()
			// Like a fetched Kafka record, whose payload the decoded labels share.
			request, spans, err := record.DecodeRecordWithLabelSpans(1, bytes)
			require.NoError(t, err)
			hashes := SeriesHashes(&request)
			decoded := time.Now()
			keys := segment.SeriesKeysWithLabelBytes("tenant", &request, bytes, spans)
			hashed := time.Now()
			frame, err := log.Encode(*offset, 0, now, "tenant", &request, keys)
			require.NoError(t, err)
			frames = append(frames, frame)
			phases.decode += decoded.Sub(started)
			phases.keys += hashed.Sub(decoded)
			phases.encode += time.Since(hashed)
			prepared = append(prepared, IngestRecord{Tenant: "tenant", Request: request, IngestedMs: now, TrackRate: true, Bytes: len(bytes), SeriesHashes: hashes})
		}
		started := time.Now()
		_, err := s.IngestFlushes(prepared)
		require.NoError(t, err)
		applied := time.Now()
		for _, frame := range frames {
			require.NoError(t, log.AppendCompressed(frame))
		}
		phases.apply += applied.Sub(started)
		phases.write += time.Since(applied)
	}
}

func directoryBytes(directory string) int64 {
	var total int64
	_ = filepath.Walk(directory, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

func envInt(name string, fallback int) int {
	if value, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return value
	}
	return fallback
}

// TestSteadyIngestBench is the Rust steady_ingest bench: CPU per sample on the steady-state Kafka
// path, small records for series the store already has, decoded, logged to the segment log and
// applied in small batches, like a caught-up pod.
func TestSteadyIngestBench(t *testing.T) {
	requireBench(t)
	series := envInt("STEADY_SERIES", 200_000)
	rounds := envInt("STEADY_ROUNDS", 4)
	threads := runtime.GOMAXPROCS(0)
	start := nowMs() - int64(rounds+2)*scrapeMs
	for _, batch := range []int{1, 8} {
		directory := t.TempDir()
		s, err := NewWithShards(20*60*1000, Retention{}, directory, 16, threads)
		require.NoError(t, err)
		log, _, err := segment.Open(filepath.Join(directory, "segments"), 0, "bench", 0, segment.Retention{})
		require.NoError(t, err)
		var offset int64
		// Creating the series, like a replay: every sample is a new series, in full batches.
		first := steadyRecords(0, series, start)
		var creation steadyPhases
		user, system := cpuSeconds()
		wallStart := time.Now()
		steadyIngest(t, s, log, first, 64, &offset, &creation)
		userEnd, systemEnd := cpuSeconds()
		perSeries := func(duration time.Duration) float64 { return float64(duration.Nanoseconds()) / float64(series) }
		cpu := (userEnd - user) + (systemEnd - system)
		fmt.Printf("create (64-record batches): %.0f ns CPU/series, %.2f cores; wall: decode %.0f, keys %.0f, encode %.0f, apply %.0f, write %.0f ns/series\n",
			cpu*1e9/float64(series), cpu/time.Since(wallStart).Seconds(),
			perSeries(creation.decode), perSeries(creation.keys), perSeries(creation.encode), perSeries(creation.apply), perSeries(creation.write))
		var phases steadyPhases
		require.NoError(t, log.Flush())
		initialBytes := directoryBytes(filepath.Join(directory, "segments"))
		var totalUser, totalSystem, wall float64
		group := batch * steadySeriesPerRecord
		for round := 1; round <= rounds; round++ {
			for first := 0; first < series; first += group {
				// Records are encoded just before they are ingested, as they arrive from Kafka.
				records := steadyRecords(first, min(first+group, series), start+int64(round)*scrapeMs)
				user, system := cpuSeconds()
				wallStart := time.Now()
				steadyIngest(t, s, log, records, batch, &offset, &phases)
				userEnd, systemEnd := cpuSeconds()
				totalUser += userEnd - user
				totalSystem += systemEnd - system
				wall += time.Since(wallStart).Seconds()
			}
		}
		samples := float64(series * rounds)
		require.NoError(t, log.Close())
		segmentBytes := directoryBytes(filepath.Join(directory, "segments")) - initialBytes
		perSample := func(duration time.Duration) float64 { return float64(duration.Nanoseconds()) / samples }
		fmt.Printf("batch=%d: %.0f ns CPU/sample (%.0f user, %.0f system), %.2f cores; wall: decode %.0f, keys %.0f, encode %.0f, apply %.0f, write %.0f ns/sample; segment log %.1f bytes/sample\n",
			batch, (totalUser+totalSystem)*1e9/samples, totalUser*1e9/samples, totalSystem*1e9/samples, (totalUser+totalSystem)/wall,
			perSample(phases.decode), perSample(phases.keys), perSample(phases.encode), perSample(phases.apply), perSample(phases.write),
			float64(segmentBytes)/samples)
		require.NoError(t, s.Close())
	}
}

const recoveryBaseRecords = 9_739
const recoverySeriesPerRecord = 20

func recoveryRequest(frame, uniqueRecords int, histogramHeavy bool) record.DecodedRequest {
	sourceFrame := frame % uniqueRecords
	request := record.DecodedRequest{}
	for index := range recoverySeriesPerRecord {
		seriesID := sourceFrame*recoverySeriesPerRecord + index
		histogramSeries := histogramHeavy && seriesID%10 == 0
		pairs := [][2]string{{"__name__", fmt.Sprintf("metric_%d", seriesID)}}
		for label := range 19 {
			pairs = append(pairs, [2]string{fmt.Sprintf("label_%d", label), fmt.Sprintf("value_%d_%d", seriesID%1_000, label)})
		}
		series := record.DecodedSeries{Labels: pairs}
		for sample := range int64(20) {
			if histogramSeries {
				deltas := make([]int64, 50)
				for i := range deltas {
					deltas[i] = 1
				}
				series.Histograms = append(series.Histograms, mimirpb.Histogram{
					Timestamp: int64(frame)*1_000 + sample, Count: &mimirpb.Histogram_CountInt{CountInt: 50},
					PositiveSpans: []mimirpb.BucketSpan{{Offset: 0, Length: 50}}, PositiveDeltas: deltas,
				})
			} else {
				series.Samples = append(series.Samples, mimirpb.Sample{TimestampMs: int64(frame)*1_000 + sample, Value: float64(seriesID) + float64(sample)})
			}
		}
		request.Series = append(request.Series, series)
	}
	return request
}

// TestRecoveryBench is the Rust recovery bench: replaying a segment log into a new store.
func TestRecoveryBench(t *testing.T) {
	requireBench(t)
	scale := envInt("MIMIR_RECOVERY_SCALE", 1)
	records := recoveryBaseRecords * scale
	unique := records * 9 / 10
	histogramHeavy := os.Getenv("MIMIR_RECOVERY_HISTOGRAM_FIXTURE") != ""
	directory := t.TempDir()
	log, _, err := segment.Open(directory, 0, "fixture", 0, segment.Retention{})
	require.NoError(t, err)
	for frame := range records {
		request := recoveryRequest(frame, unique, histogramHeavy)
		require.NoError(t, log.Append(int64(frame), 1_000_000+int64(frame), "benchmark", &request))
	}
	require.NoError(t, log.Close())
	fmt.Printf("variant=segment bytes=%d\n", directoryBytes(directory))
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	beforeUser, beforeSystem := cpuSeconds()
	started := time.Now()
	s := Default()
	count := 0
	replayed, err := segment.OpenReplaying(directory, 0, "fixture", 0, segment.Retention{}, 2, func(recovered segment.RecoveredRecord) error {
		count++
		return s.IngestRecovered(recovered.Tenant, recovered.Request, recovered.IngestedMs)
	})
	require.NoError(t, err)
	last, ok := replayed.LastOffset()
	require.True(t, ok)
	require.Equal(t, int64(records-1), last)
	require.Equal(t, records, count)
	restore := time.Since(started)
	restoreUser, restoreSystem := cpuSeconds()
	var restored runtime.MemStats
	runtime.ReadMemStats(&restored)
	started = time.Now()
	selected, err := s.SelectChunks("benchmark", 0, math.MaxInt64, nil)
	require.NoError(t, err)
	require.Len(t, selected, unique*recoverySeriesPerRecord)
	query := time.Since(started)
	queryUser, querySystem := cpuSeconds()
	var queried runtime.MemStats
	runtime.ReadMemStats(&queried)
	fmt.Printf("variant=segment series=%d restore_ms=%.2f query_ms=%.2f restore_cpu_ms=%.2f query_cpu_ms=%.2f restore_heap_delta_B=%d query_heap_B=%d\n",
		len(selected), float64(restore.Microseconds())/1000, float64(query.Microseconds())/1000,
		((restoreUser-beforeUser)+(restoreSystem-beforeSystem))*1000, ((queryUser-restoreUser)+(querySystem-restoreSystem))*1000,
		int64(restored.HeapInuse)-int64(before.HeapInuse), queried.HeapInuse)
	require.NoError(t, replayed.Close())
}
