// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/record"
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
		// One observation in each bucket, matching the count.
		deltas[0] = 1
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

func envInt(name string, fallback int) int {
	if value, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return value
	}
	return fallback
}

const recoveryBaseRecords = 9_739
const recoverySeriesPerRecord = 20
