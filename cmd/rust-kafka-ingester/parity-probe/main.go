// SPDX-License-Identifier: AGPL-3.0-only

// parity-probe compares decoded QueryStream results from a Rust ingester against Go ingesters that own the same partition.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/grafana/dskit/clusterutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	promvalue "github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
)

type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

var clusterLabel string

func outgoing(tenant string) context.Context {
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-scope-orgid", tenant)
	if clusterLabel != "" {
		ctx = clusterutil.PutClusterIntoOutgoingContext(ctx, clusterLabel)
	}
	return ctx
}

type result struct {
	series   map[string][]string
	counts   map[string]int
	bytes    int
	chunks   int
	spanMs   int64
	duration time.Duration
}

func main() {
	var goAddrs, tenants, selectors stringList
	rustAddr := flag.String("rust", "", "Rust ingester gRPC address")
	flag.Var(&goAddrs, "go", "Go ingester gRPC address owning the same partition (repeatable)")
	flag.Var(&tenants, "tenant", "tenant to compare (repeatable); defaults to the largest tenants reported by the first Go ingester")
	flag.Var(&selectors, "selector", "series selector (repeatable)")
	topTenants := flag.Int("top-tenants", 3, "tenants to discover when -tenant is not set")
	window := flag.Duration("window", time.Minute, "query window length")
	settle := flag.Duration("settle", 2*time.Minute, "gap between the window end and now so both ingesters have consumed it")
	repeat := flag.Int("repeat", 1, "serial queries per ingester; timings are reported for each")
	flag.StringVar(&clusterLabel, "cluster-label", "", "cluster validation label expected by the ingesters, such as the namespace")
	flag.Parse()
	if *rustAddr == "" || len(goAddrs) == 0 {
		fmt.Fprintln(os.Stderr, "-rust and at least one -go are required")
		os.Exit(2)
	}
	if len(selectors) == 0 {
		selectors = stringList{`{__name__=~".+"}`}
	}

	rust := dial(*rustAddr)
	gos := make([]client.IngesterClient, len(goAddrs))
	for i, addr := range goAddrs {
		gos[i] = dial(addr)
	}
	if len(tenants) == 0 {
		tenants = discoverTenants(gos[0], *topTenants)
	}

	end := time.Now().Add(-*settle).Truncate(time.Second)
	start := end.Add(-*window)
	fmt.Printf("window=%s..%s tenants=%v\n", start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339), tenants)

	mismatches := 0
	for _, tenant := range tenants {
		for _, selector := range selectors {
			request := buildRequest(selector, start, end)
			rustResults := query(rust, tenant, request, *repeat)
			for i, goClient := range gos {
				goResults := query(goClient, tenant, request, *repeat)
				diff := compare(rustResults[0].series, goResults[0].series)
				mismatches += diff
				fmt.Printf("tenant=%s selector=%s go=%s rust=[%s] go=[%s] mismatched_series=%d\n",
					tenant, selector, goAddrs[i], summarize(rustResults), summarize(goResults), diff)
			}
		}
	}
	if mismatches > 0 {
		os.Exit(1)
	}
}

func dial(addr string) client.IngesterClient {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(256<<20)))
	check(err)
	return client.NewIngesterClient(conn)
}

func discoverTenants(api client.IngesterClient, n int) []string {
	// The ingester auth interceptor rejects calls without an org ID, even though AllUserStats spans all tenants.
	ctx, cancel := context.WithTimeout(outgoing("parity-probe"), time.Minute)
	defer cancel()
	stats, err := api.AllUserStats(ctx, &client.UserStatsRequest{})
	check(err)
	sort.Slice(stats.Stats, func(i, j int) bool { return stats.Stats[i].Data.NumSeries > stats.Stats[j].Data.NumSeries })
	var tenants []string
	for _, s := range stats.Stats {
		if len(tenants) == n {
			break
		}
		tenants = append(tenants, s.UserId)
	}
	return tenants
}

func buildRequest(selector string, start, end time.Time) *client.QueryRequest {
	matchers, err := parser.NewParser(parser.Options{}).ParseMetricSelector(selector)
	check(err)
	request, err := client.ToQueryRequest(model.TimeFromUnixNano(start.UnixNano()), model.TimeFromUnixNano(end.UnixNano()), matchers)
	check(err)
	request.StreamingChunksBatchSize = 64
	return request
}

func query(api client.IngesterClient, tenant string, request *client.QueryRequest, repeat int) []result {
	results := make([]result, repeat)
	for i := range results {
		ctx, cancel := context.WithTimeout(outgoing(tenant), 5*time.Minute)
		started := time.Now()
		results[i] = decode(ctx, api, request)
		results[i].duration = time.Since(started)
		cancel()
	}
	return results
}

func decode(ctx context.Context, api client.IngesterClient, request *client.QueryRequest) result {
	stream, err := api.QueryStream(ctx, request)
	check(err)
	out := result{series: map[string][]string{}, counts: map[string]int{}}
	var keys []string
	// Responses may alias a reused receive buffer, so everything is copied out before the next Recv.
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		check(err)
		for _, s := range response.StreamingSeries {
			key := mimirpb.FromLabelAdaptersToLabels(s.Labels).String()
			if _, ok := out.series[key]; ok {
				fail("duplicate series %s", key)
			}
			out.series[key] = nil
			keys = append(keys, key)
		}
		for _, group := range response.StreamingSeriesChunks {
			if group.SeriesIndex >= uint64(len(keys)) {
				fail("chunk group references series %d of %d", group.SeriesIndex, len(keys))
			}
			key := keys[group.SeriesIndex]
			for _, wire := range group.Chunks {
				out.bytes += len(wire.Data)
				out.chunks++
				out.spanMs += wire.EndTimestampMs - wire.StartTimestampMs
				out.series[key] = append(out.series[key], samples(wire, request, out.counts)...)
			}
		}
	}
	for key, values := range out.series {
		// Go returns head series without samples in the window; they carry no data to compare.
		if len(values) == 0 {
			delete(out.series, key)
			continue
		}
		sort.Strings(values)
	}
	return out
}

func samples(wire client.Chunk, request *client.QueryRequest, counts map[string]int) []string {
	var encoding chunkenc.Encoding
	switch chunk.Encoding(wire.Encoding) {
	case chunk.PrometheusXorChunk:
		encoding = chunkenc.EncXOR
	case chunk.PrometheusXor2Chunk:
		encoding = chunkenc.EncXOR2
	case chunk.PrometheusHistogramChunk:
		encoding = chunkenc.EncHistogram
	case chunk.PrometheusFloatHistogramChunk:
		encoding = chunkenc.EncFloatHistogram
	default:
		fail("unknown wire chunk encoding %d", wire.Encoding)
	}
	decoded, err := chunkenc.FromData(encoding, wire.Data)
	check(err)
	var values []string
	it := decoded.Iterator(nil)
	for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
		var ts int64
		var value, kind string
		switch typ {
		case chunkenc.ValFloat:
			var v float64
			ts, v = it.At()
			kind, value = "float", fmt.Sprintf("f:%016x", math.Float64bits(v))
			if promvalue.IsStaleNaN(v) {
				kind, value = "stale", "stale"
			}
		case chunkenc.ValHistogram:
			var h *histogram.Histogram
			ts, h = it.AtHistogram(nil)
			// The returned histogram shares slices with the iterator, so compact a copy.
			h = h.Copy()
			h.CounterResetHint = histogram.UnknownCounterReset
			// Go re-codes appended histograms into a widened bucket layout with explicit zero buckets; compacting
			// both sides compares bucket counts rather than layout.
			h.Compact(0)
			normalizeEmpty(&h.PositiveSpans, &h.NegativeSpans, &h.PositiveBuckets, &h.NegativeBuckets)
			kind, value = "histogram", fmt.Sprintf("h:%#v", *h)
			// Go writes a histogram stale marker for histogram series; Rust writes a float one. PromQL treats both as stale.
			if promvalue.IsStaleNaN(h.Sum) {
				kind, value = "stale", "stale"
			}
		case chunkenc.ValFloatHistogram:
			var h *histogram.FloatHistogram
			ts, h = it.AtFloatHistogram(nil)
			h = h.Copy()
			h.CounterResetHint = histogram.UnknownCounterReset
			h.Compact(0)
			normalizeEmpty(&h.PositiveSpans, &h.NegativeSpans, &h.PositiveBuckets, &h.NegativeBuckets)
			kind, value = "float_histogram", fmt.Sprintf("fh:%#v", *h)
			if promvalue.IsStaleNaN(h.Sum) {
				kind, value = "stale", "stale"
			}
		default:
			fail("unexpected chunk value type %v", typ)
		}
		if ts >= request.StartTimestampMs && ts <= request.EndTimestampMs {
			counts[kind]++
			values = append(values, fmt.Sprintf("%d:%s", ts, value))
		}
	}
	check(it.Err())
	return values
}

// Compact leaves an empty, non-nil slice where the input had one; nil and empty are the same histogram.
func normalizeEmpty[B any](positiveSpans, negativeSpans *[]histogram.Span, positiveBuckets, negativeBuckets *[]B) {
	for _, spans := range []*[]histogram.Span{positiveSpans, negativeSpans} {
		if len(*spans) == 0 {
			*spans = nil
		}
	}
	for _, buckets := range []*[]B{positiveBuckets, negativeBuckets} {
		if len(*buckets) == 0 {
			*buckets = nil
		}
	}
}

func compare(rust, golang map[string][]string) int {
	mismatched := 0
	report := func(format string, args ...any) {
		mismatched++
		if mismatched <= 10 {
			fmt.Printf("  "+format+"\n", args...)
		}
	}
	for key, r := range rust {
		g, ok := golang[key]
		switch {
		case !ok:
			report("only in rust: %s (%d samples)", key, len(r))
		case strings.Join(r, "\n") != strings.Join(g, "\n"):
			rv, gv := firstDifference(r, g)
			report("samples differ: %s rust=%d go=%d\n    rust: %s\n    go:   %s", key, len(r), len(g), rv, gv)
		}
	}
	for key, g := range golang {
		if _, ok := rust[key]; !ok {
			report("only in go: %s (%d samples)", key, len(g))
		}
	}
	return mismatched
}

func firstDifference(rust, golang []string) (string, string) {
	for i := 0; i < len(rust) || i < len(golang); i++ {
		var r, g string
		if i < len(rust) {
			r = rust[i]
		}
		if i < len(golang) {
			g = golang[i]
		}
		if r != g {
			return r, g
		}
	}
	return "", ""
}

func summarize(results []result) string {
	r := results[0]
	durations := make([]string, len(results))
	for i, res := range results {
		durations[i] = res.duration.Round(10 * time.Millisecond).String()
	}
	var meanSpan time.Duration
	if r.chunks > 0 {
		meanSpan = time.Duration(r.spanMs/int64(r.chunks)) * time.Millisecond
	}
	return fmt.Sprintf("series=%d float=%d histogram=%d float_histogram=%d stale=%d chunks=%d mean_chunk_span=%s chunk_bytes=%d durations=%s",
		len(r.series), r.counts["float"], r.counts["histogram"], r.counts["float_histogram"], r.counts["stale"], r.chunks, meanSpan.Round(time.Second), r.bytes, strings.Join(durations, "/"))
}

func check(err error) {
	if err != nil {
		fail("%v", err)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
