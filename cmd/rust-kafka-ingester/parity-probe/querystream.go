// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	promvalue "github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
)

// The decoded samples of a QueryStream answer, by series.
type result struct {
	series   map[string][]string
	counts   map[string]int
	chunks   int
	unsorted int
	// The newest sample in the window, in milliseconds.
	newest int64
}

func decode(ctx context.Context, api client.IngesterClient, request *client.QueryRequest) (result, error) {
	stream, err := api.QueryStream(ctx, request)
	if err != nil {
		return result{}, err
	}
	out := result{series: map[string][]string{}, counts: map[string]int{}}
	var keys []string
	var previous labels.Labels
	// Responses may alias a reused receive buffer, so everything is copied out before the next Recv.
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return result{}, err
		}
		for _, s := range response.StreamingSeries {
			current := mimirpb.FromLabelAdaptersToLabels(s.Labels).Copy()
			// The distributor k-way merges ingester streams, so each one must be sorted by labels.
			if len(keys) > 0 && labels.Compare(previous, current) >= 0 {
				out.unsorted++
			}
			previous = current
			key := current.String()
			if _, ok := out.series[key]; ok {
				return result{}, fmt.Errorf("duplicate series %s", key)
			}
			out.series[key] = nil
			keys = append(keys, key)
		}
		for _, group := range response.StreamingSeriesChunks {
			if group.SeriesIndex >= uint64(len(keys)) {
				return result{}, fmt.Errorf("chunk group references series %d of %d", group.SeriesIndex, len(keys))
			}
			key := keys[group.SeriesIndex]
			for _, wire := range group.Chunks {
				out.chunks++
				values, err := samples(wire, request, out.counts)
				if err != nil {
					return result{}, err
				}
				for _, value := range values {
					var ts int64
					if _, err := fmt.Sscanf(value, "%d:", &ts); err == nil && ts > out.newest {
						out.newest = ts
					}
				}
				out.series[key] = append(out.series[key], values...)
			}
		}
	}
	for key, values := range out.series {
		// Go returns head series without samples in the window; they carry no data to compare.
		if len(values) == 0 {
			delete(out.series, key)
			continue
		}
		// A querier merges a series' chunks and drops samples repeated across them.
		sort.Strings(values)
		out.series[key] = dedupe(values)
	}
	return out, nil
}

func dedupe(values []string) []string {
	out := values[:0]
	for i, value := range values {
		if i == 0 || value != values[i-1] {
			out = append(out, value)
		}
	}
	return out
}

func samples(wire client.Chunk, request *client.QueryRequest, counts map[string]int) ([]string, error) {
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
		return nil, fmt.Errorf("unknown wire chunk encoding %d", wire.Encoding)
	}
	decoded, err := chunkenc.FromData(encoding, wire.Data)
	if err != nil {
		return nil, err
	}
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
			return nil, fmt.Errorf("unexpected chunk value type %v", typ)
		}
		if ts >= request.StartTimestampMs && ts <= request.EndTimestampMs {
			counts[kind]++
			values = append(values, fmt.Sprintf("%d:%s", ts, value))
		}
	}
	return values, it.Err()
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

// compare returns the series whose samples differ between the reference and the compared answer,
// described for the report.
func compare(reference, compared map[string][]string) []string {
	var differences []string
	for key, r := range reference {
		c, ok := compared[key]
		switch {
		case !ok:
			differences = append(differences, fmt.Sprintf("only in reference: %s (%d samples)", key, len(r)))
		case strings.Join(r, "\n") != strings.Join(c, "\n"):
			rv, cv := firstDifference(r, c)
			differences = append(differences, fmt.Sprintf("samples differ: %s reference=%d compared=%d\n      reference: %s\n      compared:  %s", key, len(r), len(c), rv, cv))
		}
	}
	for key, c := range compared {
		if _, ok := reference[key]; !ok {
			differences = append(differences, fmt.Sprintf("only in compared: %s (%d samples)", key, len(c)))
		}
	}
	sort.Strings(differences)
	return differences
}

func firstDifference(reference, compared []string) (string, string) {
	for i := 0; i < len(reference) || i < len(compared); i++ {
		var r, c string
		if i < len(reference) {
			r = reference[i]
		}
		if i < len(compared) {
			c = compared[i]
		}
		if r != c {
			return r, c
		}
	}
	return "", ""
}

// union merges the answers of a selector's query shards.
func union(results []result) map[string][]string {
	out := map[string][]string{}
	for _, r := range results {
		for key, values := range r.series {
			out[key] = append(out[key], values...)
		}
	}
	for key, values := range out {
		sort.Strings(values)
		out[key] = dedupe(values)
	}
	return out
}
