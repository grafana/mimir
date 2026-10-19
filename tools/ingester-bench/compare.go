// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/storage/chunk"
)

var errTooLarge = errors.New("response too large to compare")

// maxItems bounds what a response is normalized to: each is 8 bytes, the hash of the item.
const maxItems = 20_000_000

// readContext is the tenant's context for a read that each ingester answers once it consumed the offset.
func readContext(ctx context.Context, tenant string, offset int64) context.Context {
	ctx = user.InjectOrgID(ctx, tenant)
	ctx = metadata.AppendToOutgoingContext(ctx, "x-scope-orgid", tenant)
	if offset >= 0 {
		ctx = querierapi.ContextWithReadConsistencyLevel(ctx, querierapi.ReadConsistencyStrong)
		ctx = querierapi.ContextWithReadConsistencyEncodedOffsets(ctx, querierapi.EncodeOffsetsV1(map[int32]int64{0: offset}))
	}
	return ctx
}

// fetch sends the request to the ingester and returns its response messages, all received: the read's latency is the
// fetch's, without what the bench does with them after.
func fetch(ctx context.Context, c client.IngesterClient, req *request) ([]any, error) {
	var out []any
	drain := func(recv func() (any, error)) error {
		for {
			resp, err := recv()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			out = append(out, resp)
		}
	}
	var err error
	switch {
	case req.query != nil:
		var stream client.Ingester_QueryStreamClient
		if stream, err = c.QueryStream(ctx, req.query); err == nil {
			err = drain(func() (any, error) { return stream.Recv() })
		}
	case req.activeSeries != nil:
		var stream client.Ingester_ActiveSeriesClient
		if stream, err = c.ActiveSeries(ctx, req.activeSeries); err == nil {
			err = drain(func() (any, error) { return stream.Recv() })
		}
	case req.namesAndValues != nil:
		var stream client.Ingester_LabelNamesAndValuesClient
		if stream, err = c.LabelNamesAndValues(ctx, req.namesAndValues); err == nil {
			err = drain(func() (any, error) { return stream.Recv() })
		}
	case req.cardinality != nil:
		var stream client.Ingester_LabelValuesCardinalityClient
		if stream, err = c.LabelValuesCardinality(ctx, req.cardinality); err == nil {
			err = drain(func() (any, error) { return stream.Recv() })
		}
	case req.labelValues != nil:
		var resp *client.LabelValuesResponse
		if resp, err = c.LabelValues(ctx, req.labelValues); err == nil {
			out = append(out, resp)
		}
	case req.labelNames != nil:
		var resp *client.LabelNamesResponse
		if resp, err = c.LabelNames(ctx, req.labelNames); err == nil {
			out = append(out, resp)
		}
	case req.metricsFor != nil:
		var resp *client.MetricsForLabelMatchersResponse
		if resp, err = c.MetricsForLabelMatchers(ctx, req.metricsFor); err == nil {
			out = append(out, resp)
		}
	case req.exemplars != nil:
		var resp *client.ExemplarQueryResponse
		if resp, err = c.QueryExemplars(ctx, req.exemplars); err == nil {
			out = append(out, resp)
		}
	case req.userStats != nil:
		var resp *client.UserStatsResponse
		if resp, err = c.UserStats(ctx, req.userStats); err == nil {
			out = append(out, resp)
		}
	}
	return out, err
}

// normalize returns the response's items as their sorted hashes, comparable with another's: the items of the largest
// responses, by many reads at once, would not fit in memory as text. The items whose hashes explain has are kept as
// text in it, to show what differs.
func normalize(req *request, responses []any, explain map[uint64]string) ([]uint64, error) {
	var items []uint64
	add := func(parts ...string) error {
		if len(items) >= maxItems {
			return errTooLarge
		}
		var h maphash.Hash
		h.SetSeed(seedHash)
		for _, part := range parts {
			h.WriteString(part)
			h.WriteByte(0)
		}
		sum := h.Sum64()
		items = append(items, sum)
		if _, ok := explain[sum]; ok {
			explain[sum] = strings.Join(parts, " ")
		}
		return nil
	}
	var keys []string
	for _, response := range responses {
		var err error
		switch r := response.(type) {
		case *client.QueryStreamResponse:
			for _, s := range r.StreamingSeries {
				keys = append(keys, labelsKey(s.Labels))
			}
			for _, s := range r.StreamingSeriesChunks {
				if s.SeriesIndex >= uint64(len(keys)) {
					return nil, fmt.Errorf("series index %d of %d", s.SeriesIndex, len(keys))
				}
				key := keys[s.SeriesIndex]
				for _, wire := range s.Chunks {
					// Series without chunks in the range read as nothing, as a querier sees them.
					if err = chunkSamples(wire, req.query.StartTimestampMs, req.query.EndTimestampMs, func(sample string) error { return add(key, sample) }); err != nil {
						return nil, err
					}
				}
			}
		case *client.ActiveSeriesResponse:
			for _, m := range r.Metric {
				if err = add(labelsKey(m.Labels)); err != nil {
					return nil, err
				}
			}
		case *client.LabelNamesAndValuesResponse:
			for _, item := range r.Items {
				for _, v := range item.Values {
					if err = add(item.LabelName, v); err != nil {
						return nil, err
					}
				}
			}
		case *client.LabelValuesCardinalityResponse:
			for _, item := range r.Items {
				for v, count := range item.LabelValueSeries {
					if err = add(item.LabelName, v, strconv.FormatUint(count, 10)); err != nil {
						return nil, err
					}
				}
			}
		case *client.LabelValuesResponse:
			for _, v := range r.LabelValues {
				if err = add(v); err != nil {
					return nil, err
				}
			}
		case *client.LabelNamesResponse:
			for _, v := range r.LabelNames {
				if err = add(v); err != nil {
					return nil, err
				}
			}
		case *client.MetricsForLabelMatchersResponse:
			for _, m := range r.Metric {
				if err = add(labelsKey(m.Labels)); err != nil {
					return nil, err
				}
			}
		case *client.ExemplarQueryResponse:
			for _, ts := range r.Timeseries {
				for _, e := range ts.Exemplars {
					if err = add(labelsKey(ts.Labels), labelsKey(e.Labels), strconv.FormatInt(e.TimestampMs, 10), strconv.FormatUint(math.Float64bits(e.Value), 16)); err != nil {
						return nil, err
					}
				}
			}
		case *client.UserStatsResponse:
			if err = add(strconv.FormatUint(r.NumSeries, 10)); err != nil {
				return nil, err
			}
		}
	}
	slices.Sort(items)
	// A series' sample in two chunks, such as a compacted block's and the head's, is one sample to a querier.
	return slices.Compact(items), nil
}

// read fetches the request and normalizes its response.
func read(ctx context.Context, c client.IngesterClient, req *request, explain map[uint64]string) ([]uint64, error) {
	responses, err := fetch(ctx, c, req)
	if err != nil {
		return nil, err
	}
	return normalize(req, responses, explain)
}

// chunkSamples calls f with each sample of the chunk in [start, end], as its timestamp and exact value.
func chunkSamples(wire client.Chunk, start, end int64, f func(string) error) error {
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
		return fmt.Errorf("unknown chunk encoding %d", wire.Encoding)
	}
	c, err := chunkenc.FromData(encoding, wire.Data)
	if err != nil {
		return err
	}
	it := c.Iterator(nil)
	for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
		t := it.AtT()
		if t < start || t > end {
			continue
		}
		var v string
		switch typ {
		case chunkenc.ValFloat:
			_, x := it.At()
			v = "f" + strconv.FormatUint(math.Float64bits(x), 16)
		default:
			_, h := it.AtFloatHistogram(nil)
			// Counter reset hints depend on chunk boundaries, which differ between engines.
			if h.CounterResetHint != histogram.GaugeType {
				h.CounterResetHint = histogram.UnknownCounterReset
			}
			v = "h" + h.Compact(0).String()
		}
		if err := f(strconv.FormatInt(t, 10) + "\x00" + v); err != nil {
			return err
		}
	}
	return it.Err()
}

func labelsKey(labels []mimirpb.LabelAdapter) string {
	labels = slices.Clone(labels)
	sort.Slice(labels, func(a, b int) bool { return labels[a].Name < labels[b].Name })
	var b strings.Builder
	for _, l := range labels {
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteString(l.Value)
		b.WriteByte(',')
	}
	return b.String()
}

// difference returns the items one response has that the other doesn't, as 1 when only a's has them and -1 when
// only b's does.
func difference(a, b []uint64) map[uint64]int {
	out := map[uint64]int{}
	i, j := 0, 0
	for i < len(a) || j < len(b) {
		switch {
		case j == len(b) || (i < len(a) && a[i] < b[j]):
			out[a[i]] = 1
			i++
		case i == len(a) || b[j] < a[i]:
			out[b[j]] = -1
			j++
		default:
			i++
			j++
		}
	}
	return out
}

// outcome is what one read of both ingesters gave.
type outcome struct {
	api      string
	latency  [2]time.Duration
	errs     [2]error
	items    int
	diff     map[uint64]int
	tooLarge bool
}
