// SPDX-License-Identifier: AGPL-3.0-only

package ingesterquerytee

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math"
	"reflect"
	"slices"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

var errMismatch = errors.New("responses differ")

func comparable(method string) bool {
	switch method {
	case "QueryStream", "QueryExemplars", "LabelNames", "LabelValues", "MetricsForLabelMatchers", "MetricsMetadata", "LabelNamesAndValues", "LabelValuesCardinality", "ActiveSeries", "SearchLabelNames", "SearchLabelValues":
		return true
	default:
		return false
	}
}

func compareResponses(method string, req frame, primary, shadow []frame, cfg Config, now time.Time) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("invalid comparison response")
		}
	}()
	if method == "QueryStream" {
		var query client.QueryRequest
		if err := query.Unmarshal(req); err != nil {
			return err
		}
		a, err := querySamples(primary, &query, cfg, now)
		if err != nil {
			return err
		}
		b, err := querySamples(shadow, &query, cfg, now)
		if err != nil {
			return err
		}
		if !equalSamples(a, b) {
			return errMismatch
		}
		return nil
	}
	a, err := responseItems(method, primary)
	if err != nil {
		return err
	}
	b, err := responseItems(method, shadow)
	if err != nil {
		return err
	}
	if !slices.Equal(a, b) {
		return errMismatch
	}
	return nil
}

type sampleKey struct{ timestamp, startTimestamp int64 }
type sampleDigest [sha256.Size]byte
type normalizedSample struct {
	digest sampleDigest
	hint   histogram.CounterResetHint
}
type samplesBySeries map[string]map[sampleKey]normalizedSample

func equalSample(a, b normalizedSample) bool {
	return a.digest == b.digest && (a.hint == b.hint || a.hint == histogram.UnknownCounterReset || b.hint == histogram.UnknownCounterReset)
}

func equalSamples(a, b samplesBySeries) bool {
	if len(a) != len(b) {
		return false
	}
	for labels, samples := range a {
		other, exists := b[labels]
		if !exists || len(samples) != len(other) {
			return false
		}
		for key, sample := range samples {
			otherSample, exists := other[key]
			if !exists || !equalSample(sample, otherSample) {
				return false
			}
		}
	}
	return true
}

func querySamples(frames []frame, req *client.QueryRequest, cfg Config, now time.Time) (out samplesBySeries, err error) {
	// Prometheus chunk iterators assume structurally valid chunk bytes. A corrupt
	// shadow response must fail its comparison rather than terminate the proxy.
	defer func() {
		if recover() != nil {
			out = nil
			err = errors.New("invalid query chunk")
		}
	}()
	out = samplesBySeries{}
	var keys []string
	var expected, received []int64
	ended := false
	count := 0
	end := req.EndTimestampMs
	if cfg.SkipRecentSamples > 0 {
		end = min(end, now.Add(-cfg.SkipRecentSamples).UnixMilli())
	}
	for _, f := range frames {
		var msg client.QueryStreamResponse
		if err := msg.Unmarshal(f); err != nil {
			return nil, err
		}
		if len(msg.StreamingSeries) > 0 && ended {
			return nil, errors.New("series after end marker")
		}
		if len(msg.StreamingSeries) > 0 && len(msg.StreamingSeriesChunks) > 0 {
			return nil, errors.New("mixed series and chunks")
		}
		for _, series := range msg.StreamingSeries {
			if len(keys) >= cfg.MaxSamples {
				return nil, errResponseLimit
			}
			if series.ChunkCount < 0 {
				return nil, errors.New("negative chunk count")
			}
			key := labelKey(series.Labels)
			if _, exists := out[key]; exists {
				return nil, errors.New("duplicate series")
			}
			keys = append(keys, key)
			expected = append(expected, series.ChunkCount)
			received = append(received, 0)
			out[key] = map[sampleKey]normalizedSample{}
		}
		if msg.IsEndOfSeriesStream {
			if ended {
				return nil, errors.New("duplicate end marker")
			}
			ended = true
		}
		for _, series := range msg.StreamingSeriesChunks {
			if !ended || series.SeriesIndex >= uint64(len(keys)) {
				return nil, errors.New("invalid series index or missing end marker")
			}
			received[series.SeriesIndex] += int64(len(series.Chunks))
			for _, wire := range series.Chunks {
				// Mimir's wire encoding numbers 4..9 correspond to Prometheus encodings 1..6.
				if wire.Encoding < 4 || wire.Encoding > 9 {
					return nil, errors.New("unsupported chunk encoding")
				}
				chunk, err := chunkenc.FromData(chunkenc.Encoding(wire.Encoding-3), wire.Data)
				if err != nil {
					return nil, err
				}
				it := chunk.Iterator(nil)
				for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
					count++
					if count > cfg.MaxSamples {
						return nil, errResponseLimit
					}
					t := it.AtT()
					if t < req.StartTimestampMs || t > end {
						continue
					}
					var sample normalizedSample
					if typ == chunkenc.ValFloat {
						_, v := it.At()
						sample.digest = digestValue(struct{ Value float64 }{v})
					} else {
						_, h := it.AtFloatHistogram(nil)
						sample.hint = h.CounterResetHint
						// Unknown reset hints at chunk boundaries can match either counter hint.
						// Gauge identity must remain in the digest so it cannot match a counter.
						if h.CounterResetHint != histogram.GaugeType {
							h.CounterResetHint = histogram.UnknownCounterReset
						}
						sample.digest = digestValue(h.Compact(0))
					}
					key := sampleKey{t, it.AtST()}
					seriesSamples := out[keys[series.SeriesIndex]]
					if previous, exists := seriesSamples[key]; exists {
						if !equalSample(previous, sample) {
							return nil, errors.New("conflicting duplicate sample")
						}
						if sample.hint == histogram.UnknownCounterReset {
							sample.hint = previous.hint
						}
					}
					seriesSamples[key] = sample
				}
				if err := it.Err(); err != nil {
					return nil, err
				}
			}
		}
	}
	if !ended || !slices.Equal(expected, received) {
		return nil, errors.New("incomplete query stream")
	}
	for key, samples := range out {
		if len(samples) == 0 {
			delete(out, key)
		}
	}
	return out, nil
}

func labelKey(labels []mimirpb.LabelAdapter) string {
	labels = slices.Clone(labels)
	slices.SortFunc(labels, func(a, b mimirpb.LabelAdapter) int {
		return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.Value, b.Value))
	})
	var data []byte
	for _, label := range labels {
		data = binary.AppendUvarint(data, uint64(len(label.Name)))
		data = append(data, label.Name...)
		data = binary.AppendUvarint(data, uint64(len(label.Value)))
		data = append(data, label.Value...)
	}
	return string(data)
}

func digestValue(v any) sampleDigest {
	h := sha256.New()
	hashValue(h, reflect.ValueOf(v))
	var digest sampleDigest
	copy(digest[:], h.Sum(nil))
	return digest
}

// Length prefixes preserve distinctions between adjacent strings, fields and slices.
// Float bits also retain stale NaNs, infinities and signed zero without JSON encoding.
func hashValue(h hash.Hash, v reflect.Value) {
	put := func(n uint64) { var b [8]byte; binary.LittleEndian.PutUint64(b[:], n); _, _ = h.Write(b[:]) }
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	put(uint64(v.Kind()))
	switch v.Kind() {
	case reflect.Struct:
		put(uint64(v.NumField()))
		for i := 0; i < v.NumField(); i++ {
			hashValue(h, v.Field(i))
		}
	case reflect.Slice, reflect.Array:
		put(uint64(v.Len()))
		for i := 0; i < v.Len(); i++ {
			hashValue(h, v.Index(i))
		}
	case reflect.Float64:
		put(math.Float64bits(v.Float()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		put(uint64(v.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		put(v.Uint())
	case reflect.String:
		put(uint64(v.Len()))
		_, _ = h.Write([]byte(v.String()))
	default:
		panic(fmt.Sprintf("unsupported comparison type %s", v.Kind()))
	}
}

func responseItems(method string, frames []frame) ([]string, error) {
	switch method {
	case "LabelNames", "LabelValues", "MetricsForLabelMatchers", "MetricsMetadata", "QueryExemplars":
		if len(frames) != 1 {
			return nil, errors.New("invalid unary response count")
		}
	}
	items := make([]string, 0)
	ordered := method == "SearchLabelNames" || method == "SearchLabelValues"
	add := func(v any) { d := digestValue(v); items = append(items, string(d[:])) }
	var warnings []string
	for _, f := range frames {
		var msg proto.Message
		switch method {
		case "LabelNames":
			msg = &client.LabelNamesResponse{}
		case "LabelValues":
			msg = &client.LabelValuesResponse{}
		case "MetricsForLabelMatchers":
			msg = &client.MetricsForLabelMatchersResponse{}
		case "MetricsMetadata":
			msg = &client.MetricsMetadataResponse{}
		case "QueryExemplars":
			msg = &client.ExemplarQueryResponse{}
		case "LabelNamesAndValues":
			msg = &client.LabelNamesAndValuesResponse{}
		case "LabelValuesCardinality":
			msg = &client.LabelValuesCardinalityResponse{}
		case "ActiveSeries":
			msg = &client.ActiveSeriesResponse{}
		case "SearchLabelNames", "SearchLabelValues":
			msg = &client.SearchResultBatch{}
		default:
			return nil, errors.New("unsupported comparison method")
		}
		if err := proto.Unmarshal(f, msg); err != nil {
			return nil, err
		}
		switch r := msg.(type) {
		case *client.LabelNamesResponse:
			for _, name := range r.LabelNames {
				add(name)
			}
		case *client.LabelValuesResponse:
			for _, value := range r.LabelValues {
				add(value)
			}
		case *client.MetricsForLabelMatchersResponse:
			for _, metric := range r.Metric {
				add(labelKey(metric.Labels))
			}
		case *client.MetricsMetadataResponse:
			for _, md := range r.Metadata {
				add(md)
			}
		case *client.ExemplarQueryResponse:
			for _, series := range r.Timeseries {
				for _, exemplar := range series.Exemplars {
					add(struct {
						Series, Labels string
						Timestamp      int64
						Value          float64
					}{labelKey(series.Labels), labelKey(exemplar.Labels), exemplar.TimestampMs, exemplar.Value})
				}
			}
		case *client.LabelNamesAndValuesResponse:
			for _, label := range r.Items {
				for _, value := range label.Values {
					add(struct{ Name, Value string }{label.LabelName, value})
				}
			}
		case *client.LabelValuesCardinalityResponse:
			for _, label := range r.Items {
				for value, count := range label.LabelValueSeries {
					add(struct {
						Name, Value string
						Count       uint64
					}{label.LabelName, value, count})
				}
			}
		case *client.ActiveSeriesResponse:
			if len(r.BucketCount) != 0 && len(r.BucketCount) != len(r.Metric) {
				return nil, errors.New("invalid active series bucket counts")
			}
			for i, metric := range r.Metric {
				var count uint64
				if len(r.BucketCount) > 0 {
					count = r.BucketCount[i]
				}
				add(struct {
					Labels     string
					Count      uint64
					HasBuckets uint8
				}{labelKey(metric.Labels), count, uint8(min(len(r.BucketCount), 1))})
			}
		case *client.SearchResultBatch:
			for _, result := range r.Results {
				add(result)
			}
			warnings = append(warnings, r.Warnings...)
		}
	}
	if !ordered {
		slices.Sort(items)
	} else {
		slices.Sort(warnings)
		for _, warning := range warnings {
			add(warning)
		}
	}
	return items, nil
}
