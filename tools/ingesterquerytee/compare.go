// SPDX-License-Identifier: AGPL-3.0-only

package ingesterquerytee

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"maps"
	"math"
	"reflect"
	"slices"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/batch"
	storagechunk "github.com/grafana/mimir/pkg/storage/chunk"
)

var errMismatch = errors.New("responses differ")

// errBeyondRetention is for QueryStream responses that differ only in samples just past the retention period.
var errBeyondRetention = errors.New("responses differ beyond the retention period")

func comparable(method string) bool {
	switch method {
	case "QueryStream", "QueryExemplars", "LabelNames", "LabelValues", "MetricsForLabelMatchers", "MetricsMetadata", "LabelNamesAndValues", "LabelValuesCardinality", "ActiveSeries", "SearchLabelNames", "SearchLabelValues", "NativeMetricMetadata", "NativeResources":
		return true
	default:
		return false
	}
}

func compareResponses(method string, req frame, primary, shadow []frame, cfg Config, now time.Time) error {
	return compareResponsesSkipping(method, req, primary, shadow, cfg, now, nil)
}

// compareResponsesSkipping is compareResponses that also sets skipped, unless it's nil, to how many series of a native
// metadata lookup it left out of the comparison, as either backend coalesced their changes.
func compareResponsesSkipping(method string, req frame, primary, shadow []frame, cfg Config, now time.Time, skipped *int) (err error) {
	backend := "request"
	defer func() {
		if recover() != nil {
			err = errors.New("invalid comparison response")
		}
		if err != nil && !errors.Is(err, errMismatch) && !errors.Is(err, errResponseLimit) && !errors.Is(err, errBeyondRetention) {
			err = &comparisonError{backend: backend, err: err}
		}
	}()
	if method == "QueryStream" {
		var query client.QueryRequest
		if err := query.Unmarshal(req); err != nil {
			return err
		}
		backend = "primary"
		a, err := querySamples(primary, &query, cfg, now)
		if err != nil {
			return err
		}
		backend = "shadow"
		b, err := querySamples(shadow, &query, cfg, now)
		if err != nil {
			return err
		}
		backend = "comparison"
		if !equalSamples(a, b) {
			mismatch := describeSampleMismatch(a, b)
			mismatch.items = sampleDifferences(a, b)
			return mismatch
		}
		if from, to := retentionEdge(cfg, now); cfg.RetentionPeriod <= 0 || query.StartTimestampMs >= to || query.EndTimestampMs < from {
			return nil
		}
		// Ingesters delete a block past the retention once it was also written longer than the retention ago, and
		// the ones written by early compactions follow restarts and ring changes, so their samples there differ.
		all := cfg
		all.RetentionPeriod = 0
		backend = "primary"
		if a, err = querySamples(primary, &query, all, now); err != nil {
			return err
		}
		backend = "shadow"
		if b, err = querySamples(shadow, &query, all, now); err != nil {
			return err
		}
		if !equalSamples(a, b) {
			return errBeyondRetention
		}
		return nil
	}
	if isNativeLookup(method) {
		backend = "primary"
		a, err := decodeNativeLookup(method, primary)
		if err != nil {
			return err
		}
		backend = "shadow"
		b, err := decodeNativeLookup(method, shadow)
		if err != nil {
			return err
		}
		backend = "comparison"
		itemsA, itemsB, n := nativeLookupItems(a, b)
		if skipped != nil {
			*skipped = n
		}
		if !slices.Equal(itemsA, itemsB) {
			return &mismatchError{fields: []any{"primary_items", len(itemsA), "shadow_items", len(itemsB)}, items: itemDifference(itemsA, itemsB)}
		}
		return nil
	}
	backend = "primary"
	a, err := responseItems(method, primary)
	if err != nil {
		return err
	}
	backend = "shadow"
	b, err := responseItems(method, shadow)
	if err != nil {
		return err
	}
	backend = "comparison"
	if limit := requestLimit(method, req); limit > 0 && (len(a) >= limit || len(b) >= limit) {
		// An ingester returns any limit of the values or names when there are more: each engine's own, which aren't
		// comparable.
		return errResponseLimit
	}
	if !slices.Equal(a, b) {
		return &mismatchError{fields: []any{"primary_items", len(a), "shadow_items", len(b)}, items: itemDifference(a, b)}
	}
	return nil
}

// requestLimit is the limit a label values or names request sets on its response, 0 for none.
func requestLimit(method string, req frame) int {
	switch method {
	case "LabelValues":
		var request client.LabelValuesRequest
		if request.Unmarshal(req) == nil {
			return int(request.Limit)
		}
	case "LabelNames":
		var request client.LabelNamesRequest
		if request.Unmarshal(req) == nil {
			return int(request.Limit)
		}
	}
	return 0
}

// itemDifference returns the items of a or b that the other doesn't have, as many times as they are missing.
func itemDifference(a, b []string) map[string]int {
	counts := map[string]int{}
	for _, item := range a {
		counts[item]++
	}
	for _, item := range b {
		counts[item]--
	}
	for item, count := range counts {
		if count == 0 {
			delete(counts, item)
		}
	}
	return counts
}

type comparisonError struct {
	backend string
	err     error
}

func (e *comparisonError) Error() string { return e.err.Error() }
func (e *comparisonError) Unwrap() error { return e.err }

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

type mismatchError struct {
	fields     []any
	difference *sampleDifference
	// The differing items of a comparison of items, by how many more the primary has, or sampleDifferences.
	items map[string]int
}

func (*mismatchError) Error() string { return errMismatch.Error() }
func (*mismatchError) Unwrap() error { return errMismatch }

// sampleDifferences returns the samples of a or b that the other doesn't have, keyed by series and timestamps, as 1 when
// the shadow misses them, -1 when only the shadow has them, and 2 when their values differ.
func sampleDifferences(a, b samplesBySeries) map[string]int {
	differences := map[string]int{}
	key := func(labels string, sample sampleKey) string {
		return labels + string(binary.LittleEndian.AppendUint64(binary.LittleEndian.AppendUint64(nil, uint64(sample.timestamp)), uint64(sample.startTimestamp)))
	}
	for labels, samples := range a {
		for k, sample := range samples {
			if other, exists := b[labels][k]; !exists {
				differences[key(labels, k)] = 1
			} else if !equalSample(sample, other) {
				differences[key(labels, k)] = 2
			}
		}
	}
	for labels, samples := range b {
		for k := range samples {
			if _, exists := a[labels][k]; !exists {
				differences[key(labels, k)] = -1
			}
		}
	}
	return differences
}

// differences returns the items or samples of the responses that differ, as itemDifference or sampleDifferences.
func differences(method string, req frame, primary, shadow []frame, cfg Config, start time.Time) (map[string]int, error) {
	if isNativeLookup(method) {
		a, err := decodeNativeLookup(method, primary)
		if err != nil {
			return nil, err
		}
		b, err := decodeNativeLookup(method, shadow)
		if err != nil {
			return nil, err
		}
		// The retries' responses need the same pre-pass as the first ones, or every restart's differences persist.
		itemsA, itemsB, _ := nativeLookupItems(a, b)
		return itemDifference(itemsA, itemsB), nil
	}
	if method != "QueryStream" {
		a, err := responseItems(method, primary)
		if err != nil {
			return nil, err
		}
		b, err := responseItems(method, shadow)
		if err != nil {
			return nil, err
		}
		return itemDifference(a, b), nil
	}
	var query client.QueryRequest
	if err := query.Unmarshal(req); err != nil {
		return nil, err
	}
	a, err := querySamples(primary, &query, cfg, start)
	if err != nil {
		return nil, err
	}
	b, err := querySamples(shadow, &query, cfg, start)
	if err != nil {
		return nil, err
	}
	return sampleDifferences(a, b), nil
}

func describeSampleMismatch(a, b samplesBySeries) *mismatchError {
	var primaryLabels, shadowLabels, primarySamples, shadowSamples int
	var missingSeries, extraSeries, missingSamples, extraSamples, differentValues int
	labelCount := func(key string) int {
		data := []byte(key)
		count := 0
		for len(data) > 0 {
			for range 2 {
				n, size := binary.Uvarint(data)
				data = data[size+int(n):]
			}
			count++
		}
		return count
	}
	for labels, samples := range a {
		primaryLabels += labelCount(labels)
		primarySamples += len(samples)
		other, exists := b[labels]
		if !exists {
			missingSeries++
		}
		for key, sample := range samples {
			otherSample, exists := other[key]
			if !exists {
				missingSamples++
			} else if !equalSample(sample, otherSample) {
				differentValues++
			}
		}
	}
	for labels, samples := range b {
		shadowLabels += labelCount(labels)
		shadowSamples += len(samples)
		other, exists := a[labels]
		if !exists {
			extraSeries++
		}
		for key := range samples {
			if _, exists := other[key]; !exists {
				extraSamples++
			}
		}
	}
	return &mismatchError{difference: firstSampleDifference(a, b), fields: []any{
		"primary_series", len(a), "shadow_series", len(b),
		"primary_labels", primaryLabels, "shadow_labels", shadowLabels,
		"primary_samples", primarySamples, "shadow_samples", shadowSamples,
		"series_missing_from_shadow", missingSeries, "series_extra_in_shadow", extraSeries,
		"samples_missing_from_shadow", missingSamples, "samples_extra_in_shadow", extraSamples,
		"samples_with_different_values", differentValues,
	}}
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

// retentionEdge is the range of samples that only some of the ingesters' in-order blocks past the retention hold:
// from -retention-edge before the retention to the retention, from which both have all of their samples.
func retentionEdge(cfg Config, now time.Time) (from, to int64) {
	return now.Add(-cfg.RetentionPeriod - cfg.RetentionEdge).UnixMilli(), now.Add(-cfg.RetentionPeriod).UnixMilli()
}

func querySamples(frames []frame, req *client.QueryRequest, cfg Config, now time.Time) (out samplesBySeries, err error) {
	// Corrupt chunk bytes must fail the comparison rather than terminate the proxy.
	defer func() {
		if recover() != nil {
			out = nil
			err = errors.New("invalid query chunk")
		}
	}()
	out = samplesBySeries{}
	if len(frames) == 0 {
		return out, nil
	}
	type capturedSeries struct {
		key    string
		labels promlabels.Labels
		chunks []storagechunk.Chunk
	}
	var series []capturedSeries
	ended, count := false, 0
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
		for _, s := range msg.StreamingSeries {
			if len(series) >= cfg.MaxSamples {
				return nil, errResponseLimit
			}
			if s.ChunkCount < 0 {
				return nil, errors.New("negative chunk count")
			}
			key := labelKey(s.Labels)
			if _, exists := out[key]; exists {
				return nil, errors.New("duplicate series")
			}
			out[key] = map[sampleKey]normalizedSample{}
			series = append(series, capturedSeries{key: key, labels: mimirpb.FromLabelAdaptersToLabels(s.Labels)})
		}
		if msg.IsEndOfSeriesStream {
			if ended {
				return nil, errors.New("duplicate end marker")
			}
			ended = true
		}
		for _, s := range msg.StreamingSeriesChunks {
			if !ended || s.SeriesIndex >= uint64(len(series)) {
				return nil, errors.New("invalid series index or missing end marker")
			}
			for _, w := range s.Chunks {
				if w.Encoding < 4 || w.Encoding > 9 {
					return nil, errors.New("unsupported chunk encoding")
				}
				c, err := chunkenc.FromData(chunkenc.Encoding(w.Encoding-3), w.Data)
				if err != nil {
					return nil, err
				}
				// Raw samples still consume the limit even when merging removes duplicates.
				it := c.Iterator(nil)
				for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
					count++
					if count > cfg.MaxSamples {
						return nil, errResponseLimit
					}
				}
				if err := it.Err(); err != nil {
					return nil, err
				}
				decoded, err := storagechunk.NewForEncoding(storagechunk.Encoding(w.Encoding))
				if err != nil {
					return nil, err
				}
				if err = decoded.UnmarshalFromBuf(w.Data); err != nil {
					return nil, err
				}
				item := &series[s.SeriesIndex]
				item.chunks = append(item.chunks, storagechunk.NewChunk(item.labels, decoded, model.Time(w.StartTimestampMs), model.Time(w.EndTimestampMs)))
			}
		}
	}
	if !ended {
		return nil, errors.New("incomplete query stream")
	}
	end := req.EndTimestampMs
	if cfg.SkipRecentSamples > 0 {
		end = min(end, now.Add(-cfg.SkipRecentSamples).UnixMilli())
	}
	edgeFrom, edgeTo := int64(math.MaxInt64), int64(math.MinInt64)
	if cfg.RetentionPeriod > 0 {
		edgeFrom, edgeTo = retentionEdge(cfg, now)
	}
	for _, s := range series {
		if len(s.chunks) == 0 {
			delete(out, s.key)
			continue
		}
		// QueryStream chunks can overlap; comparisons must use the querier's conflict resolution.
		it := batch.NewChunkMergeIterator(nil, s.labels, s.chunks)
		for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
			timestamp := it.AtT()
			if timestamp < req.StartTimestampMs || timestamp > end || (timestamp >= edgeFrom && timestamp < edgeTo) {
				continue
			}
			var sample normalizedSample
			if typ == chunkenc.ValFloat {
				_, v := it.At()
				sample.digest = digestValue(struct{ Value float64 }{v})
			} else {
				_, h := it.AtFloatHistogram(nil)
				// Compaction mutates iterator-owned slices.
				h = h.Copy()
				sample.hint = h.CounterResetHint
				if h.CounterResetHint != histogram.GaugeType {
					h.CounterResetHint = histogram.UnknownCounterReset
				}
				sample.digest = digestValue(h.Compact(0))
			}
			key := sampleKey{timestamp, it.AtST()}
			if previous, exists := out[s.key][key]; exists {
				if !equalSample(previous, sample) {
					return nil, errors.New("conflicting duplicate sample")
				}
				if sample.hint == histogram.UnknownCounterReset {
					sample.hint = previous.hint
				}
			}
			out[s.key][key] = sample
		}
		if err := it.Err(); err != nil {
			return nil, err
		}
		if len(out[s.key]) == 0 {
			delete(out, s.key)
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

// isNativeLookup reports whether the method is a native metadata lookup, whose responses only compare after a pre-pass
// over both of them: see nativeLookupItems.
func isNativeLookup(method string) bool {
	return method == "NativeMetricMetadata" || method == "NativeResources"
}

// nativeLookup is a native metadata lookup's response: when its ingester's native metadata starts, and its series, or
// targets, by labels.
type nativeLookup struct {
	coverageStart int64
	series        map[string]*nativeSeries
}

// nativeSeries is a series' versions as change points, oldest first, with neighbours of the same content merged.
type nativeSeries struct {
	points []nativePoint
	// The series' newest sample, which its last version is in force until.
	newest               int64
	truncated, coalesced bool
}

// nativePoint is when a version starts, and a digest of its content.
type nativePoint struct {
	time    int64
	content sampleDigest
}

// add appends a version, unless it continues the last one: an engine can start a version on a change that the compared
// content leaves out, such as of an attribute's split into identifying and descriptive.
func (s *nativeSeries) add(minTime, maxTime int64, content sampleDigest) {
	s.newest = maxTime
	if n := len(s.points); n > 0 && s.points[n-1].content == content {
		return
	}
	s.points = append(s.points, nativePoint{time: minTime, content: content})
}

// clampFrom leaves out the versions that ended before from, and starts the one in force then at from. It reports
// whether one was: none is when the series' newest sample is from or older, which an engine whose coverage starts at
// from didn't observe.
func (s *nativeSeries) clampFrom(from int64) bool {
	if s.newest <= from {
		return false
	}
	i := 0
	for i+1 < len(s.points) && s.points[i+1].time <= from {
		i++
	}
	s.points = s.points[i:]
	s.points[0].time = max(s.points[0].time, from)
	return true
}

// decodeNativeLookup decodes a native metadata lookup's response, with each version's content as its flat attributes:
// the type, unit and help, or the resource's identifying and descriptive attributes together. It keeps the coverage
// start of the first batch, which has the lookup's, but not limited or series_visited, and of the versions' ends only
// a series' last, to tell whether it has samples past the cutover: they differ for as long as the backends consume
// their partition at different offsets.
func decodeNativeLookup(method string, frames []frame) (nativeLookup, error) {
	if len(frames) == 0 {
		return nativeLookup{}, errors.New("missing native lookup batch")
	}
	out := nativeLookup{series: map[string]*nativeSeries{}}
	add := func(labels []mimirpb.LabelAdapter, s *nativeSeries) error {
		key := labelKey(labels)
		if _, exists := out.series[key]; exists {
			return errors.New("duplicate series")
		}
		// A series without versions has none in force, as when it's missing.
		if len(s.points) > 0 {
			out.series[key] = s
		}
		return nil
	}
	for i, f := range frames {
		switch method {
		case "NativeMetricMetadata":
			var msg client.NativeMetricMetadataResponse
			if err := msg.Unmarshal(f); err != nil {
				return nativeLookup{}, err
			}
			if i == 0 {
				out.coverageStart = msg.CoverageStartMs
			}
			for _, series := range msg.Series {
				s := &nativeSeries{truncated: series.Truncated, coalesced: series.Coalesced}
				for _, v := range series.Versions {
					s.add(v.MinTimeMs, v.MaxTimeMs, digestValue(struct {
						Type       int32
						Unit, Help string
					}{int32(v.Type), v.Unit, v.Help}))
				}
				if err := add(series.Labels, s); err != nil {
					return nativeLookup{}, err
				}
			}
		case "NativeResources":
			var msg client.NativeResourcesResponse
			if err := msg.Unmarshal(f); err != nil {
				return nativeLookup{}, err
			}
			if i == 0 {
				out.coverageStart = msg.CoverageStartMs
			}
			for _, series := range msg.Series {
				s := &nativeSeries{coalesced: series.Coalesced}
				for _, v := range series.Versions {
					attributes := make(map[string]string, len(v.Identifying)+len(v.Descriptive))
					maps.Copy(attributes, v.Descriptive)
					maps.Copy(attributes, v.Identifying)
					flat := make([]string, 0, 2*len(attributes))
					for _, name := range slices.Sorted(maps.Keys(attributes)) {
						flat = append(flat, name, attributes[name])
					}
					s.add(v.MinTimeMs, v.MaxTimeMs, digestValue(flat))
				}
				if err := add(series.Labels, s); err != nil {
					return nativeLookup{}, err
				}
			}
		}
	}
	return out, nil
}

// nativeLookupItems is the pre-pass that makes two native metadata lookups' responses comparable, which it changes, and
// then each one's change points as items. It also returns how many series it left out, as either response coalesced
// their changes: each engine does so over its own limits.
func nativeLookupItems(a, b nativeLookup) (itemsA, itemsB []string, skipped int) {
	lookups := [2]nativeLookup{a, b}
	// Both compare from the cutover, the later coverage start: an engine that reopened has none of its native metadata
	// from before, which it doesn't persist.
	cutover := max(a.coverageStart, b.coverageStart)
	later := -1
	if a.coverageStart > b.coverageStart {
		later = 0
	} else if b.coverageStart > a.coverageStart {
		later = 1
	}
	keys := map[string]struct{}{}
	for _, lookup := range lookups {
		for key := range lookup.series {
			keys[key] = struct{}{}
		}
	}
	for key := range keys {
		sides := [2]*nativeSeries{a.series[key], b.series[key]}
		if (sides[0] != nil && sides[0].coalesced) || (sides[1] != nil && sides[1].coalesced) {
			skipped++
			delete(a.series, key)
			delete(b.series, key)
			continue
		}
		// A series truncated on either side compares from its oldest version there: the TSDB keeps fewer versions per
		// series than the seriesstore engine.
		from := cutover
		for _, s := range sides {
			if s != nil && s.truncated {
				from = max(from, s.points[0].time)
			}
		}
		for i, s := range sides {
			if s != nil && !s.clampFrom(from) {
				delete(lookups[i].series, key)
				sides[i] = nil
			}
		}
		if later < 0 || sides[0] == nil || sides[1] == nil {
			continue
		}
		// On the side whose coverage starts later, a series' first change point is at its first sample since, while the
		// other side has the version from before: when that has the same content, the first change point starts at the
		// cutover too.
		if first, other := &sides[later].points[0], sides[1-later].points[0]; first.time > from && other.time == from && first.content == other.content {
			first.time = from
		}
	}
	return a.items(), b.items(), skipped
}

// items are the lookup's change points, as digests of their series' labels, start and content.
func (l nativeLookup) items() []string {
	items := make([]string, 0)
	for labels, s := range l.series {
		for _, p := range s.points {
			d := digestValue(struct {
				Labels  string
				Time    int64
				Content sampleDigest
			}{labels, p.time, p.content})
			items = append(items, string(d[:]))
		}
	}
	slices.Sort(items)
	return items
}
