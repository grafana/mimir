// SPDX-License-Identifier: AGPL-3.0-only

package record

import (
	"bufio"
	"compress/gzip"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// The canonical form the Rust golden generator prints for a decoded record.
func canonical(request DecodedRequest, spans []Span) string {
	var out strings.Builder
	fmt.Fprintf(&out, "source=%d", request.Source)
	for i, series := range request.Series {
		out.WriteString(" series[")
		for _, pair := range series.Labels {
			fmt.Fprintf(&out, "l=%x:%x,", pair[0], pair[1])
		}
		for _, sample := range series.Samples {
			fmt.Fprintf(&out, "s=%d:%016x,", sample.TimestampMs, math.Float64bits(sample.Value))
		}
		for _, histogram := range series.Histograms {
			encoded, err := histogram.Marshal()
			if err != nil {
				panic(err)
			}
			fmt.Fprintf(&out, "h=%x,", encoded)
		}
		for _, exemplar := range series.Exemplars {
			encoded, err := exemplar.Marshal()
			if err != nil {
				panic(err)
			}
			fmt.Fprintf(&out, "e=%x,", encoded)
		}
		fmt.Fprintf(&out, "c=%d,", series.CreatedTimestamp)
		if span := spans[i]; span.OK {
			fmt.Fprintf(&out, "span=%d-%d", span.Start, span.End)
		} else {
			out.WriteString("span=none")
		}
		out.WriteByte(']')
	}
	for _, metadata := range request.Metadata {
		encoded, err := metadata.Marshal()
		if err != nil {
			panic(err)
		}
		fmt.Fprintf(&out, " m=%x", encoded)
	}
	return out.String()
}

// Records the Rust ingester decoded, generated like its own decoding test does: valid,
// truncated and corrupted records, which must fail or decode the same.
func TestVersion1RecordsDecodeLikeRust(t *testing.T) {
	file, err := os.Open("testdata/rust-decoded-records.txt.gz")
	require.NoError(t, err)
	defer file.Close()
	reader, err := gzip.NewReader(file)
	require.NoError(t, err)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(nil, 16<<20)
	compared, decoded := 0, 0
	for scanner.Scan() {
		encoded, expected, _ := strings.Cut(scanner.Text(), " ")
		record, err := hex.DecodeString(encoded)
		require.NoError(t, err)
		request, spans, err := DecodeRecordWithLabelSpans(1, record)
		actual := "error"
		if err == nil {
			actual = canonical(request, spans)
			decoded++
			// A series' label bytes alone decode to its labels.
			for i, span := range spans {
				if span.OK {
					alone, _, err := decodeSeries(record, record[span.Start:span.End])
					require.NoError(t, err)
					require.Equal(t, request.Series[i].Labels, alone.Labels)
				}
			}
		}
		require.Equal(t, expected, actual, "record %x", record)
		compared++
	}
	require.NoError(t, scanner.Err())
	require.Greater(t, compared, 1000)
	require.Greater(t, decoded, 300)
}

type random uint64

func (r *random) next() uint64 {
	*r ^= *r << 13
	*r ^= *r >> 7
	*r ^= *r << 17
	return uint64(*r)
}

func (r *random) below(bound uint64) uint64 { return r.next() % bound }

func (r *random) text() string {
	alphabet := []string{"a", "_", "9", "é", "\xff", ""}
	var out strings.Builder
	for n := r.below(6); n > 0; n-- {
		out.WriteString(alphabet[r.below(6)])
	}
	return out.String()
}

func randomRequest(r *random) mimirpb.WriteRequest {
	var request mimirpb.WriteRequest
	for n := r.below(4); n > 0; n-- {
		var series mimirpb.TimeSeries
		for m := r.below(5); m > 0; m-- {
			series.Labels = append(series.Labels, mimirpb.LabelAdapter{Name: r.text(), Value: r.text()})
		}
		for m := r.below(3); m > 0; m-- {
			series.Samples = append(series.Samples, mimirpb.Sample{Value: float64(r.below(1000)) - 500.5, TimestampMs: int64(r.next())})
		}
		for m := r.below(2); m > 0; m-- {
			series.Exemplars = append(series.Exemplars, mimirpb.Exemplar{
				Labels: []mimirpb.LabelAdapter{{Name: r.text(), Value: r.text()}}, Value: 1.5, TimestampMs: int64(r.below(100)),
			})
		}
		for m := r.below(2); m > 0; m-- {
			series.Histograms = append(series.Histograms, mimirpb.Histogram{
				Sum: 2, Schema: 3, PositiveDeltas: []int64{1, -1, int64(r.below(9))}, Timestamp: int64(r.below(100)),
				Count: &mimirpb.Histogram_CountInt{CountInt: r.below(9)},
			})
		}
		series.CreatedTimestamp = int64(r.below(3))
		request.Timeseries = append(request.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &series})
	}
	request.Source = mimirpb.WriteRequest_SourceEnum(r.below(3))
	return request
}

func lossyPairs(pairs []mimirpb.LabelAdapter) [][2]string {
	out := make([][2]string, 0, len(pairs))
	for _, pair := range pairs {
		out = append(out, [2]string{Label([]byte(pair.Name)), Label([]byte(pair.Value))})
	}
	return out
}

// Valid records decode to what was encoded; truncated and corrupted ones fail or decode, never
// panic.
func TestRandomRecordsRoundTrip(t *testing.T) {
	r := random(0x9e3779b97f4a7c15)
	for range 3000 {
		request := randomRequest(&r)
		record, err := request.Marshal()
		require.NoError(t, err)
		decoded, spans, err := DecodeRecordWithLabelSpans(1, record)
		require.NoError(t, err)
		require.Equal(t, int32(request.Source), decoded.Source)
		require.Len(t, decoded.Series, len(request.Timeseries))
		for i, series := range request.Timeseries {
			got := decoded.Series[i]
			require.Equal(t, lossyPairs(series.Labels), nonNil(got.Labels))
			require.Equal(t, len(series.Samples), len(got.Samples))
			for j := range series.Samples {
				require.Equal(t, math.Float64bits(series.Samples[j].Value), math.Float64bits(got.Samples[j].Value))
				require.Equal(t, series.Samples[j].TimestampMs, got.Samples[j].TimestampMs)
			}
			require.Equal(t, len(series.Exemplars), len(got.Exemplars))
			for j := range series.Exemplars {
				require.Equal(t, series.Exemplars[j], got.Exemplars[j])
			}
			require.Equal(t, len(series.Histograms), len(got.Histograms))
			for j := range series.Histograms {
				require.True(t, series.Histograms[j].Equal(got.Histograms[j]))
			}
			require.Equal(t, series.CreatedTimestamp, got.CreatedTimestamp)
			require.Equal(t, len(series.Labels) > 0, spans[i].OK)
		}
		for range 3 {
			if len(record) == 0 {
				continue
			}
			_, _, _ = DecodeRecordWithLabelSpans(1, record[:r.below(uint64(len(record)))])
			corrupted := append([]byte(nil), record...)
			corrupted[r.below(uint64(len(record)))] ^= 1 << r.below(8)
			_, _, _ = DecodeRecordWithLabelSpans(1, corrupted)
		}
	}
}

func nonNil(pairs [][2]string) [][2]string {
	if pairs == nil {
		return [][2]string{}
	}
	return pairs
}

func TestLabelsShareOneCopyOfTheRecordAndReplaceInvalidUTF8(t *testing.T) {
	var request mimirpb.WriteRequest
	for index := range 20 {
		request.Timeseries = append(request.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: fmt.Sprintf("metric_%d", index)}, {Name: "job", Value: "api"}},
		}})
	}
	request.Timeseries = append(request.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
		Labels: []mimirpb.LabelAdapter{{Name: "bad", Value: "a\xffb"}},
	}})
	encoded, err := request.Marshal()
	require.NoError(t, err)
	decoded, err := DecodeRecord(1, encoded)
	require.NoError(t, err)
	require.Equal(t, [2]string{"__name__", "metric_3"}, decoded.Series[3].Labels[0])
	require.Equal(t, "a�b", decoded.Series[20].Labels[0][1])
	var lowest, highest uintptr = math.MaxUint64 >> 1, 0
	for _, series := range decoded.Series[:20] {
		for _, pair := range series.Labels {
			for _, s := range pair {
				address := uintptr(unsafe.Pointer(unsafe.StringData(s)))
				lowest, highest = min(lowest, address), max(highest, address)
			}
		}
	}
	require.Less(t, int(highest-lowest), len(encoded), "labels were copied one by one")
}

func TestInvalidUTF8IsReplacedLikeRust(t *testing.T) {
	for input, expected := range map[string]string{
		"a\xffb":           "a�b",
		"a\xff\xffb":       "a��b",
		"a\xe2\x82b":       "a�b",
		"\xed\xa0\x80":     "���",
		"\xf0\x9f\x98":     "�",
		"\xf0\x9f\x98\x80": "\U0001F600",
		"\xc0\xaf":         "��",
		"\xf4\x90\x80\x80": "����",
		"\xe0\x80\x80":     "���",
		"é\x80":            "é�",
	} {
		require.Equal(t, expected, Label([]byte(input)), "%q", input)
	}
}

func TestVersion2RecordsResolveSymbols(t *testing.T) {
	symbols := []string{"requests", "api", "trace_id", "abc", "help text", "seconds"}
	ref := func(symbol string) uint32 {
		for i, s := range symbols {
			if s == symbol {
				return uint32(i + symbolOffset)
			}
		}
		for i, s := range commonSymbols {
			if s == symbol {
				return uint32(i)
			}
		}
		panic(symbol)
	}
	request := mimirpb.WriteRequest{
		Source:     mimirpb.RULE,
		SymbolsRW2: symbols,
		TimeseriesRW2: []mimirpb.TimeSeriesRW2{{
			LabelsRefs:       []uint32{ref("__name__"), ref("requests"), ref("job"), ref("api")},
			Samples:          []mimirpb.Sample{{TimestampMs: 5, Value: 1.5}},
			Exemplars:        []mimirpb.ExemplarRW2{{LabelsRefs: []uint32{ref("trace_id"), ref("abc")}, Value: 2, Timestamp: 4}},
			Metadata:         mimirpb.MetadataRW2{Type: mimirpb.METRIC_TYPE_COUNTER, HelpRef: ref("help text"), UnitRef: ref("seconds")},
			CreatedTimestamp: 3,
		}, {
			// Metadata that says nothing isn't kept.
			LabelsRefs: []uint32{ref("__name__"), ref("api")},
		}},
	}
	encoded, err := request.Marshal()
	require.NoError(t, err)
	decoded, err := DecodeRecordBytes(2, encoded)
	require.NoError(t, err)
	require.Equal(t, int32(1), decoded.Source)
	require.Len(t, decoded.Series, 2)
	require.Equal(t, [][2]string{{"__name__", "requests"}, {"job", "api"}}, decoded.Series[0].Labels)
	require.Equal(t, []mimirpb.Sample{{TimestampMs: 5, Value: 1.5}}, decoded.Series[0].Samples)
	require.Equal(t, []mimirpb.Exemplar{{Labels: []mimirpb.LabelAdapter{{Name: "trace_id", Value: "abc"}}, Value: 2, TimestampMs: 4}}, decoded.Series[0].Exemplars)
	require.Equal(t, int64(3), decoded.Series[0].CreatedTimestamp)
	require.Equal(t, []mimirpb.MetricMetadata{{Type: mimirpb.COUNTER, MetricFamilyName: "requests", Help: "help text", Unit: "seconds"}}, decoded.Metadata)

	request.TimeseriesRW2[1].LabelsRefs = []uint32{40}
	encoded, err = request.Marshal()
	require.NoError(t, err)
	_, err = DecodeRecordBytes(2, encoded)
	require.ErrorContains(t, err, "odd number")
	request.TimeseriesRW2[1].LabelsRefs = []uint32{40, 1}
	encoded, err = request.Marshal()
	require.NoError(t, err)
	_, err = DecodeRecordBytes(2, encoded)
	require.ErrorContains(t, err, "reserved RW2 symbol reference 40")
	_, err = DecodeRecordBytes(3, encoded)
	require.ErrorContains(t, err, "unsupported ingest-storage record version 3")
}

func TestMetadataNamesAreNormalizedLikeMimir(t *testing.T) {
	var request mimirpb.WriteRequest
	for _, item := range []mimirpb.MetricMetadata{
		{Type: mimirpb.SUMMARY, MetricFamilyName: "rpc_count"},
		{Type: mimirpb.HISTOGRAM, MetricFamilyName: "latency_bucket"},
		{Type: mimirpb.UNKNOWN, MetricFamilyName: "empty"},
		{Type: mimirpb.UNKNOWN, MetricFamilyName: "", Help: "no name"},
		{Type: mimirpb.GAUGE, MetricFamilyName: "up_sum"},
	} {
		request.Metadata = append(request.Metadata, &item)
	}
	encoded, err := request.Marshal()
	require.NoError(t, err)
	decoded, err := DecodeRecord(1, encoded)
	require.NoError(t, err)
	names := []string{}
	for _, item := range decoded.Metadata {
		names = append(names, item.MetricFamilyName)
	}
	require.Equal(t, []string{"rpc", "latency", "up_sum"}, names)
}

func BenchmarkDecodeRecord(b *testing.B) {
	var request mimirpb.WriteRequest
	for index := range 350 {
		labels := []mimirpb.LabelAdapter{{Name: "__name__", Value: fmt.Sprintf("metric_%d", index%40)}}
		for label := range 18 {
			labels = append(labels, mimirpb.LabelAdapter{Name: fmt.Sprintf("label_%02d", label), Value: fmt.Sprintf("value_%d_%d", index, label)})
		}
		request.Timeseries = append(request.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels: labels, Samples: []mimirpb.Sample{{TimestampMs: int64(index), Value: 1}},
		}})
	}
	encoded, err := request.Marshal()
	require.NoError(b, err)
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := DecodeRecordWithLabelSpans(1, encoded); err != nil {
			b.Fatal(err)
		}
	}
}
