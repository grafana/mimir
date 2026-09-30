// SPDX-License-Identifier: AGPL-3.0-only

package segment

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/record"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func request() *record.DecodedRequest {
	return &record.DecodedRequest{
		Source: 2,
		Metadata: []mimirpb.MetricMetadata{{
			Type: 1, MetricFamilyName: "requests", Help: "help", Unit: "seconds",
		}},
		Series: []record.DecodedSeries{{
			Labels:  [][2]string{{"__name__", "requests"}, {"job", "api"}},
			Samples: []mimirpb.Sample{{TimestampMs: 123, Value: math.Float64frombits(0x7ff0_0000_0000_0002)}},
			Histograms: []mimirpb.Histogram{{
				Count: &mimirpb.Histogram_CountInt{CountInt: 3}, Sum: 4.5, Timestamp: 124,
			}},
			Exemplars: []mimirpb.Exemplar{{
				Labels: []mimirpb.LabelAdapter{{Name: "trace_id", Value: "abc"}}, Value: 1.5, TimestampMs: 122,
			}},
			CreatedTimestamp: 100,
		}},
	}
}

// As bytes: stale markers are NaN, which never equal themselves.
func requestBytes(t testing.TB, request *record.DecodedRequest) []byte {
	bytes, err := encodeRequest(nil, request)
	require.NoError(t, err)
	return bytes
}

func openLog(t testing.TB, root string, partition int32, retention Retention) (*Log, []RecoveredRecord) {
	log, records, err := Open(root, 0, "topic", partition, retention)
	require.NoError(t, err)
	return log, records
}

func offsets(records []RecoveredRecord) []int64 {
	out := []int64{}
	for _, r := range records {
		out = append(out, r.Offset)
	}
	return out
}

func rangeOf(from, to int64) []int64 {
	out := []int64{}
	for offset := from; offset < to; offset++ {
		out = append(out, offset)
	}
	return out
}

func noRetention() Retention { return Retention{} }

func hourRetention() Retention { return Retention{Ms: hourMs, Set: true} }

func frameContentSize(t *testing.T, compressed []byte) uint64 {
	var header zstd.Header
	require.NoError(t, header.Decode(compressed))
	require.True(t, header.HasFCS, "frames record their decoded size")
	return header.FrameContentSize
}

func TestFramesRecordTheirSizeAndDefineEachSeriesOncePerFile(t *testing.T) {
	root := t.TempDir()
	log, _ := openLog(t, root, 0, noRetention())
	now := nowMs()
	require.NoError(t, log.BeginBatch(now))
	otherTenant := request()
	otherTenant.Series[0].Samples[0].TimestampMs = 200
	var sizes []uint64
	for _, item := range []struct {
		offset  int64
		tenant  string
		request *record.DecodedRequest
	}{{1, "tenant", request()}, {2, "tenant", request()}, {3, "other", otherTenant}} {
		frame, err := log.Encode(item.offset, 1, now, item.tenant, item.request, SeriesKeys(item.tenant, item.request))
		require.NoError(t, err)
		// Recovery batches frames by the size each one records.
		sizes = append(sizes, frameContentSize(t, frame.frame[frameHeaderLen+framePrefixLen:]))
		require.NoError(t, log.AppendCompressed(frame))
	}
	// The second frame refers to the series the first defined; another tenant's series with the
	// same labels is its own.
	require.Less(t, sizes[1], sizes[0])
	require.Equal(t, sizes[0]+uint64(len("other")), sizes[2]+uint64(len("tenant")))
	// A new file defines its series again.
	require.NoError(t, log.BeginBatch(now+hourMs))
	frame, err := log.Encode(4, 1, now+hourMs, "tenant", request(), SeriesKeys("tenant", request()))
	require.NoError(t, err)
	require.NoError(t, log.AppendCompressed(frame))
	require.NoError(t, log.Close())
	_, recovered := openLog(t, root, 0, noRetention())
	require.Len(t, recovered, 4)
	for _, rec := range recovered {
		require.Equal(t, request().Series[0].Labels, rec.Request.Series[0].Labels)
		require.Equal(t, request().Series[0].Exemplars, rec.Request.Series[0].Exemplars)
	}
	require.Equal(t, "other", recovered[2].Tenant)
	require.Equal(t, int64(200), recovered[2].Request.Series[0].Samples[0].TimestampMs)
}

func TestKeysOfEncodedLabelsFollowTheLabelsAndTheTenant(t *testing.T) {
	encoded := func(value string, timestampMs int64) []byte {
		write := mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
			Labels:  []mimirpb.LabelAdapter{{Name: "job", Value: value}},
			Samples: []mimirpb.Sample{{TimestampMs: timestampMs, Value: 1}},
		}}}}
		bytes, err := write.Marshal()
		require.NoError(t, err)
		return bytes
	}
	key := func(tenant string, rec []byte) SeriesKey {
		request, spans, err := record.DecodeRecordWithLabelSpans(1, rec)
		require.NoError(t, err)
		require.True(t, spans[0].OK)
		return SeriesKeysWithLabelBytes(tenant, &request, rec, spans)[0]
	}
	api := key("tenant", encoded("api", 1))
	require.Equal(t, api, key("tenant", encoded("api", 2)), "samples are not labels")
	require.NotEqual(t, api, key("other", encoded("api", 1)))
	require.NotEqual(t, api, key("tenant", encoded("apj", 1)))
}

func TestFramesAreOnlyWrittenToTheFileTheyWereEncodedFor(t *testing.T) {
	log, _ := openLog(t, t.TempDir(), 0, noRetention())
	defer log.Close()
	require.NoError(t, log.BeginBatch(hourMs))
	frame, err := log.Encode(1, 1, hourMs, "tenant", request(), SeriesKeys("tenant", request()))
	require.NoError(t, err)
	require.NoError(t, log.BeginBatch(2*hourMs))
	require.Error(t, log.AppendCompressed(frame))
}

func TestExpiredFramesStillDefineTheSeriesLaterFramesUse(t *testing.T) {
	root := t.TempDir()
	log, _ := openLog(t, root, 0, noRetention())
	hour := hourStart(nowMs())
	// Both frames go to the current hour's file, the first ingested before the retention.
	require.NoError(t, log.BeginBatch(hour))
	for _, item := range []struct{ offset, ingestedMs int64 }{{1, hour - 2*hourMs}, {2, nowMs()}} {
		frame, err := log.Encode(item.offset, 1, item.ingestedMs, "tenant", request(), SeriesKeys("tenant", request()))
		require.NoError(t, err)
		require.NoError(t, log.AppendCompressed(frame))
	}
	require.NoError(t, log.Close())
	var recovered []RecoveredRecord
	log, err := OpenReplaying(root, 0, "topic", 0, hourRetention(), 2, func(rec RecoveredRecord) error {
		recovered = append(recovered, rec)
		return nil
	})
	require.NoError(t, err)
	last, ok := log.LastOffset()
	require.True(t, ok)
	require.Equal(t, int64(2), last)
	require.Len(t, recovered, 1)
	require.Equal(t, request().Series[0].Labels, recovered[0].Request.Series[0].Labels)
	require.NoError(t, log.Close())
}

// A frame of the first two versions, before compression.
func uncompressedFrame(t *testing.T, offset, timestampMs int64, tenant string, req *record.DecodedRequest, ingestedMs int64) []byte {
	body := putI64(nil, offset)
	body = putI64(body, timestampMs)
	body = putI64(body, ingestedMs)
	body, err := putString(body, tenant)
	require.NoError(t, err)
	body, err = encodeRequest(body, req)
	require.NoError(t, err)
	return body
}

func writeHeader(t *testing.T, file *os.File, version uint32, hour int64) {
	header := append([]byte(nil), fileMagic...)
	header = binary.LittleEndian.AppendUint32(header, version)
	header = binary.LittleEndian.AppendUint32(header, 0)
	header = binary.LittleEndian.AppendUint32(header, 0)
	header = binary.LittleEndian.AppendUint64(header, uint64(hour))
	_, err := file.Write(header)
	require.NoError(t, err)
}

func writeFrame(t *testing.T, file *os.File, body []byte) {
	frame := binary.LittleEndian.AppendUint32(nil, uint32(len(body)))
	frame = binary.LittleEndian.AppendUint32(frame, crc32.ChecksumIEEE(body))
	_, err := file.Write(append(frame, body...))
	require.NoError(t, err)
}

// A version 2 frame: the prefix, then the payload compressed on its own.
func compressedV2Frame(t *testing.T, offset int64) []byte {
	body := uncompressedFrame(t, offset, 100, "tenant", request(), nowMs())
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	require.NoError(t, err)
	return encoder.EncodeAll(body[framePrefixLen:], append([]byte(nil), body[:framePrefixLen]...))
}

func TestReadsFilesWhereEveryFrameHasItsLabels(t *testing.T) {
	root := t.TempDir()
	log, _ := openLog(t, root, 0, noRetention())
	hour := hourStart(nowMs())
	file, err := os.Create(filepath.Join(log.directory, fmt.Sprintf("%020d.segment", hour)))
	require.NoError(t, err)
	writeHeader(t, file, compressedFileVersion, hour)
	for _, offset := range []int64{1, 2} {
		writeFrame(t, file, compressedV2Frame(t, offset))
	}
	require.NoError(t, file.Close())
	require.NoError(t, log.Close())
	log, recovered := openLog(t, root, 0, noRetention())
	require.Len(t, recovered, 2)
	require.Equal(t, request().Series[0].Labels, recovered[1].Request.Series[0].Labels)
	// New frames of the same hour go to a new file.
	require.NoError(t, log.Append(3, 101, "tenant", request()))
	require.NoError(t, log.Close())
	_, recovered = openLog(t, root, 0, noRetention())
	require.Equal(t, []int64{1, 2, 3}, offsets(recovered))
}

func TestSealsAnHourLeftBothSealedAndOpenWithoutLosingRecords(t *testing.T) {
	root := t.TempDir()
	log, _ := openLog(t, root, 0, noRetention())
	hour := hourStart(nowMs())
	// Like the log before sequences, after appending a record prepared before the hour ended to
	// the hour it had sealed.
	for _, item := range []struct {
		offset    int64
		extension string
	}{{1, "segment"}, {2, "open"}} {
		file, err := os.Create(filepath.Join(log.directory, fmt.Sprintf("%020d.%s", hour, item.extension)))
		require.NoError(t, err)
		writeHeader(t, file, compressedFileVersion, hour)
		writeFrame(t, file, compressedV2Frame(t, item.offset))
		require.NoError(t, file.Close())
	}
	require.NoError(t, log.Close())
	log, recovered := openLog(t, root, 0, noRetention())
	require.Len(t, recovered, 2)
	require.NoError(t, log.Append(3, 101, "tenant", request()))
	require.NoError(t, log.Close())
	_, recovered = openLog(t, root, 0, noRetention())
	require.Equal(t, []int64{1, 2, 3}, offsets(recovered))
}

func benchmarkRequest(rec uint64) *record.DecodedRequest {
	series := make([]record.DecodedSeries, 0, 350)
	for index := range uint64(350) {
		id := rec*350 + index
		pairs := make([][2]string, 0, 19)
		pairs = append(pairs, [2]string{"__name__", fmt.Sprintf("metric_%016x%017x", mix(id), mix(id+1))})
		for label := uint64(1); label < 19; label++ {
			pairs = append(pairs, [2]string{
				fmt.Sprintf("label_%04x", (id*19+label)%2500),
				fmt.Sprintf("value_%011x", mix(id*19+label)&0x0000_00ff_ffff_ffff),
			})
		}
		series = append(series, record.DecodedSeries{
			Labels: pairs,
			Samples: []mimirpb.Sample{
				{TimestampMs: 1_800_000_000_000 + int64(rec), Value: math.Float64frombits(0x3ff0_0000_0000_0000 | (mix(id) & 0x000f_ffff_ffff_ffff))},
				{TimestampMs: 1_800_000_000_001 + int64(rec), Value: math.Float64frombits(0x3ff0_0000_0000_0000 | (mix(id+1) & 0x000f_ffff_ffff_ffff))},
			},
		})
	}
	return &record.DecodedRequest{Source: 2, Series: series}
}

func mix(value uint64) uint64 {
	value = (value ^ (value >> 30)) * 0xbf58_476d_1ce4_e5b9
	value = (value ^ (value >> 27)) * 0x94d0_49bb_1331_11eb
	return value ^ (value >> 31)
}

func processCPU() time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		panic(err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

func BenchmarkHighCardinalitySegments(b *testing.B) {
	const records = 100
	requests := make([]*record.DecodedRequest, 0, records)
	for rec := range uint64(records) {
		requests = append(requests, benchmarkRequest(rec))
	}
	for b.Loop() {
		root := b.TempDir()
		log, _ := openLog(b, root, 0, noRetention())
		start, cpu := time.Now(), processCPU()
		for offset, req := range requests {
			require.NoError(b, log.Append(int64(offset), 1_800_000_000_000, "tenant", req))
		}
		require.NoError(b, log.Flush())
		elapsed, writeCPU := time.Since(start), processCPU()-cpu
		paths, err := segmentPaths(log.directory)
		require.NoError(b, err)
		var bytes int64
		for _, path := range paths {
			info, err := os.Stat(path)
			require.NoError(b, err)
			bytes += info.Size()
		}
		require.NoError(b, log.Close())
		recoveryStart, recoveryCPU := time.Now(), processCPU()
		reopened, recovered := openLog(b, root, 0, noRetention())
		b.ReportMetric(float64(bytes)/records, "bytes/record")
		b.ReportMetric(float64(writeCPU.Milliseconds()), "write-cpu-ms")
		b.ReportMetric(float64(elapsed.Milliseconds()), "write-ms")
		b.ReportMetric(float64(time.Since(recoveryStart).Milliseconds()), "recovery-ms")
		b.ReportMetric(float64((processCPU() - recoveryCPU).Milliseconds()), "recovery-cpu-ms")
		last, _ := reopened.LastOffset()
		require.Equal(b, int64(records-1), last)
		require.Len(b, recovered, records)
		for i, rec := range recovered {
			require.Equal(b, requestBytes(b, requests[i]), requestBytes(b, &rec.Request))
		}
		require.NoError(b, reopened.Close())
	}
}

// Records that vary like real ones, so a dictionary can be trained on a few of them.
func variedRequest(rec int64) *record.DecodedRequest {
	series := make([]record.DecodedSeries, 0, 20)
	for index := range int64(20) {
		series = append(series, record.DecodedSeries{
			Labels: [][2]string{
				{"__name__", fmt.Sprintf("metric_%d", index%7)},
				{"job", fmt.Sprintf("job-%d", (rec+index)%13)},
				{"pod", fmt.Sprintf("pod-%d-%d", rec%50, index)},
			},
			Samples: []mimirpb.Sample{{TimestampMs: 1_800_000_000_000 + rec*15_000, Value: float64(rec*31+index) / 7}},
		})
	}
	return &record.DecodedRequest{Series: series}
}

type frameInfo struct {
	offset     int64
	dictionary uint32
}

// framesOf returns each frame of a segment file: its offset and the dictionary id its payload
// names.
func framesOf(t *testing.T, path string) []frameInfo {
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var frames []frameInfo
	for at := fileHeaderLen; at+frameHeaderLen <= len(data); {
		length := int(binary.LittleEndian.Uint32(data[at : at+4]))
		body := data[at+frameHeaderLen : at+frameHeaderLen+length]
		frame := frameInfo{offset: int64(binary.LittleEndian.Uint64(body[:8]))}
		if frame.offset != dictionaryOffset {
			var header zstd.Header
			require.NoError(t, header.Decode(body[framePrefixLen:]))
			frame.dictionary = header.DictionaryID
		}
		frames = append(frames, frame)
		at += frameHeaderLen + length
	}
	return frames
}

// appendPastDictionary appends varied records from from until the current file compresses with
// its dictionary, then after more, and returns the next offset.
func appendPastDictionary(t *testing.T, log *Log, from, after, ingestedMs int64) int64 {
	deadline := time.Now().Add(10 * time.Second)
	offset := from
	for log.current == nil || log.current.dictionary.state != ready {
		require.True(t, time.Now().Before(deadline), "no dictionary after %d records", offset)
		require.NoError(t, log.appendAt(offset, offset, "tenant", variedRequest(offset), ingestedMs))
		offset++
	}
	for range after {
		require.NoError(t, log.appendAt(offset, offset, "tenant", variedRequest(offset), ingestedMs))
		offset++
	}
	return offset
}

func openSegment(t *testing.T, directory string) string {
	paths, err := segmentPaths(directory)
	require.NoError(t, err)
	for _, path := range paths {
		if filepath.Ext(path) == ".open" {
			return path
		}
	}
	t.Fatal("no open segment")
	return ""
}

func TestFramesAfterAFilesDictionaryReplayAsWritten(t *testing.T) {
	root := t.TempDir()
	log, _ := openLog(t, root, 0, noRetention())
	log.dictionaryFrames = 50
	now := nowMs()
	end := appendPastDictionary(t, log, 1, 100, now)
	path := openSegment(t, log.directory)
	require.NoError(t, log.Close())
	frames := framesOf(t, path)
	// One dictionary frame, before every frame that names it, and those after all do.
	at := slices.IndexFunc(frames, func(f frameInfo) bool { return f.offset == dictionaryOffset })
	require.GreaterOrEqual(t, at, 50, "trained on the first 50 frames")
	count := 0
	for _, frame := range frames {
		if frame.offset == dictionaryOffset {
			count++
		}
	}
	require.Equal(t, 1, count)
	for _, frame := range frames[:at] {
		require.Zero(t, frame.dictionary)
	}
	id := frames[at+1].dictionary
	require.NotZero(t, id, "frames after it use the dictionary")
	for _, frame := range frames[at+1:] {
		require.Equal(t, id, frame.dictionary)
	}
	log, recovered := openLog(t, root, 0, noRetention())
	require.Equal(t, rangeOf(1, end), offsets(recovered))
	for _, rec := range recovered {
		require.Equal(t, rec.Offset, rec.KafkaTimestampMs)
		require.Equal(t, requestBytes(t, variedRequest(rec.Offset)), requestBytes(t, &rec.Request))
	}
	// A restart continues in a new file, which trains its own dictionary.
	log.dictionaryFrames = 50
	next := appendPastDictionary(t, log, end, 10, now)
	require.NoError(t, log.Close())
	_, recovered = openLog(t, root, 0, noRetention())
	require.Equal(t, rangeOf(1, next), offsets(recovered))
}

func TestRestartsFromVersion3Files(t *testing.T) {
	root := t.TempDir()
	directory := directoryOf(root, 0, "topic", 0)
	require.NoError(t, os.MkdirAll(directory, 0o755))
	// Written by the Rust version 3 log: 20 records of request(), each with its own sample.
	fixture, err := os.ReadFile("../../../rust-kafka-ingester/testdata/segment-v3/00000000000018000000-0000.segment")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(directory, "00000000000018000000-0000.segment"), fixture, 0o644))
	expected := func(offset int64) *record.DecodedRequest {
		req := request()
		req.Series[0].Samples[0].TimestampMs = offset * 1000
		req.Series[0].Samples[0].Value = float64(offset)
		return req
	}
	log, recovered := openLog(t, root, 0, noRetention())
	last, _ := log.LastOffset()
	require.Equal(t, int64(20), last)
	require.Len(t, recovered, 20)
	for _, rec := range recovered {
		require.Equal(t, 1000+rec.Offset, rec.KafkaTimestampMs)
		require.Equal(t, 5*hourMs+rec.Offset, rec.IngestedMs)
		require.Equal(t, requestBytes(t, expected(rec.Offset)), requestBytes(t, &rec.Request))
	}
	// The new log writes version 4 files next to it.
	log.dictionaryFrames = 50
	end := appendPastDictionary(t, log, 21, 5, nowMs())
	require.NoError(t, log.Close())
	_, recovered = openLog(t, root, 0, noRetention())
	require.Equal(t, rangeOf(1, end), offsets(recovered))
	require.Equal(t, requestBytes(t, expected(5)), requestBytes(t, &recovered[4].Request))
	require.Equal(t, requestBytes(t, variedRequest(31)), requestBytes(t, &recovered[30].Request))
}

// The small records the Rust golden generator wrote its version 4 file with.
func tinyRequest(offset int64) *record.DecodedRequest {
	req := &record.DecodedRequest{
		Source: int32(offset % 3),
		Series: []record.DecodedSeries{{
			Labels: [][2]string{
				{"__name__", fmt.Sprintf("metric_%d", offset%7)},
				{"job", fmt.Sprintf("job-%d", offset%13)},
				{"pod", fmt.Sprintf("pod-%d", offset%50)},
			},
			Samples: []mimirpb.Sample{{TimestampMs: 1_800_000_000_000 + offset*15_000, Value: float64(offset) / 7}},
		}},
	}
	if offset%100 == 0 {
		req.Series[0].Histograms = []mimirpb.Histogram{{Count: &mimirpb.Histogram_CountInt{CountInt: 3}, Sum: 4.5, Timestamp: offset}}
	}
	if offset%500 == 0 {
		req.Metadata = []mimirpb.MetricMetadata{{Type: 1, MetricFamilyName: "metric_0", Help: "help"}}
	}
	return req
}

const rustV4Fixture = "testdata/rust-v4-00000001790780400000-0000.segment"

// A version 4 file the Rust ingester wrote, with a dictionary it trained after 2000 frames, replays
// in Go; and Go encodes the same payloads, which only the compression then tells apart.
func TestReplaysVersion4FilesTheRustIngesterWrote(t *testing.T) {
	root := t.TempDir()
	directory := directoryOf(root, 0, "topic", 0)
	require.NoError(t, os.MkdirAll(directory, 0o755))
	fixture, err := os.ReadFile(rustV4Fixture)
	require.NoError(t, err)
	name := strings.TrimPrefix(filepath.Base(rustV4Fixture), "rust-v4-")
	require.NoError(t, os.WriteFile(filepath.Join(directory, name), fixture, 0o644))

	frames := framesOf(t, filepath.Join(directory, name))
	at := slices.IndexFunc(frames, func(f frameInfo) bool { return f.offset == dictionaryOffset })
	require.Positive(t, at, "the file holds a dictionary")
	require.NotZero(t, frames[len(frames)-1].dictionary, "frames after it use the dictionary")

	log, recovered := openLog(t, root, 0, noRetention())
	require.Equal(t, rangeOf(1, 2051), offsets(recovered))
	for _, rec := range recovered {
		require.Equal(t, rec.Offset, rec.KafkaTimestampMs)
		require.Equal(t, "tenant", rec.Tenant)
		require.Equal(t, requestBytes(t, tinyRequest(rec.Offset)), requestBytes(t, &rec.Request))
	}
	require.NoError(t, log.Close())

	// Decompress Rust's payloads, and encode the same records in a fresh Go file.
	plain, err := zstd.NewReader(nil)
	require.NoError(t, err)
	var payloads [][]byte
	var decoder *zstd.Decoder
	for at := fileHeaderLen; at < len(fixture); {
		length := int(binary.LittleEndian.Uint32(fixture[at : at+4]))
		body := fixture[at+frameHeaderLen : at+frameHeaderLen+length]
		at += frameHeaderLen + length
		if int64(binary.LittleEndian.Uint64(body[:8])) == dictionaryOffset {
			decoder, err = zstd.NewReader(nil, zstd.WithDecoderDicts(body[framePrefixLen:]))
			require.NoError(t, err)
			continue
		}
		var header zstd.Header
		require.NoError(t, header.Decode(body[framePrefixLen:]))
		use := plain
		if header.DictionaryID != 0 {
			use = decoder
		}
		payload, err := use.DecodeAll(body[framePrefixLen:], nil)
		require.NoError(t, err)
		payloads = append(payloads, payload)
	}
	goLog, _ := openLog(t, t.TempDir(), 0, noRetention())
	defer goLog.Close()
	require.NoError(t, goLog.BeginBatch(nowMs()))
	for offset := int64(1); offset <= 2050; offset++ {
		req := tinyRequest(offset)
		frame, err := goLog.Encode(offset, offset, nowMs(), "tenant", req, SeriesKeys("tenant", req))
		require.NoError(t, err)
		require.Equal(t, payloads[offset-1], goLog.payload, "payload of offset %d", offset)
		require.Equal(t, offset, int64(binary.LittleEndian.Uint64(frame.frame[frameHeaderLen:])))
	}
}

// dictionaryFrame returns where the dictionary frame starts in the file, and the offset of the
// frame before it.
func dictionaryFrame(t *testing.T, path string) (int, int64) {
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	at, previous := fileHeaderLen, int64(math.MinInt64)
	for {
		length := int(binary.LittleEndian.Uint32(data[at : at+4]))
		body := at + frameHeaderLen
		offset := int64(binary.LittleEndian.Uint64(data[body : body+8]))
		if offset == dictionaryOffset {
			require.NotEqual(t, int64(math.MinInt64), previous, "frames before the dictionary")
			return at, previous
		}
		previous = offset
		at = body + length
	}
}

func TestTruncatesATornDictionaryFrameAndResumes(t *testing.T) {
	root := t.TempDir()
	log, _ := openLog(t, root, 0, noRetention())
	log.dictionaryFrames = 50
	now := nowMs()
	end := appendPastDictionary(t, log, 1, 3, now)
	path := openSegment(t, log.directory)
	require.NoError(t, log.Close())
	// A crash in the middle of the dictionary frame loses it and every frame after, which were
	// written after the last flush.
	at, before := dictionaryFrame(t, path)
	require.NoError(t, os.Truncate(path, int64(at)+100))
	require.NoError(t, writeCheckpoint(filepath.Dir(path), before))
	log, recovered := openLog(t, root, 0, noRetention())
	require.Equal(t, rangeOf(1, before+1), offsets(recovered))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(at), info.Size())
	// Consumption resumes after what the log holds.
	last, _ := log.LastOffset()
	require.Equal(t, before, last)
	for offset := before + 1; offset < end; offset++ {
		require.NoError(t, log.appendAt(offset, offset, "tenant", variedRequest(offset), now))
	}
	require.NoError(t, log.Close())
	_, recovered = openLog(t, root, 0, noRetention())
	require.Equal(t, rangeOf(1, end), offsets(recovered))
	for _, rec := range recovered {
		require.Equal(t, requestBytes(t, variedRequest(rec.Offset)), requestBytes(t, &rec.Request))
	}
}

func TestRejectsFramesWhoseDictionaryTheFileLacks(t *testing.T) {
	root := t.TempDir()
	log, _ := openLog(t, root, 0, noRetention())
	log.dictionaryFrames = 50
	hour := hourStart(nowMs())
	appendPastDictionary(t, log, 1, 5, hour)
	require.NoError(t, log.sealCurrent())
	require.NoError(t, log.Close())
	path := filepath.Join(directoryOf(root, 0, "topic", 0), segmentName(hour, 0, "segment"))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	// Cut the dictionary frame out: the frames around it are well formed, so only its absence
	// shows.
	start, _ := dictionaryFrame(t, path)
	length := int(binary.LittleEndian.Uint32(data[start : start+4]))
	end := start + frameHeaderLen + length
	require.NoError(t, os.WriteFile(path, append(append([]byte(nil), data[:start]...), data[end:]...), 0o644))
	_, _, err = Open(root, 0, "topic", 0, noRetention())
	require.ErrorContains(t, err, "needs a dictionary")
}

// Rewrites and replays a real segment file: run with SEGMENT_FILE=<path>.
func BenchmarkRealSegment(b *testing.B) {
	source := os.Getenv("SEGMENT_FILE")
	if source == "" {
		b.Skip("SEGMENT_FILE is not set")
	}
	input := b.TempDir()
	directory := directoryOf(input, 0, "ingest", 0)
	require.NoError(b, os.MkdirAll(directory, 0o755))
	data, err := os.ReadFile(source)
	require.NoError(b, err)
	// As an open file, a copy cut short mid-frame replays up to the cut.
	require.NoError(b, os.WriteFile(filepath.Join(directory, strings.Replace(filepath.Base(source), ".segment", ".open", 1)), data, 0o644))
	_, records, err := Open(input, 0, "ingest", 0, noRetention())
	require.NoError(b, err)
	keys := make([][]SeriesKey, 0, len(records))
	for i := range records {
		keys = append(keys, SeriesKeys(records[i].Tenant, &records[i].Request))
	}
	for b.Loop() {
		output := b.TempDir()
		log, _, err := Open(output, 0, "ingest", 0, noRetention())
		require.NoError(b, err)
		cpu, started := processCPU(), time.Now()
		for i := range records {
			rec := &records[i]
			require.NoError(b, log.BeginBatch(rec.IngestedMs))
			frame, err := log.Encode(rec.Offset, rec.KafkaTimestampMs, rec.IngestedMs, rec.Tenant, &rec.Request, keys[i])
			require.NoError(b, err)
			require.NoError(b, log.AppendCompressed(frame))
		}
		require.NoError(b, log.Flush())
		writeCPU, writeWall := processCPU()-cpu, time.Since(started)
		require.NoError(b, log.Close())
		cpu, started = processCPU(), time.Now()
		_, replayed, err := Open(output, 0, "ingest", 0, noRetention())
		require.NoError(b, err)
		b.ReportMetric(writeCPU.Seconds(), "write-cpu-s")
		b.ReportMetric(writeWall.Seconds(), "write-s")
		b.ReportMetric((processCPU() - cpu).Seconds(), "replay-cpu-s")
		b.ReportMetric(time.Since(started).Seconds(), "replay-s")
		require.Len(b, replayed, len(records))
		for i := range replayed {
			require.Equal(b, records[i].Offset, replayed[i].Offset)
			require.Equal(b, requestBytes(b, &records[i].Request), requestBytes(b, &replayed[i].Request))
		}
	}
}

func TestRoundTripAndResumeOffset(t *testing.T) {
	root := t.TempDir()
	log, recovered := openLog(t, root, 7, noRetention())
	require.Empty(t, recovered)
	require.NoError(t, log.Append(41, 500, "tenant-a", request()))
	require.NoError(t, log.Close())

	log, recovered = openLog(t, root, 7, noRetention())
	last, _ := log.LastOffset()
	require.Equal(t, int64(41), last)
	require.Len(t, recovered, 1)
	rec := recovered[0]
	require.Equal(t, int64(41), rec.Offset)
	require.Equal(t, int64(500), rec.KafkaTimestampMs)
	require.Positive(t, rec.IngestedMs)
	require.Equal(t, "tenant-a", rec.Tenant)
	expected := request()
	require.Equal(t, expected.Source, rec.Request.Source)
	require.Equal(t, expected.Metadata, rec.Request.Metadata)
	require.Equal(t, expected.Series[0].Labels, rec.Request.Series[0].Labels)
	require.Equal(t, math.Float64bits(expected.Series[0].Samples[0].Value), math.Float64bits(rec.Request.Series[0].Samples[0].Value))
	require.Equal(t, len(expected.Series[0].Histograms), len(rec.Request.Series[0].Histograms))
	require.True(t, expected.Series[0].Histograms[0].Equal(rec.Request.Series[0].Histograms[0]))
	require.Equal(t, expected.Series[0].Exemplars, rec.Request.Series[0].Exemplars)
	require.NoError(t, log.Close())
	log, _ = openLog(t, root, 7, noRetention())
	require.NoError(t, log.Append(42, 501, "tenant-a", request()))
	require.NoError(t, log.Close())
	_, recovered = openLog(t, root, 7, noRetention())
	require.Equal(t, []int64{41, 42}, offsets(recovered))
}

func TestReplaysRecordsIncrementallyAfterRestart(t *testing.T) {
	root := t.TempDir()
	log, _ := openLog(t, root, 0, noRetention())
	for offset := range int64(100) {
		require.NoError(t, log.Append(offset, offset, "tenant", request()))
	}
	require.NoError(t, log.Close())
	var replayed []int64
	recovered, err := OpenReplaying(root, 0, "topic", 0, noRetention(), 2, func(rec RecoveredRecord) error {
		replayed = append(replayed, rec.Offset)
		return nil
	})
	require.NoError(t, err)
	last, _ := recovered.LastOffset()
	require.Equal(t, int64(99), last)
	require.Equal(t, rangeOf(0, 100), replayed)
	require.NoError(t, recovered.Close())
}

func TestSkipsExpiredFramesWithoutLosingResumeOffset(t *testing.T) {
	root := t.TempDir()
	log, _ := openLog(t, root, 0, noRetention())
	require.NoError(t, log.appendAt(1, 100, "tenant", request(), nowMs()-2*hourMs))
	require.NoError(t, log.appendAt(2, 200, "tenant", request(), nowMs()))
	require.NoError(t, log.Close())
	var replayed []int64
	recovered, err := OpenReplaying(root, 0, "topic", 0, hourRetention(), 2, func(rec RecoveredRecord) error {
		replayed = append(replayed, rec.Offset)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []int64{2}, replayed)
	last, _ := recovered.LastOffset()
	require.Equal(t, int64(2), last)
	require.NoError(t, recovered.Close())
}

func TestUpgradesOpenLegacySegmentWithoutLosingRecords(t *testing.T) {
	root := t.TempDir()
	log, _ := openLog(t, root, 0, noRetention())
	now := nowMs()
	hour := hourStart(now)
	file, err := os.Create(filepath.Join(log.directory, fmt.Sprintf("%020d.open", hour)))
	require.NoError(t, err)
	writeHeader(t, file, legacyFileVersion, hour)
	writeFrame(t, file, uncompressedFrame(t, 1, 100, "tenant", request(), now))
	require.NoError(t, file.Close())
	require.NoError(t, log.Close())

	log, recovered := openLog(t, root, 0, noRetention())
	require.Len(t, recovered, 1)
	require.NoError(t, log.appendAt(2, 200, "tenant", request(), now))
	require.FileExists(t, filepath.Join(log.directory, fmt.Sprintf("%020d.segment", hour)))
	require.NoError(t, log.Close())

	log, recovered = openLog(t, root, 0, noRetention())
	last, _ := log.LastOffset()
	require.Equal(t, int64(2), last)
	require.Equal(t, []int64{1, 2}, offsets(recovered))
	require.NoError(t, log.Close())
}

func TestSealsCompletedHours(t *testing.T) {
	root := t.TempDir()
	log, _ := openLog(t, root, 0, noRetention())
	require.NoError(t, log.appendAt(1, 100, "tenant", request(), hourMs))
	require.NoError(t, log.appendAt(2, 200, "tenant", request(), 2*hourMs))
	paths, err := segmentPaths(log.directory)
	require.NoError(t, err)
	has := func(wantHour int64, extension string) bool {
		for _, path := range paths {
			if hour, _, ok := segmentID(path); ok && hour == wantHour && filepath.Ext(path) == extension {
				return true
			}
		}
		return false
	}
	require.True(t, has(hourMs, ".segment"))
	require.True(t, has(2*hourMs, ".open"))
	require.NoError(t, log.Close())
	_, recovered := openLog(t, root, 0, noRetention())
	require.Len(t, recovered, 2)
}

func appendToFile(t *testing.T, path string, bytes []byte) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = file.Write(bytes)
	require.NoError(t, err)
	require.NoError(t, file.Close())
}

func size(t *testing.T, path string) int64 {
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Size()
}

func TestTruncatesTornOpenFrame(t *testing.T) {
	root := t.TempDir()
	log, _ := openLog(t, root, 0, noRetention())
	require.NoError(t, log.Append(1, 100, "tenant", request()))
	openPath := openSegment(t, log.directory)
	require.NoError(t, log.Close())
	validLen := size(t, openPath)
	torn := binary.LittleEndian.AppendUint32(nil, 100)
	torn = binary.LittleEndian.AppendUint32(torn, 0)
	appendToFile(t, openPath, append(torn, "partial"...))

	_, recovered := openLog(t, root, 0, noRetention())
	require.Len(t, recovered, 1)
	require.Equal(t, validLen, size(t, openPath))
}

func TestTruncatesZeroFilledOpenTailAndResumes(t *testing.T) {
	root := t.TempDir()
	log, _ := openLog(t, root, 0, noRetention())
	require.NoError(t, log.Append(1, 100, "tenant", request()))
	require.NoError(t, log.Append(2, 101, "tenant", request()))
	openPath := openSegment(t, log.directory)
	require.NoError(t, log.Close())
	validLen := size(t, openPath)
	appendToFile(t, openPath, make([]byte, 1458))

	log, recovered := openLog(t, root, 0, noRetention())
	require.Len(t, recovered, 2)
	last, _ := log.LastOffset()
	require.Equal(t, int64(2), last)
	require.Equal(t, validLen, size(t, openPath))
	require.NoError(t, log.Append(3, 102, "tenant", request()))
	require.NoError(t, log.Close())

	log, recovered = openLog(t, root, 0, noRetention())
	require.Equal(t, []int64{1, 2, 3}, offsets(recovered))
	require.NoError(t, log.Close())
}

func TestRejectsZeroFilledSealedTail(t *testing.T) {
	root := t.TempDir()
	log, _ := openLog(t, root, 0, noRetention())
	require.NoError(t, log.appendAt(1, 100, "tenant", request(), hourMs))
	require.NoError(t, log.sealCurrent())
	sealed := filepath.Join(log.directory, segmentName(hourMs, 0, "segment"))
	require.NoError(t, log.Close())
	appendToFile(t, sealed, make([]byte, 64))

	_, _, err := Open(root, 0, "topic", 0, noRetention())
	require.ErrorContains(t, err, "is corrupt")
}

func TestOpensAtMatchingCheckpointWithoutReplay(t *testing.T) {
	root := t.TempDir()
	log, _ := openLog(t, root, 0, noRetention())
	require.NoError(t, log.Append(5, 100, "tenant", request()))
	require.NoError(t, log.Close())
	mismatched, err := OpenAtCheckpoint(root, 0, "topic", 0, noRetention(), 4, true)
	require.NoError(t, err)
	require.Nil(t, mismatched)
	log, err = OpenAtCheckpoint(root, 0, "topic", 0, noRetention(), 5, true)
	require.NoError(t, err)
	require.NotNil(t, log)
	last, _ := log.LastOffset()
	require.Equal(t, int64(5), last)
	require.NoError(t, log.Append(6, 101, "tenant", request()))
	require.NoError(t, log.Close())
	log, recovered := openLog(t, root, 0, noRetention())
	last, _ = log.LastOffset()
	require.Equal(t, int64(6), last)
	require.Equal(t, []int64{5, 6}, offsets(recovered))
	require.NoError(t, log.Close())
}

func TestRemovesExpiredSegmentWithoutNewRecords(t *testing.T) {
	root := t.TempDir()
	log, _ := openLog(t, root, 0, hourRetention())
	require.NoError(t, log.appendAt(1, 100, "tenant", request(), hourMs))
	paths, err := segmentPaths(log.directory)
	require.NoError(t, err)
	require.Len(t, paths, 1)

	require.NoError(t, log.Maintain())
	paths, err = segmentPaths(log.directory)
	require.NoError(t, err)
	require.Empty(t, paths)
	last, _ := log.LastOffset()
	require.Equal(t, int64(1), last)
	require.NoError(t, log.Close())

	log, recovered := openLog(t, root, 0, hourRetention())
	last, _ = log.LastOffset()
	require.Equal(t, int64(1), last)
	require.Empty(t, recovered)
	require.NoError(t, log.Close())
}

func BenchmarkEncodeFrame(b *testing.B) {
	log, _ := openLog(b, b.TempDir(), 0, noRetention())
	defer log.Close()
	require.NoError(b, log.BeginBatch(nowMs()))
	req := benchmarkRequest(0)
	keys := SeriesKeys("tenant", req)
	// After its first frame, the file refers to the series by id, like steady ingestion.
	_, err := log.Encode(0, 0, nowMs(), "tenant", req, keys)
	require.NoError(b, err)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := log.Encode(1, 0, nowMs(), "tenant", req, keys); err != nil {
			b.Fatal(err)
		}
	}
}

func TestTrainingOnIdenticalFramesLeavesTheFileWithoutADictionary(t *testing.T) {
	log, _ := openLog(t, t.TempDir(), 0, noRetention())
	defer log.Close()
	require.NoError(t, log.BeginBatch(nowMs()))
	req := benchmarkRequest(0)
	keys := SeriesKeys("tenant", req)
	log.dictionaryFrames = 434
	for offset := range int64(434) {
		_, err := log.Encode(offset, 0, nowMs(), "tenant", req, keys)
		require.NoError(t, err)
	}
	// Without a dictionary, the file's frames are compressed on their own.
	require.Eventually(t, func() bool {
		log.current.dictionary.poll()
		return log.current.dictionary.state == unavailable
	}, 30*time.Second, 10*time.Millisecond)
	_, err := log.Encode(434, 0, nowMs(), "tenant", req, keys)
	require.NoError(t, err)
}

// Steady ingestion encodes most frames with the file's trained dictionary.
func BenchmarkEncodeFrameWithDictionary(b *testing.B) {
	log, _ := openLog(b, b.TempDir(), 0, noRetention())
	defer log.Close()
	log.dictionaryFrames = 50
	deadline := time.Now().Add(10 * time.Second)
	for offset := int64(1); log.current == nil || log.current.dictionary.state != ready; offset++ {
		require.True(b, time.Now().Before(deadline))
		require.NoError(b, log.Append(offset, offset, "tenant", variedRequest(offset)))
	}
	req := benchmarkRequest(0)
	keys := SeriesKeys("tenant", req)
	_, err := log.Encode(0, 0, nowMs(), "tenant", req, keys)
	require.NoError(b, err)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := log.Encode(1, 0, nowMs(), "tenant", req, keys); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSeriesKeys(b *testing.B) {
	req := benchmarkRequest(0)
	b.ReportAllocs()
	for b.Loop() {
		SeriesKeys("tenant", req)
	}
}

// The Rust ingester replays a version 4 file Go wrote, with a dictionary Go trained. Needs the
// Rust golden tool, which prints each record it replays: RUST_SEGMENT_REPLAY=<binary>.
func TestRustReplaysVersion4FilesGoWrote(t *testing.T) {
	binary := os.Getenv("RUST_SEGMENT_REPLAY")
	if binary == "" {
		t.Skip("RUST_SEGMENT_REPLAY is not set")
	}
	root := t.TempDir()
	log, _ := openLog(t, root, 0, noRetention())
	log.dictionaryFrames = 50
	var end int64
	deadline := time.Now().Add(10 * time.Second)
	for end = 1; log.current == nil || log.current.dictionary.state != ready; end++ {
		require.True(t, time.Now().Before(deadline))
		require.NoError(t, log.Append(end, end, "tenant", variedRequest(end)))
	}
	for stop := end + 20; end < stop; end++ {
		require.NoError(t, log.Append(end, end, "tenant", variedRequest(end)))
	}
	require.NoError(t, log.Close())
	frames := framesOf(t, openSegment(t, directoryOf(root, 0, "topic", 0)))
	require.NotZero(t, frames[len(frames)-1].dictionary)

	output, err := exec.Command(binary, "replay", root, "topic").Output()
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	require.Len(t, lines, int(end-1))
	for i, line := range lines {
		offset := int64(i + 1)
		require.Equal(t, fmt.Sprintf("%d %d tenant %s", offset, offset, canonical(variedRequest(offset))), line)
	}
}

// The Rust golden tool's canonical form of a replayed request, whose series have no label spans.
func canonical(request *record.DecodedRequest) string {
	var out strings.Builder
	fmt.Fprintf(&out, "source=%d", request.Source)
	for _, series := range request.Series {
		out.WriteString(" series[")
		for _, pair := range series.Labels {
			fmt.Fprintf(&out, "l=%x:%x,", pair[0], pair[1])
		}
		for _, sample := range series.Samples {
			fmt.Fprintf(&out, "s=%d:%016x,", sample.TimestampMs, math.Float64bits(sample.Value))
		}
		for _, histogram := range series.Histograms {
			encoded, _ := histogram.Marshal()
			fmt.Fprintf(&out, "h=%x,", encoded)
		}
		for _, exemplar := range series.Exemplars {
			encoded, _ := exemplar.Marshal()
			fmt.Fprintf(&out, "e=%x,", encoded)
		}
		fmt.Fprintf(&out, "c=%d,span=none]", series.CreatedTimestamp)
	}
	for _, metadata := range request.Metadata {
		encoded, _ := metadata.Marshal()
		fmt.Fprintf(&out, " m=%x", encoded)
	}
	return out.String()
}

// Compresses the payloads of a segment file with each candidate codec, SEGMENT_FILE or the Rust
// fixture; SEGMENT_BATCH=n compresses n frames together, to see what fewer, larger frames give.
func BenchmarkSegmentCodecs(b *testing.B) {
	path := os.Getenv("SEGMENT_FILE")
	if path == "" {
		path = rustV4Fixture
	}
	data, err := os.ReadFile(path)
	require.NoError(b, err)
	plain, err := zstd.NewReader(nil)
	require.NoError(b, err)
	decoder := plain
	var payloads [][]byte
	// Stops at the first torn frame, like replay.
	for at := fileHeaderLen; at+frameHeaderLen <= len(data); {
		length := int(binary.LittleEndian.Uint32(data[at : at+4]))
		body := at + frameHeaderLen
		if length < framePrefixLen || body+length > len(data) {
			break
		}
		frame := data[body : body+length]
		at = body + length
		if int64(binary.LittleEndian.Uint64(frame[:8])) == dictionaryOffset {
			decoder, err = zstd.NewReader(nil, zstd.WithDecoderDicts(frame[framePrefixLen:]))
			require.NoError(b, err)
			continue
		}
		payload, err := decoder.DecodeAll(frame[framePrefixLen:], nil)
		if err != nil {
			payload, err = plain.DecodeAll(frame[framePrefixLen:], nil)
		}
		require.NoError(b, err)
		payloads = append(payloads, payload)
	}
	if batch, _ := strconv.Atoi(os.Getenv("SEGMENT_BATCH")); batch > 1 {
		var batched [][]byte
		for start := 0; start < len(payloads); start += batch {
			batched = append(batched, slices.Concat(payloads[start:min(start+batch, len(payloads))]...))
		}
		payloads = batched
	}
	raw := 0
	for _, payload := range payloads {
		raw += len(payload)
	}
	trained := trainDictionary(slices.Concat(payloads[:min(len(payloads), dictionaryFrames)]...), func() []int {
		sizes := []int{}
		for _, payload := range payloads[:min(len(payloads), dictionaryFrames)] {
			sizes = append(sizes, len(payload))
		}
		return sizes
	}())
	type codec struct {
		name       string
		compress   func([]byte) []byte
		decompress func([]byte) []byte
	}
	zstdCodec := func(name string, level zstd.EncoderLevel, dictionary []byte) codec {
		encoderOpts := []zstd.EOption{zstd.WithEncoderLevel(level), zstd.WithEncoderCRC(false), zstd.WithSingleSegment(true)}
		var decoderOpts []zstd.DOption
		if dictionary != nil {
			encoderOpts = append(encoderOpts, zstd.WithEncoderDict(dictionary))
			decoderOpts = append(decoderOpts, zstd.WithDecoderDicts(dictionary))
		}
		encoder, err := zstd.NewWriter(nil, encoderOpts...)
		require.NoError(b, err)
		decoder, err := zstd.NewReader(nil, decoderOpts...)
		require.NoError(b, err)
		return codec{
			name:     name,
			compress: func(payload []byte) []byte { return encoder.EncodeAll(payload, nil) },
			decompress: func(compressed []byte) []byte {
				out, err := decoder.DecodeAll(compressed, nil)
				require.NoError(b, err)
				return out
			},
		}
	}
	codecs := []codec{
		zstdCodec("zstd fastest", zstd.SpeedFastest, nil),
		zstdCodec("zstd default", zstd.SpeedDefault, nil),
		{
			name:     "s2",
			compress: func(payload []byte) []byte { return s2.Encode(nil, payload) },
			decompress: func(compressed []byte) []byte {
				out, err := s2.Decode(nil, compressed)
				require.NoError(b, err)
				return out
			},
		},
		{
			name:     "snappy",
			compress: func(payload []byte) []byte { return snappy.Encode(nil, payload) },
			decompress: func(compressed []byte) []byte {
				out, err := snappy.Decode(nil, compressed)
				require.NoError(b, err)
				return out
			},
		},
	}
	if trained != nil {
		codecs = append([]codec{
			zstdCodec("zstd fastest dict", zstd.SpeedFastest, trained),
			zstdCodec("zstd default dict", zstd.SpeedDefault, trained),
		}, codecs...)
	}
	for _, c := range codecs {
		b.Run(c.name, func(b *testing.B) {
			b.SetBytes(int64(raw))
			var compressed [][]byte
			for b.Loop() {
				compressed = compressed[:0]
				for _, payload := range payloads {
					compressed = append(compressed, c.compress(payload))
				}
			}
			size := 0
			for i, frame := range compressed {
				size += len(frame)
				require.Equal(b, payloads[i], c.decompress(frame))
			}
			b.ReportMetric(float64(raw)/float64(size), "ratio")
		})
	}
}
