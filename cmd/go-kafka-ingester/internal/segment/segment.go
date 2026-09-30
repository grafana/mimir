// SPDX-License-Identifier: AGPL-3.0-only

// Package segment is the local log of consumed Kafka records, which a restart replays instead of
// consuming the partition again. Its files are byte-compatible with the Rust ingester's.
package segment

import (
	"bufio"
	"cmp"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/klauspost/compress/dict"
	"github.com/klauspost/compress/zstd"
	"github.com/zeebo/xxh3"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/labels"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/record"
	"github.com/grafana/mimir/pkg/mimirpb"
)

var (
	fileMagic       = []byte("MIMIRH01")
	checkpointMagic = []byte("MIMIRCP1")
)

const (
	legacyFileVersion = 1
	// Every frame holds its series' labels.
	compressedFileVersion = 2
	// Like the Prometheus WAL, a file holds a series' labels once and refers to them afterwards.
	seriesIDFileVersion = 3
	// A file may hold a dictionary trained on its first frames, which the frames after it use.
	fileFormatVersion = 4
	checkpointVersion = 1
	fileHeaderLen     = 28
	frameHeaderLen    = 8
	framePrefixLen    = 24
	hourMs            = 60 * 60 * 1000
	maxFrameLen       = 128 * 1024 * 1024
	// A frame holds one Kafka record of a few KB, too little for zstd to learn from; a dictionary
	// trained on a file's first frames gives the rest of the file what they have in common.
	dictionaryFrames      = 2000
	dictionarySampleBytes = 8 << 20
	dictionaryBytes       = 112 * 1024
	// What a dictionary frame records as its offset: no Kafka record has it.
	dictionaryOffset = -1
	// Bounds what one replay batch decodes, so highly compressible records cannot grow startup
	// memory without limit.
	batchDecodedBytes = 16 * 1024 * 1024
	batchFrames       = 32
)

// zstd's fastest level: with a file's dictionary, frames are still smaller than a stronger level
// gives without one, and compress twice as fast. Frames hold one Kafka record, so setting up an
// encoder per frame would cost more than compressing it.
var plainEncoder = mustEncoder()

func mustEncoder(opts ...zstd.EOption) *zstd.Encoder {
	encoder, err := zstd.NewWriter(nil, encoderOptions(opts...)...)
	if err != nil {
		panic(err)
	}
	return encoder
}

// encoderOptions compress like the Rust ingester's libzstd: single segment frames always record
// their decoded size, which replay batches frames by.
func encoderOptions(opts ...zstd.EOption) []zstd.EOption {
	return append([]zstd.EOption{
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderCRC(false),
		zstd.WithSingleSegment(true),
	}, opts...)
}

type RecoveredRecord struct {
	Offset           int64
	KafkaTimestampMs int64
	IngestedMs       int64
	Tenant           string
	Request          record.DecodedRequest
}

// SeriesKey is a series' identity within a segment file: its tenant and labels, hashed with a seed
// chosen by the process, so crafted labels cannot make two series share an id. Files are only
// written by the process that created them, so keys never outlive their seed.
type SeriesKey = xxh3.Uint128

var keySeed = func() uint64 {
	var seed [8]byte
	if _, err := rand.Read(seed[:]); err != nil {
		panic(err)
	}
	return binary.LittleEndian.Uint64(seed[:])
}()

// Keeps keys of encoded labels apart from keys of copied labels.
const encodedLabels = 0x9e3779b97f4a7c15

var keyBuffers = sync.Pool{New: func() any { b := make([]byte, 0, 1024); return &b }}

// SeriesKeys returns the SeriesKey of each series of request.
func SeriesKeys(tenant string, request *record.DecodedRequest) []SeriesKey {
	bufferPointer := keyBuffers.Get().(*[]byte)
	defer keyBuffers.Put(bufferPointer)
	keys := make([]SeriesKey, 0, len(request.Series))
	for i := range request.Series {
		buffer := putKeyPart((*bufferPointer)[:0], tenant)
		for _, pair := range request.Series[i].Labels {
			buffer = putKeyPart(buffer, pair[0])
			buffer = putKeyPart(buffer, pair[1])
		}
		*bufferPointer = buffer
		keys = append(keys, xxh3.Hash128Seed(buffer, keySeed))
	}
	return keys
}

// SeriesKeysWithLabelBytes is like SeriesKeys, hashing a series' encoded labels in rec where spans
// has them, rather than copying its labels. Those keys differ from SeriesKeys', so a series whose
// labels come either way is defined twice in a file, which only costs its labels.
func SeriesKeysWithLabelBytes(tenant string, request *record.DecodedRequest, rec []byte, spans []record.Span) []SeriesKey {
	if len(spans) != len(request.Series) {
		return SeriesKeys(tenant, request)
	}
	for _, span := range spans {
		if !span.OK {
			return SeriesKeys(tenant, request)
		}
	}
	// The tenant picks the seed, so series of different tenants never share a key.
	seed := xxh3.Hash128Seed(unsafe.Slice(unsafe.StringData(tenant), len(tenant)), keySeed^encodedLabels).Lo
	keys := make([]SeriesKey, 0, len(spans))
	for _, span := range spans {
		keys = append(keys, xxh3.Hash128Seed(rec[span.Start:span.End], seed))
	}
	return keys
}

func putKeyPart(buffer []byte, part string) []byte {
	buffer = binary.LittleEndian.AppendUint32(buffer, uint32(len(part)))
	return append(buffer, part...)
}

type dictionaryState int

const (
	sampling dictionaryState = iota
	// Trained on another goroutine, so ingestion doesn't wait.
	training
	ready
	// Training failed, or the file started without samples to spare; frames go without one.
	unavailable
)

// dictionary is a file's dictionary, from its first frames' payloads to the encoder of the frames
// after.
type dictionary struct {
	state   dictionaryState
	samples []byte
	sizes   []int
	trained chan []byte
	bytes   []byte
	encoder *zstd.Encoder
}

// poll takes a trained dictionary once it is there.
func (d *dictionary) poll() {
	if d.state != training {
		return
	}
	select {
	case trained := <-d.trained:
		d.state = unavailable
		if trained == nil {
			return
		}
		encoder, err := zstd.NewWriter(nil, encoderOptions(zstd.WithEncoderDict(trained))...)
		if err != nil {
			return
		}
		d.state, d.bytes, d.encoder = ready, trained, encoder
	default:
	}
}

// sample keeps payload to train on, and starts training once there are enough.
func (d *dictionary) sample(payload []byte, frames int) {
	if d.state != sampling {
		return
	}
	d.samples = append(d.samples, payload...)
	d.sizes = append(d.sizes, len(payload))
	if len(d.sizes) < frames && len(d.samples) < dictionarySampleBytes {
		return
	}
	samples, sizes := d.samples, d.sizes
	d.samples, d.sizes = nil, nil
	d.trained = make(chan []byte, 1)
	d.state = training
	go func(trained chan<- []byte) { trained <- trainDictionary(samples, sizes) }(d.trained)
}

// trainDictionary returns a dictionary for the payloads in samples, or nil when none can be
// trained from them. Training wants samples many times the dictionary's size, and frames can only
// name a dictionary by id.
func trainDictionary(samples []byte, sizes []int) []byte {
	size := min(dictionaryBytes, len(samples)/20)
	if size < 1024 {
		return nil
	}
	inputs := make([][]byte, 0, len(sizes))
	at := 0
	for _, length := range sizes {
		inputs = append(inputs, samples[at:at+length])
		at += length
	}
	trained, err := dict.BuildZstdDict(inputs, dict.Options{MaxDictSize: size, HashBytes: 6, ZstdLevel: zstd.SpeedFastest})
	if err != nil || len(trained) < 8 || binary.LittleEndian.Uint32(trained[4:8]) == 0 {
		return nil
	}
	return trained
}

type currentFile struct {
	hour     int64
	sequence uint32
	file     *os.File
	// Which log instance's file this is, so a frame is only written to the file it was encoded
	// for.
	id uint64
	// The id of each series the file holds, in the order the file defined them.
	series     map[SeriesKey]uint32
	dictionary dictionary
	// Whether the dictionary frame is in the file, which it must be before a frame that uses it.
	dictionaryWritten bool
}

type Log struct {
	directory     string
	cluster       uint32
	partition     int32
	retentionMs   int64
	hasRetention  bool
	current       *currentFile
	nextFileID    uint64
	lastOffset    int64
	hasLast       bool
	pendingOffset int64
	hasPending    bool
	payload       []byte
	// Frames a file's dictionary is trained on.
	dictionaryFrames int
}

// CompressedFrame is a record encoded for the current segment file, written once the store has
// applied it.
type CompressedFrame struct {
	offset int64
	file   uint64
	// The frame's header, filled in when written, then its body: the file gets it in one write.
	frame []byte
	// The dictionary the frame was compressed with, which the file must hold before it.
	dictionary []byte
}

func (f *CompressedFrame) Offset() int64 { return f.offset }

// Retention is how long records are kept, when set.
type Retention struct {
	Ms  int64
	Set bool
}

func directoryOf(root string, cluster uint32, topic string, partition int32) string {
	return filepath.Join(root, fmt.Sprintf("cluster-%d-partition-%d-topic-%s", cluster, partition, hex.EncodeToString([]byte(topic))))
}

// Open opens the log, returning every record it holds.
func Open(root string, cluster int, topic string, partition int32, retention Retention) (*Log, []RecoveredRecord, error) {
	var records []RecoveredRecord
	log, err := OpenReplaying(root, cluster, topic, partition, retention, 2, func(record RecoveredRecord) error {
		records = append(records, record)
		return nil
	})
	return log, records, err
}

// OpenReplaying opens the log, passing each record it holds to replay, in offset order.
func OpenReplaying(root string, cluster int, topic string, partition int32, retention Retention, decodeThreads int, replay func(RecoveredRecord) error) (*Log, error) {
	if cluster < 0 || cluster > math.MaxUint32 {
		return nil, errors.New("Kafka cluster index exceeds u32")
	}
	directory := directoryOf(root, uint32(cluster), topic, partition)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("create segment directory %s: %w", directory, err)
	}
	paths, err := segmentPaths(directory)
	if err != nil {
		return nil, err
	}
	order := func(path string) int {
		switch filepath.Ext(path) {
		case ".legacy":
			return 0
		case ".segment":
			return 1
		}
		return 2
	}
	slices.SortFunc(paths, func(a, b string) int {
		ha, sa, oka := segmentID(a)
		hb, sb, okb := segmentID(b)
		// Like Rust's Option ordering: names without an id come first.
		if c := cmp.Compare(boolInt(oka), boolInt(okb)); c != 0 {
			return c
		}
		if c := cmp.Compare(ha, hb); c != 0 {
			return c
		}
		if c := cmp.Compare(sa, sb); c != 0 {
			return c
		}
		return cmp.Compare(order(a), order(b))
	})
	var segmentOffset, replayedOffset optional
	var cutoff optional
	if retention.Set {
		cutoff = optional{value: nowMs() - retention.Ms, ok: true}
	}
	for _, path := range paths {
		mutable := filepath.Ext(path) == ".open"
		offset, err := readSegment(path, uint32(cluster), partition, mutable, cutoff, decodeThreads, func(record RecoveredRecord) error {
			if !replayedOffset.ok || record.Offset > replayedOffset.value {
				replayedOffset = optional{value: record.Offset, ok: true}
				return replay(record)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		segmentOffset = maxOffset(segmentOffset, offset)
	}
	checkpoint, err := readCheckpoint(directory)
	if err != nil {
		return nil, err
	}
	if segmentOffset.ok && checkpoint.ok && checkpoint.value > segmentOffset.value {
		return nil, fmt.Errorf("checkpoint in %s is ahead of durable segment data", directory)
	}
	last := maxOffset(segmentOffset, checkpoint)
	log := &Log{
		directory:        directory,
		cluster:          uint32(cluster),
		partition:        partition,
		retentionMs:      retention.Ms,
		hasRetention:     retention.Set,
		lastOffset:       last.value,
		hasLast:          last.ok,
		dictionaryFrames: dictionaryFrames,
	}
	if err := log.removeExpired(); err != nil {
		return nil, err
	}
	return log, nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// OpenAtCheckpoint opens the log without replaying it when its checkpoint equals expected, which a
// head snapshot written after the final flush guarantees; otherwise it returns nil.
func OpenAtCheckpoint(root string, cluster int, topic string, partition int32, retention Retention, expected int64, hasExpected bool) (*Log, error) {
	if cluster < 0 || cluster > math.MaxUint32 {
		return nil, errors.New("Kafka cluster index exceeds u32")
	}
	directory := directoryOf(root, uint32(cluster), topic, partition)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("create segment directory %s: %w", directory, err)
	}
	checkpoint, err := readCheckpoint(directory)
	if err != nil {
		return nil, err
	}
	if checkpoint.ok != hasExpected || (checkpoint.ok && checkpoint.value != expected) {
		return nil, nil
	}
	log := &Log{
		directory:        directory,
		cluster:          uint32(cluster),
		partition:        partition,
		retentionMs:      retention.Ms,
		hasRetention:     retention.Set,
		lastOffset:       checkpoint.value,
		hasLast:          checkpoint.ok,
		dictionaryFrames: dictionaryFrames,
	}
	if err := log.removeExpired(); err != nil {
		return nil, err
	}
	return log, nil
}

// LastOffset is the offset of the last record the log holds or checkpointed.
func (l *Log) LastOffset() (int64, bool) { return l.lastOffset, l.hasLast }

func (l *Log) Append(offset, timestampMs int64, tenant string, request *record.DecodedRequest) error {
	return l.appendAt(offset, timestampMs, tenant, request, nowMs())
}

func (l *Log) appendAt(offset, timestampMs int64, tenant string, request *record.DecodedRequest, ingestedMs int64) error {
	if err := l.CheckOffset(offset); err != nil {
		return err
	}
	if err := l.BeginBatch(ingestedMs); err != nil {
		return err
	}
	frame, err := l.Encode(offset, timestampMs, ingestedMs, tenant, request, SeriesKeys(tenant, request))
	if err != nil {
		return err
	}
	return l.AppendCompressed(frame)
}

// BeginBatch starts the frames of one batch: they all go to the file of ingestedMs's hour, since a
// frame refers to series its file defined before it.
func (l *Log) BeginBatch(ingestedMs int64) error {
	return l.rotate(hourStart(ingestedMs))
}

// Encode encodes a record for the current file, with a series' labels the first time the file has
// it and its id in the file afterwards. keys are the SeriesKeys of request.
func (l *Log) Encode(offset, timestampMs, ingestedMs int64, tenant string, request *record.DecodedRequest, keys []SeriesKey) (*CompressedFrame, error) {
	if len(keys) != len(request.Series) {
		return nil, errors.New("segment frame needs one key per series")
	}
	current := l.current
	if current == nil {
		return nil, errors.New("segment file is not open")
	}
	payload := l.payload[:0]
	var err error
	if payload, err = putString(payload, tenant); err != nil {
		return nil, err
	}
	payload = putI32(payload, request.Source)
	if payload, err = putMetadata(payload, request.Metadata); err != nil {
		return nil, err
	}
	if payload, err = putLen(payload, len(request.Series)); err != nil {
		return nil, err
	}
	for i := range request.Series {
		series := &request.Series[i]
		if id, ok := current.series[keys[i]]; ok {
			payload = labels.PutVarint(payload, uint64(id)+1)
		} else {
			current.series[keys[i]] = uint32(len(current.series))
			payload = labels.PutVarint(payload, 0)
			if payload, err = putLen(payload, len(series.Labels)); err != nil {
				return nil, err
			}
			for _, pair := range series.Labels {
				if payload, err = putString(payload, pair[0]); err != nil {
					return nil, err
				}
				if payload, err = putString(payload, pair[1]); err != nil {
					return nil, err
				}
			}
		}
		if payload, err = putSeriesData(payload, series); err != nil {
			return nil, err
		}
	}
	l.payload = payload
	current.dictionary.poll()
	frame := make([]byte, frameHeaderLen, frameHeaderLen+framePrefixLen+len(payload)/2+64)
	frame = putI64(frame, offset)
	frame = putI64(frame, timestampMs)
	frame = putI64(frame, ingestedMs)
	var dictionaryBytes []byte
	if current.dictionary.state == ready {
		frame = current.dictionary.encoder.EncodeAll(payload, frame)
		dictionaryBytes = current.dictionary.bytes
	} else {
		frame = plainEncoder.EncodeAll(payload, frame)
	}
	current.dictionary.sample(payload, l.dictionaryFrames)
	if len(frame)-frameHeaderLen > maxFrameLen {
		return nil, errors.New("compressed segment frame exceeds 128 MiB")
	}
	return &CompressedFrame{offset: offset, file: current.id, frame: frame, dictionary: dictionaryBytes}, nil
}

func (l *Log) CheckOffset(offset int64) error {
	if l.hasLast && offset <= l.lastOffset {
		return fmt.Errorf("Kafka offset %d is not after persisted offset %d", offset, l.lastOffset)
	}
	return nil
}

func (l *Log) AppendCompressed(frame *CompressedFrame) error {
	if err := l.CheckOffset(frame.offset); err != nil {
		return err
	}
	if err := fillFrameHeader(frame.frame); err != nil {
		return err
	}
	current := l.current
	if current == nil {
		return errors.New("segment file is not open")
	}
	if current.id != frame.file {
		return errors.New("segment frame was encoded for another file")
	}
	if frame.dictionary != nil && !current.dictionaryWritten {
		dictionaryFrame := make([]byte, frameHeaderLen, frameHeaderLen+framePrefixLen+len(frame.dictionary))
		dictionaryFrame = putI64(dictionaryFrame, dictionaryOffset)
		dictionaryFrame = putI64(dictionaryFrame, 0)
		dictionaryFrame = putI64(dictionaryFrame, 0)
		dictionaryFrame = append(dictionaryFrame, frame.dictionary...)
		if err := fillFrameHeader(dictionaryFrame); err != nil {
			return err
		}
		if _, err := current.file.Write(dictionaryFrame); err != nil {
			return err
		}
		current.dictionaryWritten = true
	}
	if _, err := current.file.Write(frame.frame); err != nil {
		return err
	}
	l.lastOffset, l.hasLast = frame.offset, true
	l.pendingOffset, l.hasPending = frame.offset, true
	return nil
}

// Flush makes what was appended durable, and checkpoints its offset.
func (l *Log) Flush() error {
	if !l.hasPending {
		return nil
	}
	if l.current == nil {
		return errors.New("segment file is not open")
	}
	if err := l.current.file.Sync(); err != nil {
		return err
	}
	if err := writeCheckpoint(l.directory, l.pendingOffset); err != nil {
		return err
	}
	l.hasPending = false
	return nil
}

// Close flushes the log and closes its file, which stays open for the next process to seal.
// Without a file there is nothing to flush.
func (l *Log) Close() error {
	if l.current == nil {
		return nil
	}
	err := l.Flush()
	err = errors.Join(err, l.current.file.Close())
	l.current = nil
	return err
}

func (l *Log) Maintain() error {
	if err := l.Flush(); err != nil {
		return err
	}
	if l.current != nil && l.current.hour < hourStart(nowMs()) {
		if err := l.sealCurrent(); err != nil {
			return err
		}
	}
	return l.removeExpired()
}

func (l *Log) rotate(hour int64) error {
	if l.current != nil && l.current.hour == hour {
		return nil
	}
	if err := l.Flush(); err != nil {
		return err
	}
	if err := l.sealCurrent(); err != nil {
		return err
	}
	// A file left open by an earlier process is sealed rather than appended to: the series ids it
	// defined went with that process.
	paths, err := segmentPaths(l.directory)
	if err != nil {
		return err
	}
	nextSequence := map[int64]uint32{}
	for _, path := range paths {
		if pathHour, pathSequence, ok := segmentID(path); ok {
			nextSequence[pathHour] = max(nextSequence[pathHour], pathSequence+1)
		}
	}
	for _, path := range paths {
		if filepath.Ext(path) != ".open" {
			continue
		}
		to := strings.TrimSuffix(path, ".open") + ".segment"
		if exists(to) {
			// The log before sequences could reopen an hour it had sealed, when a record prepared
			// before the hour ended was appended after, leaving both files.
			pathHour, _, ok := segmentID(path)
			if !ok {
				return errors.New("segment name has no hour")
			}
			to = filepath.Join(l.directory, segmentName(pathHour, nextSequence[pathHour], "segment"))
			nextSequence[pathHour]++
		}
		if exists(to) {
			return fmt.Errorf("refusing to overwrite sealed segment %s", to)
		}
		if err := os.Rename(path, to); err != nil {
			return fmt.Errorf("seal recovered segment %s: %w", path, err)
		}
		if err := syncDirectory(l.directory); err != nil {
			return err
		}
	}
	if err := l.removeExpired(); err != nil {
		return err
	}
	sequence := nextSequence[hour]
	path := filepath.Join(l.directory, segmentName(hour, sequence, "open"))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("open segment %s: %w", path, err)
	}
	if err := writeFileHeader(file, l.cluster, l.partition, hour); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := syncDirectory(l.directory); err != nil {
		_ = file.Close()
		return err
	}
	l.current = &currentFile{hour: hour, sequence: sequence, file: file, id: l.nextFileID, series: map[SeriesKey]uint32{}}
	l.nextFileID++
	return nil
}

func (l *Log) sealCurrent() error {
	current := l.current
	if current == nil {
		return nil
	}
	l.current = nil
	if err := current.file.Sync(); err != nil {
		return err
	}
	if err := current.file.Close(); err != nil {
		return err
	}
	from := filepath.Join(l.directory, segmentName(current.hour, current.sequence, "open"))
	to := strings.TrimSuffix(from, ".open") + ".segment"
	if err := os.Rename(from, to); err != nil {
		return fmt.Errorf("seal segment %s: %w", from, err)
	}
	return syncDirectory(l.directory)
}

func (l *Log) removeExpired() error {
	if !l.hasRetention {
		return nil
	}
	cutoff := nowMs() - l.retentionMs
	paths, err := segmentPaths(l.directory)
	if err != nil {
		return err
	}
	for _, path := range paths {
		hour, _, ok := segmentID(path)
		if !ok {
			continue
		}
		if (l.current == nil || hour != l.current.hour) && hour+hourMs < cutoff {
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("remove expired segment %s: %w", path, err)
			}
		}
	}
	return nil
}

// fillFrameHeader writes the length and checksum of the body after it into a frame's header.
func fillFrameHeader(frame []byte) error {
	body := frame[frameHeaderLen:]
	if uint64(len(body)) > math.MaxUint32 {
		return errors.New("segment frame exceeds 4 GiB")
	}
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(body))
	return nil
}

type optional struct {
	value int64
	ok    bool
}

func maxOffset(first, second optional) optional {
	switch {
	case first.ok && second.ok:
		return optional{value: max(first.value, second.value), ok: true}
	case first.ok:
		return first
	}
	return second
}

// What replaying a file carries from one batch of frames to the next.
type replayState struct {
	version uint32
	cutoff  optional
	// The labels of the series the file defined, by id.
	series []definedLabels
	// The file's dictionary, once replay read it.
	decoder *zstd.Decoder
}

func (s *replayState) close() {
	if s.decoder != nil {
		s.decoder.Close()
	}
}

// Frames without a dictionary are decoded by one shared decoder.
var plainDecoder = func() *zstd.Decoder {
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxFrameLen), zstd.WithDecoderConcurrency(0))
	if err != nil {
		panic(err)
	}
	return decoder
}()

type pendingFrame struct {
	at   int64
	body []byte
}

func readSegment(path string, expectedCluster uint32, expectedPartition int32, mutable bool, cutoff optional, decodeThreads int, replay func(RecoveredRecord) error) (optional, error) {
	flags := os.O_RDONLY
	if mutable {
		flags = os.O_RDWR
	}
	file, err := os.OpenFile(path, flags, 0)
	if err != nil {
		return optional{}, fmt.Errorf("open segment %s: %w", path, err)
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, 1024*1024)
	header := make([]byte, fileHeaderLen)
	if _, err := io.ReadFull(reader, header); err != nil {
		return optional{}, fmt.Errorf("segment %s has a truncated header: %w", path, err)
	}
	if string(header[:8]) != string(fileMagic) {
		return optional{}, fmt.Errorf("segment %s has invalid magic", path)
	}
	version := binary.LittleEndian.Uint32(header[8:12])
	if version < legacyFileVersion || version > fileFormatVersion {
		return optional{}, fmt.Errorf("segment %s has unsupported format version", path)
	}
	if binary.LittleEndian.Uint32(header[12:16]) != expectedCluster || int32(binary.LittleEndian.Uint32(header[16:20])) != expectedPartition {
		return optional{}, fmt.Errorf("segment %s belongs to another Kafka source", path)
	}
	headerHour := int64(binary.LittleEndian.Uint64(header[20:28]))
	if hour, _, ok := segmentID(path); !ok || hour != headerHour {
		return optional{}, fmt.Errorf("segment %s hour does not match its filename", path)
	}

	var lastOffset optional
	validLen := int64(fileHeaderLen)
	var frames []pendingFrame
	var batchBytes uint64
	state := &replayState{version: version, cutoff: cutoff}
	defer state.close()
	flush := func() error {
		err := replayBatch(state, path, frames, max(decodeThreads, 1), replay, &lastOffset)
		frames = frames[:0]
		batchBytes = 0
		return err
	}
	torn := func() (optional, error) {
		if err := flush(); err != nil {
			return optional{}, err
		}
		if !mutable {
			return optional{}, fmt.Errorf("sealed segment %s is corrupt", path)
		}
		if err := file.Truncate(validLen); err != nil {
			return optional{}, err
		}
		if err := file.Sync(); err != nil {
			return optional{}, err
		}
		return lastOffset, nil
	}
	frameHeader := make([]byte, frameHeaderLen)
	for {
		if _, err := io.ReadFull(reader, frameHeader); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return torn()
			}
			if flushErr := flush(); flushErr != nil {
				return optional{}, flushErr
			}
			return optional{}, err
		}
		length := binary.LittleEndian.Uint32(frameHeader[:4])
		checksum := binary.LittleEndian.Uint32(frameHeader[4:])
		// A crash can leave a zero-filled tail up to the next block; its length 0 and CRC 0 would
		// otherwise pass the checksum because the CRC of an empty body is 0.
		if length < framePrefixLen || length > maxFrameLen {
			return torn()
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(reader, body); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return torn()
			}
			if flushErr := flush(); flushErr != nil {
				return optional{}, flushErr
			}
			return optional{}, err
		}
		if crc32.ChecksumIEEE(body) != checksum {
			return torn()
		}
		frameOffset := int64(binary.LittleEndian.Uint64(body[:8]))
		// The frames after it are decoded with the file's dictionary.
		if version >= fileFormatVersion && frameOffset == dictionaryOffset {
			if err := flush(); err != nil {
				return optional{}, err
			}
			if state.decoder != nil {
				state.decoder.Close()
			}
			decoder, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxFrameLen), zstd.WithDecoderConcurrency(0),
				zstd.WithDecoderDicts(append([]byte(nil), body[framePrefixLen:]...)))
			if err != nil {
				return optional{}, fmt.Errorf("load the dictionary of %s: %w", path, err)
			}
			state.decoder = decoder
			validLen += frameHeaderLen + int64(length)
			continue
		}
		// Expired frames of a file with series ids still define series that later frames use.
		if cutoff.ok && version < seriesIDFileVersion {
			if ingestedMs := int64(binary.LittleEndian.Uint64(body[16:24])); ingestedMs < cutoff.value {
				if err := flush(); err != nil {
					return optional{}, err
				}
				lastOffset = optional{value: frameOffset, ok: true}
				validLen += frameHeaderLen + int64(length)
				continue
			}
		}
		decodedSize := uint64(length)
		if version >= compressedFileVersion {
			decodedSize = maxFrameLen
			var frameInfo zstd.Header
			if frameInfo.Decode(body[framePrefixLen:]) == nil && frameInfo.HasFCS {
				decodedSize = frameInfo.FrameContentSize
			}
		}
		if len(frames) > 0 && batchBytes+decodedSize > batchDecodedBytes {
			if err := flush(); err != nil {
				return optional{}, err
			}
		}
		batchBytes += decodedSize
		frames = append(frames, pendingFrame{at: validLen, body: body})
		validLen += frameHeaderLen + int64(length)
		if len(frames) >= batchFrames || batchBytes >= batchDecodedBytes {
			if err := flush(); err != nil {
				return optional{}, err
			}
		}
	}
	if err := flush(); err != nil {
		return optional{}, err
	}
	return lastOffset, nil
}

// A decoded frame: a whole record, or one whose series refer to the file's series by id.
type decodedFrame struct {
	record  RecoveredRecord
	withIDs *frameWithSeriesIDs
	err     error
}

func replayBatch(state *replayState, path string, frames []pendingFrame, threads int, replay func(RecoveredRecord) error, lastOffset *optional) error {
	if len(frames) == 0 {
		return nil
	}
	decoded := make([]decodedFrame, len(frames))
	var group sync.WaitGroup
	next := make(chan int)
	for range min(threads, len(frames)) {
		group.Go(func() {
			for index := range next {
				decoded[index] = decodeBody(state, path, frames[index])
			}
		})
	}
	for index := range frames {
		next <- index
	}
	close(next)
	group.Wait()
	// Replaying in file order preserves offset and duplicate handling across batches, and defines
	// series before frames use them.
	for _, frame := range decoded {
		if frame.err != nil {
			return frame.err
		}
		rec := frame.record
		if frame.withIDs != nil {
			expired := state.cutoff.ok && frame.withIDs.ingestedMs < state.cutoff.value
			resolved, err := frame.withIDs.resolve(&state.series)
			if err != nil {
				return fmt.Errorf("resolve series in %s: %w", path, err)
			}
			if expired {
				*lastOffset = optional{value: resolved.Offset, ok: true}
				continue
			}
			rec = resolved
		}
		*lastOffset = optional{value: rec.Offset, ok: true}
		if err := replay(rec); err != nil {
			return err
		}
	}
	return nil
}

func decodeBody(state *replayState, path string, frame pendingFrame) decodedFrame {
	wrap := func(err error) decodedFrame {
		return decodedFrame{err: fmt.Errorf("decode segment frame at byte %d in %s: %w", frame.at, path, err)}
	}
	body := frame.body
	if state.version == legacyFileVersion {
		rec, err := decodeFrame(body)
		if err != nil {
			return wrap(err)
		}
		return decodedFrame{record: rec}
	}
	if len(body) < framePrefixLen {
		return decodedFrame{err: fmt.Errorf("compressed segment frame is too short in %s", path)}
	}
	compressed := body[framePrefixLen:]
	// Frames compressed with the file's dictionary name it; those before it don't.
	var frameInfo zstd.Header
	decoder := plainDecoder
	if frameInfo.Decode(compressed) == nil && frameInfo.DictionaryID != 0 {
		if state.decoder == nil {
			return decodedFrame{err: fmt.Errorf("segment frame needs a dictionary %s doesn't hold before it", path)}
		}
		decoder = state.decoder
	}
	payload, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		return decodedFrame{err: fmt.Errorf("decompress segment frame in %s: %w", path, err)}
	}
	if len(payload) > maxFrameLen {
		return decodedFrame{err: fmt.Errorf("decompress segment frame in %s: frame exceeds 128 MiB", path)}
	}
	if state.version == compressedFileVersion {
		rec, err := decodeFrame(append(append(make([]byte, 0, framePrefixLen+len(payload)), body[:framePrefixLen]...), payload...))
		if err != nil {
			return wrap(err)
		}
		return decodedFrame{record: rec}
	}
	withIDs, err := decodeFrameWithSeriesIDs(body[:framePrefixLen], payload)
	if err != nil {
		return wrap(err)
	}
	return decodedFrame{withIDs: withIDs}
}

func decodeFrame(bytes []byte) (RecoveredRecord, error) {
	c := cursor{bytes: bytes}
	var rec RecoveredRecord
	var err error
	if rec.Offset, err = c.i64(); err != nil {
		return RecoveredRecord{}, err
	}
	if rec.KafkaTimestampMs, err = c.i64(); err != nil {
		return RecoveredRecord{}, err
	}
	if rec.IngestedMs, err = c.i64(); err != nil {
		return RecoveredRecord{}, err
	}
	if rec.Tenant, err = c.string(); err != nil {
		return RecoveredRecord{}, err
	}
	if rec.Request, err = decodeRequest(&c); err != nil {
		return RecoveredRecord{}, err
	}
	if c.remaining() != 0 {
		return RecoveredRecord{}, errors.New("trailing bytes in segment frame")
	}
	return rec, nil
}

// definedLabels is a series a file defined, while replaying it: an hour's file defines every
// series it holds, so they are kept as interned names and values, without the frames that
// defined them.
type definedLabels string

func newDefinedLabels(pairs [][2]string) definedLabels {
	size := 0
	for _, pair := range pairs {
		size += 10 + len(pair[1])
	}
	bytes := make([]byte, 0, size)
	for _, pair := range pairs {
		bytes = labels.PutVarint(bytes, uint64(labels.Intern(pair[0])))
		bytes = labels.PutVarint(bytes, uint64(len(pair[1])))
		bytes = append(bytes, pair[1]...)
	}
	return definedLabels(bytes)
}

func (d definedLabels) pairs() [][2]string {
	names := labels.TakeSnapshot()
	var out [][2]string
	rest := string(d)
	for len(rest) > 0 {
		name := names.Name(uint32(takeVarintString(&rest)))
		length := int(takeVarintString(&rest))
		out = append(out, [2]string{name, rest[:length]})
		rest = rest[length:]
	}
	return out
}

func takeVarintString(s *string) uint64 {
	var value uint64
	var shift uint
	for i := 0; ; i++ {
		c := (*s)[i]
		value |= uint64(c&0x7f) << shift
		if c < 0x80 {
			*s = (*s)[i+1:]
			return value
		}
		shift += 7
	}
}

type frameSeries struct {
	// The series' labels when the frame defines it, or its id.
	labels  [][2]string
	defines bool
	id      uint32
	series  record.DecodedSeries
}

type frameWithSeriesIDs struct {
	offset           int64
	kafkaTimestampMs int64
	ingestedMs       int64
	tenant           string
	source           int32
	metadata         []mimirpb.MetricMetadata
	series           []frameSeries
}

func (f *frameWithSeriesIDs) resolve(defined *[]definedLabels) (RecoveredRecord, error) {
	series := make([]record.DecodedSeries, 0, len(f.series))
	for _, item := range f.series {
		if item.defines {
			*defined = append(*defined, newDefinedLabels(item.labels))
			item.series.Labels = item.labels
		} else {
			if int(item.id) >= len(*defined) {
				return RecoveredRecord{}, fmt.Errorf("series %d is not defined before its use", item.id)
			}
			item.series.Labels = (*defined)[item.id].pairs()
		}
		series = append(series, item.series)
	}
	return RecoveredRecord{
		Offset:           f.offset,
		KafkaTimestampMs: f.kafkaTimestampMs,
		IngestedMs:       f.ingestedMs,
		Tenant:           f.tenant,
		Request:          record.DecodedRequest{Source: f.source, Series: series, Metadata: f.metadata},
	}, nil
}

func decodeFrameWithSeriesIDs(prefix, payload []byte) (*frameWithSeriesIDs, error) {
	frame := &frameWithSeriesIDs{
		offset:           int64(binary.LittleEndian.Uint64(prefix[:8])),
		kafkaTimestampMs: int64(binary.LittleEndian.Uint64(prefix[8:16])),
		ingestedMs:       int64(binary.LittleEndian.Uint64(prefix[16:24])),
	}
	c := cursor{bytes: payload}
	var err error
	if frame.tenant, err = c.string(); err != nil {
		return nil, err
	}
	if frame.source, err = c.i32(); err != nil {
		return nil, err
	}
	if frame.metadata, err = decodeMetadata(&c); err != nil {
		return nil, err
	}
	count, err := c.u32()
	if err != nil {
		return nil, err
	}
	frame.series = make([]frameSeries, 0, min(int(count), c.remaining()))
	for range count {
		var item frameSeries
		id, err := c.varint()
		if err != nil {
			return nil, err
		}
		if id == 0 {
			item.defines = true
			pairs, err := c.u32()
			if err != nil {
				return nil, err
			}
			item.labels = make([][2]string, 0, min(int(pairs), c.remaining()))
			for range pairs {
				name, err := c.label()
				if err != nil {
					return nil, err
				}
				value, err := c.label()
				if err != nil {
					return nil, err
				}
				item.labels = append(item.labels, [2]string{name, value})
			}
		} else {
			if id-1 > math.MaxUint32 {
				return nil, errors.New("series id exceeds u32")
			}
			item.id = uint32(id - 1)
		}
		if item.series, err = decodeSeriesData(&c); err != nil {
			return nil, err
		}
		frame.series = append(frame.series, item)
	}
	if c.remaining() != 0 {
		return nil, errors.New("trailing bytes in segment frame")
	}
	return frame, nil
}

func putMetadata(bytes []byte, metadata []mimirpb.MetricMetadata) ([]byte, error) {
	bytes, err := putLen(bytes, len(metadata))
	if err != nil {
		return nil, err
	}
	for _, item := range metadata {
		bytes = putI32(bytes, int32(item.Type))
		for _, value := range []string{item.MetricFamilyName, item.Help, item.Unit} {
			if bytes, err = putString(bytes, value); err != nil {
				return nil, err
			}
		}
	}
	return bytes, nil
}

func decodeMetadata(c *cursor) ([]mimirpb.MetricMetadata, error) {
	count, err := c.u32()
	if err != nil {
		return nil, err
	}
	var metadata []mimirpb.MetricMetadata
	for range count {
		var item mimirpb.MetricMetadata
		metricType, err := c.i32()
		if err != nil {
			return nil, err
		}
		item.Type = mimirpb.MetricMetadata_MetricType(metricType)
		if item.MetricFamilyName, err = c.string(); err != nil {
			return nil, err
		}
		if item.Help, err = c.string(); err != nil {
			return nil, err
		}
		if item.Unit, err = c.string(); err != nil {
			return nil, err
		}
		metadata = append(metadata, item)
	}
	return metadata, nil
}

// putSeriesData appends everything of a series but its labels.
func putSeriesData(bytes []byte, series *record.DecodedSeries) ([]byte, error) {
	bytes = putI64(bytes, series.CreatedTimestamp)
	bytes, err := putLen(bytes, len(series.Samples))
	if err != nil {
		return nil, err
	}
	for _, sample := range series.Samples {
		bytes = putI64(bytes, sample.TimestampMs)
		bytes = binary.LittleEndian.AppendUint64(bytes, math.Float64bits(sample.Value))
	}
	if bytes, err = putLen(bytes, len(series.Histograms)); err != nil {
		return nil, err
	}
	for i := range series.Histograms {
		if bytes, err = putMessage(bytes, &series.Histograms[i]); err != nil {
			return nil, err
		}
	}
	if bytes, err = putLen(bytes, len(series.Exemplars)); err != nil {
		return nil, err
	}
	for i := range series.Exemplars {
		if bytes, err = putMessage(bytes, &series.Exemplars[i]); err != nil {
			return nil, err
		}
	}
	return bytes, nil
}

type message interface {
	Size() int
	MarshalToSizedBuffer([]byte) (int, error)
}

func putMessage(bytes []byte, m message) ([]byte, error) {
	size := m.Size()
	bytes, err := putLen(bytes, size)
	if err != nil {
		return nil, err
	}
	bytes = slices.Grow(bytes, size)[:len(bytes)+size]
	if _, err := m.MarshalToSizedBuffer(bytes[len(bytes)-size:]); err != nil {
		return nil, err
	}
	return bytes, nil
}

func decodeSeriesData(c *cursor) (record.DecodedSeries, error) {
	var series record.DecodedSeries
	var err error
	if series.CreatedTimestamp, err = c.i64(); err != nil {
		return series, err
	}
	count, err := c.u32()
	if err != nil {
		return series, err
	}
	series.Samples = make([]mimirpb.Sample, 0, min(int(count), c.remaining()/16))
	for range count {
		timestamp, err := c.i64()
		if err != nil {
			return series, err
		}
		bits, err := c.u64()
		if err != nil {
			return series, err
		}
		series.Samples = append(series.Samples, mimirpb.Sample{TimestampMs: timestamp, Value: math.Float64frombits(bits)})
	}
	if count, err = c.u32(); err != nil {
		return series, err
	}
	for range count {
		body, err := c.bytesField()
		if err != nil {
			return series, err
		}
		var histogram mimirpb.Histogram
		if err := histogram.Unmarshal(body); err != nil {
			return series, err
		}
		series.Histograms = append(series.Histograms, histogram)
	}
	if count, err = c.u32(); err != nil {
		return series, err
	}
	for range count {
		body, err := c.bytesField()
		if err != nil {
			return series, err
		}
		var exemplar mimirpb.Exemplar
		if err := exemplar.Unmarshal(body); err != nil {
			return series, err
		}
		series.Exemplars = append(series.Exemplars, exemplar)
	}
	return series, nil
}

// encodeRequest encodes version 1 and 2 frames, where every series has its labels.
func encodeRequest(bytes []byte, request *record.DecodedRequest) ([]byte, error) {
	bytes = putI32(bytes, request.Source)
	bytes, err := putMetadata(bytes, request.Metadata)
	if err != nil {
		return nil, err
	}
	if bytes, err = putLen(bytes, len(request.Series)); err != nil {
		return nil, err
	}
	for i := range request.Series {
		series := &request.Series[i]
		if bytes, err = putLen(bytes, len(series.Labels)); err != nil {
			return nil, err
		}
		for _, pair := range series.Labels {
			if bytes, err = putString(bytes, pair[0]); err != nil {
				return nil, err
			}
			if bytes, err = putString(bytes, pair[1]); err != nil {
				return nil, err
			}
		}
		if bytes, err = putSeriesData(bytes, series); err != nil {
			return nil, err
		}
	}
	return bytes, nil
}

func decodeRequest(c *cursor) (record.DecodedRequest, error) {
	var request record.DecodedRequest
	var err error
	if request.Source, err = c.i32(); err != nil {
		return request, err
	}
	if request.Metadata, err = decodeMetadata(c); err != nil {
		return request, err
	}
	count, err := c.u32()
	if err != nil {
		return request, err
	}
	for range count {
		pairs, err := c.u32()
		if err != nil {
			return request, err
		}
		labelPairs := make([][2]string, 0, min(int(pairs), c.remaining()))
		for range pairs {
			name, err := c.string()
			if err != nil {
				return request, err
			}
			value, err := c.string()
			if err != nil {
				return request, err
			}
			labelPairs = append(labelPairs, [2]string{name, value})
		}
		series, err := decodeSeriesData(c)
		if err != nil {
			return request, err
		}
		series.Labels = labelPairs
		request.Series = append(request.Series, series)
	}
	return request, nil
}

func writeFileHeader(file *os.File, cluster uint32, partition int32, hour int64) error {
	header := make([]byte, 0, fileHeaderLen)
	header = append(header, fileMagic...)
	header = binary.LittleEndian.AppendUint32(header, fileFormatVersion)
	header = binary.LittleEndian.AppendUint32(header, cluster)
	header = binary.LittleEndian.AppendUint32(header, uint32(partition))
	header = binary.LittleEndian.AppendUint64(header, uint64(hour))
	_, err := file.Write(header)
	return err
}

func writeCheckpoint(directory string, offset int64) error {
	bytes := make([]byte, 0, 24)
	bytes = append(bytes, checkpointMagic...)
	bytes = binary.LittleEndian.AppendUint32(bytes, checkpointVersion)
	bytes = binary.LittleEndian.AppendUint64(bytes, uint64(offset))
	bytes = binary.LittleEndian.AppendUint32(bytes, crc32.ChecksumIEEE(bytes))
	temporary := filepath.Join(directory, "checkpoint.tmp")
	file, err := os.Create(temporary)
	if err != nil {
		return err
	}
	if _, err := file.Write(bytes); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, filepath.Join(directory, "checkpoint")); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func readCheckpoint(directory string) (optional, error) {
	path := filepath.Join(directory, "checkpoint")
	bytes, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return optional{}, nil
	}
	if err != nil {
		return optional{}, err
	}
	if len(bytes) != 24 || string(bytes[:8]) != string(checkpointMagic) {
		return optional{}, fmt.Errorf("checkpoint %s is corrupt", path)
	}
	if crc32.ChecksumIEEE(bytes[:20]) != binary.LittleEndian.Uint32(bytes[20:24]) {
		return optional{}, fmt.Errorf("checkpoint %s checksum mismatch", path)
	}
	if binary.LittleEndian.Uint32(bytes[8:12]) != checkpointVersion {
		return optional{}, fmt.Errorf("checkpoint %s has unsupported version", path)
	}
	return optional{value: int64(binary.LittleEndian.Uint64(bytes[12:20])), ok: true}, nil
}

func segmentPaths(directory string) ([]string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, entry := range entries {
		switch filepath.Ext(entry.Name()) {
		case ".open", ".segment", ".legacy":
			paths = append(paths, filepath.Join(directory, entry.Name()))
		}
	}
	return paths, nil
}

func segmentName(hour int64, sequence uint32, extension string) string {
	return fmt.Sprintf("%020d-%04d.%s", hour, sequence, extension)
}

// segmentID returns the hour and sequence of a segment file; files from before sequences are the
// first of their hour.
func segmentID(path string) (int64, uint32, bool) {
	base := filepath.Base(path)
	stem := strings.TrimSuffix(base, filepath.Ext(base))
	if hour, sequence, ok := strings.Cut(stem, "-"); ok {
		h, err := strconv.ParseInt(hour, 10, 64)
		if err != nil {
			return 0, 0, false
		}
		s, err := strconv.ParseUint(sequence, 10, 32)
		if err != nil {
			return 0, 0, false
		}
		return h, uint32(s), true
	}
	h, err := strconv.ParseInt(stem, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return h, 0, true
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func syncDirectory(directory string) error {
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func hourStart(timestampMs int64) int64 {
	hour := timestampMs / hourMs
	if timestampMs%hourMs < 0 {
		hour--
	}
	return hour * hourMs
}

func nowMs() int64 { return time.Now().UnixMilli() }

func putLen(bytes []byte, length int) ([]byte, error) {
	if uint64(length) > math.MaxUint32 {
		return nil, errors.New("segment collection exceeds u32")
	}
	return binary.LittleEndian.AppendUint32(bytes, uint32(length)), nil
}

func putString(bytes []byte, value string) ([]byte, error) {
	bytes, err := putLen(bytes, len(value))
	if err != nil {
		return nil, err
	}
	return append(bytes, value...), nil
}

func putI32(bytes []byte, value int32) []byte {
	return binary.LittleEndian.AppendUint32(bytes, uint32(value))
}

func putI64(bytes []byte, value int64) []byte {
	return binary.LittleEndian.AppendUint64(bytes, uint64(value))
}

var errTruncated = errors.New("truncated segment data")

type cursor struct {
	bytes    []byte
	position int
}

func (c *cursor) remaining() int { return len(c.bytes) - c.position }

func (c *cursor) take(length int) ([]byte, error) {
	if length < 0 || length > c.remaining() {
		return nil, errTruncated
	}
	value := c.bytes[c.position : c.position+length]
	c.position += length
	return value, nil
}

func (c *cursor) u32() (uint32, error) {
	b, err := c.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func (c *cursor) i32() (int32, error) {
	value, err := c.u32()
	return int32(value), err
}

func (c *cursor) u64() (uint64, error) {
	b, err := c.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func (c *cursor) i64() (int64, error) {
	value, err := c.u64()
	return int64(value), err
}

func (c *cursor) varint() (uint64, error) {
	var value uint64
	for shift := uint(0); shift < 64; shift += 7 {
		b, err := c.take(1)
		if err != nil {
			return 0, err
		}
		value |= uint64(b[0]&0x7f) << shift
		if b[0] < 0x80 {
			return value, nil
		}
	}
	return 0, errors.New("segment varint overflows u64")
}

func (c *cursor) bytesField() ([]byte, error) {
	length, err := c.u32()
	if err != nil {
		return nil, err
	}
	return c.take(int(length))
}

// label returns a string of the payload this cursor reads, sharing its buffer.
func (c *cursor) label() (string, error) {
	b, err := c.bytesField()
	if err != nil {
		return "", err
	}
	return record.Label(b), nil
}

func (c *cursor) string() (string, error) {
	b, err := c.bytesField()
	if err != nil {
		return "", err
	}
	if !utf8.Valid(b) {
		return "", errors.New("invalid utf-8 in segment string")
	}
	return string(b), nil
}
