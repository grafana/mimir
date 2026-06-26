package wgo

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// encBufPool pools scratch buffers for intermediate encoding steps to reduce
// heap allocations on the produce hot path. It is package-level rather than
// per-client because encode operations are stateless and sharing the pool
// across client instances improves buffer reuse without introducing any
// correctness concern.
var encBufPool = sync.Pool{New: func() any {
	b := make([]byte, 0, 128*1024)
	return &b
}}

// maxPooledBufCap is the maximum buffer capacity retained in encBufPool.
// Oversized buffers (from unusually large batches) are dropped to prevent
// unbounded pool growth.
const maxPooledBufCap = 16 * 1024 * 1024

// batchFixedFieldsAfterLength is the fixed byte count of RecordBatch fields
// that appear after the Length field, excluding the Records payload.
// PartitionLeaderEpoch(4) + Magic(1) + CRC(4) + Attributes(2) +
// LastOffsetDelta(4) + FirstTimestamp(8) + MaxTimestamp(8) +
// ProducerID(8) + ProducerEpoch(2) + FirstSequence(4) + NumRecords(4) = 49.
const batchFixedFieldsAfterLength = 49

// crcOffset is the byte offset of the CRC field within a serialised RecordBatch.
// FirstOffset(8) + Length(4) + PartitionLeaderEpoch(4) + Magic(1) = 17.
const crcOffset = 17

// recordBatchHeaderBytes is the wire-byte overhead a RecordBatch carries
// in a ProduceRequest before any record body — the cap "tryBuffer" must
// reserve room for. Equals franz-go's recordBatchOverhead. The breakdown
// is the records-bytes length prefix (4) + FirstOffset (8) + Length (4) +
// batchFixedFieldsAfterLength.
const recordBatchHeaderBytes = 4 + 8 + 4 + batchFixedFieldsAfterLength

// batchMaxBytesCeiling is a sanity cap on Config.BatchMaxBytes. A value above
// this is almost certainly misconfiguration — the broker would reject such a
// request anyway — so we fail validation early rather than buffer toward it.
const batchMaxBytesCeiling int32 = 1 << 30 // 1 GiB

// ensureRecordTimestamp defaults an unset record timestamp to now (truncated to
// the millisecond resolution Kafka stores), mirroring franz-go's bufferRecord.
// An already-set timestamp is left untouched.
func ensureRecordTimestamp(record *kgo.Record, now time.Time) {
	if record.Timestamp.IsZero() {
		record.Timestamp = now.Truncate(time.Millisecond)
	}
}

// recordEstimateBytes returns the on-wire byte size of r encoded at the
// given offsetDelta and tsDelta — the length-prefix varint plus the
// length-prefixed body. Used to keep the batch counter aligned with the
// bytes the encoder will eventually emit. The result is int64 so the sum
// can't overflow for a pathologically large record (a 2 GiB value would wrap
// an int32 to a negative size and slip past the per-record gate).
func recordEstimateBytes(r *kgo.Record, offsetDelta int32, tsDelta int64) int64 {
	lengthField := int64(1) + // Attributes (int8, unused)
		int64(kbin.VarlongLen(tsDelta)) +
		int64(kbin.VarintLen(offsetDelta)) +
		int64(kbin.VarintLen(int32(len(r.Key)))) + int64(len(r.Key)) +
		int64(kbin.VarintLen(int32(len(r.Value)))) + int64(len(r.Value)) +
		int64(kbin.VarintLen(int32(len(r.Headers))))
	for _, h := range r.Headers {
		lengthField += int64(kbin.VarintLen(int32(len(h.Key)))) + int64(len(h.Key)) +
			int64(kbin.VarintLen(int32(len(h.Value)))) + int64(len(h.Value))
	}
	return int64(kbin.VarintLen(int32(lengthField))) + lengthField
}

// singleRecordBatchEstimateBytes returns the wire-byte size of a fresh single-
// record batch carrying r. Used as the per-record rejection gate: a record
// whose batch alone exceeds BatchMaxBytes can never be produced, so we fail
// it synchronously instead of letting the broker reject the eventual
// request. The estimate is on the uncompressed payload — Snappy can only
// shrink it, so the gate stays sound under compression.
func singleRecordBatchEstimateBytes(r *kgo.Record) int64 {
	return recordBatchHeaderBytes + recordEstimateBytes(r, 0, 0)
}

// multiRecordBatchEstimateBytes returns the wire-byte size of records encoded
// as one fresh RecordBatch: the header plus each record at its 0-based offset
// and a timestamp delta relative to the first record. singleRecordBatchEstimateBytes
// is the n=1 case.
func multiRecordBatchEstimateBytes(records []*kgo.Record) int64 {
	if len(records) == 0 {
		return 0
	}
	firstTS := records[0].Timestamp.UnixMilli()
	bytes := int64(recordBatchHeaderBytes)
	for i, r := range records {
		bytes += recordEstimateBytes(r, int32(i), r.Timestamp.UnixMilli()-firstTS)
	}
	return bytes
}

// produceRequestStats holds the aggregate producer-state counts for one
// ProduceRequest.
type produceRequestStats struct {
	records           int64
	batches           int64
	uncompressedBytes int64
	compressedBytes   int64
}

// buildMultiTopicProduceRequest builds a ProduceRequest covering records that
// may span multiple topics. topicID maps a topic name to its UUID, returning
// ok=false when the topic is unknown to the caller; an unknown topic fails
// the build with an error. Both topic name and UUID are populated per topic
// so the request is valid across the v0-v12 / v13+ API divide. The returned
// stats feed the producer-state metrics.
func buildMultiTopicProduceRequest(version int16, topicID func(string) ([16]byte, bool), records []*kgo.Record) (*kmsg.ProduceRequest, produceRequestStats, error) {
	byTopic := make(map[string][]*kgo.Record)
	for _, r := range records {
		byTopic[r.Topic] = append(byTopic[r.Topic], r)
	}

	var stats produceRequestStats
	req := kmsg.NewProduceRequest()
	req.Version = version
	req.Acks = -1 // all ISR
	for topic, topicRecords := range byTopic {
		id, ok := topicID(topic)
		if !ok {
			return nil, produceRequestStats{}, fmt.Errorf("topic %q not known", topic)
		}
		byPartition := groupByPartition(topicRecords)
		partitions := make([]kmsg.ProduceRequestTopicPartition, 0, len(byPartition))
		for partition, recs := range byPartition {
			batch, uncompressed, compressed := encodeBatch(recs)
			partitions = append(partitions, kmsg.ProduceRequestTopicPartition{
				Partition: partition,
				Records:   batch,
			})
			stats.records += int64(len(recs))
			stats.batches++
			stats.uncompressedBytes += int64(uncompressed)
			stats.compressedBytes += int64(compressed)
		}
		req.Topics = append(req.Topics, kmsg.ProduceRequestTopic{
			Topic:      topic,
			TopicID:    id,
			Partitions: partitions,
		})
	}
	return &req, stats, nil
}

// groupByPartition groups records by their partition field.
func groupByPartition(records []*kgo.Record) map[int32][]*kgo.Record {
	m := make(map[int32][]*kgo.Record)
	for _, r := range records {
		m[r.Partition] = append(m[r.Partition], r)
	}
	return m
}

// parseProduceResponse returns the first per-partition error, or nil on no error.
func parseProduceResponse(resp *kmsg.ProduceResponse) error {
	for i := range resp.Topics {
		for j := range resp.Topics[i].Partitions {
			if code := resp.Topics[i].Partitions[j].ErrorCode; code != kerrNoError {
				return kerr.ErrorForCode(code)
			}
		}
	}
	return nil
}

// partitionErrorsFromResp returns a map of (topic, partition) → error for
// every partition with a non-zero ErrorCode in resp. Partitions missing from
// the map were either successful or not reported. Used by the flush handler
// to decide which records succeeded and which need re-buffering.
func partitionErrorsFromResp(resp *kmsg.ProduceResponse) map[topicPartition]error {
	if resp == nil {
		return nil
	}
	out := make(map[topicPartition]error)
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode != kerrNoError {
				out[topicPartition{topic: t.Topic, partition: p.Partition}] = kerr.ErrorForCode(p.ErrorCode)
			}
		}
	}
	return out
}

// partitionErrorFromResp returns the error reported for (topic, partition) in
// resp, or nil if not present or success. Allocation-free fast path used
// when only one partition's outcome is needed.
func partitionErrorFromResp(resp *kmsg.ProduceResponse, topic string, partition int32) error {
	if resp == nil {
		return nil
	}
	for _, t := range resp.Topics {
		if t.Topic != topic {
			continue
		}
		for _, p := range t.Partitions {
			if p.Partition != partition {
				continue
			}
			if p.ErrorCode == kerrNoError {
				return nil
			}
			return kerr.ErrorForCode(p.ErrorCode)
		}
	}
	return nil
}

// encodeBatch serialises records into a Kafka RecordBatch (magic=2) with Snappy
// compression. It also returns the uncompressed and compressed record-payload
// sizes (excluding the batch wrapper); the two are equal when compression does
// not shrink the payload.
func encodeBatch(records []*kgo.Record) (buf []byte, uncompressedBytes, compressedBytes int) {
	firstTS := records[0].Timestamp.UnixMilli()
	maxTS := firstTS
	for _, r := range records[1:] {
		if ts := r.Timestamp.UnixMilli(); ts > maxTS {
			maxTS = ts
		}
	}

	// Borrow scratch buffers from the pool to avoid per-call heap allocations.
	rawPtr := encBufPool.Get().(*[]byte)
	raw := (*rawPtr)[:0]
	raw = encodeRecordsInto(raw, records, firstTS)

	compPtr := encBufPool.Get().(*[]byte)
	comp := (*compPtr)[:0]
	comp = s2.EncodeSnappy(comp, raw)

	var (
		payload    []byte
		attributes int16
	)
	if len(comp) < len(raw) {
		payload = comp
		attributes = 2 // CodecSnappy
	} else {
		payload = raw
	}

	uncompressedBytes = len(raw)
	compressedBytes = len(payload)

	batch := kmsg.RecordBatch{
		PartitionLeaderEpoch: -1,
		Magic:                2,
		Attributes:           attributes,
		LastOffsetDelta:      int32(len(records) - 1),
		FirstTimestamp:       firstTS,
		MaxTimestamp:         maxTS,
		ProducerID:           -1,
		ProducerEpoch:        -1,
		FirstSequence:        -1,
		NumRecords:           int32(len(records)),
		Records:              payload,
	}
	batch.Length = batchFixedFieldsAfterLength + int32(len(payload))

	buf = make([]byte, 0, 8+4+int(batch.Length))
	buf = batch.AppendTo(buf)

	// The CRC range is defined by the Kafka protocol; it covers all bytes after the CRC field.
	checksum := crc32.Checksum(buf[crcOffset+4:], crc32cTable)
	binary.BigEndian.PutUint32(buf[crcOffset:], checksum)

	// Return scratch buffers after CRC is computed and payload is no longer referenced.
	putBuf(rawPtr, raw)
	putBuf(compPtr, comp)

	return buf, uncompressedBytes, compressedBytes
}

// putBuf returns a buffer to the pool, dropping it if it grew too large.
func putBuf(ptr *[]byte, buf []byte) {
	if cap(buf) <= maxPooledBufCap {
		*ptr = buf
		encBufPool.Put(ptr)
	}
}

// encodeRecordsInto serialises each record into dst using kmsg.Record.AppendTo.
func encodeRecordsInto(dst []byte, records []*kgo.Record, firstTimestamp int64) []byte {
	for i, r := range records {
		dst = encodeRecord(dst, r, int32(i), firstTimestamp)
	}
	return dst
}

// encodeRecord appends one record to dst using the Kafka record wire format.
func encodeRecord(dst []byte, r *kgo.Record, offsetDelta int32, firstTimestamp int64) []byte {
	tsDelta := r.Timestamp.UnixMilli() - firstTimestamp

	headers := make([]kmsg.Header, len(r.Headers))
	for i, h := range r.Headers {
		headers[i] = kmsg.Header{Key: h.Key, Value: h.Value}
	}

	rec := kmsg.Record{
		TimestampDelta64: tsDelta,
		OffsetDelta:      offsetDelta,
		Key:              r.Key,
		Value:            r.Value,
		Headers:          headers,
	}
	rec.Length = recordLength(&rec)
	return rec.AppendTo(dst)
}

// recordLength computes the Length field for a kmsg.Record (byte count of everything
// after the Length varint itself).
func recordLength(r *kmsg.Record) int32 {
	l := 1 + // Attributes (int8, unused)
		kbin.VarlongLen(r.TimestampDelta64) +
		kbin.VarintLen(r.OffsetDelta) +
		kbin.VarintLen(int32(len(r.Key))) + len(r.Key) +
		kbin.VarintLen(int32(len(r.Value))) + len(r.Value) +
		kbin.VarintLen(int32(len(r.Headers)))
	for _, h := range r.Headers {
		l += kbin.VarintLen(int32(len(h.Key))) + len(h.Key) +
			kbin.VarintLen(int32(len(h.Value))) + len(h.Value)
	}
	return int32(l)
}
