// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"encoding/binary"
	"hash/crc32"
	"sync"

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

// buildProduceRequest builds a ProduceRequest for a set of records belonging
// to the same topic. Records may span multiple partitions; one RecordBatch is
// built per partition and all are included in the single request.
func buildProduceRequest(topic string, version int16, records []*kgo.Record) *kmsg.ProduceRequest {
	byPartition := groupByPartition(records)

	partitions := make([]kmsg.ProduceRequestTopicPartition, 0, len(byPartition))
	for partition, recs := range byPartition {
		partitions = append(partitions, kmsg.ProduceRequestTopicPartition{
			Partition: partition,
			Records:   encodeBatch(recs),
		})
	}

	req := kmsg.NewProduceRequest()
	req.Version = version
	req.Acks = -1 // all ISR
	req.Topics = []kmsg.ProduceRequestTopic{
		{Topic: topic, Partitions: partitions},
	}
	return &req
}

// groupByPartition groups records by their partition field.
func groupByPartition(records []*kgo.Record) map[int32][]*kgo.Record {
	m := make(map[int32][]*kgo.Record)
	for _, r := range records {
		m[r.Partition] = append(m[r.Partition], r)
	}
	return m
}

// parseProduceResponse returns the first per-partition error, or nil on full success.
func parseProduceResponse(resp *kmsg.ProduceResponse) error {
	for i := range resp.Topics {
		for j := range resp.Topics[i].Partitions {
			if code := resp.Topics[i].Partitions[j].ErrorCode; code != 0 {
				return kerr.ErrorForCode(code)
			}
		}
	}
	return nil
}

// encodeBatch serialises records into a Kafka RecordBatch (magic=2) with Snappy compression.
func encodeBatch(records []*kgo.Record) []byte {
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

	buf := make([]byte, 0, 8+4+int(batch.Length))
	buf = batch.AppendTo(buf)

	// The CRC range is defined by the Kafka protocol; it covers all bytes after the CRC field.
	checksum := crc32.Checksum(buf[crcOffset+4:], crc32cTable)
	binary.BigEndian.PutUint32(buf[crcOffset:], checksum)

	// Return scratch buffers after CRC is computed and payload is no longer referenced.
	putBuf(rawPtr, raw)
	putBuf(compPtr, comp)

	return buf
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
	rec.Length = recordLength(rec)
	return rec.AppendTo(dst)
}

// recordLength computes the Length field for a kmsg.Record (byte count of everything
// after the Length varint itself).
func recordLength(r kmsg.Record) int32 {
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
