// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"hash/crc32"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestBuildProduceRequest(t *testing.T) {
	tests := map[string]struct {
		topic      string
		records    []*kgo.Record
		wantCounts map[int32]int // partition → expected record count
	}{
		"single record, single partition": {
			topic:      "t",
			records:    makeRecords(0, "v1"),
			wantCounts: map[int32]int{0: 1},
		},
		"multiple records, single partition": {
			topic:      "t",
			records:    makeRecords(0, "v1", "v2", "v3"),
			wantCounts: map[int32]int{0: 3},
		},
		"records across two partitions": {
			topic:      "t",
			records:    append(makeRecords(0, "a"), makeRecords(1, "b", "c")...),
			wantCounts: map[int32]int{0: 1, 1: 2},
		},
		"records across three partitions": {
			topic:      "t",
			records:    append(makeRecords(2, "z"), append(makeRecords(0, "a"), makeRecords(1, "b")...)...),
			wantCounts: map[int32]int{0: 1, 1: 1, 2: 1},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			req := buildProduceRequest(tc.topic, 9, tc.records)

			require.Len(t, req.Topics, 1)
			assert.Equal(t, tc.topic, req.Topics[0].Topic)
			assert.Equal(t, int16(-1), req.Acks)
			assert.Nil(t, req.TransactionID)

			partitions := req.Topics[0].Partitions
			require.Len(t, partitions, len(tc.wantCounts))

			for _, p := range partitions {
				want, ok := tc.wantCounts[p.Partition]
				require.True(t, ok, "unexpected partition %d in response", p.Partition)
				rb := decodeRecordBatch(t, p.Records)
				assert.Equal(t, want, int(rb.NumRecords), "partition %d", p.Partition)
			}
		})
	}
}

func TestBuildProduceRequestRoundTrip(t *testing.T) {
	t.Run("key, value, headers and timestamps survive encoding", func(t *testing.T) {
		ts := time.Now().Truncate(time.Millisecond)
		input := []*kgo.Record{
			{
				Partition: 0,
				Key:       []byte("my-key"),
				Value:     []byte("my-value"),
				Headers: []kgo.RecordHeader{
					{Key: "hk1", Value: []byte("hv1")},
					{Key: "hk2", Value: []byte("hv2")},
				},
				Timestamp: ts,
			},
			{
				Partition: 0,
				Key:       nil,
				Value:     []byte("second"),
				Timestamp: ts.Add(5 * time.Millisecond),
			},
		}

		req := buildProduceRequest("my-topic", 9, input)
		require.Len(t, req.Topics[0].Partitions, 1)

		rb := decodeRecordBatch(t, req.Topics[0].Partitions[0].Records)
		require.Equal(t, int32(2), rb.NumRecords)

		recs := decodeRecords(t, rb)
		require.Len(t, recs, 2)

		assert.Equal(t, []byte("my-key"), recs[0].Key)
		assert.Equal(t, []byte("my-value"), recs[0].Value)
		assert.Equal(t, int32(2), int32(len(recs[0].Headers)))
		assert.Equal(t, "hk1", recs[0].Headers[0].Key)
		assert.Equal(t, []byte("hv1"), recs[0].Headers[0].Value)
		assert.Equal(t, "hk2", recs[0].Headers[1].Key)
		assert.Equal(t, []byte("hv2"), recs[0].Headers[1].Value)
		assert.Equal(t, int64(0), recs[0].TimestampDelta64)

		assert.Nil(t, recs[1].Key)
		assert.Equal(t, []byte("second"), recs[1].Value)
		assert.Equal(t, int64(5), recs[1].TimestampDelta64)
	})

	t.Run("compressed output is used when shorter than raw", func(t *testing.T) {
		// Highly compressible: long run of identical bytes.
		value := make([]byte, 1024)
		req := buildProduceRequest("t", 9, []*kgo.Record{
			{Partition: 0, Value: value, Timestamp: time.Now()},
		})
		rb := decodeRecordBatch(t, req.Topics[0].Partitions[0].Records)
		assert.Equal(t, int16(2), rb.Attributes&0x7, "attributes should show snappy compression")
	})

	t.Run("raw payload is used when snappy would be larger", func(t *testing.T) {
		// Already-snappy-compressed bytes expand when compressed again.
		src := make([]byte, 16)
		for i := range src {
			src[i] = byte(i)
		}
		// Use already-compressed bytes as the value; Snappy of Snappy is always larger.
		value := s2.EncodeSnappy(nil, src)
		req := buildProduceRequest("t", 9, []*kgo.Record{
			{Partition: 0, Value: value, Timestamp: time.Now()},
		})
		rb := decodeRecordBatch(t, req.Topics[0].Partitions[0].Records)
		assert.Equal(t, int16(0), rb.Attributes&0x7, "attributes should show no compression")
	})
}

func TestBuildProduceRequestBatchFields(t *testing.T) {
	t.Run("RecordBatch magic is 2", func(t *testing.T) {
		req := buildProduceRequest("t", 9, makeRecords(0, "v"))
		rb := decodeRecordBatch(t, req.Topics[0].Partitions[0].Records)
		assert.Equal(t, int8(2), rb.Magic)
	})

	t.Run("producer fields indicate no idempotence", func(t *testing.T) {
		req := buildProduceRequest("t", 9, makeRecords(0, "v"))
		rb := decodeRecordBatch(t, req.Topics[0].Partitions[0].Records)
		assert.Equal(t, int64(-1), rb.ProducerID)
		assert.Equal(t, int16(-1), rb.ProducerEpoch)
		assert.Equal(t, int32(-1), rb.FirstSequence)
	})

	t.Run("PartitionLeaderEpoch is -1", func(t *testing.T) {
		req := buildProduceRequest("t", 9, makeRecords(0, "v"))
		rb := decodeRecordBatch(t, req.Topics[0].Partitions[0].Records)
		assert.Equal(t, int32(-1), rb.PartitionLeaderEpoch)
	})

	t.Run("CRC is valid", func(t *testing.T) {
		req := buildProduceRequest("t", 9, makeRecords(0, "v1", "v2"))
		raw := req.Topics[0].Partitions[0].Records
		// The CRC is over everything after the CRC field itself.
		want := int32(crc32.Checksum(raw[crcOffset+4:], crc32cTable))
		rb := decodeRecordBatch(t, raw)
		assert.Equal(t, want, rb.CRC)
	})

	t.Run("MaxTimestamp reflects latest record timestamp", func(t *testing.T) {
		t1 := time.Now().Truncate(time.Millisecond)
		t2 := t1.Add(10 * time.Millisecond)
		records := []*kgo.Record{
			{Partition: 0, Value: []byte("a"), Timestamp: t1},
			{Partition: 0, Value: []byte("b"), Timestamp: t2},
		}
		req := buildProduceRequest("t", 9, records)
		rb := decodeRecordBatch(t, req.Topics[0].Partitions[0].Records)
		assert.Equal(t, t1.UnixMilli(), rb.FirstTimestamp)
		assert.Equal(t, t2.UnixMilli(), rb.MaxTimestamp)
	})
}

func TestParseProduceResponse(t *testing.T) {
	tests := map[string]struct {
		resp    *kmsg.ProduceResponse
		wantErr error
	}{
		"all partitions success": {
			resp: makeProduceResponse(0, 0),
		},
		"one partition error": {
			resp:    makeProduceResponse(int16(kerr.UnknownTopicOrPartition.Code), 0),
			wantErr: kerr.UnknownTopicOrPartition,
		},
		"multiple topics, error in second": {
			resp:    makeProduceResponseMultiTopic(0, int16(kerr.MessageTooLarge.Code)),
			wantErr: kerr.MessageTooLarge,
		},
		"zero topics": {
			resp: &kmsg.ProduceResponse{},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := parseProduceResponse(tc.resp)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func BenchmarkBuildProduceRequest(b *testing.B) {
	records := makeBenchRecords(100, 1000)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		_ = buildProduceRequest("mimir-ingest", 9, records)
	}
}

func BenchmarkParseProduceResponse(b *testing.B) {
	resp := makeProduceResponseLarge(100)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		_ = parseProduceResponse(resp)
	}
}

// decodeRecordBatch parses a serialised RecordBatch and verifies the CRC.
func decodeRecordBatch(t *testing.T, raw []byte) kmsg.RecordBatch {
	t.Helper()
	var rb kmsg.RecordBatch
	require.NoError(t, rb.ReadFrom(raw), "RecordBatch.ReadFrom failed")

	want := int32(crc32.Checksum(raw[crcOffset+4:], crc32cTable))
	assert.Equal(t, want, rb.CRC, "CRC mismatch")
	return rb
}

// decodeRecords decompresses and parses individual kmsg.Records from a RecordBatch.
func decodeRecords(t *testing.T, rb kmsg.RecordBatch) []kmsg.Record {
	t.Helper()
	payload := rb.Records
	if rb.Attributes&0x7 == 2 { // CodecSnappy
		dec, err := s2.Decode(nil, payload)
		require.NoError(t, err, "Snappy decompress failed")
		payload = dec
	}

	records := make([]kmsg.Record, 0, rb.NumRecords)
	for len(payload) > 0 {
		var rec kmsg.Record
		require.NoError(t, rec.ReadFrom(payload), "Record.ReadFrom failed")
		records = append(records, rec)
		// Advance past this record: Length field (varint) + Length bytes.
		advLen := kbin.VarintLen(rec.Length) + int(rec.Length)
		payload = payload[advLen:]
	}
	return records
}

func makeRecords(partition int32, values ...string) []*kgo.Record {
	records := make([]*kgo.Record, len(values))
	ts := time.Now()
	for i, v := range values {
		records[i] = &kgo.Record{
			Partition: partition,
			Value:     []byte(v),
			Timestamp: ts,
		}
	}
	return records
}

func makeBenchRecords(count, valueSize int) []*kgo.Record {
	value := make([]byte, valueSize)
	records := make([]*kgo.Record, count)
	ts := time.Now()
	for i := range records {
		records[i] = &kgo.Record{
			Partition: 0,
			Value:     value,
			Timestamp: ts,
		}
	}
	return records
}

func makeProduceResponse(errorCode int16, partition int32) *kmsg.ProduceResponse {
	return &kmsg.ProduceResponse{
		Topics: []kmsg.ProduceResponseTopic{{
			Topic: "t",
			Partitions: []kmsg.ProduceResponseTopicPartition{
				{Partition: partition, ErrorCode: errorCode},
			},
		}},
	}
}

func makeProduceResponseMultiTopic(errCode1, errCode2 int16) *kmsg.ProduceResponse {
	return &kmsg.ProduceResponse{
		Topics: []kmsg.ProduceResponseTopic{
			{Topic: "t1", Partitions: []kmsg.ProduceResponseTopicPartition{{ErrorCode: errCode1}}},
			{Topic: "t2", Partitions: []kmsg.ProduceResponseTopicPartition{{ErrorCode: errCode2}}},
		},
	}
}

func makeProduceResponseLarge(partitionCount int) *kmsg.ProduceResponse {
	parts := make([]kmsg.ProduceResponseTopicPartition, partitionCount)
	for i := range parts {
		parts[i] = kmsg.ProduceResponseTopicPartition{Partition: int32(i)}
	}
	return &kmsg.ProduceResponse{
		Topics: []kmsg.ProduceResponseTopic{{Topic: "t", Partitions: parts}},
	}
}
