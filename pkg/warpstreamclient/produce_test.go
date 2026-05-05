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

// buildProduceRequest is a test-only helper that builds a single-topic
// ProduceRequest. Tests don't always populate r.Topic on the records they
// construct, so we set it here before delegating to the multi-topic builder.
// Mutation is safe: tests own the records they pass in.
func buildProduceRequest(version int16, topic string, topicID [16]byte, records []*kgo.Record) *kmsg.ProduceRequest {
	for _, r := range records {
		r.Topic = topic
	}
	resolveTopicID := func(string) ([16]byte, bool) { return topicID, true }
	// The resolver always returns ok=true, so the error path is unreachable.
	req, _ := buildMultiTopicProduceRequest(version, resolveTopicID, records)
	return req
}

func TestBuildProduceRequest_RoundTrip(t *testing.T) {
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

		req := buildProduceRequest(9, "my-topic", [16]byte{}, input)
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
		req := buildProduceRequest(9, "t", [16]byte{}, []*kgo.Record{
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
		req := buildProduceRequest(9, "t", [16]byte{}, []*kgo.Record{
			{Partition: 0, Value: value, Timestamp: time.Now()},
		})
		rb := decodeRecordBatch(t, req.Topics[0].Partitions[0].Records)
		assert.Equal(t, int16(0), rb.Attributes&0x7, "attributes should show no compression")
	})
}

func TestBuildProduceRequest_BatchFields(t *testing.T) {
	t.Run("RecordBatch magic is 2", func(t *testing.T) {
		req := buildProduceRequest(9, "t", [16]byte{}, makeRecords(0, "v"))
		rb := decodeRecordBatch(t, req.Topics[0].Partitions[0].Records)
		assert.Equal(t, int8(2), rb.Magic)
	})

	t.Run("producer fields indicate no idempotence", func(t *testing.T) {
		req := buildProduceRequest(9, "t", [16]byte{}, makeRecords(0, "v"))
		rb := decodeRecordBatch(t, req.Topics[0].Partitions[0].Records)
		assert.Equal(t, int64(-1), rb.ProducerID)
		assert.Equal(t, int16(-1), rb.ProducerEpoch)
		assert.Equal(t, int32(-1), rb.FirstSequence)
	})

	t.Run("PartitionLeaderEpoch is -1", func(t *testing.T) {
		req := buildProduceRequest(9, "t", [16]byte{}, makeRecords(0, "v"))
		rb := decodeRecordBatch(t, req.Topics[0].Partitions[0].Records)
		assert.Equal(t, int32(-1), rb.PartitionLeaderEpoch)
	})

	t.Run("CRC is valid", func(t *testing.T) {
		req := buildProduceRequest(9, "t", [16]byte{}, makeRecords(0, "v1", "v2"))
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
		req := buildProduceRequest(9, "t", [16]byte{}, records)
		rb := decodeRecordBatch(t, req.Topics[0].Partitions[0].Records)
		assert.Equal(t, t1.UnixMilli(), rb.FirstTimestamp)
		assert.Equal(t, t2.UnixMilli(), rb.MaxTimestamp)
	})
}

func TestBuildMultiTopicProduceRequest(t *testing.T) {
	makeRecord := func(topic string, partition int32, value string) *kgo.Record {
		return &kgo.Record{Topic: topic, Partition: partition, Value: []byte(value), Timestamp: time.Now()}
	}

	t.Run("groups records by topic and by partition", func(t *testing.T) {
		records := []*kgo.Record{
			makeRecord("a", 0, "a0-1"),
			makeRecord("a", 0, "a0-2"),
			makeRecord("a", 1, "a1"),
			makeRecord("b", 0, "b0"),
		}
		idA := [16]byte{0xaa}
		idB := [16]byte{0xbb}
		resolve := func(topic string) ([16]byte, bool) {
			switch topic {
			case "a":
				return idA, true
			case "b":
				return idB, true
			}
			return [16]byte{}, false
		}

		req, err := buildMultiTopicProduceRequest(11, resolve, records)
		require.NoError(t, err)
		require.NotNil(t, req)
		require.Equal(t, int16(11), req.Version)
		require.Equal(t, int16(-1), req.Acks)
		require.Len(t, req.Topics, 2)

		topics := map[string]kmsg.ProduceRequestTopic{}
		for _, t := range req.Topics {
			topics[t.Topic] = t
		}
		require.Equal(t, idA, topics["a"].TopicID)
		require.Equal(t, idB, topics["b"].TopicID)
		assert.Len(t, topics["a"].Partitions, 2)
		assert.Len(t, topics["b"].Partitions, 1)
	})

	t.Run("populates the requested API version and acks", func(t *testing.T) {
		records := []*kgo.Record{makeRecord("t", 0, "v")}
		resolve := func(string) ([16]byte, bool) { return [16]byte{}, true }

		req, err := buildMultiTopicProduceRequest(13, resolve, records)
		require.NoError(t, err)
		assert.Equal(t, int16(13), req.Version)
		assert.Equal(t, int16(-1), req.Acks)
	})

	t.Run("returns an error when a topic is unknown", func(t *testing.T) {
		records := []*kgo.Record{
			makeRecord("known", 0, "v1"),
			makeRecord("unknown", 0, "v2"),
		}
		resolve := func(topic string) ([16]byte, bool) {
			if topic == "known" {
				return [16]byte{0x01}, true
			}
			return [16]byte{}, false
		}

		req, err := buildMultiTopicProduceRequest(11, resolve, records)
		require.Error(t, err)
		assert.Nil(t, req)
		assert.ErrorContains(t, err, "unknown")
	})

	t.Run("empty records: returns a request with no topics", func(t *testing.T) {
		resolve := func(string) ([16]byte, bool) { return [16]byte{}, true }
		req, err := buildMultiTopicProduceRequest(11, resolve, nil)
		require.NoError(t, err)
		require.NotNil(t, req)
		assert.Empty(t, req.Topics)
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
		_ = buildProduceRequest(9, "mimir-ingest", [16]byte{}, records)
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
	require.NoError(t, rb.ReadFrom(raw))

	want := int32(crc32.Checksum(raw[crcOffset+4:], crc32cTable))
	assert.Equal(t, want, rb.CRC)
	return rb
}

// decodeRecords decompresses and parses individual kmsg.Records from a RecordBatch.
func decodeRecords(t *testing.T, rb kmsg.RecordBatch) []kmsg.Record {
	t.Helper()
	payload := rb.Records
	if rb.Attributes&0x7 == 2 { // CodecSnappy
		dec, err := s2.Decode(nil, payload)
		require.NoError(t, err)
		payload = dec
	}

	records := make([]kmsg.Record, 0, rb.NumRecords)
	for len(payload) > 0 {
		var rec kmsg.Record
		require.NoError(t, rec.ReadFrom(payload))
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
