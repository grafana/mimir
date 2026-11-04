// SPDX-License-Identifier: AGPL-3.0-only
package ingest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type benchmarkDecompressPool = decompressBufferPool

func benchmarkFetchBatch(tb testing.TB, size int, codec kgo.CompressionCodec) ([]byte, []byte) {
	return benchmarkFetchBatchShape(tb, size, codec, false)
}
func benchmarkFetchBatchShape(tb testing.TB, size int, codec kgo.CompressionCodec, mixed bool) ([]byte, []byte) {
	tb.Helper()
	var records []byte
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte((i*37 + i/127) % 251)
	}
	rng := rand.New(rand.NewPCG(10, 20))
	for i := range 128 {
		if mixed {
			for at := size * 3 / 4; at+8 <= size; at += 8 {
				binary.LittleEndian.PutUint64(payload[at:], rng.Uint64())
			}
		}
		r := kmsg.Record{OffsetDelta: int32(i), Value: payload}
		body := r.AppendTo(nil)
		r.Length = int32(len(body) - 1)
		records = r.AppendTo(records)
	}
	c, err := kgo.DefaultCompressor(codec)
	if err != nil {
		tb.Fatal(err)
	}
	var compressed bytes.Buffer
	encoded, typ := c.Compress(&compressed, records)
	batch := kmsg.RecordBatch{Magic: 2, Attributes: int16(typ), LastOffsetDelta: 127, NumRecords: 128, Records: encoded, ProducerID: -1, ProducerEpoch: -1, FirstSequence: -1}
	wire := batch.AppendTo(nil)
	batch.Length = int32(len(wire) - 12)
	wire = batch.AppendTo(nil)
	binary.BigEndian.PutUint32(wire[17:21], crc32.Checksum(wire[21:], crc32.MakeTable(crc32.Castagnoli)))
	return wire, payload
}
func BenchmarkOptimizationKafkaPool(b *testing.B) {
	for _, size := range []int{512, 8192} {
		for _, codec := range []struct {
			name  string
			value kgo.CompressionCodec
		}{{"lz4", kgo.Lz4Compression()}, {"snappy", kgo.SnappyCompression()}} {
			for _, pooled := range []bool{false, true} {
				name := "baseline"
				if pooled {
					name = "pooled"
				}
				b.Run(fmt.Sprintf("%s/%d/%s", codec.name, size, name), func(b *testing.B) {
					wire, payload := benchmarkFetchBatch(b, size, codec.value)
					pool := &benchmarkDecompressPool{}
					var pools []kgo.Pool
					if pooled {
						pools = []kgo.Pool{pool}
					}
					decompressor := kgo.DefaultDecompressor(pools...)
					opts := kgo.ProcessFetchPartitionOpts{Topic: "synthetic", Pools: pools}
					raw := kmsg.FetchResponseTopicPartition{RecordBatches: wire}
					b.SetBytes(int64(size * 128))
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						fp, _ := kgo.ProcessFetchPartition(opts, &raw, decompressor, nil)
						if fp.Err != nil {
							b.Fatal(fp.Err)
						}
						if len(fp.Records) != 128 {
							b.Fatalf("records: %d", len(fp.Records))
						}
						if len(fp.Records[0].Value) != len(payload) {
							b.Fatal("payload length")
						}
						for _, record := range fp.Records {
							record.Recycle()
						}
					}
				})
			}
		}
	}
}
func TestOptimizationKafkaPoolLifetime(t *testing.T) {
	wire, payload := benchmarkFetchBatch(t, 512, kgo.Lz4Compression())
	pool := &benchmarkDecompressPool{}
	pools := []kgo.Pool{pool}
	opts := kgo.ProcessFetchPartitionOpts{Topic: "synthetic", Pools: pools}
	raw := kmsg.FetchResponseTopicPartition{RecordBatches: wire}
	d := kgo.DefaultDecompressor(pools...)
	first, _ := kgo.ProcessFetchPartition(opts, &raw, d, nil)
	if first.Err != nil {
		t.Fatal(first.Err)
	}
	second, _ := kgo.ProcessFetchPartition(opts, &raw, d, nil)
	if second.Err != nil {
		t.Fatal(second.Err)
	}
	if &first.Records[0].Value[0] == &second.Records[0].Value[0] {
		t.Fatal("concurrent batches share a live buffer")
	}
	for _, r := range first.Records[:127] {
		r.Recycle()
	}
	for _, r := range second.Records {
		if !slices.Equal(r.Value, payload) {
			t.Fatal("live record overwritten")
		}
		r.Recycle()
	}
	if !slices.Equal(first.Records[127].Value, payload) {
		t.Fatal("last record overwritten")
	}
	first.Records[127].Recycle()
}

func BenchmarkOptimizationKafkaMixed(b *testing.B) {
	for _, pooled := range []bool{false, true} {
		name := "baseline"
		if pooled {
			name = "pooled"
		}
		b.Run(name, func(b *testing.B) {
			wire, _ := benchmarkFetchBatchShape(b, 8192, kgo.Lz4Compression(), true)
			pool := &benchmarkDecompressPool{}
			var pools []kgo.Pool
			if pooled {
				pools = []kgo.Pool{pool}
			}
			d := kgo.DefaultDecompressor(pools...)
			opts := kgo.ProcessFetchPartitionOpts{Topic: "synthetic", Pools: pools}
			raw := kmsg.FetchResponseTopicPartition{RecordBatches: wire}
			b.SetBytes(128 * 8192)
			b.ReportAllocs()
			b.ReportMetric(float64(len(wire))/float64(128*8192), "wire/raw")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				fp, _ := kgo.ProcessFetchPartition(opts, &raw, d, nil)
				if fp.Err != nil {
					b.Fatal(fp.Err)
				}
				if len(fp.Records) != 128 {
					b.Fatal("missing records")
				}
				for _, r := range fp.Records {
					r.Recycle()
				}
			}
		})
	}
}

func TestOptimizationKafkaPoolConcurrent(t *testing.T) {
	wire, payload := benchmarkFetchBatch(t, 512, kgo.Lz4Compression())
	pool := &benchmarkDecompressPool{}
	pools := []kgo.Pool{pool}
	opts := kgo.ProcessFetchPartitionOpts{Topic: "synthetic", Pools: pools}
	raw := kmsg.FetchResponseTopicPartition{RecordBatches: wire}
	d := kgo.DefaultDecompressor(pools...)
	failures := make(chan string, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 25 {
				fp, _ := kgo.ProcessFetchPartition(opts, &raw, d, nil)
				if fp.Err != nil {
					failures <- fp.Err.Error()
					return
				}
				if len(fp.Records) != 128 {
					failures <- "missing records"
					return
				}
				for i := len(fp.Records) - 1; i >= 0; i-- {
					r := fp.Records[i]
					if !slices.Equal(r.Value, payload) {
						failures <- "recycled buffer changed live bytes"
						return
					}
					r.Recycle()
				}
			}
		}()
	}
	wg.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	mixed, _ := benchmarkFetchBatchShape(t, 8192, kgo.Lz4Compression(), true)
	t.Logf("mixed LZ4 wire/raw=%.3f", float64(len(mixed))/float64(128*8192))
}
