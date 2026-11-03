// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"io"
	"maps"
	"math/rand"
	"net"
	"runtime"
	"runtime/debug"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/test/bufconn"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
)

// benchLabelNamesAndValuesServer serves pre-encoded messages, so that the in-process servers allocate almost nothing
// and the measured heap is the querier side's. Unsorted values are in random order, like a head's insertion order.
type benchLabelNamesAndValuesServer struct {
	ingester_client.UnimplementedIngesterServer
	unsorted []benchPreEncoded
	sorted   map[uint64][]benchPreEncoded
}

// benchPreEncoded is an already marshalled message, sent as is by benchServerCodec.
type benchPreEncoded []byte

// benchServerCodec sends benchPreEncoded messages without copying them, and uses the default codec for everything else.
type benchServerCodec struct {
	encoding.CodecV2
}

func (c benchServerCodec) Marshal(v any) (mem.BufferSlice, error) {
	if p, ok := v.(benchPreEncoded); ok {
		return mem.BufferSlice{mem.SliceBuffer(p)}, nil
	}
	return c.CodecV2.Marshal(v)
}

func preEncode(msgs []*ingester_client.LabelNamesAndValuesResponse) []benchPreEncoded {
	out := make([]benchPreEncoded, 0, len(msgs))
	for _, msg := range msgs {
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		out = append(out, data)
	}
	return out
}

func newBenchLabelNamesAndValuesServer(labels map[string][]string, sortedSizes []uint64) *benchLabelNamesAndValuesServer {
	var unsorted, sorted []*ingester_client.LabelValues
	for _, name := range slices.Sorted(maps.Keys(labels)) {
		unsorted = append(unsorted, &ingester_client.LabelValues{LabelName: name, Values: labels[name]})
		sorted = append(sorted, &ingester_client.LabelValues{LabelName: name, Values: slices.Sorted(slices.Values(labels[name]))})
	}
	unsortedMsgs := splitLabelNamesAndValues(unsorted, labelNamesAndValuesBenchDefaultSize)
	for _, msg := range unsortedMsgs {
		msg.ValuesSorted = false
	}
	s := &benchLabelNamesAndValuesServer{
		unsorted: preEncode(unsortedMsgs),
		sorted:   map[uint64][]benchPreEncoded{},
	}
	for _, size := range sortedSizes {
		s.sorted[size] = preEncode(splitLabelNamesAndValues(sorted, int(size)))
	}
	return s
}

// labelNamesAndValuesBenchDefaultSize is the ingester's default message size.
const labelNamesAndValuesBenchDefaultSize = 1024 * 1024

func (s *benchLabelNamesAndValuesServer) LabelNamesAndValues(req *ingester_client.LabelNamesAndValuesRequest, stream ingester_client.Ingester_LabelNamesAndValuesServer) error {
	msgs := s.unsorted
	if req.SortValues {
		msgs = s.sorted[req.TargetMessageSizeBytes]
	}
	for _, msg := range msgs {
		if err := stream.SendMsg(msg); err != nil {
			return err
		}
	}
	return nil
}

// benchLabelValuesDataset builds per-ingester label values shaped like the plan's synthetic dataset, scaled down:
// low-cardinality labels on every ingester, pod-like labels whose values are on a few ingesters, and id-like labels
// whose values are on one ingester. Every value is 32 bytes.
func benchLabelValuesDataset(ingesters int) []map[string][]string {
	const (
		lowLabels, lowValues          = 20, 300
		podLabels, podValues          = 2, 4800
		podReplicas                   = 2
		idLabels, idValuesPerIngester = 1, 36000
	)
	rnd := rand.New(rand.NewSource(1))
	value := func(prefix string, i int) string { return fmt.Sprintf("%s-%0*d", prefix, 31-len(prefix), i) }

	data := make([]map[string][]string, ingesters)
	for i := range data {
		data[i] = map[string][]string{}
	}
	for l := range lowLabels {
		name := fmt.Sprintf("low_%02d", l)
		for i := range data {
			for v := range lowValues {
				data[i][name] = append(data[i][name], value("low", v))
			}
		}
	}
	for l := range podLabels {
		name := fmt.Sprintf("pod_%02d", l)
		for v := range podValues {
			first := rnd.Intn(ingesters)
			for r := range podReplicas {
				i := (first + r) % ingesters
				data[i][name] = append(data[i][name], value("pod", v))
			}
		}
	}
	for l := range idLabels {
		name := fmt.Sprintf("id_%02d", l)
		for i := range data {
			for v := range idValuesPerIngester {
				data[i][name] = append(data[i][name], value("id", i*idValuesPerIngester+v))
			}
		}
	}
	for _, labels := range data {
		for _, values := range labels {
			rnd.Shuffle(len(values), func(a, b int) { values[a], values[b] = values[b], values[a] })
		}
	}
	return data
}

// BenchmarkLabelValuesCount compares the in-memory and streaming merges over real gRPC streams,
// so that decoding and flow control are included.
func BenchmarkLabelValuesCount(b *testing.B) {
	const ingesters = 30
	sizes := []uint64{16 * 1024, 64 * 1024, 256 * 1024}
	data := benchLabelValuesDataset(ingesters)

	clients := make([]ingester_client.IngesterClient, ingesters)
	for i := range ingesters {
		listener := bufconn.Listen(1024 * 1024)
		server := grpc.NewServer(grpc.ForceServerCodecV2(benchServerCodec{CodecV2: encoding.GetCodecV2(proto.Name)}))
		ingester_client.RegisterIngesterServer(server, newBenchLabelNamesAndValuesServer(data[i], sizes))
		go func() { _ = server.Serve(listener) }()
		b.Cleanup(server.Stop)

		// nolint:staticcheck // grpc.DialContext() has been deprecated; we'll address it before upgrading to gRPC 2.
		conn, err := grpc.DialContext(b.Context(), "bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(b, err)
		b.Cleanup(func() { _ = conn.Close() })
		clients[i] = ingester_client.NewIngesterClient(conn)
	}
	data = nil

	openStreams := func(b *testing.B, req *ingester_client.LabelNamesAndValuesRequest) []*labelNamesAndValuesStream {
		streams := make([]*labelNamesAndValuesStream, ingesters)
		for i, c := range clients {
			ctx, cancel := context.WithCancelCause(b.Context())
			stream, err := c.LabelNamesAndValues(ctx, req)
			require.NoError(b, err)
			first, err := stream.Recv()
			if err == io.EOF {
				first = nil
			} else {
				require.NoError(b, err)
			}
			streams[i] = &labelNamesAndValuesStream{instance: ring.InstanceDesc{Addr: fmt.Sprint(i)}, stream: stream, cancel: cancel, first: first}
		}
		return streams
	}

	run := func(b *testing.B, merge func([]*labelNamesAndValuesStream) (map[string]int, error), req *ingester_client.LabelNamesAndValuesRequest) {
		peak := trackPeakHeap(b)
		for b.Loop() {
			streams := openStreams(b, req)
			counts, err := merge(streams)
			for _, s := range streams {
				s.close()
			}
			require.NoError(b, err)
			require.Len(b, counts, 23)
		}
		b.ReportMetric(float64(peak())/1024/1024, "peak-heap-above-baseline-MiB")
	}

	b.Run("in-memory merge", func(b *testing.B) {
		run(b, func(streams []*labelNamesAndValuesStream) (map[string]int, error) {
			return mergeLabelNamesAndValuesStreamsInMemory(streams, 1<<40)
		}, &ingester_client.LabelNamesAndValuesRequest{})
	})
	for _, size := range sizes {
		b.Run(fmt.Sprintf("streaming merge, %dKiB messages", size/1024), func(b *testing.B) {
			run(b, func(streams []*labelNamesAndValuesStream) (map[string]int, error) {
				counts, _, err := countDistinctLabelValues(streams)
				return counts, err
			}, &ingester_client.LabelNamesAndValuesRequest{SortValues: true, TargetMessageSizeBytes: size})
		})
	}
}

// trackPeakHeap samples the in-use heap until the benchmark ends, and returns a function that reports the peak
// above the in-use heap at the start, which holds the servers' precomputed messages.
func trackPeakHeap(b *testing.B) func() uint64 {
	var (
		mtx  sync.Mutex
		peak uint64
		done = make(chan struct{})
		wg   sync.WaitGroup
	)
	// A low GOGC keeps garbage small relative to the large baseline, so the peak tracks live memory.
	prevGCPercent := debug.SetGCPercent(5)
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	wg.Add(1)
	go func() {
		defer wg.Done()
		var ms runtime.MemStats
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				runtime.ReadMemStats(&ms)
				mtx.Lock()
				peak = max(peak, ms.HeapInuse)
				mtx.Unlock()
			}
		}
	}()
	return func() uint64 {
		close(done)
		wg.Wait()
		debug.SetGCPercent(prevGCPercent)
		mtx.Lock()
		defer mtx.Unlock()
		return peak - min(peak, baseline.HeapInuse)
	}
}
