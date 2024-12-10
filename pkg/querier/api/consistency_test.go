// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	// The following fixture has been generated looking at the actual metadata received by Grafana Mimir.
	exampleIncomingMetadata = metadata.New(map[string]string{
		"authority":            "1.1.1.1",
		"content-type":         "application/grpc",
		"grpc-accept-encoding": "snappy,gzip",
		"uber-trace-id":        "xxx",
		"user-agent":           "grpc-go/1.61.1",
		"x-scope-orgid":        "user-1",
	})
)

func TestConsistencyMiddleware(t *testing.T) {
	// Create an HTTP handler that captures the downstream request.
	var downstreamReq *http.Request
	downstreamHandler := http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		downstreamReq = req
	})

	encodedOffsets := EncodeOffsets(map[int32]int64{0: 1, 1: 2})
	inputReq := httptest.NewRequest(http.MethodGet, "/", nil)
	inputReq.Header.Set(ReadConsistencyHeader, ReadConsistencyStrong)
	inputReq.Header.Set(ReadConsistencyOffsetsHeader, string(encodedOffsets))

	middleware := ConsistencyMiddleware()
	middleware.Wrap(downstreamHandler).ServeHTTP(httptest.NewRecorder(), inputReq)

	require.NotNil(t, downstreamReq)
	assert.Equal(t, ReadConsistencyStrong, downstreamReq.Header.Get(ReadConsistencyHeader))
	assert.Equal(t, string(encodedOffsets), downstreamReq.Header.Get(ReadConsistencyOffsetsHeader))

	// Should inject consistency settings in the context.
	actualLevel, ok := ReadConsistencyLevelFromContext(downstreamReq.Context())
	require.True(t, ok)
	assert.Equal(t, ReadConsistencyStrong, actualLevel)

	actualOffsets, ok := ReadConsistencyEncodedOffsetsFromContext(downstreamReq.Context())
	require.True(t, ok)
	assert.Equal(t, encodedOffsets, actualOffsets)
}

func TestReadConsistencyClientUnaryInterceptor_And_ReadConsistencyServerUnaryInterceptor(t *testing.T) {
	encodedOffsets := EncodeOffsets(map[int32]int64{0: 1, 1: 2})

	// Run the gRPC client interceptor.
	clientIncomingCtx := context.Background()
	clientIncomingCtx = ContextWithReadConsistencyLevel(clientIncomingCtx, ReadConsistencyStrong)
	clientIncomingCtx = ContextWithReadConsistencyEncodedOffsets(clientIncomingCtx, encodedOffsets)

	var clientOutgoingCtx context.Context
	clientHandler := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		clientOutgoingCtx = ctx
		return nil
	}

	require.NoError(t, ReadConsistencyClientUnaryInterceptor(clientIncomingCtx, "", nil, nil, nil, clientHandler, nil))

	// The server only gets the gRPC metadata in the incoming context.
	clientOutgoingMetadata, ok := metadata.FromOutgoingContext(clientOutgoingCtx)
	require.True(t, ok)

	serverIncomingCtx := metadata.NewIncomingContext(context.Background(), clientOutgoingMetadata)

	// Run the gRPC server interceptor.
	var serverOutgoingCtx context.Context
	serverHandler := func(ctx context.Context, _ any) (any, error) {
		serverOutgoingCtx = ctx
		return nil, nil
	}

	_, err := ReadConsistencyServerUnaryInterceptor(serverIncomingCtx, nil, nil, serverHandler)
	require.NoError(t, err)

	// Should inject consistency settings in the context.
	actualLevel, ok := ReadConsistencyLevelFromContext(serverOutgoingCtx)
	require.True(t, ok)
	assert.Equal(t, ReadConsistencyStrong, actualLevel)

	actualOffsets, ok := ReadConsistencyEncodedOffsetsFromContext(serverOutgoingCtx)
	require.True(t, ok)
	assert.Equal(t, encodedOffsets, actualOffsets)
}

func TestReadConsistencyClientStreamInterceptor_And_ReadConsistencyServerStreamInterceptor(t *testing.T) {
	encodedOffsets := EncodeOffsets(map[int32]int64{0: 1, 1: 2})

	// Run the gRPC client interceptor.
	clientIncomingCtx := context.Background()
	clientIncomingCtx = ContextWithReadConsistencyLevel(clientIncomingCtx, ReadConsistencyStrong)
	clientIncomingCtx = ContextWithReadConsistencyEncodedOffsets(clientIncomingCtx, encodedOffsets)

	var clientOutgoingCtx context.Context
	clientHandler := func(ctx context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
		clientOutgoingCtx = ctx
		return nil, nil
	}

	_, err := ReadConsistencyClientStreamInterceptor(clientIncomingCtx, nil, nil, "", clientHandler, nil)
	require.NoError(t, err)

	// The server only gets the gRPC metadata in the incoming context.
	clientOutgoingMetadata, ok := metadata.FromOutgoingContext(clientOutgoingCtx)
	require.True(t, ok)

	serverIncomingCtx := metadata.NewIncomingContext(context.Background(), clientOutgoingMetadata)

	// Run the gRPC server interceptor.
	var serverOutgoingCtx context.Context
	serverHandler := func(_ any, stream grpc.ServerStream) error {
		serverOutgoingCtx = stream.Context()
		return nil
	}

	require.NoError(t, ReadConsistencyServerStreamInterceptor(nil, &serverStreamMock{ctx: serverIncomingCtx}, nil, serverHandler))

	// Should inject consistency settings in the context.
	actualLevel, ok := ReadConsistencyLevelFromContext(serverOutgoingCtx)
	require.True(t, ok)
	assert.Equal(t, ReadConsistencyStrong, actualLevel)

	actualOffsets, ok := ReadConsistencyEncodedOffsetsFromContext(serverOutgoingCtx)
	require.True(t, ok)
	assert.Equal(t, encodedOffsets, actualOffsets)
}

func BenchmarkReadConsistencyServerUnaryInterceptor(b *testing.B) {
	const numPartitions = 1000

	for _, withReadConsistency := range []bool{true, false} {
		b.Run(fmt.Sprintf("with read consistency: %t", withReadConsistency), func(b *testing.B) {
			md := exampleIncomingMetadata
			if withReadConsistency {
				md = metadata.Join(md, metadata.New(map[string]string{
					consistencyLevelGrpcMdKey:   ReadConsistencyStrong,
					consistencyOffsetsGrpcMdKey: string(EncodeOffsets(generateTestOffsets(numPartitions))),
				}))
			}

			ctx := metadata.NewIncomingContext(context.Background(), md)

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				_, _ = ReadConsistencyServerUnaryInterceptor(ctx, nil, nil, func(context.Context, any) (any, error) {
					return nil, nil
				})
			}
		})
	}
}

func BenchmarkReadConsistencyServerStreamInterceptor(b *testing.B) {
	const numPartitions = 1000

	for _, withReadConsistency := range []bool{true, false} {
		b.Run(fmt.Sprintf("with read consistency: %t", withReadConsistency), func(b *testing.B) {
			md := exampleIncomingMetadata
			if withReadConsistency {
				md = metadata.Join(md, metadata.New(map[string]string{
					consistencyLevelGrpcMdKey:   ReadConsistencyStrong,
					consistencyOffsetsGrpcMdKey: string(EncodeOffsets(generateTestOffsets(numPartitions))),
				}))
			}

			stream := serverStreamMock{ctx: metadata.NewIncomingContext(context.Background(), md)}

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				_ = ReadConsistencyServerStreamInterceptor(nil, stream, nil, func(_ any, _ grpc.ServerStream) error {
					return nil
				})
			}
		})
	}
}

type serverStreamMock struct {
	grpc.ServerStream

	ctx context.Context
}

func (m serverStreamMock) Context() context.Context {
	return m.ctx
}

func TestEncodeOffsets(t *testing.T) {
	t.Run("empty offsets", func(t *testing.T) {
		assert.Equal(t, EncodedOffsets(""), EncodeOffsets(nil))
		assert.Equal(t, EncodedOffsets(""), EncodeOffsets(map[int32]int64{}))
	})

	t.Run("1 offset", func(t *testing.T) {
		assert.Equal(t, EncodedOffsets("v1=0:1000"), EncodeOffsets(map[int32]int64{0: 1000}))
		assert.Equal(t, EncodedOffsets("v1=123456:654321"), EncodeOffsets(map[int32]int64{123456: 654321}))
	})

	t.Run("multiple offsets", func(t *testing.T) {
		assert.ElementsMatch(t, []string{"1:1", "2:2", "10:9", "123:456"}, strings.Split(string(EncodeOffsets(map[int32]int64{1: 1, 2: 2, 10: 9, 123: 456})[3:]), ","))
	})

	t.Run("should not skip empty partitions", func(t *testing.T) {
		assert.ElementsMatch(t, []string{"123:321", "456:-1"}, strings.Split(string(EncodeOffsets(map[int32]int64{123: 321, 456: -1})[3:]), ","))
	})

	t.Run("should allocate only once", func(t *testing.T) {
		for _, numPartitions := range []int{1, 10, 100, 1000} {
			t.Run(fmt.Sprintf("num offsets: %d", numPartitions), func(t *testing.T) {
				offsets := generateTestOffsets(numPartitions)

				assert.Equal(t, 1.0, testing.AllocsPerRun(100, func() {
					EncodeOffsets(offsets)
				}))
			})
		}

	})
}

func TestEncodedOffsets_Lookup_SpecialCases(t *testing.T) {
	tests := map[string]struct {
		encoded              EncodedOffsets
		expectedPartitions   map[int32]int64
		unexpectedPartitions []int32
	}{
		"empty": {
			encoded:              "",
			unexpectedPartitions: []int32{0},
		},
		"missing version": {
			encoded:              "0:1",
			unexpectedPartitions: []int32{0},
		},
		"corruption when reading the partition ID": {
			encoded:              "v1=x",
			unexpectedPartitions: []int32{0},
		},
		"corruption when reading the offset": {
			encoded:              "v1=1:x",
			unexpectedPartitions: []int32{0},
		},
		"single partition": {
			encoded: EncodeOffsets(map[int32]int64{
				0: 123,
			}),
			expectedPartitions:   map[int32]int64{0: 123},
			unexpectedPartitions: []int32{1},
		},
		"single partition with negative offset": {
			encoded: EncodeOffsets(map[int32]int64{
				0: -123,
			}),
			expectedPartitions:   map[int32]int64{0: -123},
			unexpectedPartitions: []int32{1},
		},
		"multiple partitions": {
			encoded: EncodeOffsets(map[int32]int64{
				0: -123,
				1: 456,
				2: math.MaxInt64,
			}),
			expectedPartitions:   map[int32]int64{0: -123, 1: 456, 2: math.MaxInt64},
			unexpectedPartitions: []int32{3},
		},
		"multiple partitions where partition IDs are in reverse order": {
			encoded:              "v1=10:100,1:10",
			expectedPartitions:   map[int32]int64{1: 10, 10: 100},
			unexpectedPartitions: []int32{0, 100},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for expectedPartitionID, expectedOffset := range testData.expectedPartitions {
				actualOffset, ok := testData.encoded.Lookup(expectedPartitionID)
				require.True(t, ok)
				assert.Equalf(t, expectedOffset, actualOffset, "partition ID: %d", expectedPartitionID)
			}

			for _, unexpectedPartitionID := range testData.unexpectedPartitions {
				_, ok := testData.encoded.Lookup(unexpectedPartitionID)
				assert.Falsef(t, ok, "partition ID: %d", unexpectedPartitionID)
			}
		})
	}
}

func TestEncodedOffsets_Lookup_First1000Partitions(t *testing.T) {
	const numPartitions = 1000

	offsets := generateTestOffsets(numPartitions)
	encoded := EncodeOffsets(offsets)

	for partitionID, expected := range offsets {
		actual, ok := encoded.Lookup(partitionID)
		assert.True(t, ok)
		assert.Equal(t, expected, actual)
	}
}

func TestEncodedOffsets_Lookup_Fuzzy(t *testing.T) {
	const (
		numRuns       = 100
		numPartitions = 1000
	)

	// Randomise the seed but log it in case we need to reproduce the test on failure.
	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	t.Log("random generator seed:", seed)

	for r := 0; r < numRuns; r++ {
		offsets := make(map[int32]int64, numPartitions)
		for i := 0; i < numPartitions; i++ {
			offsets[rnd.Int31n(math.MaxInt32)] = rnd.Int63n(math.MaxInt64)
		}

		encoded := EncodeOffsets(offsets)

		for partitionID, expected := range offsets {
			actual, ok := encoded.Lookup(partitionID)
			assert.True(t, ok)
			assert.Equal(t, expected, actual)
		}
	}
}

func BenchmarkEncodeOffsets(b *testing.B) {
	for _, numPartitions := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("num partitions: %d", numPartitions), func(b *testing.B) {
			offsets := generateTestOffsets(numPartitions)

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				EncodeOffsets(offsets)
			}
		})
	}
}

func BenchmarkEncodedOffsets_Lookup(b *testing.B) {
	for _, numPartitions := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("num partitions: %d", numPartitions), func(b *testing.B) {
			encoded := EncodeOffsets(generateTestOffsets(numPartitions))

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				partitionID := int32(n % numPartitions)
				_, ok := encoded.Lookup(partitionID)
				if !ok {
					b.Fatalf("not found offset for partition %d", partitionID)
				}
			}
		})
	}
}

func generateTestOffsets(numPartitions int) map[int32]int64 {
	offsets := make(map[int32]int64, numPartitions)
	for i := 0; i < numPartitions; i++ {
		// Create offsets, using the worst case scenario for the value.
		offsets[int32(i)] = math.MaxInt64 - int64(i)
	}
	return offsets
}
