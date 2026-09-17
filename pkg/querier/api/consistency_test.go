// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	//lint:ignore faillint Allow importing the kmeta package, since it's an isolated package (doesn't come with many other dependencies).
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	//lint:ignore faillint Allow importing the propagation package, since it's an isolated package (doesn't come with many other dependencies).
	"github.com/grafana/mimir/pkg/util/propagation"
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

func TestConsistencyExtractor(t *testing.T) {
	encodedOffsets := EncodeOffsetsV1(map[int32]int64{0: 1, 1: 2})
	headers := http.Header{}
	headers.Set(ReadConsistencyHeader, ReadConsistencyStrong)
	headers.Set(ReadConsistencyOffsetsHeader, string(encodedOffsets))
	headers.Set(ReadConsistencyMaxDelayHeader, "1m")

	extractor := ConsistencyExtractor{}
	ctx, err := extractor.ExtractFromCarrier(context.Background(), propagation.HttpHeaderCarrier(headers))
	require.NoError(t, err)

	// Should inject consistency settings in the context.
	actualLevel, ok := ReadConsistencyLevelFromContext(ctx)
	require.True(t, ok)
	assert.Equal(t, ReadConsistencyStrong, actualLevel)

	actualOffsets, ok := ReadConsistencyEncodedOffsetsFromContext(ctx)
	require.True(t, ok)
	assert.Equal(t, encodedOffsets, actualOffsets)

	actualDelay, ok := ReadConsistencyMaxDelayFromContext(ctx)
	require.True(t, ok)
	assert.Equal(t, time.Minute, actualDelay)
}

func TestConsistencyInjector(t *testing.T) {
	ctx := context.Background()
	ctx = ContextWithReadConsistencyLevel(ctx, ReadConsistencyStrong)
	ctx = ContextWithReadConsistencyMaxDelay(ctx, time.Minute)
	ctx = ContextWithReadConsistencyEncodedOffsets(ctx, EncodeOffsetsV1(map[int32]int64{0: 1, 1: 2}))

	headers := http.Header{}

	injector := &ConsistencyInjector{}
	require.NoError(t, injector.InjectToCarrier(ctx, propagation.HttpHeaderCarrier(headers)))

	assert.Equal(t, ReadConsistencyStrong, headers.Get(ReadConsistencyHeader))
	assert.Equal(t, time.Minute.String(), headers.Get(ReadConsistencyMaxDelayHeader))

	// Offsets should not be propagated, because internally we propagate them differently when required.
	assert.Empty(t, headers.Get(ReadConsistencyOffsetsHeader))
}

func TestReadConsistencyClientUnaryInterceptor_And_ReadConsistencyServerUnaryInterceptor(t *testing.T) {
	encodedOffsets := EncodeOffsetsV1(map[int32]int64{0: 1, 1: 2})

	// Run the gRPC client interceptor.
	clientIncomingCtx := context.Background()
	clientIncomingCtx = ContextWithReadConsistencyLevel(clientIncomingCtx, ReadConsistencyStrong)
	clientIncomingCtx = ContextWithReadConsistencyEncodedOffsets(clientIncomingCtx, encodedOffsets)
	clientIncomingCtx = ContextWithReadConsistencyMaxDelay(clientIncomingCtx, time.Minute)

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

	actualDelay, ok := ReadConsistencyMaxDelayFromContext(serverOutgoingCtx)
	require.True(t, ok)
	assert.Equal(t, time.Minute, actualDelay)
}

func TestReadConsistencyClientStreamInterceptor_And_ReadConsistencyServerStreamInterceptor(t *testing.T) {
	encodedOffsets := EncodeOffsetsV1(map[int32]int64{0: 1, 1: 2})

	// Run the gRPC client interceptor.
	clientIncomingCtx := context.Background()
	clientIncomingCtx = ContextWithReadConsistencyLevel(clientIncomingCtx, ReadConsistencyStrong)
	clientIncomingCtx = ContextWithReadConsistencyEncodedOffsets(clientIncomingCtx, encodedOffsets)
	clientIncomingCtx = ContextWithReadConsistencyMaxDelay(clientIncomingCtx, time.Minute)

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

	actualDelay, ok := ReadConsistencyMaxDelayFromContext(serverOutgoingCtx)
	require.True(t, ok)
	assert.Equal(t, time.Minute, actualDelay)
}

func BenchmarkReadConsistencyServerUnaryInterceptor(b *testing.B) {
	const numPartitions = 1000

	for _, withReadConsistency := range []bool{true, false} {
		b.Run(fmt.Sprintf("with read consistency: %t", withReadConsistency), func(b *testing.B) {
			md := exampleIncomingMetadata
			if withReadConsistency {
				md = metadata.Join(md, metadata.New(map[string]string{
					consistencyLevelGrpcMdKey:    ReadConsistencyStrong,
					consistencyOffsetsGrpcMdKey:  string(EncodeOffsetsV1(generateTestOffsets(numPartitions))),
					consistencyMaxDelayGrpcMdKey: "1m",
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
					consistencyLevelGrpcMdKey:    ReadConsistencyStrong,
					consistencyOffsetsGrpcMdKey:  string(EncodeOffsetsV1(generateTestOffsets(numPartitions))),
					consistencyMaxDelayGrpcMdKey: "1m",
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

func TestEncodeOffsetsV1(t *testing.T) {
	t.Run("empty offsets", func(t *testing.T) {
		assert.Equal(t, EncodedOffsets(""), EncodeOffsetsV1(nil))
		assert.Equal(t, EncodedOffsets(""), EncodeOffsetsV1(map[int32]int64{}))
	})

	t.Run("1 offset", func(t *testing.T) {
		assert.Equal(t, EncodedOffsets("v1=0:1000"), EncodeOffsetsV1(map[int32]int64{0: 1000}))
		assert.Equal(t, EncodedOffsets("v1=123456:654321"), EncodeOffsetsV1(map[int32]int64{123456: 654321}))
	})

	t.Run("multiple offsets", func(t *testing.T) {
		assert.ElementsMatch(t, []string{"1:1", "2:2", "10:9", "123:456"}, strings.Split(string(EncodeOffsetsV1(map[int32]int64{1: 1, 2: 2, 10: 9, 123: 456})[3:]), ","))
	})

	t.Run("should not skip empty partitions", func(t *testing.T) {
		assert.ElementsMatch(t, []string{"123:321", "456:-1"}, strings.Split(string(EncodeOffsetsV1(map[int32]int64{123: 321, 456: -1})[3:]), ","))
	})

	t.Run("should allocate only once", func(t *testing.T) {
		for _, numPartitions := range []int{1, 10, 100, 1000} {
			t.Run(fmt.Sprintf("num offsets: %d", numPartitions), func(t *testing.T) {
				offsets := generateTestOffsets(numPartitions)

				assert.Equal(t, 1.0, testing.AllocsPerRun(100, func() {
					EncodeOffsetsV1(offsets)
				}))
			})
		}

	})
}

func TestEncodedOffsets_LookupV1_SpecialCases(t *testing.T) {
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
			encoded: EncodeOffsetsV1(map[int32]int64{
				0: 123,
			}),
			expectedPartitions:   map[int32]int64{0: 123},
			unexpectedPartitions: []int32{1},
		},
		"single partition with negative offset": {
			encoded: EncodeOffsetsV1(map[int32]int64{
				0: -123,
			}),
			expectedPartitions:   map[int32]int64{0: -123},
			unexpectedPartitions: []int32{1},
		},
		"multiple partitions": {
			encoded: EncodeOffsetsV1(map[int32]int64{
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
				actualOffset, ok := testData.encoded.lookupV1(expectedPartitionID)
				require.True(t, ok)
				assert.Equalf(t, expectedOffset, actualOffset, "partition ID: %d", expectedPartitionID)
			}

			for _, unexpectedPartitionID := range testData.unexpectedPartitions {
				_, ok := testData.encoded.lookupV1(unexpectedPartitionID)
				assert.Falsef(t, ok, "partition ID: %d", unexpectedPartitionID)
			}
		})
	}
}

func TestEncodedOffsets_LookupV1_First1000Partitions(t *testing.T) {
	const numPartitions = 1000

	offsets := generateTestOffsets(numPartitions)
	encoded := EncodeOffsetsV1(offsets)

	for partitionID, expected := range offsets {
		actual, ok := encoded.lookupV1(partitionID)
		assert.True(t, ok)
		assert.Equal(t, expected, actual)
	}
}

func TestEncodedOffsets_LookupV1_Fuzzy(t *testing.T) {
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

		encoded := EncodeOffsetsV1(offsets)

		for partitionID, expected := range offsets {
			actual, ok := encoded.lookupV1(partitionID)
			assert.True(t, ok)
			assert.Equal(t, expected, actual)
		}
	}
}

func BenchmarkEncodeOffsetsV1(b *testing.B) {
	for _, numPartitions := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("num partitions: %d", numPartitions), func(b *testing.B) {
			offsets := generateTestOffsets(numPartitions)

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				EncodeOffsetsV1(offsets)
			}
		})
	}
}

func BenchmarkEncodedOffsets_LookupV1(b *testing.B) {
	for _, numPartitions := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("num partitions: %d", numPartitions), func(b *testing.B) {
			encoded := EncodeOffsetsV1(generateTestOffsets(numPartitions))

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				partitionID := int32(n % numPartitions)
				_, ok := encoded.lookupV1(partitionID)
				if !ok {
					b.Fatalf("not found offset for partition %d", partitionID)
				}
			}
		})
	}
}

func BenchmarkEncodeOffsetsV2(b *testing.B) {
	const (
		numReadCompartments = 4
		numKafkaClusters    = 3
	)

	for _, numPartitions := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("num partitions: %d", numPartitions), func(b *testing.B) {
			offsets := generateTestOffsetsV2(numReadCompartments, numPartitions, numKafkaClusters)

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				EncodeOffsetsV2(offsets)
			}
		})
	}
}

func BenchmarkEncodedOffsets_LookupV2(b *testing.B) {
	const (
		numReadCompartments = 4
		numKafkaClusters    = 3
	)

	for _, numPartitions := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("num partitions: %d", numPartitions), func(b *testing.B) {
			encoded := EncodeOffsetsV2(generateTestOffsetsV2(numReadCompartments, numPartitions, numKafkaClusters))

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				readCompartment := n % numReadCompartments
				partitionID := int32(n % numPartitions)
				_, ok := encoded.lookupV2(readCompartment, partitionID)
				if !ok {
					b.Fatalf("not found offsets for read compartment %d partition %d", readCompartment, partitionID)
				}
			}
		})
	}
}

func TestEncodeOffsetsV2(t *testing.T) {
	t.Run("empty offsets", func(t *testing.T) {
		assert.Equal(t, EncodedOffsets(""), EncodeOffsetsV2(nil))
		assert.Equal(t, EncodedOffsets(""), EncodeOffsetsV2(map[int]map[int32]kmeta.PartitionOffsets{}))
		assert.Equal(t, EncodedOffsets(""), EncodeOffsetsV2(map[int]map[int32]kmeta.PartitionOffsets{0: {}}))
	})

	t.Run("single read compartment, single partition, single Kafka cluster", func(t *testing.T) {
		assert.Equal(t, EncodedOffsets("v2=0/0:1000"), EncodeOffsetsV2(map[int]map[int32]kmeta.PartitionOffsets{0: {0: kmeta.NewMultiClusterPartitionOffsets([]int64{1000})}}))
	})

	t.Run("single entry with multiple Kafka clusters, should not skip empty partitions", func(t *testing.T) {
		assert.Equal(t, EncodedOffsets("v2=3/7:100;200;-1"), EncodeOffsetsV2(map[int]map[int32]kmeta.PartitionOffsets{3: {7: kmeta.NewMultiClusterPartitionOffsets([]int64{100, 200, -1})}}))
	})

	t.Run("multiple entries", func(t *testing.T) {
		encoded := EncodeOffsetsV2(map[int]map[int32]kmeta.PartitionOffsets{
			0: {1: kmeta.NewMultiClusterPartitionOffsets([]int64{10, 11}), 2: kmeta.NewMultiClusterPartitionOffsets([]int64{20, 21})},
			1: {1: kmeta.NewMultiClusterPartitionOffsets([]int64{30, 31})},
		})
		require.True(t, strings.HasPrefix(string(encoded), "v2="))
		assert.ElementsMatch(t, []string{"0/1:10;11", "0/2:20;21", "1/1:30;31"}, strings.Split(string(encoded[3:]), ","))
	})

	t.Run("should allocate only once", func(t *testing.T) {
		offsets := map[int]map[int32]kmeta.PartitionOffsets{
			0: {0: kmeta.NewMultiClusterPartitionOffsets([]int64{1, 2}), 1: kmeta.NewMultiClusterPartitionOffsets([]int64{3, 4}), 2: kmeta.NewMultiClusterPartitionOffsets([]int64{5, 6})},
			1: {0: kmeta.NewMultiClusterPartitionOffsets([]int64{7, 8}), 1: kmeta.NewMultiClusterPartitionOffsets([]int64{9, 10})},
		}
		assert.Equal(t, 1.0, testing.AllocsPerRun(100, func() {
			EncodeOffsetsV2(offsets)
		}))
	})
}

func TestEncodedOffsets_LookupV2_SpecialCases(t *testing.T) {
	type key struct {
		readCompartment int
		partition       int32
	}

	tests := map[string]struct {
		encoded    EncodedOffsets
		expected   map[key]kmeta.PartitionOffsets
		unexpected []key
	}{
		"empty": {
			encoded:    "",
			unexpected: []key{{0, 0}},
		},
		"missing version": {
			encoded:    "0/0:1",
			unexpected: []key{{0, 0}},
		},
		"corruption when reading the key": {
			encoded:    "v2=x",
			unexpected: []key{{0, 0}},
		},
		"corruption when reading the offset": {
			encoded:    "v2=0/0:x",
			unexpected: []key{{0, 0}},
		},
		"single entry, single Kafka cluster": {
			encoded:    EncodeOffsetsV2(map[int]map[int32]kmeta.PartitionOffsets{0: {0: kmeta.NewMultiClusterPartitionOffsets([]int64{123})}}),
			expected:   map[key]kmeta.PartitionOffsets{{0, 0}: kmeta.NewMultiClusterPartitionOffsets([]int64{123})},
			unexpected: []key{{0, 1}, {1, 0}},
		},
		"single entry with negative offset": {
			encoded:    EncodeOffsetsV2(map[int]map[int32]kmeta.PartitionOffsets{2: {5: kmeta.NewMultiClusterPartitionOffsets([]int64{-123, -1})}}),
			expected:   map[key]kmeta.PartitionOffsets{{2, 5}: kmeta.NewMultiClusterPartitionOffsets([]int64{-123, -1})},
			unexpected: []key{{5, 2}},
		},
		"multiple entries": {
			encoded: EncodeOffsetsV2(map[int]map[int32]kmeta.PartitionOffsets{
				0: {1: kmeta.NewMultiClusterPartitionOffsets([]int64{1, 2}), 2: kmeta.NewMultiClusterPartitionOffsets([]int64{3, math.MaxInt64})},
				1: {1: kmeta.NewMultiClusterPartitionOffsets([]int64{5, 6})},
			}),
			expected: map[key]kmeta.PartitionOffsets{
				{0, 1}: kmeta.NewMultiClusterPartitionOffsets([]int64{1, 2}),
				{0, 2}: kmeta.NewMultiClusterPartitionOffsets([]int64{3, math.MaxInt64}),
				{1, 1}: kmeta.NewMultiClusterPartitionOffsets([]int64{5, 6}),
			},
			unexpected: []key{{0, 0}, {1, 2}, {2, 1}},
		},
		"entries in reverse order": {
			encoded: "v2=1/10:100;101,1/1:10;11",
			expected: map[key]kmeta.PartitionOffsets{
				{1, 1}:  kmeta.NewMultiClusterPartitionOffsets([]int64{10, 11}),
				{1, 10}: kmeta.NewMultiClusterPartitionOffsets([]int64{100, 101}),
			},
			unexpected: []key{{1, 0}, {1, 100}},
		},
		"adjacent keys must not be confused": {
			encoded: "v2=1/2:5;6,11/2:7;8,1/22:9;10",
			expected: map[key]kmeta.PartitionOffsets{
				{1, 2}:  kmeta.NewMultiClusterPartitionOffsets([]int64{5, 6}),
				{11, 2}: kmeta.NewMultiClusterPartitionOffsets([]int64{7, 8}),
				{1, 22}: kmeta.NewMultiClusterPartitionOffsets([]int64{9, 10}),
			},
			unexpected: []key{{1, 1}},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for k, expected := range testData.expected {
				actual, ok := testData.encoded.lookupV2(k.readCompartment, k.partition)
				require.Truef(t, ok, "read compartment: %d partition: %d", k.readCompartment, k.partition)
				assert.Equalf(t, expected, actual, "read compartment: %d partition: %d", k.readCompartment, k.partition)
			}

			for _, k := range testData.unexpected {
				_, ok := testData.encoded.lookupV2(k.readCompartment, k.partition)
				assert.Falsef(t, ok, "read compartment: %d partition: %d", k.readCompartment, k.partition)
			}
		})
	}
}

func TestEncodedOffsets_LookupV2_First1000Partitions(t *testing.T) {
	// 4 read compartments × 250 partitions = 1000 entries, each with 3 Kafka clusters.
	const (
		numReadCompartments = 4
		numPartitions       = 250
		numKafkaClusters    = 3
	)

	offsets := generateTestOffsetsV2(numReadCompartments, numPartitions, numKafkaClusters)
	encoded := EncodeOffsetsV2(offsets)

	for readCompartment, partitions := range offsets {
		for partitionID, expected := range partitions {
			actual, ok := encoded.lookupV2(readCompartment, partitionID)
			assert.True(t, ok)
			assert.Equal(t, expected, actual)
		}
	}
}

func TestEncodedOffsets_LookupV2_Fuzzy(t *testing.T) {
	const (
		numRuns             = 100
		numReadCompartments = 8
		numPartitions       = 128
		maxKafkaClusters    = 4
	)

	// Randomise the seed but log it in case we need to reproduce the test on failure.
	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	t.Log("random generator seed:", seed)

	for r := 0; r < numRuns; r++ {
		// Every read compartment shares the same number of Kafka clusters, as in the real encoding.
		numKafkaClusters := 1 + rnd.Intn(maxKafkaClusters)

		offsets := make(map[int]map[int32]kmeta.PartitionOffsets, numReadCompartments)
		for readCompartment := 0; readCompartment < numReadCompartments; readCompartment++ {
			partitions := make(map[int32]kmeta.PartitionOffsets, numPartitions)
			for i := 0; i < numPartitions; i++ {
				clusterOffsets := make([]int64, numKafkaClusters)
				for c := range clusterOffsets {
					clusterOffsets[c] = rnd.Int63n(math.MaxInt64)
				}
				partitions[rnd.Int31n(math.MaxInt32)] = kmeta.NewMultiClusterPartitionOffsets(clusterOffsets)
			}
			offsets[readCompartment] = partitions
		}

		encoded := EncodeOffsetsV2(offsets)

		for readCompartment, partitions := range offsets {
			for partitionID, expected := range partitions {
				actual, ok := encoded.lookupV2(readCompartment, partitionID)
				assert.True(t, ok)
				assert.Equal(t, expected, actual)
			}
		}
	}
}

func TestEncodedOffsets_Lookup(t *testing.T) {
	type lookup struct {
		readCompartment int
		partition       int32
		expected        kmeta.PartitionOffsets
		found           bool
	}

	tests := map[string]struct {
		encoded EncodedOffsets
		lookups []lookup
	}{
		"empty": {
			encoded: "",
			lookups: []lookup{{0, 0, kmeta.PartitionOffsets{}, false}},
		},
		"unknown version": {
			encoded: "v3=0/0:1",
			lookups: []lookup{{0, 0, kmeta.PartitionOffsets{}, false}},
		},
		"v1 single cluster, read compartment 0": {
			encoded: EncodeOffsetsV1(map[int32]int64{0: 123, 1: -1}),
			lookups: []lookup{
				{0, 0, kmeta.NewSingleClusterPartitionOffsets(123), true},
				{0, 1, kmeta.NewSingleClusterPartitionOffsets(-1), true},
				{0, 2, kmeta.PartitionOffsets{}, false},
			},
		},
		"v1 is only matched for read compartment 0": {
			encoded: EncodeOffsetsV1(map[int32]int64{0: 123}),
			lookups: []lookup{
				{1, 0, kmeta.PartitionOffsets{}, false},
			},
		},
		"v1 corruption when reading the offset": {
			encoded: "v1=0:x",
			lookups: []lookup{{0, 0, kmeta.PartitionOffsets{}, false}},
		},
		"v2 single entry, single Kafka cluster": {
			encoded: EncodeOffsetsV2(map[int]map[int32]kmeta.PartitionOffsets{0: {0: kmeta.NewMultiClusterPartitionOffsets([]int64{123})}}),
			lookups: []lookup{
				{0, 0, kmeta.NewMultiClusterPartitionOffsets([]int64{123}), true},
				{0, 1, kmeta.PartitionOffsets{}, false},
				{1, 0, kmeta.PartitionOffsets{}, false},
			},
		},
		"v2 single entry, multiple Kafka clusters with negative and max offsets": {
			encoded: EncodeOffsetsV2(map[int]map[int32]kmeta.PartitionOffsets{2: {5: kmeta.NewMultiClusterPartitionOffsets([]int64{100, -1, math.MaxInt64})}}),
			lookups: []lookup{
				{2, 5, kmeta.NewMultiClusterPartitionOffsets([]int64{100, -1, math.MaxInt64}), true},
				{5, 2, kmeta.PartitionOffsets{}, false},
			},
		},
		"v2 adjacent keys must not be confused": {
			encoded: "v2=1/2:5;6,11/2:7;8,1/22:9;10",
			lookups: []lookup{
				{1, 2, kmeta.NewMultiClusterPartitionOffsets([]int64{5, 6}), true},
				{11, 2, kmeta.NewMultiClusterPartitionOffsets([]int64{7, 8}), true},
				{1, 22, kmeta.NewMultiClusterPartitionOffsets([]int64{9, 10}), true},
				{1, 1, kmeta.PartitionOffsets{}, false},
			},
		},
		"v2 corruption when reading the offset": {
			encoded: "v2=0/0:x",
			lookups: []lookup{{0, 0, kmeta.PartitionOffsets{}, false}},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for _, l := range testData.lookups {
				actual, ok := testData.encoded.Lookup(l.readCompartment, l.partition)
				assert.Equalf(t, l.found, ok, "read compartment: %d partition: %d", l.readCompartment, l.partition)
				if l.found {
					assert.Equalf(t, l.expected, actual, "read compartment: %d partition: %d", l.readCompartment, l.partition)
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

func generateTestOffsetsV2(numReadCompartments, numPartitions, numKafkaClusters int) map[int]map[int32]kmeta.PartitionOffsets {
	offsets := make(map[int]map[int32]kmeta.PartitionOffsets, numReadCompartments)
	for readCompartment := 0; readCompartment < numReadCompartments; readCompartment++ {
		partitions := make(map[int32]kmeta.PartitionOffsets, numPartitions)
		for p := 0; p < numPartitions; p++ {
			clusterOffsets := make([]int64, numKafkaClusters)
			for c := range clusterOffsets {
				// Create offsets, using the worst case scenario for the value.
				clusterOffsets[c] = math.MaxInt64 - int64(readCompartment) - int64(p) - int64(c)
			}
			partitions[int32(p)] = kmeta.NewMultiClusterPartitionOffsets(clusterOffsets)
		}
		offsets[readCompartment] = partitions
	}
	return offsets
}
