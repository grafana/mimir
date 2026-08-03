// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	//lint:ignore faillint Allow importing the kmeta package, since it's an isolated package (doesn't come with many other dependencies).
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	//lint:ignore faillint Allow to import the math util package, since it's an isolated package (doesn't come with many other deps).
	"github.com/grafana/mimir/pkg/util/math"
	//lint:ignore faillint Allow importing the propagation package, since it's an isolated package (doesn't come with many other dependencies).
	"github.com/grafana/mimir/pkg/util/propagation"
)

const (
	ReadConsistencyHeader         = "X-Read-Consistency"
	ReadConsistencyOffsetsHeader  = "X-Read-Consistency-Offsets"
	ReadConsistencyMaxDelayHeader = "X-Read-Consistency-Max-Delay"

	// ReadConsistencyStrong means that a query sent by the same client will always observe the writes
	// that have completed before issuing the query.
	ReadConsistencyStrong = "strong"

	// ReadConsistencyEventual is the default consistency level for all queries.
	// This level means that a query sent by a client may not observe some of the writes that the same client has recently made.
	ReadConsistencyEventual = "eventual"
)

var ReadConsistencies = []string{ReadConsistencyStrong, ReadConsistencyEventual}

func IsValidReadConsistency(lvl string) bool {
	return slices.Contains(ReadConsistencies, lvl)
}

func IsValidReadConsistencyMaxDelay(delay time.Duration) bool {
	return delay > 0
}

type contextKey int

const (
	consistencyContextKey        contextKey = 1
	consistencyOffsetsContextKey contextKey = 2
	consistencyMaxDelayKey       contextKey = 3
)

// ContextWithReadConsistencyLevel returns a new context with the given consistency level.
// The consistency level can be retrieved with ReadConsistencyLevelFromContext.
func ContextWithReadConsistencyLevel(parent context.Context, level string) context.Context {
	return context.WithValue(parent, consistencyContextKey, level)
}

// ReadConsistencyLevelFromContext returns the consistency level from the context if set via ContextWithReadConsistencyLevel.
// The second return value is true if the consistency level was found in the context and is valid.
func ReadConsistencyLevelFromContext(ctx context.Context) (string, bool) {
	level, _ := ctx.Value(consistencyContextKey).(string)
	return level, IsValidReadConsistency(level)
}

// ContextWithReadConsistencyEncodedOffsets returns a new context with the given partition offsets.
// The offsets can be retrieved with ReadConsistencyEncodedOffsetsFromContext.
func ContextWithReadConsistencyEncodedOffsets(ctx context.Context, offsets EncodedOffsets) context.Context {
	return context.WithValue(ctx, consistencyOffsetsContextKey, offsets)
}

// ReadConsistencyEncodedOffsetsFromContext returns the partition offsets to enforce strong read consistency.
// The second return value is true if offsets were found in the context.
func ReadConsistencyEncodedOffsetsFromContext(ctx context.Context) (EncodedOffsets, bool) {
	encoded, ok := ctx.Value(consistencyOffsetsContextKey).(EncodedOffsets)
	if !ok {
		return "", false
	}

	return encoded, true
}

// ContextWithReadConsistencyMaxDelay returns a new context with the given max delay configured.
// The delay can be retrieved with ReadConsistencyMaxDelayFromContext.
func ContextWithReadConsistencyMaxDelay(ctx context.Context, delay time.Duration) context.Context {
	return context.WithValue(ctx, consistencyMaxDelayKey, delay)
}

// ContextWithReadConsistencyMaxDelayString is like ContextWithReadConsistencyMaxDelay but accepts
// the delay as a string in input, and returns an error if the provided string can't be parsed as duration.
func ContextWithReadConsistencyMaxDelayString(ctx context.Context, delay string) (context.Context, error) {
	parsedDelay, err := time.ParseDuration(delay)
	if err != nil {
		return ctx, err
	}
	return ContextWithReadConsistencyMaxDelay(ctx, parsedDelay), nil
}

// ReadConsistencyMaxDelayFromContext returns max delay / staleness to enforce on eventually consistent requests.
// The second return value is true if the setting is found in the context.
func ReadConsistencyMaxDelayFromContext(ctx context.Context) (time.Duration, bool) {
	delay, _ := ctx.Value(consistencyMaxDelayKey).(time.Duration)
	return delay, IsValidReadConsistencyMaxDelay(delay)
}

// ConsistencyExtractor takes the consistency level from the X-Read-Consistency header and sets it in the context.
type ConsistencyExtractor struct{}

func (e *ConsistencyExtractor) ExtractFromCarrier(ctx context.Context, carrier propagation.Carrier) (context.Context, error) {
	if level := carrier.Get(ReadConsistencyHeader); IsValidReadConsistency(level) {
		ctx = ContextWithReadConsistencyLevel(ctx, level)
	}

	if offsets := carrier.Get(ReadConsistencyOffsetsHeader); len(offsets) > 0 {
		ctx = ContextWithReadConsistencyEncodedOffsets(ctx, EncodedOffsets(offsets))
	}

	if delay := carrier.Get(ReadConsistencyMaxDelayHeader); len(delay) > 0 {
		// Ignore the error since there's not much we can do. In case of error, the original context is returned.
		ctx, _ = ContextWithReadConsistencyMaxDelayString(ctx, delay)
	}

	return ctx, nil
}

// ConsistencyInjector injects the consistency level from the context (if any) into the carrier.
//
// It does not add the offsets to the carrier, as these are handled by the query-frontend's list of HTTP headers to propagate.
type ConsistencyInjector struct{}

func (i *ConsistencyInjector) InjectToCarrier(ctx context.Context, carrier propagation.Carrier) error {
	if level, ok := ReadConsistencyLevelFromContext(ctx); ok {
		carrier.Add(ReadConsistencyHeader, level)
	}

	if delay, ok := ReadConsistencyMaxDelayFromContext(ctx); ok {
		carrier.Add(ReadConsistencyMaxDelayHeader, delay.String())
	}

	return nil
}

const (
	consistencyLevelGrpcMdKey    = "__consistency_level__"
	consistencyOffsetsGrpcMdKey  = "__consistency_offsets__"
	consistencyMaxDelayGrpcMdKey = "__consistency_max_delay__"
)

func ReadConsistencyClientUnaryInterceptor(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if value, ok := ReadConsistencyLevelFromContext(ctx); ok {
		ctx = metadata.AppendToOutgoingContext(ctx, consistencyLevelGrpcMdKey, value)
	}
	if value, ok := ReadConsistencyEncodedOffsetsFromContext(ctx); ok {
		ctx = metadata.AppendToOutgoingContext(ctx, consistencyOffsetsGrpcMdKey, string(value))
	}
	if value, ok := ReadConsistencyMaxDelayFromContext(ctx); ok {
		ctx = metadata.AppendToOutgoingContext(ctx, consistencyMaxDelayGrpcMdKey, value.String())
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

func ReadConsistencyServerUnaryInterceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	levels := metadata.ValueFromIncomingContext(ctx, consistencyLevelGrpcMdKey)
	if len(levels) > 0 && IsValidReadConsistency(levels[0]) {
		ctx = ContextWithReadConsistencyLevel(ctx, levels[0])
	}

	offsets := metadata.ValueFromIncomingContext(ctx, consistencyOffsetsGrpcMdKey)
	if len(offsets) > 0 {
		ctx = ContextWithReadConsistencyEncodedOffsets(ctx, EncodedOffsets(offsets[0]))
	}

	delay := metadata.ValueFromIncomingContext(ctx, consistencyMaxDelayGrpcMdKey)
	if len(delay) > 0 {
		// Ignore the error, given there's not much we can do.
		ctx, _ = ContextWithReadConsistencyMaxDelayString(ctx, delay[0])
	}

	return handler(ctx, req)
}

func ReadConsistencyClientStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if value, ok := ReadConsistencyLevelFromContext(ctx); ok {
		ctx = metadata.AppendToOutgoingContext(ctx, consistencyLevelGrpcMdKey, value)
	}
	if value, ok := ReadConsistencyEncodedOffsetsFromContext(ctx); ok {
		ctx = metadata.AppendToOutgoingContext(ctx, consistencyOffsetsGrpcMdKey, string(value))
	}
	if value, ok := ReadConsistencyMaxDelayFromContext(ctx); ok {
		ctx = metadata.AppendToOutgoingContext(ctx, consistencyMaxDelayGrpcMdKey, value.String())
	}
	return streamer(ctx, desc, cc, method, opts...)
}

func ReadConsistencyServerStreamInterceptor(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := ss.Context()

	levels := metadata.ValueFromIncomingContext(ss.Context(), consistencyLevelGrpcMdKey)
	if len(levels) > 0 && IsValidReadConsistency(levels[0]) {
		ctx = ContextWithReadConsistencyLevel(ctx, levels[0])
	}

	offsets := metadata.ValueFromIncomingContext(ctx, consistencyOffsetsGrpcMdKey)
	if len(offsets) > 0 {
		ctx = ContextWithReadConsistencyEncodedOffsets(ctx, EncodedOffsets(offsets[0]))
	}

	delay := metadata.ValueFromIncomingContext(ctx, consistencyMaxDelayGrpcMdKey)
	if len(delay) > 0 {
		// Ignore the error, given there's not much we can do.
		ctx, _ = ContextWithReadConsistencyMaxDelayString(ctx, delay[0])
	}

	ss = ctxStream{
		ctx:          ctx,
		ServerStream: ss,
	}
	return handler(srv, ss)
}

type ctxStream struct {
	ctx context.Context
	grpc.ServerStream
}

func (ss ctxStream) Context() context.Context {
	return ss.ctx
}

// EncodedOffsets holds the encoded partition offsets.
type EncodedOffsets string

// Lookup returns the offsets for the input read compartment and partition.
func (p EncodedOffsets) Lookup(readCompartment int, partitionID int32) (kmeta.PartitionOffsets, bool) {
	const versionLen = 3
	if len(p) < versionLen {
		return kmeta.PartitionOffsets{}, false
	}

	switch p[:versionLen] {
	case "v1=":
		if readCompartment != 0 {
			return kmeta.PartitionOffsets{}, false
		}
		offset, ok := p.lookupV1(partitionID)
		if !ok {
			return kmeta.PartitionOffsets{}, false
		}
		return kmeta.NewSingleClusterPartitionOffsets(offset), true
	case "v2=":
		return p.lookupV2(readCompartment, partitionID)
	default:
		return kmeta.PartitionOffsets{}, false
	}
}

// lookupV1 returns the offset for the input partitionID from the v1 encoding produced by EncodeOffsetsV1.
// It assumes the caller has already verified the "v1=" version prefix and so doesn't re-check it.
func (p EncodedOffsets) lookupV1(partitionID int32) (int64, bool) {
	// Find the position of the partition. The partition can either be:
	// - At the beginning, right after the version (so after "=")
	// - In the middle or end, right after another partition (so after ",")
	partitionIDString := strconv.FormatInt(int64(partitionID), 10)
	partitionKey := "," + partitionIDString + ":"
	partitionIdx := strings.Index(string(p), partitionKey)
	if partitionIdx < 0 {
		partitionKey = "=" + partitionIDString + ":"
		partitionIdx = strings.Index(string(p), partitionKey)
	}
	if partitionIdx < 0 {
		return 0, false
	}

	// Find the end index of the offset.
	offsetEndIdx := strings.Index(string(p[partitionIdx+len(partitionKey):]), ",")
	if offsetEndIdx >= 0 {
		offsetEndIdx += partitionIdx + len(partitionKey)
	} else {
		offsetEndIdx = len(p)
	}

	// Extract the offset.
	offset, err := strconv.ParseInt(string(p[partitionIdx+len(partitionKey):offsetEndIdx]), 10, 64)
	if err != nil {
		return 0, false
	}

	return offset, true
}

// EncodeOffsetsV1 serialise the input offsets into a string which is safe to be used as HTTP header value.
// Empty partitions (offset is -1) are NOT skipped.
func EncodeOffsetsV1(offsets map[int32]int64) EncodedOffsets {
	const versionLen = 3

	if len(offsets) == 0 {
		return ""
	}

	// Count the number of digits (eventually including the minus sign). To count digits we use an estimation
	// function which is expected to be faster than the precise count and should return an estimated number of
	// digits which is >= the actual one. This means we may slightly overallocate memory, but we should avoid
	// re-allocations because we under-counted digits.
	size := versionLen
	for partitionID, offset := range offsets {
		sizeSeparator := 0
		if size > 0 {
			sizeSeparator = 1
		}

		size += math.EstimatedDigitsInt32(partitionID) + 1 + math.EstimatedDigitsInt64(offset) + sizeSeparator
	}

	// Encode the key-value pairs using ASCII characters, so that they can be safely included
	// in an HTTP header.
	buffer := make([]byte, 0, size)

	// Add versioning, so that it will be easier to change encoding format in the future (if required).
	buffer = append(buffer, []byte("v1=")...)

	for partitionID, offset := range offsets {
		// Add the separator, unless it's the first entry.
		if len(buffer) > versionLen {
			buffer = append(buffer, ',')
		}

		buffer = strconv.AppendInt(buffer, int64(partitionID), 10)
		buffer = append(buffer, ':')
		buffer = strconv.AppendInt(buffer, offset, 10)
	}

	return EncodedOffsets(unsafe.String(unsafe.SliceData(buffer), len(buffer)))
}

// EncodeOffsetsV2 serialises the input per-read-compartment kmeta.PartitionsOffsets into a
// string which is safe to be used as an HTTP header value.
//
// The encoded format is:
// "v2=<read-compartment>/<partition>:<offset>;<offset>;..."
//
// with offsets indexed by Kafka cluster ID.
func EncodeOffsetsV2(offsets map[int]kmeta.PartitionsOffsets) EncodedOffsets {
	const version = "v2="

	numEntries := 0
	for _, partitions := range offsets {
		numEntries += len(partitions)
	}
	if numEntries == 0 {
		return ""
	}

	// Pre-size the buffer to avoid reallocations, slightly overallocating because the digit count is an
	// estimation that is expected to be >= the actual one.
	size := len(version)
	for readCompartment, partitions := range offsets {
		for partitionID, clusterOffsets := range partitions {
			// "<read-compartment>" + "/" + "<partition>" + ":" + entry separator.
			size += math.EstimatedDigitsInt64(int64(readCompartment)) + 1 + math.EstimatedDigitsInt32(partitionID) + 1 + 1
			for kafkaClusterID := 0; kafkaClusterID < clusterOffsets.NumKafkaClusters(); kafkaClusterID++ {
				// "<offset>" + cluster separator.
				size += math.EstimatedDigitsInt64(clusterOffsets.ForKafkaCluster(kafkaClusterID)) + 1
			}
		}
	}

	buffer := make([]byte, 0, size)
	buffer = append(buffer, version...)

	first := true
	for readCompartment, partitions := range offsets {
		for partitionID, clusterOffsets := range partitions {
			// Add the entry separator, unless it's the first entry.
			if !first {
				buffer = append(buffer, ',')
			}
			first = false

			buffer = strconv.AppendInt(buffer, int64(readCompartment), 10)
			buffer = append(buffer, '/')
			buffer = strconv.AppendInt(buffer, int64(partitionID), 10)
			buffer = append(buffer, ':')

			for kafkaClusterID := 0; kafkaClusterID < clusterOffsets.NumKafkaClusters(); kafkaClusterID++ {
				if kafkaClusterID > 0 {
					buffer = append(buffer, ';')
				}
				buffer = strconv.AppendInt(buffer, clusterOffsets.ForKafkaCluster(kafkaClusterID), 10)
			}
		}
	}

	// buffer is freshly allocated here, becomes the returned string's only backing, and is never mutated
	// or pooled afterwards, so the zero-copy conversion is safe.
	return EncodedOffsets(unsafe.String(unsafe.SliceData(buffer), len(buffer))) // #nosec G103 -- nosemgrep
}

// lookupV2 returns the per-Kafka-cluster offsets for the input read compartment and partition, as encoded
// by EncodeOffsetsV2.
func (p EncodedOffsets) lookupV2(readCompartment int, partitionID int32) (kmeta.PartitionOffsets, bool) {
	const versionLen = 3

	if len(p) < versionLen || p[:versionLen] != "v2=" {
		return kmeta.PartitionOffsets{}, false
	}

	// The entry key is "<read-compartment>/<partition>:". It is anchored by the delimiter that precedes it
	// (a "," between entries, or the "=" of the version prefix for the first entry) so that, for example,
	// looking up "1/2" doesn't match "11/2". The "/" and ":" never appear in offset values, so the key can
	// only match at a real entry boundary.
	key := strconv.Itoa(readCompartment) + "/" + strconv.FormatInt(int64(partitionID), 10) + ":"
	idx := strings.Index(string(p), ","+key)
	if idx < 0 {
		idx = strings.Index(string(p), "="+key)
	}
	if idx < 0 {
		return kmeta.PartitionOffsets{}, false
	}

	// Skip the leading delimiter and the key.
	start := idx + 1 + len(key)

	// The value runs until the next entry separator or the end of the string.
	end := strings.IndexByte(string(p[start:]), ',')
	if end >= 0 {
		end += start
	} else {
		end = len(p)
	}

	// Parse the ";"-separated per-Kafka-cluster offsets.
	parts := strings.Split(string(p[start:end]), ";")
	clusterOffsets := make([]int64, 0, len(parts))
	for _, part := range parts {
		offset, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return kmeta.PartitionOffsets{}, false
		}
		clusterOffsets = append(clusterOffsets, offset)
	}

	return kmeta.NewMultiClusterPartitionOffsets(clusterOffsets), true
}
