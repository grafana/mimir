// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"context"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"github.com/grafana/dskit/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	//lint:ignore faillint Allow to import the math util package, since it's an isolated package (doesn't come with many other deps).
	"github.com/grafana/mimir/pkg/util/math"
)

const (
	ReadConsistencyHeader        = "X-Read-Consistency"
	ReadConsistencyOffsetsHeader = "X-Read-Consistency-Offsets"

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

type contextKey int

const (
	consistencyContextKey        contextKey = 1
	consistencyOffsetsContextKey contextKey = 2
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

// ConsistencyMiddleware takes the consistency level from the X-Read-Consistency header and sets it in the context.
// It can be retrieved with ReadConsistencyLevelFromContext.
func ConsistencyMiddleware() middleware.Interface {
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if level := r.Header.Get(ReadConsistencyHeader); IsValidReadConsistency(level) {
				r = r.WithContext(ContextWithReadConsistencyLevel(r.Context(), level))
			}

			if offsets := r.Header.Get(ReadConsistencyOffsetsHeader); len(offsets) > 0 {
				r = r.WithContext(ContextWithReadConsistencyEncodedOffsets(r.Context(), EncodedOffsets(offsets)))
			}

			next.ServeHTTP(w, r)
		})
	})
}

const (
	consistencyLevelGrpcMdKey   = "__consistency_level__"
	consistencyOffsetsGrpcMdKey = "__consistency_offsets__"
)

func ReadConsistencyClientUnaryInterceptor(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if value, ok := ReadConsistencyLevelFromContext(ctx); ok {
		ctx = metadata.AppendToOutgoingContext(ctx, consistencyLevelGrpcMdKey, value)
	}
	if value, ok := ReadConsistencyEncodedOffsetsFromContext(ctx); ok {
		ctx = metadata.AppendToOutgoingContext(ctx, consistencyOffsetsGrpcMdKey, string(value))
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

	return handler(ctx, req)
}

func ReadConsistencyClientStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if value, ok := ReadConsistencyLevelFromContext(ctx); ok {
		ctx = metadata.AppendToOutgoingContext(ctx, consistencyLevelGrpcMdKey, value)
	}
	if value, ok := ReadConsistencyEncodedOffsetsFromContext(ctx); ok {
		ctx = metadata.AppendToOutgoingContext(ctx, consistencyOffsetsGrpcMdKey, string(value))
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

// Lookup the offset for the input partitionID.
func (p EncodedOffsets) Lookup(partitionID int32) (int64, bool) {
	const versionLen = 3

	if len(p) < versionLen {
		return 0, false
	}

	// Check the version.
	if p[:3] != "v1=" {
		return 0, false
	}

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

// EncodeOffsets serialise the input offsets into a string which is safe to be used as HTTP header value.
// Empty partitions (offset is -1) are NOT skipped.
func EncodeOffsets(offsets map[int32]int64) EncodedOffsets {
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
