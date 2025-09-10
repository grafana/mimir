package grpcutil

import (
	"context"
	"strconv"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	// PriorityLevelKey is the gRPC metadata key for priority level propagation
	PriorityLevelKey = "x-mimir-priority-level"
)

// PriorityClientInterceptor propagates priority levels to downstream gRPC calls
func PriorityClientInterceptor(logger log.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{},
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {

		// Extract priority level from context and propagate it
		if priorityLevel := ctx.Value(PriorityLevelKey); priorityLevel != nil {
			if levelInt, ok := priorityLevel.(int); ok {
				ctx = metadata.AppendToOutgoingContext(ctx, PriorityLevelKey, strconv.Itoa(levelInt))

				level.Debug(logger).Log(
					"msg", "propagating priority level",
					"method", method,
					"level", levelInt,
				)
			}
		}

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// PriorityServerInterceptor extracts priority levels from incoming gRPC calls
func PriorityServerInterceptor(logger log.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (interface{}, error) {

		// Extract priority level from gRPC metadata
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if values := md.Get(PriorityLevelKey); len(values) > 0 {
				if levelInt, err := strconv.Atoi(values[0]); err == nil {
					ctx = context.WithValue(ctx, PriorityLevelKey, levelInt)

					level.Debug(logger).Log(
						"msg", "extracted priority level",
						"method", info.FullMethod,
						"level", levelInt,
					)
				}
			}
		}

		return handler(ctx, req)
	}
}

// GetPriorityFromContext extracts priority level from context, returns 0 if not found
func GetPriorityFromContext(ctx context.Context) int {
	if priority := ctx.Value(PriorityLevelKey); priority != nil {
		if levelInt, ok := priority.(int); ok {
			return levelInt
		}
	}
	return 0 // Default to VeryLow priority if not found
}

// GetPriorityLabel returns a human-readable priority label for metrics
func GetPriorityLabel(priority int) string {
	switch {
	case priority >= 400:
		return "very_high"
	case priority >= 300:
		return "high"
	case priority >= 200:
		return "medium"
	case priority >= 100:
		return "low"
	default:
		return "very_low"
	}
}

// GetPriorityLevel extracts priority level (0-499) from context
func GetPriorityLevel(ctx context.Context) int {
	if priorityLevel := ctx.Value(PriorityLevelKey); priorityLevel != nil {
		if levelInt, ok := priorityLevel.(int); ok {
			return levelInt
		}
	}
	return 200 // Default to PriorityMedium base (200-299 range)
}

// WithPriorityLevel adds priority level to context
func WithPriorityLevel(ctx context.Context, priorityLevel int) context.Context {
	return context.WithValue(ctx, PriorityLevelKey, priorityLevel)
}