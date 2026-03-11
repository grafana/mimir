// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// lowerHTTPHeaderKey is the lowercase form of HTTPHeaderKey, required by gRPC metadata.
const lowerHTTPHeaderKey = "x-prom-label-policy"

// ClientLBACInterceptor is a gRPC unary client interceptor that propagates LBAC policies
// from the context into outgoing gRPC metadata.
func ClientLBACInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ctx, err := injectIntoGRPCRequest(ctx)
	if err != nil {
		return err
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

// StreamClientLBACInterceptor is a gRPC streaming client interceptor that propagates LBAC
// policies from the context into outgoing gRPC metadata.
func StreamClientLBACInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	ctx, err := injectIntoGRPCRequest(ctx)
	if err != nil {
		return nil, err
	}
	return streamer(ctx, desc, cc, method, opts...)
}

// ServerLBACInterceptor is a gRPC unary server interceptor that extracts LBAC policies
// from incoming gRPC metadata and injects them into the context.
func ServerLBACInterceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ctx, err := extractFromGRPCRequest(ctx)
	if err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// StreamServerLBACInterceptor is a gRPC streaming server interceptor that extracts LBAC
// policies from incoming gRPC metadata and injects them into the context.
func StreamServerLBACInterceptor(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx, err := extractFromGRPCRequest(ss.Context())
	if err != nil {
		return err
	}
	return handler(srv, &wrappedServerStream{ctx: ctx, ServerStream: ss})
}

// wrappedServerStream overrides Context() so the handler receives the enriched context.
type wrappedServerStream struct {
	ctx context.Context
	grpc.ServerStream
}

func (s *wrappedServerStream) Context() context.Context {
	return s.ctx
}

// injectIntoGRPCRequest reads LBAC policies from ctx and writes them into the outgoing
// gRPC metadata. If no policies are present the context is returned unchanged.
func injectIntoGRPCRequest(ctx context.Context) (context.Context, error) {
	labelPolicySet, err := ExtractLabelMatchersContext(ctx)
	if err != nil {
		// No policies in context — nothing to propagate.
		return ctx, nil
	}

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	}
	md = md.Copy()

	var headerValues []string
	for instanceName, policies := range labelPolicySet {
		for _, policy := range policies {
			encoded, err := policyToHeaderValue(instanceName, policy)
			if err != nil {
				return ctx, err
			}
			headerValues = append(headerValues, encoded)
		}
	}
	md.Set(lowerHTTPHeaderKey, headerValues...)

	return metadata.NewOutgoingContext(ctx, md), nil
}

// extractFromGRPCRequest reads LBAC policies from incoming gRPC metadata and injects them
// into the context. If no metadata is present the context is returned unchanged.
func extractFromGRPCRequest(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, nil
	}

	headerValues, ok := md[lowerHTTPHeaderKey]
	if !ok {
		return ctx, nil
	}

	instancePolicyMap := LabelPolicySet{}
	for _, headerValue := range headerValues {
		for _, v := range strings.Split(headerValue, ",") {
			instanceName, policy, err := policyFromHeaderValue(v)
			if err != nil {
				return ctx, err
			}
			instancePolicyMap[instanceName] = append(instancePolicyMap[instanceName], policy)
		}
	}

	return InjectLabelMatchersContext(ctx, instancePolicyMap), nil
}
