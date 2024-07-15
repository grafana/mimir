// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"context"
	"fmt"
	"testing"

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

func BenchmarkReadConsistencyServerUnaryInterceptor(b *testing.B) {
	for _, withReadConsistency := range []bool{true, false} {
		b.Run(fmt.Sprintf("with read consistency: %t", withReadConsistency), func(b *testing.B) {
			md := exampleIncomingMetadata
			if withReadConsistency {
				md = metadata.Join(md, metadata.New(map[string]string{consistencyLevelGrpcMdKey: ReadConsistencyStrong}))
			}

			ctx := metadata.NewIncomingContext(context.Background(), md)

			for n := 0; n < b.N; n++ {
				_, _ = ReadConsistencyServerUnaryInterceptor(ctx, nil, nil, func(context.Context, any) (any, error) {
					return nil, nil
				})
			}
		})
	}
}

func BenchmarkReadConsistencyServerStreamInterceptor(b *testing.B) {
	for _, withReadConsistency := range []bool{true, false} {
		b.Run(fmt.Sprintf("with read consistency: %t", withReadConsistency), func(b *testing.B) {
			md := exampleIncomingMetadata
			if withReadConsistency {
				md = metadata.Join(md, metadata.New(map[string]string{consistencyLevelGrpcMdKey: ReadConsistencyStrong}))
			}

			stream := serverStreamMock{ctx: metadata.NewIncomingContext(context.Background(), md)}

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
