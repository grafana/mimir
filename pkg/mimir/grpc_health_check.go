// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"context"

	"go.uber.org/atomic"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func newCompositeCheck(checks ...grpc_health_v1.HealthServer) grpc_health_v1.HealthServer {
	return &compositeHealthCheck{
		checks: checks,
	}
}

type compositeHealthCheck struct {
	grpc_health_v1.UnimplementedHealthServer

	checks []grpc_health_v1.HealthServer
}

func (c *compositeHealthCheck) Check(ctx context.Context, request *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	for _, srv := range c.checks {
		res, err := srv.Check(ctx, request)
		if err != nil {
			return nil, err
		}

		if res.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			return res, nil
		}
	}

	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil

}

func newAtomicHealthCheck(healthy *atomic.Bool) grpc_health_v1.HealthServer {
	return &atomicHealthCheck{healthy: healthy}
}

type atomicHealthCheck struct {
	grpc_health_v1.UnimplementedHealthServer

	healthy *atomic.Bool
}

func (a *atomicHealthCheck) Check(_ context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if !a.healthy.Load() {
		return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING}, nil
	}

	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}
