// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/e2ecortex/service.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package e2emimir

import "github.com/grafana/e2e"

// MimirService represents a Mimir service with at least an HTTP and GRPC port exposed.
type MimirService struct {
	*e2e.HTTPService

	grpcPort int
}

func NewMimirService(
	name string,
	image string,
	command *e2e.Command,
	readiness e2e.ReadinessProbe,
	httpPort int,
	grpcPort int,
	exposeGRPCPort bool,
	otherPorts ...int,
) *MimirService {
	if exposeGRPCPort {
		otherPorts = append(otherPorts, grpcPort)
	}
	s := &MimirService{
		// We don't expose the gRPC port cause we don't need to access it from the host
		// (exposing ports have a negative performance impact on starting/stopping containers).
		HTTPService: e2e.NewHTTPService(name, image, command, readiness, httpPort, otherPorts...),
		grpcPort:    grpcPort,
	}
	// Add jaeger configuration with a 50% sampling rate so that we can trigger
	// code paths that rely on a trace being sampled.
	s.SetEnvVars(map[string]string{
		"JAEGER_SAMPLER_TYPE":  "const",
		"JAEGER_SAMPLER_PARAM": "0.5",
	})
	return s
}

func (s *MimirService) NetworkGRPCEndpoint() string {
	return s.NetworkEndpoint(s.grpcPort)
}

func (s *MimirService) GRPCEndpoint() string {
	return s.Endpoint(s.grpcPort)
}

// CompositeMimirService abstract an higher-level service composed, under the hood,
// by 2+ MimirService.
type CompositeMimirService struct {
	*e2e.CompositeHTTPService
}

func NewCompositeMimirService(services ...*MimirService) *CompositeMimirService {
	var httpServices []*e2e.HTTPService
	for _, s := range services {
		httpServices = append(httpServices, s.HTTPService)
	}

	return &CompositeMimirService{
		CompositeHTTPService: e2e.NewCompositeHTTPService(httpServices...),
	}
}
