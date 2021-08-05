package e2emimir

import "github.com/grafana/mimir/integration/e2e"

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
	otherPorts ...int,
) *MimirService {
	return &MimirService{
		HTTPService: e2e.NewHTTPService(name, image, command, readiness, httpPort, append(otherPorts, grpcPort)...),
		grpcPort:    grpcPort,
	}
}

func (s *MimirService) GRPCEndpoint() string {
	return s.Endpoint(s.grpcPort)
}

func (s *MimirService) NetworkGRPCEndpoint() string {
	return s.NetworkEndpoint(s.grpcPort)
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
