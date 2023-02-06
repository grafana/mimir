package transport

import (
	"context"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
)

// HandlerService is a service that will wait on in-flight query-frontend requests before stopping.
type HandlerService struct {
	services.Service

	handler *Handler
}

func NewHandlerService(handler *Handler, logger log.Logger) *HandlerService {
	s := &HandlerService{handler: handler}
	s.Service = services.NewBasicService(nil, s.running, s.stopping)

	return s
}

func (s *HandlerService) running(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (s *HandlerService) stopping(_ error) error {
	s.handler.stop()
	return nil
}
