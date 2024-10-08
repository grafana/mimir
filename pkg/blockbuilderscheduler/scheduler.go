package blockbuilderscheduler

import (
	"context"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
)

type BlockBuilderScheduler struct {
	services.Service

	cfg      Config
	logger   log.Logger
	register prometheus.Registerer
}

func NewScheduler(
	cfg Config,
	logger log.Logger,
	reg prometheus.Registerer,
) (*BlockBuilderScheduler, error) {
	s := &BlockBuilderScheduler{
		cfg:      cfg,
		logger:   logger,
		register: reg,
	}
	s.Service = services.NewBasicService(s.starting, s.running, s.stopping)
	return s, nil
}

func (s *BlockBuilderScheduler) starting(context.Context) (err error) {
	return nil
}

func (s *BlockBuilderScheduler) stopping(_ error) error {
	return nil
}

func (s *BlockBuilderScheduler) running(ctx context.Context) error {
	return nil
}
