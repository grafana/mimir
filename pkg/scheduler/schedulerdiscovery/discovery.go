// SPDX-License-Identifier: AGPL-3.0-only

package schedulerdiscovery

import (
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/util/servicediscovery"
)

func NewServiceDiscovery(cfg Config, schedulerAddress string, lookupPeriod time.Duration, component string, receiver servicediscovery.Notifications, logger log.Logger, reg prometheus.Registerer) (services.Service, error) {
	// Since this is a client for the query-schedulers ring, we append "query-scheduler-client" to the component to clearly differentiate it.
	component = component + "-query-scheduler-client"

	switch cfg.Mode {
	case ModeRing:
		return NewRingServiceDiscovery(cfg, component, receiver, logger, reg)
	default:
		return NewDNSServiceDiscovery(schedulerAddress, lookupPeriod, receiver)
	}
}

func NewDNSServiceDiscovery(schedulerAddress string, lookupPeriod time.Duration, receiver servicediscovery.Notifications) (services.Service, error) {
	return servicediscovery.NewDNS(schedulerAddress, lookupPeriod, receiver)
}

var (
	activeSchedulersOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)
)

func NewRingServiceDiscovery(cfg Config, component string, receiver servicediscovery.Notifications, logger log.Logger, reg prometheus.Registerer) (services.Service, error) {
	client, err := NewRingClient(cfg.SchedulerRing, component, logger, reg)
	if err != nil {
		return nil, err
	}

	return servicediscovery.NewRing(client, activeSchedulersOp, cfg.SchedulerRing.RingCheckPeriod, receiver), nil
}
