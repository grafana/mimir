// SPDX-License-Identifier: AGPL-3.0-only

package schedulerdiscovery

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/util"
)

// Notifications about address resolution. All notifications are sent on the same goroutine.
type Notifications interface {
	// AddressAdded is called each time a new query-scheduler address has been discovered by service discovery.
	AddressAdded(address string)

	// AddressRemoved is called each time query-scheduler address that was previously notified by AddressAdded()
	// is no longer available.
	AddressRemoved(address string)
}

func NewServiceDiscovery(cfg Config, schedulerAddress string, lookupPeriod time.Duration, component string, receiver Notifications, logger log.Logger, reg prometheus.Registerer) (services.Service, error) {
	// Since this is a client for the query-schedulers ring, we append "query-scheduler-client" to the component to clearly differentiate it.
	component = component + "-query-scheduler-client"

	switch cfg.Mode {
	case ModeRing:
		return NewRingServiceDiscovery(cfg, component, receiver, logger, reg)
	default:
		return NewDNSServiceDiscovery(schedulerAddress, lookupPeriod, receiver)
	}
}

func NewDNSServiceDiscovery(schedulerAddress string, lookupPeriod time.Duration, receiver Notifications) (services.Service, error) {
	return util.NewDNSWatcher(schedulerAddress, lookupPeriod, receiver)
}

var (
	activeSchedulersOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)
)

type ringServiceDiscovery struct {
	services.Service

	ring               *ring.Ring
	subservicesWatcher *services.FailureWatcher
	checkPeriod        time.Duration
	receiver           Notifications

	// Keep track of the addresses that have been discovered and notified so far.
	notifiedAddresses map[string]struct{}
}

func NewRingServiceDiscovery(cfg Config, component string, receiver Notifications, logger log.Logger, reg prometheus.Registerer) (services.Service, error) {
	client, err := NewRingClient(cfg.SchedulerRing, component, logger, reg)
	if err != nil {
		return nil, err
	}

	r := &ringServiceDiscovery{
		ring:               client,
		subservicesWatcher: services.NewFailureWatcher(),
		checkPeriod:        cfg.SchedulerRing.RingCheckPeriod,
		notifiedAddresses:  make(map[string]struct{}),
		receiver:           receiver,
	}

	r.Service = services.NewBasicService(r.starting, r.running, r.stopping)

	return r, nil
}

func (r *ringServiceDiscovery) starting(ctx context.Context) error {
	r.subservicesWatcher.WatchService(r.ring)

	return errors.Wrap(services.StartAndAwaitRunning(ctx, r.ring), "failed to start query-schedulers ring client")
}

func (r *ringServiceDiscovery) stopping(_ error) error {
	return errors.Wrap(services.StopAndAwaitTerminated(context.Background(), r.ring), "failed to stop query-schedulers ring client")
}

func (r *ringServiceDiscovery) running(ctx context.Context) error {
	ringTicker := time.NewTicker(r.checkPeriod)
	defer ringTicker.Stop()

	// Notifies the initial state.
	ringState, _ := r.ring.GetAllHealthy(activeSchedulersOp) // nolint:errcheck
	r.notifyChanges(ringState)

	for {
		select {
		case <-ringTicker.C:
			ringState, _ := r.ring.GetAllHealthy(activeSchedulersOp) // nolint:errcheck
			r.notifyChanges(ringState)
		case <-ctx.Done():
			return nil
		case err := <-r.subservicesWatcher.Chan():
			return errors.Wrap(err, "a subservice of query-schedulers ring-based service discovery has failed")
		}
	}
}

// notifyChanges is not concurrency safe.
func (r *ringServiceDiscovery) notifyChanges(discovered ring.ReplicationSet) {
	// Build a map with the discovered addresses.
	discoveredAddresses := make(map[string]struct{}, len(discovered.Instances))
	for _, instance := range discovered.Instances {
		discoveredAddresses[instance.GetAddr()] = struct{}{}
	}

	// Notify new addresses.
	for addr := range discoveredAddresses {
		if _, ok := r.notifiedAddresses[addr]; !ok {
			r.receiver.AddressAdded(addr)
		}
	}

	// Notify removed addresses.
	for addr := range r.notifiedAddresses {
		if _, ok := discoveredAddresses[addr]; !ok {
			r.receiver.AddressRemoved(addr)
		}
	}

	r.notifiedAddresses = discoveredAddresses
}
