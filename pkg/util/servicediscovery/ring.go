// SPDX-License-Identifier: AGPL-3.0-only

package servicediscovery

import (
	"context"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
)

type ringServiceDiscovery struct {
	services.Service

	ringClient         *ring.Ring
	ringOp             ring.Operation
	subservicesWatcher *services.FailureWatcher
	ringCheckPeriod    time.Duration
	receiver           Notifications

	// Keep track of the addresses that have been discovered and notified so far.
	notifiedAddresses map[string]struct{}
}

func NewRing(ringClient *ring.Ring, ringOp ring.Operation, ringCheckPeriod time.Duration, receiver Notifications) services.Service {
	r := &ringServiceDiscovery{
		ringClient:         ringClient,
		ringOp:             ringOp,
		subservicesWatcher: services.NewFailureWatcher(),
		ringCheckPeriod:    ringCheckPeriod,
		notifiedAddresses:  make(map[string]struct{}),
		receiver:           receiver,
	}

	r.Service = services.NewBasicService(r.starting, r.running, r.stopping)
	return r
}

func (r *ringServiceDiscovery) starting(ctx context.Context) error {
	r.subservicesWatcher.WatchService(r.ringClient)

	return errors.Wrap(services.StartAndAwaitRunning(ctx, r.ringClient), "failed to start ring client")
}

func (r *ringServiceDiscovery) stopping(_ error) error {
	return errors.Wrap(services.StopAndAwaitTerminated(context.Background(), r.ringClient), "failed to stop ring client")
}

func (r *ringServiceDiscovery) running(ctx context.Context) error {
	ringTicker := time.NewTicker(r.ringCheckPeriod)
	defer ringTicker.Stop()

	// Notifies the initial state.
	ringState, _ := r.ringClient.GetAllHealthy(r.ringOp) // nolint:errcheck
	r.notifyChanges(ringState)

	for {
		select {
		case <-ringTicker.C:
			ringState, _ := r.ringClient.GetAllHealthy(r.ringOp) // nolint:errcheck
			r.notifyChanges(ringState)
		case <-ctx.Done():
			return nil
		case err := <-r.subservicesWatcher.Chan():
			return errors.Wrap(err, "a subservice of ring-based service discovery has failed")
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
