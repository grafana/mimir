// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"net/http"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/weaveworks/common/signals"
	"go.uber.org/atomic"
)

// newShutdownSignalReceiver creates a new signals.SignalReceiver implementation that
// handles everything required to cleanly shut down Mimir in response to a SIGTERM or
// SIGINT signal.
func newShutdownSignalReceiver(delay time.Duration, ready *atomic.Bool, server *http.Server, manager *services.Manager) signals.SignalReceiver {
	return &shutdownSignalReceiver{
		delay:   delay,
		ready:   ready,
		server:  server,
		manager: manager,
	}
}

// shutdownSignalReceiver takes care of the process of cleanly shutting down Mimir
// in the event of a SIGTERM or SIGINT signal.
//
// * Flips an atomic boolean (which should be used elsewhere to force readiness checks to fail).
// * Disables HTTP keep-alives so that pooled connections to this instance are closed.
// * Optionally, wait for a configured delay before actually stopping any services.
// * Stops all running Mimir services via a services.Manager.
type shutdownSignalReceiver struct {
	delay   time.Duration
	ready   *atomic.Bool
	server  *http.Server
	manager *services.Manager
}

func (r *shutdownSignalReceiver) Stop() error {
	r.ready.Store(false)
	r.server.SetKeepAlivesEnabled(false)

	if r.delay > 0 {
		time.Sleep(r.delay)
	}

	r.manager.StopAsync()
	return nil
}
