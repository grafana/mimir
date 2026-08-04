// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"net/http"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"

	"github.com/grafana/mimir/pkg/util"
)

// PrepareInstanceRingDownscaleHandler prepares this readcache's
// instance-ring entry for downscaling, mirroring the usage-tracker
// endpoint of the same name so the rollout-operator can drive
// readcache scale-down with its existing machinery.
//
// Marking the entry read-only is a signal, not a mode: the readcache
// keeps serving reads and consuming Kafka. What changes is that the
// entry is flagged as leaving, and the pod is set to unregister from
// the ring on shutdown so the rebalancer stops counting it as a live
// replica of its logical slot instead of waiting out the heartbeat
// timeout.
//
// Supported methods:
//
//   - GET
//     Returns the timestamp at which the ring entry was switched to
//     read-only mode, or 0 when it is not read-only.
//
//   - POST
//     Switches the ring entry to read-only (idempotent: repeated calls
//     do not move the timestamp) and disables keeping the instance in
//     the ring on shutdown.
//
//   - DELETE
//     Reverts the entry to read-write and restores the default of
//     keeping the instance in the ring on shutdown, so an aborted
//     scale-down leaves no trace.
func (r *Readcache) PrepareInstanceRingDownscaleHandler(w http.ResponseWriter, req *http.Request) {
	if r.instanceLifecycler == nil {
		http.Error(w, "readcache instance ring is not configured", http.StatusNotImplemented)
		return
	}

	// Don't allow callers to change the shutdown configuration while
	// we're in the middle of starting or shutting down.
	if r.State() != services.Running {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	switch req.Method {
	case http.MethodPost:
		if err := r.instanceLifecycler.ChangeReadOnlyState(req.Context(), true); err != nil {
			level.Error(r.logger).Log("msg", "failed to set readcache to read-only mode in the ring", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		r.instanceLifecycler.SetKeepInstanceInTheRingOnShutdown(false)

	case http.MethodDelete:
		if err := r.instanceLifecycler.ChangeReadOnlyState(req.Context(), false); err != nil {
			level.Error(r.logger).Log("msg", "failed to clear readcache read-only mode in the ring", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		r.instanceLifecycler.SetKeepInstanceInTheRingOnShutdown(true)
	}

	ro, rots := r.instanceLifecycler.GetReadOnlyState()
	if ro {
		util.WriteJSONResponse(w, map[string]any{"timestamp": rots.Unix()})
	} else {
		util.WriteJSONResponse(w, map[string]any{"timestamp": 0})
	}
}
