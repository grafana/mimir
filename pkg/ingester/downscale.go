// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"net/http"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"

	"github.com/grafana/mimir/pkg/util"
)

// PrepareInstanceRingDownscaleHandler prepares the ingester ring entry for downscaling. It can mark ingester as read-only
// or set it back to read-write mode.
//
// Following methods are supported:
//
//   - GET
//     Returns timestamp when ingester ring entry was switched to read-only mode, or 0, if ring entry is not in read-only mode.
//
//   - POST
//     Switches the ingester ring entry to read-only mode (if it isn't yet), and returns the timestamp when the switch to
//     read-only mode happened.
//
//   - DELETE
//     Sets ingester ring entry back to read-write mode.
func (i *Ingester) PrepareInstanceRingDownscaleHandler(w http.ResponseWriter, r *http.Request) {
	// Don't allow callers to change the shutdown configuration while we're in the middle
	// of starting or shutting down.
	if i.State() != services.Running {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	// If ingest storage is used, don't allow manipulations with instance ring entry.
	if i.cfg.IngestStorageConfig.Enabled {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	switch r.Method {
	case http.MethodPost:
		// Calling this repeatedly doesn't update the read-only timestamp, if instance is already in read-only mode.
		err := i.lifecycler.ChangeReadOnlyState(r.Context(), true)
		if err != nil {
			level.Error(i.logger).Log("msg", "failed to set ingester to read-only mode in the ring", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Deactivate the push circuit breaker in read-only mode. Calling this repeatedly is fine.
		i.circuitBreaker.push.disable()

	case http.MethodDelete:
		// Clear the read-only status.
		err := i.lifecycler.ChangeReadOnlyState(r.Context(), false)
		if err != nil {
			level.Error(i.logger).Log("msg", "failed to clear ingester's read-only mode", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Activate the push circuit breaker when exiting read-only mode. Calling this repeatedly is fine.
		i.circuitBreaker.push.enable()
	}

	ro, rots := i.lifecycler.GetReadOnlyState()
	if ro {
		util.WriteJSONResponse(w, map[string]any{"timestamp": rots.Unix()})
	} else {
		util.WriteJSONResponse(w, map[string]any{"timestamp": 0})
	}
}
