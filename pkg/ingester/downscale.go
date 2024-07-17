package ingester

import (
	"net/http"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"

	"github.com/grafana/mimir/pkg/util"
)

// PrepareInstanceRingDownscaleHandler prepares the ingester ring entry for downscaling. It can mark ingester as READONLY or back to ACTIVE.
//
// Following methods are supported:
//
//   - GET
//     Returns timestamp when instance was switched to READONLY state, or 0, if partition is not in READONLY state.
//
//   - POST
//     Switches the instance to READONLY state (if not yet), and returns the timestamp when the switch to
//     READONLY state happened.
//
//   - DELETE
//     Sets partition back from READONLY to ACTIVE state.
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

	case http.MethodDelete:
		// Clear the read-only status.
		err := i.lifecycler.ChangeReadOnlyState(r.Context(), false)
		if err != nil {
			level.Error(i.logger).Log("msg", "failed to clear ingester's read-only mode", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	ro, rots := i.lifecycler.GetReadOnlyState()
	if ro {
		util.WriteJSONResponse(w, map[string]any{"timestamp": rots.Unix()})
	} else {
		util.WriteJSONResponse(w, map[string]any{"timestamp": 0})
	}
}
