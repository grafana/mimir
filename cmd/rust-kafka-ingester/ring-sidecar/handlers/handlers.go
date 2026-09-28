// SPDX-License-Identifier: AGPL-3.0-only

// Package handlers serves the Go ingester's lifecycle endpoints for the Rust ingester, which the
// rollout operator calls on every ingester of a zone.
package handlers

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"

	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/shutdownmarker"
)

// PartitionLifecycler is the part of ring.PartitionInstanceLifecycler the handlers use.
type PartitionLifecycler interface {
	GetPartitionState(ctx context.Context) (ring.PartitionState, time.Time, error)
	ChangePartitionState(ctx context.Context, toState ring.PartitionState) error
}

// PrepareShutdown is the Go ingester's PrepareShutdownHandler with ingest storage: POST persists a
// marker in markerDir, when set, so that the next shutdown leaves the rings and later starts don't
// create the partition; the preparation can't be reverted.
func PrepareShutdown(w http.ResponseWriter, req *http.Request, markerDir string, prepared *atomic.Bool, logger log.Logger) {
	markerPath := shutdownmarker.GetPath(markerDir)
	switch req.Method {
	case http.MethodGet:
		if prepared.Load() {
			util.WriteTextResponse(w, "set\n")
		} else {
			util.WriteTextResponse(w, "unset\n")
		}
	case http.MethodPost:
		if markerDir != "" {
			if err := shutdownmarker.Create(markerPath); err != nil {
				_ = level.Error(logger).Log("msg", "unable to create prepare-shutdown marker file", "path", markerPath, "err", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		prepared.Store(true)
		_ = level.Info(logger).Log("msg", "created prepare-shutdown marker file", "path", markerPath)
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		_ = level.Error(logger).Log("msg", "the ingest storage doesn't support reverting the prepared shutdown")
		w.WriteHeader(http.StatusMethodNotAllowed)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// PreparePartitionDownscale is the Go ingester's PreparePartitionDownscaleHandler: POST switches
// the partition to INACTIVE, DELETE back to ACTIVE, and every method returns when it became
// INACTIVE. A nil partition is one not registered yet. Without changesAllowed, the partition's
// states belong to someone else and changing them is a conflict.
func PreparePartitionDownscale(w http.ResponseWriter, req *http.Request, partition PartitionLifecycler, changesAllowed bool, logger log.Logger) {
	if partition == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	change := func(to ring.PartitionState) bool {
		if !changesAllowed {
			http.Error(w, "partition states in the shared ring belong to the Go ingesters", http.StatusConflict)
			return false
		}
		if err := partition.ChangePartitionState(req.Context(), to); err != nil {
			_ = level.Error(logger).Log("msg", "failed to change partition state", "to", to, "err", err)
			if errors.Is(err, ring.ErrPartitionStateChangeLocked) {
				http.Error(w, err.Error(), http.StatusConflict)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return false
		}
		return true
	}
	switch req.Method {
	case http.MethodPost:
		state, _, err := partition.GetPartitionState(req.Context())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if state == ring.PartitionPending {
			w.WriteHeader(http.StatusConflict)
			return
		}
		if !change(ring.PartitionInactive) {
			return
		}
	case http.MethodDelete:
		state, _, err := partition.GetPartitionState(req.Context())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if state == ring.PartitionInactive && !change(ring.PartitionActive) {
			return
		}
	}
	state, stateTimestamp, err := partition.GetPartitionState(req.Context())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if state == ring.PartitionInactive {
		util.WriteJSONResponse(w, map[string]any{"timestamp": stateTimestamp.Unix()})
	} else {
		util.WriteJSONResponse(w, map[string]any{"timestamp": 0})
	}
}
