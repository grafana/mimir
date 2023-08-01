// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"net/http"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/atomicfs"
	"github.com/grafana/mimir/pkg/util/shutdownmarker"
)

// PrepareShutdownHandler possibly changes the configuration of the store-gateway in such a way
// that when it is stopped, it gets unregistered from the ring.
//
// Moreover, it creates a file on disk which is used to re-apply the desired configuration if the
// store-gateway crashes and restarts before being permanently shutdown.
//
// The following methods are possible:
// * `GET` shows the status of this configuration
// * `POST` enables this configuration
// * `DELETE` disables this configuration
func (g *StoreGateway) PrepareShutdownHandler(w http.ResponseWriter, req *http.Request) {
	// Don't allow callers to change the shutdown configuration while we're in the middle
	// of starting or shutting down.
	if g.State() != services.Running {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	shutdownMarkerPath := shutdownmarker.GetPath(g.storageCfg.BucketStore.SyncDir)
	switch req.Method {
	case http.MethodGet:
		exists, err := atomicfs.Exists(shutdownMarkerPath)
		if err != nil {
			level.Error(g.logger).Log("msg", "unable to check for prepare-shutdown marker file", "path", shutdownMarkerPath, "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if exists {
			util.WriteTextResponse(w, "set\n")
		} else {
			util.WriteTextResponse(w, "unset\n")
		}
	case http.MethodPost:
		if err := shutdownmarker.Create(shutdownMarkerPath); err != nil {
			level.Error(g.logger).Log("msg", "unable to create prepare-shutdown marker file", "path", shutdownMarkerPath, "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		g.setPrepareShutdown()
		level.Info(g.logger).Log("msg", "created prepare-shutdown marker file", "path", shutdownMarkerPath)

		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		if err := shutdownmarker.Remove(shutdownMarkerPath); err != nil {
			level.Error(g.logger).Log("msg", "unable to remove prepare-shutdown marker file", "path", shutdownMarkerPath, "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		g.unsetPrepareShutdown()
		level.Info(g.logger).Log("msg", "removed prepare-shutdown marker file", "path", shutdownMarkerPath)

		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// setPrepareShutdown changes store-gateway lifecycler config to prepare for shutdown
func (g *StoreGateway) setPrepareShutdown() {
	g.ringLifecycler.SetKeepInstanceInTheRingOnShutdown(false)
	g.shutdownMarker.Set(1)
}

// unsetPrepareShutdown reverts to the shutdown settings to their default values
func (g *StoreGateway) unsetPrepareShutdown() {
	g.ringLifecycler.SetKeepInstanceInTheRingOnShutdown(!g.gatewayCfg.ShardingRing.UnregisterOnShutdown)
	g.shutdownMarker.Set(0)
}

// setPrepareShutdownFromShutdownMarker is executed on store-gateway start, and it calls setPrepareShutdown
// if shutdown marker is present. This is possible if the store-gateway crashes and restarts during a
// previous attempt to shut down.
func (g *StoreGateway) setPrepareShutdownFromShutdownMarker() error {
	shutdownMarkerPath := shutdownmarker.GetPath(g.storageCfg.BucketStore.SyncDir)
	shutdownMarkerFound, err := atomicfs.Exists(shutdownMarkerPath)
	if err != nil {
		return errors.Wrap(err, "failed to check store-gateway shutdown marker")
	}

	if shutdownMarkerFound {
		level.Info(g.logger).Log("msg", "detected existing shutdown marker, setting unregister on shutdown", "path", shutdownMarkerPath)
		g.setPrepareShutdown()
	}

	return nil
}

// unsetPrepareShutdownMarker is executed when the store-gateway successfully shuts down and removes the
// shutdown marker if it is present. It does not modify configuration in any way.
func (g *StoreGateway) unsetPrepareShutdownMarker() {
	shutdownMarkerPath := shutdownmarker.GetPath(g.storageCfg.BucketStore.SyncDir)
	if err := shutdownmarker.Remove(shutdownMarkerPath); err != nil {
		level.Warn(g.logger).Log("msg", "failed to remove shutdown marker", "path", shutdownMarkerPath, "err", err)
	}
}
