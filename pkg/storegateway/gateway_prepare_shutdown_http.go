// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	_ "embed" // Used to embed html template
	"net/http"
)

func (s *StoreGateway) PrepareShutdownHandler(w http.ResponseWriter, req *http.Request) {

}

/*
// setPrepareShutdown toggles store-gateway lifecycler config to prepare for shutdown
func (s *StoreGateway) setPrepareShutdown() {
	s.ringLifecycler.
		i.lifecycler.SetUnregisterOnShutdown(true)
	i.lifecycler.SetFlushOnShutdown(true)
	i.metrics.shutdownMarker.Set(1)
}

// unsetPrepareShutdown reverts to the shutdown settings to their default values
func (s *StoreGateway) unsetPrepareShutdown() {
	i.lifecycler.SetUnregisterOnShutdown(i.cfg.IngesterRing.UnregisterOnShutdown)
	i.lifecycler.SetFlushOnShutdown(i.cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown)
	i.metrics.shutdownMarker.Set(0)
}
*/
