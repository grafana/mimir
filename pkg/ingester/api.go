// SPDX-License-Identifier: AGPL-3.0-only
package ingester

import (
	"net/http"

	"github.com/grafana/mimir/pkg/ingester/client"
)

type IngesterAPI interface {
	client.IngesterServer
	FlushHandler(http.ResponseWriter, *http.Request)
	ShutdownHandler(http.ResponseWriter, *http.Request)
	PrepareShutdownHandler(http.ResponseWriter, *http.Request)
	PreparePartitionDownscaleHandler(http.ResponseWriter, *http.Request)
	PrepareUnregisterHandler(w http.ResponseWriter, r *http.Request)
	UserRegistryHandler(http.ResponseWriter, *http.Request)
	TenantsHandler(http.ResponseWriter, *http.Request)
	TenantTSDBHandler(http.ResponseWriter, *http.Request)
	PrepareInstanceRingDownscaleHandler(http.ResponseWriter, *http.Request)
}
