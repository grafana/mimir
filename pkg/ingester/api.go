// SPDX-License-Identifier: AGPL-3.0-only
package ingester

import (
	"context"
	"net/http"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

type API interface {
	client.IngesterServer
	FlushHandler(http.ResponseWriter, *http.Request)
	ShutdownHandler(http.ResponseWriter, *http.Request)
	PrepareShutdownHandler(http.ResponseWriter, *http.Request)
	PreparePartitionDownscaleHandler(http.ResponseWriter, *http.Request)
	PrepareUnregisterHandler(http.ResponseWriter, *http.Request)
	UserRegistryHandler(http.ResponseWriter, *http.Request)
	TenantsHandler(http.ResponseWriter, *http.Request)
	TenantTSDBHandler(http.ResponseWriter, *http.Request)
	PrepareInstanceRingDownscaleHandler(http.ResponseWriter, *http.Request)
	PushToStorageAndReleaseRequest(context.Context, *mimirpb.WriteRequest) error
	NotifyPreCommit(ctx context.Context) error
}
