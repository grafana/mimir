// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/transfer.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"

	"github.com/grafana/mimir/pkg/ingester/client"
)

// TODO: (remove-chunks) refactor method out of the code-base. Consider using ring.NoopFlushTransferer.
// TransferOut finds an ingester in PENDING state and transfers our chunks to it.
// Called as part of the ingester shutdown process.
func (i *Ingester) TransferOut(ctx context.Context) error {
	// The blocks storage doesn't support blocks transferring.
	level.Info(i.logger).Log("msg", "transfer between a LEAVING ingester and a PENDING one is not supported for the blocks storage")
	return ring.ErrTransferDisabled
}

// TODO: (remove-chunks) refactor method out of the code-base. This is expected by the ingester client.
func (i *Ingester) TransferChunks(stream client.Ingester_TransferChunksServer) error {
	return ring.ErrTransferDisabled
}
