// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"github.com/grafana/mimir/tools/grpc-tee"
)

const backendTypeStoreGateway = "store-gateway"

func NewStoreGatewayBackend() *grpctee.BackendType {
	return grpctee.NewBackendType(
		backendTypeStoreGateway,
		newStoreGatewayCodecTable(),
	)
}
