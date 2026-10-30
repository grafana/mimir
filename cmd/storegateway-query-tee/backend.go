// SPDX-License-Identifier: AGPL-3.0-only

package main

import "github.com/grafana/mimir/tools/grpc-tee/backend"

const backendTypeStoreGateway = "store-gateway"

func NewStoreGatewayBackend() *backend.BackendType {
	return backend.NewBackendType(
		backendTypeStoreGateway,
		newStoreGatewayCodecTable(),
	)
}
