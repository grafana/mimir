// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/grpc/grpc_client.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package grpc

import (
	"flag"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Config for a StorageClient
type Config struct {
	Address string `yaml:"server_address,omitempty"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Address, "grpc-store.server-address", "", "Hostname or IP of the gRPC store instance.")
}

func connectToGrpcServer(serverAddress string) (GrpcStoreClient, *grpc.ClientConn, error) {
	params := keepalive.ClientParameters{
		Time:                time.Second * 20,
		Timeout:             time.Second * 10,
		PermitWithoutStream: true,
	}
	param := grpc.WithKeepaliveParams(params)
	cc, err := grpc.Dial(serverAddress, param, grpc.WithInsecure())
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to dial grpc-store %s", serverAddress)
	}
	return NewGrpcStoreClient(cc), cc, nil
}
