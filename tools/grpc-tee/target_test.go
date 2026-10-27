// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"errors"
	"testing"

	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

func TestTargetMetadataKeysMatchTheQuerier(t *testing.T) {
	require.Equal(t, querier.GRPCTeeTargetAddrMetadataKey, targetAddrMetadataKey)
	require.Equal(t, querier.GRPCTeeTargetIDMetadataKey, targetIDMetadataKey)
}

func TestTeeHandler_CallWithTargetGoesToTheTargetAndItsMirror(t *testing.T) {
	fixedPrimary := newFakeStoreGateway("fixed-primary", seriesResponses("up"))
	fixedSecondary := newFakeStoreGateway("fixed-secondary", seriesResponses("up"))
	target := newFakeStoreGateway("target", seriesResponses("up"))
	mirror := newFakeStoreGateway("mirror", seriesResponses("up"))
	targetAddr, mirrorAddr := startStoreGatewayServer(t, target), startStoreGatewayServer(t, mirror)

	client, calls := startTeeWithTargets(t, fixedPrimary, fixedSecondary, fakeInstanceLookup{"store-gateway-1": mirrorAddr})

	ctx := metadata.AppendToOutgoingContext(t.Context(),
		targetAddrMetadataKey, targetAddr,
		targetIDMetadataKey, "store-gateway-1",
		"x-scope-orgid", "user-1",
	)
	var header metadata.MD
	stream, err := client.Series(ctx, seriesRequest(), grpc.Header(&header))
	require.NoError(t, err)
	_, err = receiveAll(stream)
	require.NoError(t, err)
	require.Equal(t, []string{"target"}, header.Get("x-backend"))

	call := calls.waitForOne(t)
	require.Equal(t, "store-gateway-1", call.TargetID)
	require.Equal(t, "primary", call.Primary.Backend)
	require.Equal(t, targetAddr, call.Primary.Addr)
	require.NotNil(t, call.Secondary)
	require.Equal(t, "secondary", call.Secondary.Backend)
	require.Equal(t, mirrorAddr, call.Secondary.Addr)
	result, err := compareCall(storeGatewayComparator{}, call)
	require.NoError(t, err)
	require.Equal(t, ComparisonMatch, result)

	require.Equal(t, 1, target.seriesCalls())
	require.Equal(t, 1, mirror.seriesCalls())
	require.Equal(t, 0, fixedPrimary.seriesCalls())
	require.Equal(t, 0, fixedSecondary.seriesCalls())

	// The backends get the metadata of the client without the target keys.
	for _, g := range []*fakeStoreGateway{target, mirror} {
		md := g.metadata()
		require.Equal(t, []string{"user-1"}, md.Get("x-scope-orgid"))
		require.Empty(t, md.Get(targetAddrMetadataKey))
		require.Empty(t, md.Get(targetIDMetadataKey))
	}
}

func TestTeeHandler_CallWithTargetWithoutMirrorGoesToTheTargetOnly(t *testing.T) {
	fixedPrimary := newFakeStoreGateway("fixed-primary", seriesResponses("up"))
	fixedSecondary := newFakeStoreGateway("fixed-secondary", seriesResponses("up"))
	target := newFakeStoreGateway("target", seriesResponses("up"))
	targetAddr := startStoreGatewayServer(t, target)

	client, calls := startTeeWithTargets(t, fixedPrimary, fixedSecondary, fakeInstanceLookup{})

	ctx := metadata.AppendToOutgoingContext(t.Context(), targetAddrMetadataKey, targetAddr, targetIDMetadataKey, "store-gateway-2")
	stream, err := client.Series(ctx, seriesRequest())
	require.NoError(t, err)
	_, err = receiveAll(stream)
	require.NoError(t, err)

	call := calls.waitForOne(t)
	require.Equal(t, "store-gateway-2", call.TargetID)
	require.Equal(t, targetAddr, call.Primary.Addr)
	require.Nil(t, call.Secondary)
	require.Equal(t, 1, target.seriesCalls())
	require.Equal(t, 0, fixedPrimary.seriesCalls())
	require.Equal(t, 0, fixedSecondary.seriesCalls())
}

func TestTeeHandler_CallWithoutTargetGoesToTheFixedBackends(t *testing.T) {
	fixedPrimary := newFakeStoreGateway("fixed-primary", seriesResponses("up"))
	fixedSecondary := newFakeStoreGateway("fixed-secondary", seriesResponses("up"))

	client, calls := startTeeWithTargets(t, fixedPrimary, fixedSecondary, fakeInstanceLookup{})

	stream, err := client.Series(t.Context(), seriesRequest())
	require.NoError(t, err)
	_, err = receiveAll(stream)
	require.NoError(t, err)

	call := calls.waitForOne(t)
	require.Empty(t, call.TargetID)
	require.NotNil(t, call.Secondary)
	require.Equal(t, 1, fixedPrimary.seriesCalls())
	require.Equal(t, 1, fixedSecondary.seriesCalls())
}

// startTeeWithTargets starts a tee with fixed backends and target routing.
func startTeeWithTargets(t *testing.T, fixedPrimary, fixedSecondary *fakeStoreGateway, secondaryRing instanceLookup) (storegatewaypb.StoreGatewayClient, *callRecorder) {
	backendType, err := newBackendType(backendTypeStoreGateway)
	require.NoError(t, err)

	conns := newConnPool()
	t.Cleanup(func() { require.NoError(t, conns.Close()) })
	targets := &targetRouter{conns: conns, secondaryRing: secondaryRing}

	calls := &callRecorder{}
	addr := startTeeServerWithTargets(t, backendType,
		dialBackend(t, "primary", startStoreGatewayServer(t, fixedPrimary)),
		dialBackend(t, "secondary", startStoreGatewayServer(t, fixedSecondary)),
		targets, calls.record)
	return storegatewaypb.NewCustomStoreGatewayClient(dial(t, addr)), calls
}

// startStoreGatewayServer starts a gRPC server for the fake store-gateway, and returns its address.
func startStoreGatewayServer(t *testing.T, g *fakeStoreGateway) string {
	server := grpc.NewServer()
	storegatewaypb.RegisterStoreGatewayServer(server, g)
	return serve(t, server)
}

// fakeInstanceLookup maps instance IDs to addresses.
type fakeInstanceLookup map[string]string

func (l fakeInstanceLookup) GetInstance(instanceID string) (ring.InstanceDesc, error) {
	addr, ok := l[instanceID]
	if !ok {
		return ring.InstanceDesc{}, errors.New("instance not found")
	}
	return ring.InstanceDesc{Id: instanceID, Addr: addr}, nil
}
