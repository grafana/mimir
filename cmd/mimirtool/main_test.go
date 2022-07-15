// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"
)

func TestUploadBlock(t *testing.T) {
	serverCfg := getServerConfig(t)
	srv, err := server.New(serverCfg)
	require.NoError(t, err)
	go func() { _ = srv.Run() }()
	t.Cleanup(srv.Stop)
}

func getServerConfig(t *testing.T) server.Config {
	t.Helper()

	grpcHost, grpcPortNum := getHostnameAndRandomPort(t)
	httpHost, httpPortNum := getHostnameAndRandomPort(t)
	return server.Config{
		HTTPListenAddress:        httpHost,
		HTTPListenPort:           httpPortNum,
		GRPCListenAddress:        grpcHost,
		GRPCListenPort:           grpcPortNum,
		GPRCServerMaxRecvMsgSize: 1024,
	}
}

func getHostnameAndRandomPort(t *testing.T) (string, int) {
	t.Helper()

	listen, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	host, port, err := net.SplitHostPort(listen.Addr().String())
	require.NoError(t, err)
	require.NoError(t, listen.Close())

	portNum, err := strconv.Atoi(port)
	require.NoError(t, err)
	return host, portNum
}
