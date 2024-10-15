// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/require"
)

func TestAwaitQueryFrontendServiceRunning_ServiceIsReady(t *testing.T) {
	run := func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}

	service := services.NewBasicService(nil, run, nil)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), service))
	defer func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), service)) }()

	err := awaitQueryFrontendServiceRunning(context.Background(), service, time.Second, log.NewNopLogger())
	require.NoError(t, err)
}

func TestAwaitQueryFrontendServiceRunning_ServiceIsNotReadyWaitDisabled(t *testing.T) {
	startChan := make(chan struct{})
	start := func(context.Context) error {
		<-startChan
		return nil
	}

	service := services.NewBasicService(start, nil, nil)
	require.NoError(t, service.StartAsync(context.Background()))
	defer func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), service)) }()

	err := awaitQueryFrontendServiceRunning(context.Background(), service, 0, log.NewNopLogger())
	require.EqualError(t, err, "frontend not running: Starting")

	close(startChan)
}

func TestAwaitQueryFrontendServiceRunning_ServiceIsNotReadyInitially(t *testing.T) {
	startChan := make(chan struct{})
	start := func(context.Context) error {
		<-startChan
		return nil
	}
	run := func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}

	service := services.NewBasicService(start, run, nil)
	require.NoError(t, service.StartAsync(context.Background()))
	defer func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), service)) }()

	go func() {
		time.Sleep(500 * time.Millisecond)
		close(startChan)
	}()

	err := awaitQueryFrontendServiceRunning(context.Background(), service, time.Second, log.NewNopLogger())
	require.NoError(t, err)
}

func TestAwaitQueryFrontendServiceRunning_ServiceIsNotReadyAfterTimeout(t *testing.T) {
	serviceChan := make(chan struct{})
	start := func(context.Context) error {
		<-serviceChan
		return nil
	}

	service := services.NewBasicService(start, nil, nil)
	require.NoError(t, service.StartAsync(context.Background()))
	defer func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), service)) }()

	err := awaitQueryFrontendServiceRunning(context.Background(), service, 100*time.Millisecond, log.NewNopLogger())
	require.EqualError(t, err, "frontend not running (is Starting), timed out waiting for it to be running after 100ms")

	close(serviceChan)
}
