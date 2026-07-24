// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"math"
	"net"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
	"github.com/grafana/mimir/pkg/nautilus/rebalancer"
)

type startupSyncRebalancer struct {
	rebalancer.UnimplementedNautilusRebalancerServer

	releaseNautilus  <-chan struct{}
	releaseReadcache <-chan struct{}
}

func (s *startupSyncRebalancer) WatchAssignments(_ *rebalancer.WatchAssignmentsRequest, stream rebalancer.NautilusRebalancer_WatchAssignmentsServer) error {
	select {
	case <-stream.Context().Done():
		return stream.Context().Err()
	case <-s.releaseNautilus:
	}

	now := time.Now()
	if err := stream.Send(&rebalancer.WatchAssignmentsResponse{
		Reset_: true,
		Entries: rebalancer.EntriesToProto([]assignment.LogEntry{{
			Range:       assignment.HashRange{Lo: 0, Hi: math.MaxUint32},
			PartitionID: 1,
			From:        now.Add(-time.Minute),
			To:          now.Add(time.Minute),
		}}),
	}); err != nil {
		return err
	}

	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *startupSyncRebalancer) WatchReadcacheAssignments(_ *rebalancer.WatchReadcacheAssignmentsRequest, stream rebalancer.NautilusRebalancer_WatchReadcacheAssignmentsServer) error {
	select {
	case <-stream.Context().Done():
		return stream.Context().Err()
	case <-s.releaseReadcache:
	}

	now := time.Now()
	if err := stream.Send(&rebalancer.WatchReadcacheAssignmentsResponse{
		Reset_: true,
		Entries: rebalancer.ReadcacheEntriesToProto([]readcacheassignment.LogEntry{{
			PartitionID: 1,
			InstanceID:  "readcache-1",
			From:        now.Add(-time.Minute),
			To:          now.Add(time.Minute),
		}}),
	}); err != nil {
		return err
	}

	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestDistributor_StartingWaitsForInitialAssignmentSync(t *testing.T) {
	tests := map[string]struct {
		releaseNautilusFirst bool
	}{
		"nautilus assignment log":  {releaseNautilusFirst: false},
		"readcache assignment log": {releaseNautilusFirst: true},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			releaseNautilus := make(chan struct{})
			releaseReadcache := make(chan struct{})
			if tc.releaseNautilusFirst {
				close(releaseNautilus)
			} else {
				close(releaseReadcache)
			}

			lis, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			rebalancer.RegisterNautilusRebalancerServer(server, &startupSyncRebalancer{
				releaseNautilus:  releaseNautilus,
				releaseReadcache: releaseReadcache,
			})
			go func() {
				_ = server.Serve(lis)
			}()
			t.Cleanup(server.Stop)

			idle := services.NewIdleService(nil, nil)
			subservices, err := services.NewManager(idle)
			require.NoError(t, err)
			watcher := services.NewFailureWatcher()
			watcher.WatchManager(subservices)

			d := &Distributor{
				cfg: Config{
					NautilusRebalancerAddress: lis.Addr().String(),
				},
				log:                         log.NewNopLogger(),
				now:                         time.Now,
				sleep:                       defaultSleep,
				subservices:                 subservices,
				subservicesWatcher:          watcher,
				nautilusAssignmentsReceived: prometheus.NewCounter(prometheus.CounterOpts{Name: "test_nautilus_assignments_received_total"}),
			}

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(func() {
				_ = d.stopping(nil)
			})
			t.Cleanup(cancel)

			started := make(chan error, 1)
			go func() {
				started <- d.starting(ctx)
			}()

			select {
			case err := <-started:
				require.NoError(t, err)
				require.Fail(t, "distributor became ready before both initial assignment logs were synchronized")
			case <-time.After(200 * time.Millisecond):
			}

			if tc.releaseNautilusFirst {
				close(releaseReadcache)
			} else {
				close(releaseNautilus)
			}

			select {
			case err := <-started:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				require.Fail(t, "distributor did not become ready after both initial assignment logs were synchronized")
			}
			require.NotNil(t, d.GetNautilusLog())
			require.NotNil(t, d.GetReadcacheLog())
		})
	}
}
