// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/client_pool_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/grafana/dskit/clusterutil"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func Test_newRulerClientFactory(t *testing.T) {
	// Create a GRPC server used to query the mocked service.
	grpcServer := grpc.NewServer()
	defer grpcServer.GracefulStop()

	srv := &mockRulerServer{}
	RegisterRulerServer(grpcServer, srv)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		require.NoError(t, grpcServer.Serve(listener))
	}()

	// Create a client factory and query back the mocked service
	// with different clients.
	cfg := grpcclient.Config{}
	flagext.DefaultValues(&cfg)

	reg := prometheus.NewPedanticRegistry()
	factory := newRulerClientFactory(cfg, reg, log.NewNopLogger())

	for i := 0; i < 2; i++ {
		inst := ring.InstanceDesc{Addr: listener.Addr().String()}
		client, err := factory.FromInstance(inst)
		require.NoError(t, err)
		defer client.Close() //nolint:errcheck

		ctx := user.InjectOrgID(context.Background(), "test")
		_, err = client.(*rulerExtendedClient).Rules(ctx, &RulesRequest{})
		assert.NoError(t, err)
	}

	// Assert on the request duration metric, but since it's a duration histogram and
	// we can't predict the exact time it took, we need to workaround it.
	metrics, err := reg.Gather()
	require.NoError(t, err)

	assert.Len(t, metrics, 1)
	assert.Equal(t, "cortex_ruler_client_request_duration_seconds", metrics[0].GetName())
	assert.Equal(t, dto.MetricType_HISTOGRAM, metrics[0].GetType())
	assert.Len(t, metrics[0].GetMetric(), 1)
	assert.Equal(t, uint64(2), metrics[0].GetMetric()[0].GetHistogram().GetSampleCount())
}

func TestClientPool_ClusterValidation(t *testing.T) {
	testCases := map[string]struct {
		serverClusterValidation clusterutil.ServerClusterValidationConfig
		clientClusterValidation clusterutil.ClusterValidationConfig
		expectedError           *status.Status
		expectedMetrics         string
	}{
		"if server and client have equal cluster validation labels and cluster validation is disabled no error is returned": {
			serverClusterValidation: clusterutil.ServerClusterValidationConfig{
				ClusterValidationConfig: clusterutil.ClusterValidationConfig{Label: "cluster"},
				GRPC:                    clusterutil.ClusterValidationProtocolConfig{Enabled: false},
			},
			clientClusterValidation: clusterutil.ClusterValidationConfig{Label: "cluster"},
			expectedError:           nil,
		},
		"if server and client have different cluster validation labels and cluster validation is disabled no error is returned": {
			serverClusterValidation: clusterutil.ServerClusterValidationConfig{
				ClusterValidationConfig: clusterutil.ClusterValidationConfig{Label: "server-cluster"},
				GRPC:                    clusterutil.ClusterValidationProtocolConfig{Enabled: false},
			},
			clientClusterValidation: clusterutil.ClusterValidationConfig{Label: "client-cluster"},
			expectedError:           nil,
		},
		"if server and client have equal cluster validation labels and cluster validation is enabled no error is returned": {
			serverClusterValidation: clusterutil.ServerClusterValidationConfig{
				ClusterValidationConfig: clusterutil.ClusterValidationConfig{Label: "cluster"},
				GRPC:                    clusterutil.ClusterValidationProtocolConfig{Enabled: true},
			},
			clientClusterValidation: clusterutil.ClusterValidationConfig{Label: "cluster"},
			expectedError:           nil,
		},
		"if server and client have different cluster validation labels and soft cluster validation is enabled no error is returned": {
			serverClusterValidation: clusterutil.ServerClusterValidationConfig{
				ClusterValidationConfig: clusterutil.ClusterValidationConfig{Label: "server-cluster"},
				GRPC: clusterutil.ClusterValidationProtocolConfig{
					Enabled:        true,
					SoftValidation: true,
				},
			},
			clientClusterValidation: clusterutil.ClusterValidationConfig{Label: "client-cluster"},
			expectedError:           nil,
		},
		"if server and client have different cluster validation labels and cluster validation is enabled an error is returned": {
			serverClusterValidation: clusterutil.ServerClusterValidationConfig{
				ClusterValidationConfig: clusterutil.ClusterValidationConfig{Label: "server-cluster"},
				GRPC: clusterutil.ClusterValidationProtocolConfig{
					Enabled:        true,
					SoftValidation: false,
				},
			},
			clientClusterValidation: clusterutil.ClusterValidationConfig{Label: "client-cluster"},
			expectedError:           grpcutil.Status(codes.Internal, `request rejected by the server: rejected request with wrong cluster validation label "client-cluster" - it should be one of [server-cluster]`),
			expectedMetrics: `
				# HELP cortex_client_invalid_cluster_validation_label_requests_total Number of requests with invalid cluster validation label.
        	    # TYPE cortex_client_invalid_cluster_validation_label_requests_total counter
        	    cortex_client_invalid_cluster_validation_label_requests_total{client="ruler",method="/ruler.Ruler/Rules",protocol="grpc"} 1
			`,
		},
		"if client has no cluster validation label and soft cluster validation is enabled no error is returned": {
			serverClusterValidation: clusterutil.ServerClusterValidationConfig{
				ClusterValidationConfig: clusterutil.ClusterValidationConfig{Label: "server-cluster"},
				GRPC: clusterutil.ClusterValidationProtocolConfig{
					Enabled:        true,
					SoftValidation: true,
				},
			},
			clientClusterValidation: clusterutil.ClusterValidationConfig{},
			expectedError:           nil,
		},
		"if client has no cluster validation label and cluster validation is enabled an error is returned": {
			serverClusterValidation: clusterutil.ServerClusterValidationConfig{
				ClusterValidationConfig: clusterutil.ClusterValidationConfig{Label: "server-cluster"},
				GRPC: clusterutil.ClusterValidationProtocolConfig{
					Enabled:        true,
					SoftValidation: false,
				},
			},
			clientClusterValidation: clusterutil.ClusterValidationConfig{},
			expectedError:           grpcutil.Status(codes.FailedPrecondition, `rejected request with empty cluster validation label - it should be one of [server-cluster]`),
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			var grpcOptions []grpc.ServerOption
			if testCase.serverClusterValidation.GRPC.Enabled {
				reg := prometheus.NewPedanticRegistry()
				grpcOptions = []grpc.ServerOption{
					grpc.ChainUnaryInterceptor(middleware.ClusterUnaryServerInterceptor(
						[]string{testCase.serverClusterValidation.Label}, testCase.serverClusterValidation.GRPC.SoftValidation,
						middleware.NewInvalidClusterRequests(reg, "cortex"), log.NewNopLogger(),
					)),
				}
			}

			grpcServer := grpc.NewServer(grpcOptions...)
			defer grpcServer.GracefulStop()

			srv := &mockRulerServer{}
			RegisterRulerServer(grpcServer, srv)

			listener, err := net.Listen("tcp", "localhost:0")
			require.NoError(t, err)

			go func() {
				require.NoError(t, grpcServer.Serve(listener))
			}()

			// Create a client factory and query back the mocked service
			// with different clients.
			cfg := grpcclient.Config{}
			flagext.DefaultValues(&cfg)
			cfg.ClusterValidation = testCase.clientClusterValidation

			reg := prometheus.NewPedanticRegistry()
			factory := newRulerClientFactory(cfg, reg, log.NewNopLogger())
			inst := ring.InstanceDesc{Addr: listener.Addr().String()}
			client, err := factory.FromInstance(inst)
			require.NoError(t, err)
			defer client.Close() //nolint:errcheck

			ctx := user.InjectOrgID(context.Background(), "test")
			_, err = client.(*rulerExtendedClient).Rules(ctx, &RulesRequest{})
			if testCase.expectedError == nil {
				require.NoError(t, err)
			} else {
				stat, ok := grpcutil.ErrorToStatus(err)
				require.True(t, ok)
				require.Equal(t, testCase.expectedError.Code(), stat.Code())
				require.Equal(t, testCase.expectedError.Message(), stat.Message())
			}
			// Check tracked Prometheus metrics
			err = testutil.GatherAndCompare(reg, strings.NewReader(testCase.expectedMetrics), "cortex_client_invalid_cluster_validation_label_requests_total")
			assert.NoError(t, err)
		})
	}
}

type mockRulerServer struct{}

func (m *mockRulerServer) Rules(context.Context, *RulesRequest) (*RulesResponse, error) {
	return &RulesResponse{}, nil
}

func (m *mockRulerServer) SyncRules(context.Context, *SyncRulesRequest) (*SyncRulesResponse, error) {
	return &SyncRulesResponse{}, nil
}
