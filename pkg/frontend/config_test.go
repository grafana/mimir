// SPDX-License-Identifier: AGPL-3.0-only

package frontend

import (
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier"
)

func TestConfig_Validate(t *testing.T) {
	testCases := map[string]struct {
		enableRemoteExecution bool
		queryEngine           string
		expectedError         string
	}{
		"remote execution disabled, MQE enabled": {
			enableRemoteExecution: false,
			queryEngine:           querier.MimirEngine,
			expectedError:         "",
		},
		"remote execution disabled, MQE disabled": {
			enableRemoteExecution: false,
			queryEngine:           querier.PrometheusEngine,
			expectedError:         "",
		},
		"remote execution enabled, MQE enabled": {
			enableRemoteExecution: true,
			queryEngine:           querier.MimirEngine,
			expectedError:         "",
		},
		"remote execution enabled, MQE disabled": {
			enableRemoteExecution: true,
			queryEngine:           querier.PrometheusEngine,
			expectedError:         "remote execution is only supported when the Mimir query engine is in use (-query-frontend.query-engine=mimir)",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			config := &CombinedFrontendConfig{}
			flagext.DefaultValues(config)
			config.QueryMiddleware.EnableRemoteExecution = testCase.enableRemoteExecution
			config.QueryMiddleware.ShardedQueries = false
			config.QueryMiddleware.SplitQueriesByInterval = 0
			config.QueryMiddleware.UseMQEForSplittingAndCachingResults = testCase.enableRemoteExecution
			config.QueryEngine = testCase.queryEngine
			err := config.Validate()

			if testCase.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, testCase.expectedError)
			}
		})
	}
}

func TestConfig_Validate_QueueReservation(t *testing.T) {
	testCases := map[string]struct {
		enableRemoteExecution bool
		useMQEForSplitting    bool
		shardedQueries        bool
		useMQEForSharding     bool
		expectedError         string
	}{
		"remote execution, MQE splitting and MQE sharding enabled": {
			enableRemoteExecution: true,
			useMQEForSplitting:    true,
			shardedQueries:        true,
			useMQEForSharding:     true,
		},
		"sharding disabled": {
			enableRemoteExecution: true,
			useMQEForSplitting:    true,
		},
		"remote execution disabled": {
			expectedError: "queue reservation is only supported when remote execution is enabled (-query-frontend.enable-remote-execution=true)",
		},
		"MQE splitting disabled": {
			enableRemoteExecution: true,
			shardedQueries:        true,
			useMQEForSharding:     true,
			expectedError:         "queue reservation is only supported when splitting and caching results inside MQE is enabled (-query-frontend.use-mimir-query-engine-for-splitting-and-caching-results=true)",
		},
		"MQE sharding disabled with sharding enabled": {
			enableRemoteExecution: true,
			useMQEForSplitting:    true,
			shardedQueries:        true,
			expectedError:         "using MQE for splitting and caching results with sharding enabled (-query-frontend.parallelize-shardable-queries=true) is only supported if running sharding inside MQE is enabled (-query-frontend.use-mimir-query-engine-for-sharding=true)",
		},
		"MQE splitting and MQE sharding disabled with sharding enabled": {
			enableRemoteExecution: true,
			shardedQueries:        true,
			expectedError:         "queue reservation is only supported when splitting and caching results inside MQE is enabled (-query-frontend.use-mimir-query-engine-for-splitting-and-caching-results=true)",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			config := &CombinedFrontendConfig{}
			flagext.DefaultValues(config)
			config.FrontendV2.QueueReservationEnabled = true
			config.QueryEngine = querier.MimirEngine
			config.QueryMiddleware.EnableRemoteExecution = testCase.enableRemoteExecution
			config.QueryMiddleware.UseMQEForSplittingAndCachingResults = testCase.useMQEForSplitting
			config.QueryMiddleware.ShardedQueries = testCase.shardedQueries
			config.QueryMiddleware.UseMQEForSharding = testCase.useMQEForSharding
			err := config.Validate()

			if testCase.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, testCase.expectedError)
			}
		})
	}
}
