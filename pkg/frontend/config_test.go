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
			expectedError:         "remote execution is only supported when the Mimir query engine is in use",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			config := &CombinedFrontendConfig{}
			flagext.DefaultValues(config)
			config.QueryMiddleware.EnableRemoteExecution = testCase.enableRemoteExecution
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
