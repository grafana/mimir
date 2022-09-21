// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/worker/worker_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package worker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/scheduler/schedulerdiscovery"
)

func TestConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		setup       func(cfg *Config)
		expectedErr string
	}{
		"should pass with default config": {
			setup: func(cfg *Config) {},
		},
		"should pass if frontend address is configured, but not scheduler address": {
			setup: func(cfg *Config) {
				cfg.FrontendAddress = "localhost:9095"
			},
		},
		"should pass if scheduler address is configured, but not frontend address": {
			setup: func(cfg *Config) {
				cfg.SchedulerAddress = "localhost:9095"
			},
		},
		"should fail if both scheduler and frontend address are configured": {
			setup: func(cfg *Config) {
				cfg.FrontendAddress = "localhost:9095"
				cfg.SchedulerAddress = "localhost:9095"
			},
			expectedErr: "frontend address and scheduler address are mutually exclusive",
		},
		"should pass if query-scheduler service discovery is set to ring, and no frontend and scheduler address is configured": {
			setup: func(cfg *Config) {
				cfg.QuerySchedulerDiscovery.Mode = schedulerdiscovery.ModeRing
			},
		},
		"should fail if query-scheduler service discovery is set to ring, and frontend address is configured": {
			setup: func(cfg *Config) {
				cfg.QuerySchedulerDiscovery.Mode = schedulerdiscovery.ModeRing
				cfg.FrontendAddress = "localhost:9095"
			},
			expectedErr: `frontend address and scheduler address cannot be specified when query-scheduler service discovery mode is set to 'ring'`,
		},
		"should fail if query-scheduler service discovery is set to ring, and scheduler address is configured": {
			setup: func(cfg *Config) {
				cfg.QuerySchedulerDiscovery.Mode = schedulerdiscovery.ModeRing
				cfg.SchedulerAddress = "localhost:9095"
			},
			expectedErr: `frontend address and scheduler address cannot be specified when query-scheduler service discovery mode is set to 'ring'`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := Config{}
			flagext.DefaultValues(&cfg)
			testData.setup(&cfg)

			actualErr := cfg.Validate(log.NewNopLogger())
			if testData.expectedErr == "" {
				require.NoError(t, actualErr)
			} else {
				require.Error(t, actualErr)
				assert.ErrorContains(t, actualErr, testData.expectedErr)
			}
		})
	}
}

func TestConfig_IsFrontendOrSchedulerConfigured(t *testing.T) {
	tests := []struct {
		cfg      Config
		expected bool
	}{
		{
			cfg:      Config{},
			expected: false,
		}, {
			cfg:      Config{FrontendAddress: "localhost:9095"},
			expected: true,
		}, {
			cfg:      Config{SchedulerAddress: "localhost:9095"},
			expected: true,
		}, {
			cfg:      Config{QuerySchedulerDiscovery: schedulerdiscovery.Config{Mode: schedulerdiscovery.ModeDNS}},
			expected: false,
		}, {
			cfg:      Config{QuerySchedulerDiscovery: schedulerdiscovery.Config{Mode: schedulerdiscovery.ModeRing}},
			expected: true,
		},
	}

	for idx, tc := range tests {
		t.Run(fmt.Sprintf("Test: %d", idx), func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.cfg.IsFrontendOrSchedulerConfigured())
		})
	}
}

func TestResetConcurrency(t *testing.T) {
	tests := []struct {
		name                string
		maxConcurrent       int
		numTargets          int
		expectedConcurrency int
	}{
		{
			name:                "Test create at least one processor per target if max concurrent = 0",
			maxConcurrent:       0,
			numTargets:          2,
			expectedConcurrency: 2,
		},
		{
			name:                "Test max concurrent dividing with a remainder",
			maxConcurrent:       7,
			numTargets:          4,
			expectedConcurrency: 7,
		},
		{
			name:                "Test max concurrent dividing evenly",
			maxConcurrent:       6,
			numTargets:          2,
			expectedConcurrency: 6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				MaxConcurrentRequests: tt.maxConcurrent,
			}

			w, err := newQuerierWorkerWithProcessor(cfg, log.NewNopLogger(), &mockProcessor{}, nil, nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), w))

			for i := 0; i < tt.numTargets; i++ {
				// gRPC connections are virtual... they don't actually try to connect until they are needed.
				// This allows us to use dummy ports, and not get any errors.
				w.AddressAdded(fmt.Sprintf("127.0.0.1:%d", i))
			}

			test.Poll(t, 250*time.Millisecond, tt.expectedConcurrency, func() interface{} {
				return getConcurrentProcessors(w)
			})

			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), w))
			assert.Equal(t, 0, getConcurrentProcessors(w))
		})
	}
}

func getConcurrentProcessors(w *querierWorker) int {
	result := 0
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, mgr := range w.managers {
		result += int(mgr.currentProcessors.Load())
	}

	return result
}

type mockProcessor struct{}

func (m mockProcessor) processQueriesOnSingleStream(ctx context.Context, _ *grpc.ClientConn, _ string) {
	<-ctx.Done()
}

func (m mockProcessor) notifyShutdown(_ context.Context, _ *grpc.ClientConn, _ string) {}
