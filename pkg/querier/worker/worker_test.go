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
	"github.com/grafana/dskit/servicediscovery"
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
			setup: func(*Config) {},
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

			actualErr := cfg.Validate()
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
		numInUseTargets     int
		expectedConcurrency int
	}{
		{
			name:                "Create at least the minimum processors per target if max concurrent = 0, with all targets in use",
			maxConcurrent:       0,
			numTargets:          2,
			numInUseTargets:     2,
			expectedConcurrency: 8,
		},
		{
			name:                "Create at least the minimum processors per target if max concurrent = 0, with some targets in use",
			maxConcurrent:       0,
			numTargets:          2,
			numInUseTargets:     1,
			expectedConcurrency: 8,
		},
		{
			name:                "Max concurrent dividing with a remainder, with all targets in use",
			maxConcurrent:       19,
			numTargets:          4,
			numInUseTargets:     4,
			expectedConcurrency: 19,
		},
		{
			name:            "Max concurrent dividing with a remainder, with some targets in use",
			maxConcurrent:   9,
			numTargets:      4,
			numInUseTargets: 2,
			expectedConcurrency:/* in use:  */ 9 + /* not in use : */ 8,
		},
		{
			name:                "Max concurrent dividing evenly, with all targets in use",
			maxConcurrent:       12,
			numTargets:          2,
			numInUseTargets:     2,
			expectedConcurrency: 12,
		},
		{
			name:            "Max concurrent dividing evenly, with some targets in use",
			maxConcurrent:   12,
			numTargets:      4,
			numInUseTargets: 2,
			expectedConcurrency:/* in use:  */ 12 + /* not in use : */ 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				MaxConcurrentRequests: tt.maxConcurrent,
			}

			w, err := newQuerierWorkerWithProcessor(cfg.QuerySchedulerGRPCClientConfig, cfg.MaxConcurrentRequests, log.NewNopLogger(), &mockProcessor{}, nil, nil, "")
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), w))

			for i := 0; i < tt.numTargets; i++ {
				// gRPC connections are virtual... they don't actually try to connect until they are needed.
				// This allows us to use dummy ports, and not get any errors.
				w.InstanceAdded(servicediscovery.Instance{
					Address: fmt.Sprintf("127.0.0.1:%d", i),
					InUse:   i < tt.numInUseTargets,
				})
			}

			test.Poll(t, 250*time.Millisecond, tt.expectedConcurrency, func() interface{} {
				return getConcurrentProcessors(w)
			})

			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), w))
			assert.Equal(t, 0, getConcurrentProcessors(w))
		})
	}
}

func TestQuerierWorker_getDesiredConcurrency(t *testing.T) {
	tests := map[string]struct {
		instances     []servicediscovery.Instance
		maxConcurrent int
		expected      map[string]int
	}{
		"should return empty map on no instances": {
			instances:     nil,
			maxConcurrent: 2,
			expected:      map[string]int{},
		},
		"should divide the max concurrency between in-use instances, and create minimum connections for each instance not in-use": {
			instances: []servicediscovery.Instance{
				{Address: "1.1.1.1", InUse: true},
				{Address: "2.2.2.2", InUse: false},
				{Address: "3.3.3.3", InUse: true},
				{Address: "4.4.4.4", InUse: false},
			},
			maxConcurrent: 12,
			expected: map[string]int{
				"1.1.1.1": 6,
				"2.2.2.2": MinConcurrencyPerRequestQueue,
				"3.3.3.3": 6,
				"4.4.4.4": MinConcurrencyPerRequestQueue,
			},
		},
		"should create the minimum connections for each instance if max concurrency is set to 0": {
			instances: []servicediscovery.Instance{
				{Address: "1.1.1.1", InUse: true},
				{Address: "2.2.2.2", InUse: false},
				{Address: "3.3.3.3", InUse: true},
				{Address: "4.4.4.4", InUse: false},
			},
			maxConcurrent: 0,
			expected: map[string]int{
				"1.1.1.1": MinConcurrencyPerRequestQueue,
				"2.2.2.2": MinConcurrencyPerRequestQueue,
				"3.3.3.3": MinConcurrencyPerRequestQueue,
				"4.4.4.4": MinConcurrencyPerRequestQueue,
			},
		},
		"should create the minimum connections for each instance if max concurrency is less than (instances * minimum connections per instance)": {
			instances: []servicediscovery.Instance{
				{Address: "1.1.1.1", InUse: true},
				{Address: "2.2.2.2", InUse: false},
				{Address: "3.3.3.3", InUse: true},
				{Address: "4.4.4.4", InUse: false},
			},
			maxConcurrent: 2,
			expected: map[string]int{
				"1.1.1.1": MinConcurrencyPerRequestQueue,
				"2.2.2.2": MinConcurrencyPerRequestQueue,
				"3.3.3.3": MinConcurrencyPerRequestQueue,
				"4.4.4.4": MinConcurrencyPerRequestQueue,
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := Config{
				MaxConcurrentRequests: testData.maxConcurrent,
			}

			w, err := newQuerierWorkerWithProcessor(cfg.QueryFrontendGRPCClientConfig, cfg.MaxConcurrentRequests, log.NewNopLogger(), &mockProcessor{}, nil, nil, "")
			require.NoError(t, err)

			for _, instance := range testData.instances {
				w.instances[instance.Address] = instance
			}

			assert.Equal(t, testData.expected, w.getDesiredConcurrency())
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
