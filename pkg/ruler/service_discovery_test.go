// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/dns"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestConfig_TranslatesToPrometheusTargetGroup(t *testing.T) {
	const sourceAddress = "doesnt-matter.com"

	testCases := []struct {
		name              string
		resolvedAddresses []string

		expectedTargetGroups []*targetgroup.Group
	}{
		{
			name:              "happy flow single address",
			resolvedAddresses: []string{"127.0.0.1"},
			expectedTargetGroups: []*targetgroup.Group{
				{
					Targets: []model.LabelSet{
						{model.AddressLabel: "127.0.0.1"},
					},
					Source: sourceAddress,
				},
			},
		},
		{
			name:              "happy flow multiple addresses",
			resolvedAddresses: []string{"127.0.0.1", "127.0.0.2"},
			expectedTargetGroups: []*targetgroup.Group{
				{
					Targets: []model.LabelSet{
						{model.AddressLabel: "127.0.0.1"},
						{model.AddressLabel: "127.0.0.2"},
					},
					Source: sourceAddress,
				},
			},
		},
		{
			name:              "happy flow no addresses",
			resolvedAddresses: []string{},
			expectedTargetGroups: []*targetgroup.Group{
				{
					Targets: []model.LabelSet{},
					Source:  sourceAddress,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &mockResolver{}
			resolver.expectAnyResolveCall()
			resolver.returnAddresses(tc.resolvedAddresses)
			reg := prometheus.NewRegistry()
			refreshMetrics := discovery.NewRefreshMetrics(reg)
			cfg := dnsServiceDiscovery{
				RefreshInterval: time.Millisecond,
				Resolver:        resolver,
				QType:           dns.A,
				Host:            sourceAddress,
				refreshMetrics:  refreshMetrics,
			}
			discoverer, err := cfg.NewDiscoverer(discovery.DiscovererOptions{})
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			groupsChan := make(chan []*targetgroup.Group)
			go discoverer.Run(ctx, groupsChan)
			groups := <-groupsChan

			assert.ElementsMatch(t, tc.expectedTargetGroups, groups)
		})
	}
}

func TestConfig_ConstructsLookupNamesCorrectly(t *testing.T) {
	testCases := []struct {
		name  string
		qType dns.QType
		host  string

		expectedAddress string
	}{
		{
			name:            "dns+",
			qType:           dns.A,
			host:            "localhost:123",
			expectedAddress: "dns+localhost:123",
		},
		{
			name:            "dnssrv+",
			qType:           dns.SRV,
			host:            "localhost:123",
			expectedAddress: "dnssrv+localhost:123",
		},
		{
			name:            "dnssrvnoa+",
			qType:           dns.SRVNoA,
			host:            "localhost:123",
			expectedAddress: "dnssrvnoa+localhost:123",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &mockResolver{}
			resolver.expectResolveCalledWith(tc.expectedAddress)
			resolver.returnAddresses(nil)

			reg := prometheus.NewRegistry()
			refreshMetrics := discovery.NewRefreshMetrics(reg)
			cfg := dnsServiceDiscovery{
				RefreshInterval: time.Millisecond,
				Resolver:        resolver,
				QType:           tc.qType,
				Host:            tc.host,
				refreshMetrics:  refreshMetrics,
			}
			discoverer, err := cfg.NewDiscoverer(discovery.DiscovererOptions{})
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)

			groupsChan := make(chan []*targetgroup.Group)
			go discoverer.Run(ctx, groupsChan)
			<-groupsChan // wait for at least one iteration
		})
	}
}

type mockResolver struct {
	mock.Mock
}

func (f *mockResolver) Resolve(ctx context.Context, toResolve []string) error {
	ret := f.Called(ctx, toResolve)
	return ret.Error(0)
}

func (f *mockResolver) Addresses() []string {
	return f.Called().Get(0).([]string)
}

func (f *mockResolver) expectResolveCalledWith(toResolve ...string) {
	f.On("Resolve", mock.Anything, toResolve).Return(nil)
}

func (f *mockResolver) expectAnyResolveCall() {
	f.On("Resolve", mock.Anything, mock.Anything).Return(nil)
}

func (f *mockResolver) returnAddresses(resolved []string) {
	f.On("Addresses").Return(resolved)
}
