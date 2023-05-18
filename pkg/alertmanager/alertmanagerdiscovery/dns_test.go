// SPDX-License-Identifier: AGPL-3.0-only

package alertmanagerdiscovery

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/dns"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewDiscoveryConfigs(t *testing.T) {
	t.Run("should build discovery config from valid url", func(t *testing.T) {
		discoveryConfigs, err := NewDiscoveryConfigs("http://0.0.0.0:1000/alertmanager", 0, nil)
		assert.NoError(t, err)
		actualLabels := discoveryConfigs["http://0.0.0.0:1000/alertmanager"].(discovery.StaticConfig)[0].Targets[0]
		assert.Equal(t, model.LabelSet{"__address__": "0.0.0.0:1000"}, actualLabels)
	})

	t.Run("should error on invalid input", func(t *testing.T) {
		tests := []struct {
			name  string
			amURL string
			err   error
		}{
			{
				name:  "with DNS service discovery and missing scheme",
				amURL: "dns+alertmanager.mimir.svc.cluster.local:8080/alertmanager",
				err:   errors.New("improperly formatted alertmanager URL \"alertmanager.mimir.svc.cluster.local:8080/alertmanager\" (maybe the scheme is missing?); see DNS Service Discovery docs"),
			},
			{
				name:  "with only dns+ prefix",
				amURL: "dns+",
				err:   errors.New("improperly formatted alertmanager URL \"\" (maybe the scheme is missing?); see DNS Service Discovery docs"),
			},
			{
				name:  "misspelled DNS SD format prefix (dnsserv+ vs dnssrv+)",
				amURL: "dnsserv+https://_http._tcp.alertmanager2.mimir.svc.cluster.local/am",
				err:   errors.New("invalid DNS service discovery prefix \"dnsserv\""),
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := NewDiscoveryConfigs(tt.amURL, 0, nil)
				require.EqualError(t, err, tt.err.Error())
			})
		}
	})
}

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

			cfg := DNSDiscoveryConfig{
				RefreshInterval: time.Millisecond,
				Resolver:        resolver,
				QType:           dns.A,
				Host:            sourceAddress,
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

			cfg := DNSDiscoveryConfig{
				RefreshInterval: time.Millisecond,
				Resolver:        resolver,
				QType:           tc.qType,
				Host:            tc.host,
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
