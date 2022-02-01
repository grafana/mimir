// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	thanosdns "github.com/thanos-io/thanos/pkg/discovery/dns"
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
			resolver := fakeResolver{addresses: tc.resolvedAddresses}
			cfg := thanosServiceDiscovery{
				RefreshInterval: time.Millisecond,
				Resolver:        resolver,
				QType:           thanosdns.A,
				Host:            sourceAddress,
			}
			discoverer, err := cfg.NewDiscoverer(discovery.DiscovererOptions{})
			require.NoError(t, err)

			groupsChan := make(chan []*targetgroup.Group)
			go discoverer.Run(context.Background(), groupsChan)
			groups := <-groupsChan

			assert.ElementsMatch(t, tc.expectedTargetGroups, groups)
		})
	}
}

type fakeResolver struct {
	addresses []string
}

func (f fakeResolver) Resolve(_ context.Context, _ []string) error { return nil }
func (f fakeResolver) Addresses() []string                         { return f.addresses }

func TestConfig_ConstructsLookupNamesCorrectly(t *testing.T) {
	testCases := []struct {
		name  string
		qType thanosdns.QType
		host  string

		expectedAddress string
	}{
		{
			name:            "dsn+",
			qType:           thanosdns.A,
			host:            "localhost:123",
			expectedAddress: "dns+localhost:123",
		},
		{
			name:            "dnssrv+",
			qType:           thanosdns.SRV,
			host:            "localhost:123",
			expectedAddress: "dnssrv+localhost:123",
		},
		{
			name:            "dnssrvnoa+",
			qType:           thanosdns.SRVNoA,
			host:            "localhost:123",
			expectedAddress: "dnssrvnoa+localhost:123",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testResolver := &spyResolver{}
			cfg := thanosServiceDiscovery{
				RefreshInterval: time.Millisecond,
				Resolver:        testResolver,
				QType:           tc.qType,
				Host:            tc.host,
			}
			discoverer, err := cfg.NewDiscoverer(discovery.DiscovererOptions{})
			require.NoError(t, err)

			groupsChan := make(chan []*targetgroup.Group)
			go discoverer.Run(context.Background(), groupsChan)
			<-groupsChan // wait for at least one iteration

			assert.Len(t, testResolver.calledWith, 1)
			assert.Equal(t, tc.expectedAddress, testResolver.calledWith[0])
		})
	}
}

type spyResolver struct {
	calledWith []string
}

func (f *spyResolver) Resolve(_ context.Context, toResolve []string) error {
	f.calledWith = toResolve
	return nil
}

func (f *spyResolver) Addresses() []string { return nil }
