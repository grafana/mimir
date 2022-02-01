// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"net/url"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/dns"
	"github.com/prometheus/prometheus/discovery/refresh"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	thanosdns "github.com/thanos-io/thanos/pkg/discovery/dns"
)

const (
	mechanismName = "thanos_dns_sd"
)

type thanosServiceDiscovery struct {
	Resolver cacheutil.AddressProvider

	RefreshInterval time.Duration
	QType           thanosdns.QType
	Host            string
}

func (thanosServiceDiscovery) Name() string {
	return mechanismName
}

func (c thanosServiceDiscovery) NewDiscoverer(opts discovery.DiscovererOptions) (discovery.Discoverer, error) {
	return refresh.NewDiscovery(opts.Logger, mechanismName, c.RefreshInterval, c.resolve), nil
}

func (c thanosServiceDiscovery) resolve(ctx context.Context) ([]*targetgroup.Group, error) {
	if err := c.Resolver.Resolve(ctx, []string{string(c.QType) + "+" + c.Host}); err != nil {
		return nil, err
	}

	resolved := c.Resolver.Addresses()
	targets := make([]model.LabelSet, len(resolved))
	for i, r := range resolved {
		targets[i] = model.LabelSet{
			model.AddressLabel: model.LabelValue(r),
		}
	}

	tg := &targetgroup.Group{
		Targets: targets,
		Source:  c.Host,
	}

	return []*targetgroup.Group{tg}, nil
}

func thanosSD(rulerConfig *Config, resolver cacheutil.AddressProvider, qType thanosdns.QType, url *url.URL) discovery.Config {
	return thanosServiceDiscovery{
		Resolver:        resolver,
		RefreshInterval: rulerConfig.AlertmanagerRefreshInterval,
		Host:            url.Host,
		QType:           qType,
	}
}

func promSD(rulerConfig *Config, url *url.URL) discovery.Config {
	return &dns.SDConfig{
		Names:           []string{url.Host},
		RefreshInterval: model.Duration(rulerConfig.AlertmanagerRefreshInterval),
		Type:            "SRV",
		Port:            0, // Ignored, because of SRV.
	}
}

func staticTarget(url *url.URL) discovery.Config {
	return discovery.StaticConfig{
		{
			Targets: []model.LabelSet{{model.AddressLabel: model.LabelValue(url.Host)}},
		},
	}
}
