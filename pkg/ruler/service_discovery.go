// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/refresh"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
)

const (
	mechanismName = "dns_sd"
)

type dnsServiceDiscovery struct {
	Resolver cacheutil.AddressProvider

	RefreshInterval time.Duration
	QType           dns.QType
	Host            string
}

func (dnsServiceDiscovery) Name() string {
	return mechanismName
}

func (c dnsServiceDiscovery) NewDiscoverer(opts discovery.DiscovererOptions) (discovery.Discoverer, error) {
	return refresh.NewDiscovery(opts.Logger, mechanismName, c.RefreshInterval, c.resolve), nil
}

func (c dnsServiceDiscovery) resolve(ctx context.Context) ([]*targetgroup.Group, error) {
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

func dnsSD(rulerConfig *Config, resolver cacheutil.AddressProvider, qType dns.QType, url *url.URL) discovery.Config {
	return dnsServiceDiscovery{
		Resolver:        resolver,
		RefreshInterval: rulerConfig.AlertmanagerRefreshInterval,
		Host:            url.Host,
		QType:           qType,
	}
}

func staticTarget(url *url.URL) discovery.Config {
	return discovery.StaticConfig{
		{
			Targets: []model.LabelSet{{model.AddressLabel: model.LabelValue(url.Host)}},
		},
	}
}

func sanitizedAlertmanagerURL(amURL string) (isServiceDiscovery bool, qType dns.QType, parsedURL *url.URL, err error) {
	rawQType, rawURL := dns.GetQTypeName(amURL)
	qType = dns.QType(rawQType)

	switch qType {
	case "", dns.A, dns.SRV, dns.SRVNoA:
	default:
		err = errors.Errorf("invalid DNS service discovery prefix %q", qType)
		return
	}

	parsedURL, err = url.Parse(rawURL)
	if err != nil {
		return
	}

	if parsedURL.String() == "" || parsedURL.Host == "" {
		err = fmt.Errorf("improperly formatted alertmanager URL %q (maybe the scheme is missing?); see DNS Service Discovery docs", rawURL)
		return
	}

	isServiceDiscovery = qType != ""

	return
}
