// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/dns"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/refresh"
	"github.com/prometheus/prometheus/discovery/targetgroup"
)

const (
	mechanismName = "dns_sd"
)

func init() {
	discovery.RegisterConfig(dnsServiceDiscovery{})
}

type dnsServiceDiscovery struct {
	refreshMetrics discovery.RefreshMetricsInstantiator

	Resolver cache.AddressProvider

	RefreshInterval time.Duration
	QType           dns.QType
	Host            string
}

func (dnsServiceDiscovery) Name() string {
	return mechanismName
}

func (c dnsServiceDiscovery) NewDiscoverer(opts discovery.DiscovererOptions) (discovery.Discoverer, error) {
	return refresh.NewDiscovery(refresh.Options{
		Logger:              opts.Logger,
		Mech:                mechanismName,
		Interval:            c.RefreshInterval,
		RefreshF:            c.resolve,
		MetricsInstantiator: c.refreshMetrics,
	}), nil
}

func (c dnsServiceDiscovery) NewDiscovererMetrics(prometheus.Registerer, discovery.RefreshMetricsInstantiator) discovery.DiscovererMetrics {
	return &discovery.NoopDiscovererMetrics{}
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

func dnsSD(rulerConfig *Config, resolver cache.AddressProvider, qType dns.QType, url *url.URL, rmi discovery.RefreshMetricsInstantiator) discovery.Config {
	return dnsServiceDiscovery{
		Resolver:        resolver,
		RefreshInterval: rulerConfig.AlertmanagerRefreshInterval,
		Host:            url.Host,
		QType:           qType,
		refreshMetrics:  rmi,
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
