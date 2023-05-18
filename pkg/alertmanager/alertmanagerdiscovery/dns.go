// SPDX-License-Identifier: AGPL-3.0-only

package alertmanagerdiscovery

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/go-kit/log"
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

// NewDNSProvider returns a new dns.Provider that can be used to build discovery.Config instances.
func NewDNSProvider(reg prometheus.Registerer, logger log.Logger) *dns.Provider {
	// We need to prefix and add a label to the metrics for the DNS resolver because, unlike other mimir components,
	// it doesn't already have the `cortex_` prefix and the `component` label to the metrics it emits
	dnsProviderReg := prometheus.WrapRegistererWithPrefix(
		"cortex_",
		prometheus.WrapRegistererWith(
			prometheus.Labels{"component": "ruler"},
			reg,
		),
	)

	return dns.NewProvider(logger, dnsProviderReg, dns.GolangResolverType)
}

// NewDiscoveryConfigs returns a map of alert manager URLs to discovery.Config instances.
func NewDiscoveryConfigs(alertManagerUrls string, alertmanagerRefreshInterval time.Duration, resolver *dns.Provider) (map[string]discovery.Config, error) {
	result := make(map[string]discovery.Config)
	if alertManagerUrls != "" {
		for _, rawURL := range strings.Split(alertManagerUrls, ",") {
			isSD, qType, amURL, err := sanitizedAlertmanagerURL(rawURL)
			if err != nil {
				return nil, err
			}
			if isSD {
				result[amURL.String()] = DNSDiscoveryConfig{
					Resolver:        resolver,
					RefreshInterval: alertmanagerRefreshInterval,
					Host:            amURL.Host,
					QType:           qType,
				}
			} else {
				result[rawURL] = NewDiscoveryConfig(amURL.Host)
			}
		}
	}

	return result, nil
}

type DNSDiscoveryConfig struct {
	Resolver        cache.AddressProvider
	RefreshInterval time.Duration
	QType           dns.QType
	Host            string
}

func (DNSDiscoveryConfig) Name() string {
	return mechanismName
}

func (c DNSDiscoveryConfig) NewDiscoverer(opts discovery.DiscovererOptions) (discovery.Discoverer, error) {
	return refresh.NewDiscovery(opts.Logger, mechanismName, c.RefreshInterval, c.resolve), nil
}

func (c DNSDiscoveryConfig) resolve(ctx context.Context) ([]*targetgroup.Group, error) {
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
