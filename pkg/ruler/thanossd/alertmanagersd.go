// SPDX-License-Identifier: AGPL-3.0-only

package thanossd

import (
	"context"
	"time"

	mmodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/refresh"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	"github.com/thanos-io/thanos/pkg/discovery/dns"
)

const (
	mechanismName = "thanos_dns_sd"
)

type Config struct {
	Resolver cacheutil.AddressProvider

	RefreshInterval time.Duration
	QType           dns.QType
	Host            string
}

func (Config) Name() string {
	return mechanismName
}

func (c Config) NewDiscoverer(opts discovery.DiscovererOptions) (discovery.Discoverer, error) {
	return refresh.NewDiscovery(opts.Logger, mechanismName, c.RefreshInterval, c.resolve), nil
}

func (c Config) resolve(ctx context.Context) ([]*targetgroup.Group, error) {
	if err := c.Resolver.Resolve(ctx, []string{string(c.QType) + "+" + c.Host}); err != nil {
		return nil, err
	}

	resolved := c.Resolver.Addresses()
	targets := make([]mmodel.LabelSet, len(resolved))
	for i, r := range resolved {
		targets[i] = mmodel.LabelSet{
			mmodel.AddressLabel: mmodel.LabelValue(r),
		}
	}

	tg := &targetgroup.Group{
		Targets: targets,
		Source:  c.Host,
	}

	return []*targetgroup.Group{tg}, nil
}
