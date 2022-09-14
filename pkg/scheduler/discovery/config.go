// SPDX-License-Identifier: AGPL-3.0-only

package discovery

import (
	"flag"
	"fmt"
	"strings"

	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/util"
)

const (
	ServiceDiscoveryModeDNS  = "dns"
	ServiceDiscoveryModeRing = "ring"
)

var (
	serviceDiscoveryModes = []string{ServiceDiscoveryModeDNS, ServiceDiscoveryModeRing}
)

type Config struct {
	Mode          string     `yaml:"service_discovery_mode" category:"experimental"`
	SchedulerRing RingConfig `yaml:"ring" doc:"description=The hash ring configuration. The query-schedulers hash ring is used for service discovery."`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.StringVar(&cfg.Mode, "query-scheduler.service-discovery-mode", ServiceDiscoveryModeDNS, fmt.Sprintf("Which service discovery mode query-frontends and queriers should use to discover query-scheduler instances.%s Supported values are: %s.", sharedOptionWithRingClient, strings.Join(serviceDiscoveryModes, ", ")))
	cfg.SchedulerRing.RegisterFlags(f, logger)
}

func (cfg *Config) Validate() error {
	if !util.StringsContain(serviceDiscoveryModes, cfg.Mode) {
		return fmt.Errorf("unsupported query-scheduler service discovery mode (supported values are: %s)", strings.Join(serviceDiscoveryModes, ", "))
	}

	return nil
}
