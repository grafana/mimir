// SPDX-License-Identifier: AGPL-3.0-only

package alertmanagerdiscovery

import (
	"flag"
	"fmt"
	"strings"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"

	"github.com/grafana/mimir/pkg/util"
)

const (
	ModeFlagName = "alertmanager.service-discovery-mode"

	ModeDNS  = "dns"
	ModeRing = "ring"

	// sharedOptionWithRingClient is a message appended to all config options that should be also
	// set on the components running the alert-manager ring client.
	sharedOptionWithRingClient = " When alert-manager ring-based service discovery is enabled, this option needs be set on alert-managers and rulers."
)

var (
	modes = []string{ModeDNS, ModeRing}
)

type Config struct {
	Mode string `yaml:"service_discovery_mode" category:"experimental"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.StringVar(&cfg.Mode, ModeFlagName, ModeDNS, fmt.Sprintf("Service discovery mode that rulers use to find alert-manager instances.%s Supported values are: %s.", sharedOptionWithRingClient, strings.Join(modes, ", ")))
}

func (cfg *Config) Validate() error {
	if !util.StringsContain(modes, cfg.Mode) {
		return fmt.Errorf("unsupported alert-manager service discovery mode (supported values are: %s)", strings.Join(modes, ", "))
	}
	return nil
}

func NewDiscoveryConfig(amURL string) discovery.Config {
	return discovery.StaticConfig{
		{
			Targets: []model.LabelSet{{model.AddressLabel: model.LabelValue(amURL)}},
		},
	}
}
