// SPDX-License-Identifier: AGPL-3.0-only

package alertmanagerdiscovery

import (
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/servicediscovery"
)

// RingKey is the key under which we store the alertmanager ring in the KVStore.
const RingKey = "alertmanager"

type RingConfig struct {
	Common util.CommonRingConfig `yaml:",inline"`
}

// NewRingProvider creates a new client ring for the clientComponent, with servicediscovery notification being sent to the receiver.
func NewRingProvider(cfg RingConfig, clientComponent string, receiver servicediscovery.Notifications, logger log.Logger, reg prometheus.Registerer) (services.Service, error) {
	// Since this is a client for the alert-managers ring, we append "alertmanager-client" to the component to clearly differentiate it.
	clientComponent += "-alertmanager-client"

	client, err := ring.New(cfg.Common.ToRingConfig(), clientComponent, RingKey, logger, prometheus.WrapRegistererWithPrefix("cortex_", reg))
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize alertmanagers' ring client")
	}
	if err != nil {
		return nil, err
	}

	const ringCheckPeriod = 5 * time.Second
	return servicediscovery.NewRing(client, ringCheckPeriod, 0, receiver), nil
}
