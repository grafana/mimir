// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/instance_limits.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/util/globalerror"
)

var (
	// We don't include values in the message to avoid leaking Mimir cluster configuration to users.
	errMaxIngestionRateReached    = errors.New(globalerror.MaxIngesterIngestionRate.MessageWithLimitConfig(maxIngestionRateFlag, "cannot push more samples: exceeded the allowed rate of push requests"))
	errMaxTenantsReached          = errors.New(globalerror.MaxTenants.MessageWithLimitConfig(maxInMemoryTenantsFlag, "cannot create TSDB: exceeded the allowed number of in-memory tenants in an ingester"))
	errMaxInMemorySeriesReached   = errors.New(globalerror.MaxInMemorySeries.MessageWithLimitConfig(maxInMemorySeriesFlag, "cannot add series: exceeded the allowed number of in-memory series in an ingester"))
	errMaxInflightRequestsReached = errors.New(globalerror.MaxInflightPushRequests.MessageWithLimitConfig(maxInflightPushRequestsFlag, "cannot push: exceeded the allowed number of inflight push requests to the ingester"))
)

// InstanceLimits describes limits used by ingester. Reaching any of these will result in Push method to return
// (internal) error.
type InstanceLimits struct {
	MaxIngestionRate        float64 `yaml:"max_ingestion_rate" category:"advanced"`
	MaxInMemoryTenants      int64   `yaml:"max_tenants" category:"advanced"`
	MaxInMemorySeries       int64   `yaml:"max_series" category:"advanced"`
	MaxInflightPushRequests int64   `yaml:"max_inflight_push_requests" category:"advanced"`
}

// Sets default limit values for unmarshalling.
var defaultInstanceLimits *InstanceLimits = nil

// UnmarshalYAML implements the yaml.Unmarshaler interface. If give
func (l *InstanceLimits) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if defaultInstanceLimits != nil {
		*l = *defaultInstanceLimits
	}
	type plain InstanceLimits // type indirection to make sure we don't go into recursive loop
	return unmarshal((*plain)(l))
}
