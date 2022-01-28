// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/instance_limits.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import "github.com/pkg/errors"

var (
	// We don't include values in the message to avoid leaking Mimir cluster configuration to users.
	errMaxSamplesPushRateLimitReached = errors.New("cannot push more samples: ingester's samples push rate limit reached")
	errMaxUsersLimitReached           = errors.New("cannot create TSDB: ingesters's max tenants limit reached")
	errMaxSeriesLimitReached          = errors.New("cannot add series: ingesters's max series limit reached")
	errTooManyInflightPushRequests    = errors.New("cannot push: too many inflight push requests in ingester")
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
