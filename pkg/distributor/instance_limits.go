// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"flag"

	"github.com/pkg/errors"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/util/globalerror"
)

const (
	maxIngestionRateFlag             = "distributor.instance-limits.max-ingestion-rate"
	maxInflightPushRequestsFlag      = "distributor.instance-limits.max-inflight-push-requests"
	maxInflightPushRequestsBytesFlag = "distributor.instance-limits.max-inflight-push-requests-bytes"
)

var (
	// Distributor instance limits errors.
	errMaxInflightRequestsReached      = errors.New(globalerror.DistributorMaxInflightPushRequests.MessageWithPerInstanceLimitConfig("the write request has been rejected because the distributor exceeded the allowed number of inflight push requests", maxInflightPushRequestsFlag))
	errMaxIngestionRateReached         = errors.New(globalerror.DistributorMaxIngestionRate.MessageWithPerInstanceLimitConfig("the write request has been rejected because the distributor exceeded the ingestion rate limit", maxIngestionRateFlag))
	errMaxInflightRequestsBytesReached = errors.New(globalerror.DistributorMaxInflightPushRequestsBytes.MessageWithPerInstanceLimitConfig("the write request has been rejected because the distributor exceeded the allowed total size in bytes of inflight push requests", maxInflightPushRequestsBytesFlag))
)

type InstanceLimits struct {
	MaxIngestionRate             float64 `yaml:"max_ingestion_rate" category:"advanced"`
	MaxInflightPushRequests      int     `yaml:"max_inflight_push_requests" category:"advanced"`
	MaxInflightPushRequestsBytes int     `yaml:"max_inflight_push_requests_bytes" category:"advanced"`
}

func (l *InstanceLimits) RegisterFlags(f *flag.FlagSet) {
	f.Float64Var(&l.MaxIngestionRate, maxIngestionRateFlag, 0, "Max ingestion rate (samples/sec) that this distributor will accept. This limit is per-distributor, not per-tenant. Additional push requests will be rejected. Current ingestion rate is computed as exponentially weighted moving average, updated every second. 0 = unlimited.")
	f.IntVar(&l.MaxInflightPushRequests, maxInflightPushRequestsFlag, 2000, "Max inflight push requests that this distributor can handle. This limit is per-distributor, not per-tenant. Additional requests will be rejected. 0 = unlimited.")
	f.IntVar(&l.MaxInflightPushRequestsBytes, maxInflightPushRequestsBytesFlag, 0, "The sum of the request sizes in bytes of inflight push requests that this distributor can handle. This limit is per-distributor, not per-tenant. Additional requests will be rejected. 0 = unlimited.")
}

// Sets default limit values for unmarshalling.
var defaultInstanceLimits *InstanceLimits

func SetDefaultInstanceLimitsForYAMLUnmarshalling(l InstanceLimits) {
	defaultInstanceLimits = &l
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (l *InstanceLimits) UnmarshalYAML(value *yaml.Node) error {
	if defaultInstanceLimits != nil {
		*l = *defaultInstanceLimits
	}
	type plain InstanceLimits // type indirection to make sure we don't go into recursive loop
	return value.DecodeWithOptions((*plain)(l), yaml.DecodeOptions{KnownFields: true})
}
