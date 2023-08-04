// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/instance_limits.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"flag"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/util/globalerror"
)

const (
	maxIngestionRateFlag        = "ingester.instance-limits.max-ingestion-rate"
	maxInMemoryTenantsFlag      = "ingester.instance-limits.max-tenants"
	maxInMemorySeriesFlag       = "ingester.instance-limits.max-series"
	maxInflightPushRequestsFlag = "ingester.instance-limits.max-inflight-push-requests"
)

// We don't include values in the messages for per-instance limits to avoid leaking Mimir cluster configuration to users.
var (
	errMaxIngestionRateReached    = newInstanceLimitError(globalerror.IngesterMaxIngestionRate.MessageWithPerInstanceLimitConfig("the write request has been rejected because the ingester exceeded the samples ingestion rate limit", maxIngestionRateFlag))
	errMaxTenantsReached          = newInstanceLimitError(globalerror.IngesterMaxTenants.MessageWithPerInstanceLimitConfig("the write request has been rejected because the ingester exceeded the allowed number of tenants", maxInMemoryTenantsFlag))
	errMaxInMemorySeriesReached   = newInstanceLimitError(globalerror.IngesterMaxInMemorySeries.MessageWithPerInstanceLimitConfig("the write request has been rejected because the ingester exceeded the allowed number of in-memory series", maxInMemorySeriesFlag))
	errMaxInflightRequestsReached = newInstanceLimitError(globalerror.IngesterMaxInflightPushRequests.MessageWithPerInstanceLimitConfig("the write request has been rejected because the ingester exceeded the allowed number of inflight push requests", maxInflightPushRequestsFlag))
)

type instanceLimitErr struct {
	msg    string
	status *status.Status
}

func newInstanceLimitError(msg string) error {
	return &instanceLimitErr{
		// Errors from hitting per-instance limits are always "unavailable" for gRPC
		status: status.New(codes.Unavailable, msg),
		msg:    msg,
	}
}

func (e *instanceLimitErr) ShouldLog(context.Context, time.Duration) bool {
	// We increment metrics when hitting per-instance limits and so there's no need to
	// log them, the error doesn't contain any interesting information for us.
	return false
}

func (e *instanceLimitErr) GRPCStatus() *status.Status {
	return e.status
}

func (e *instanceLimitErr) Error() string {
	return e.msg
}

// InstanceLimits describes limits used by ingester. Reaching any of these will result in Push method to return
// (internal) error.
type InstanceLimits struct {
	MaxIngestionRate        float64 `yaml:"max_ingestion_rate" category:"advanced"`
	MaxInMemoryTenants      int64   `yaml:"max_tenants" category:"advanced"`
	MaxInMemorySeries       int64   `yaml:"max_series" category:"advanced"`
	MaxInflightPushRequests int64   `yaml:"max_inflight_push_requests" category:"advanced"`
}

func (l *InstanceLimits) RegisterFlags(f *flag.FlagSet) {
	f.Float64Var(&l.MaxIngestionRate, maxIngestionRateFlag, 0, "Max ingestion rate (samples/sec) that ingester will accept. This limit is per-ingester, not per-tenant. Additional push requests will be rejected. Current ingestion rate is computed as exponentially weighted moving average, updated every second. 0 = unlimited.")
	f.Int64Var(&l.MaxInMemoryTenants, maxInMemoryTenantsFlag, 0, "Max tenants that this ingester can hold. Requests from additional tenants will be rejected. 0 = unlimited.")
	f.Int64Var(&l.MaxInMemorySeries, maxInMemorySeriesFlag, 0, "Max series that this ingester can hold (across all tenants). Requests to create additional series will be rejected. 0 = unlimited.")
	f.Int64Var(&l.MaxInflightPushRequests, maxInflightPushRequestsFlag, 30000, "Max inflight push requests that this ingester can handle (across all tenants). Additional requests will be rejected. 0 = unlimited.")
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
