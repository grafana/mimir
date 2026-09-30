// SPDX-License-Identifier: AGPL-3.0-only

// Package limits holds per-tenant limits: command-line flags give the defaults, like Mimir's
// `-validation.*` and `-ingester.*` flags, and the `overrides` map of the runtime config replaces
// them per tenant.
package limits

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/grafana/mimir/pkg/storage/seriesstore/trackers"
)

// ParseDurationMs parses a Prometheus `model.Duration` such as `2h`, `1h30m`, `10000d` or `0`, in
// milliseconds.
func ParseDurationMs(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "0" {
		return 0, nil
	}
	if value == "" {
		return 0, errors.New("empty duration")
	}
	var total int64
	rest := value
	for rest != "" {
		digits := strings.IndexFunc(rest, func(c rune) bool { return c < '0' || c > '9' })
		if digits < 0 {
			return 0, fmt.Errorf("duration %q is missing a unit", value)
		}
		if digits == 0 {
			return 0, fmt.Errorf("invalid duration %q", value)
		}
		amount, err := strconv.ParseInt(rest[:digits], 10, 64)
		if err != nil {
			return 0, err
		}
		rest = rest[digits:]
		unitLen := strings.IndexFunc(rest, func(c rune) bool { return c >= '0' && c <= '9' })
		if unitLen < 0 {
			unitLen = len(rest)
		}
		var unitMs int64
		switch rest[:unitLen] {
		case "ms":
			unitMs = 1
		case "s":
			unitMs = 1_000
		case "m":
			unitMs = 60_000
		case "h":
			unitMs = 3_600_000
		case "d":
			unitMs = 86_400_000
		case "w":
			unitMs = 7 * 86_400_000
		case "y":
			unitMs = 365 * 86_400_000
		default:
			return 0, fmt.Errorf("unknown unit %q in duration %q", rest[:unitLen], value)
		}
		total = saturatingAdd(total, saturatingMul(amount, unitMs))
		rest = rest[unitLen:]
	}
	return total, nil
}

func saturatingMul(a, b int64) int64 {
	if a != 0 && (a*b)/a != b {
		if (a > 0) == (b > 0) {
			return math.MaxInt64
		}
		return math.MinInt64
	}
	return a * b
}

func saturatingAdd(a, b int64) int64 {
	sum := a + b
	if a > 0 && b > 0 && sum < 0 {
		return math.MaxInt64
	}
	if a < 0 && b < 0 && sum >= 0 {
		return math.MinInt64
	}
	return sum
}

// Limits are the limits the ingester applies, named after their Mimir YAML fields.
type Limits struct {
	OutOfOrderTimeWindowMs                  int64
	CreationGracePeriodMs                   int64
	PastGracePeriodMs                       int64
	NativeHistogramsIngestionEnabled        bool
	MaxGlobalSeriesPerUser                  int64
	MaxGlobalExemplarsPerUser               int64
	MaxGlobalMetadataPerUser                int64
	MaxGlobalMetadataPerMetric              int64
	IngestionPartitionsTenantShardSize      int64
	EarlyHeadCompactionOwnedSeriesThreshold int64
	ActiveSeriesCustomTrackers              *trackers.CustomTrackers
	ActiveSeriesAdditionalCustomTrackers    *trackers.CustomTrackers
	MaxActiveSeriesAdditionalCustomTrackers int64
	CostAttributionTrackers                 trackers.CostAttributionTrackers
	AdditionalCostAttributionTrackers       trackers.CostAttributionTrackers
	MaxCostAttributionCardinality           int64
	CostAttributionCooldownMs               int64
}

// DefaultLimits are Mimir's flag defaults.
func DefaultLimits() Limits {
	return Limits{
		CreationGracePeriodMs:            10 * 60_000,
		NativeHistogramsIngestionEnabled: true,
		MaxGlobalSeriesPerUser:           150_000,
		MaxCostAttributionCardinality:    2000,
	}
}

// Args are the flags behind Limits, spelled like Mimir's.
type Args struct {
	OutOfOrderTimeWindow             string
	CreationGracePeriod              string
	PastGracePeriod                  string
	NativeHistogramsIngestionEnabled bool
	// Only reported in `cortex_ingester_local_limits`: series limits apply before Kafka.
	MaxGlobalSeriesPerUser                  int64
	MaxGlobalExemplarsPerUser               int64
	MaxGlobalMetadataPerUser                int64
	MaxGlobalMetadataPerMetric              int64
	IngestionPartitionsTenantShardSize      int64
	EarlyHeadCompactionOwnedSeriesThreshold int64
	// `<name>:<matcher>[;<name>:<matcher>]*`; the flag may repeat.
	ActiveSeriesCustomTrackers StringSlice
	// JSON map of tracker name to `{"labels": [{"input": ..., "output": ...}], "internal": ...}`.
	CostAttributionTrackers                 string
	MaxActiveSeriesAdditionalCustomTrackers int64
	MaxCostAttributionCardinality           int64
	CostAttributionCooldown                 string
}

// StringSlice is a flag that may repeat.
type StringSlice []string

func (s *StringSlice) String() string     { return strings.Join(*s, ",") }
func (s *StringSlice) Set(v string) error { *s = append(*s, v); return nil }

func (a *Args) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&a.OutOfOrderTimeWindow, "ingester.out-of-order-time-window", "0", "")
	f.StringVar(&a.CreationGracePeriod, "validation.create-grace-period", "10m", "")
	f.StringVar(&a.PastGracePeriod, "validation.past-grace-period", "0", "")
	f.BoolVar(&a.NativeHistogramsIngestionEnabled, "ingester.native-histograms-ingestion-enabled", true, "")
	f.Int64Var(&a.MaxGlobalSeriesPerUser, "ingester.max-global-series-per-user", 150_000, "Only reported in cortex_ingester_local_limits: series limits apply before Kafka.")
	f.Int64Var(&a.MaxGlobalExemplarsPerUser, "ingester.max-global-exemplars-per-user", 0, "")
	f.Int64Var(&a.MaxGlobalMetadataPerUser, "ingester.max-global-metadata-per-user", 0, "")
	f.Int64Var(&a.MaxGlobalMetadataPerMetric, "ingester.max-global-metadata-per-metric", 0, "")
	f.Int64Var(&a.IngestionPartitionsTenantShardSize, "ingest-storage.ingestion-partition-tenant-shard-size", 0, "")
	f.Int64Var(&a.EarlyHeadCompactionOwnedSeriesThreshold, "ingester.early-head-compaction-owned-series-threshold", 0, "")
	f.Var(&a.ActiveSeriesCustomTrackers, "ingester.active-series-custom-trackers", "<name>:<matcher>[;<name>:<matcher>]*; the flag may repeat.")
	f.StringVar(&a.CostAttributionTrackers, "validation.cost-attribution-trackers", "", "JSON map of tracker name to its labels.")
	f.Int64Var(&a.MaxActiveSeriesAdditionalCustomTrackers, "validation.max-active-series-additional-custom-trackers", 0, "")
	f.Int64Var(&a.MaxCostAttributionCardinality, "validation.max-cost-attribution-cardinality", 2000, "")
	f.StringVar(&a.CostAttributionCooldown, "validation.cost-attribution-cooldown", "0", "")
}

// DefaultArgs are the flags' defaults.
func DefaultArgs() Args {
	return Args{
		OutOfOrderTimeWindow:             "0",
		CreationGracePeriod:              "10m",
		PastGracePeriod:                  "0",
		NativeHistogramsIngestionEnabled: true,
		MaxGlobalSeriesPerUser:           150_000,
		MaxCostAttributionCardinality:    2000,
		CostAttributionCooldown:          "0",
	}
}

func (a *Args) ToLimits() (Limits, error) {
	customTrackers := map[string]string{}
	for _, flagValue := range a.ActiveSeriesCustomTrackers {
		pairs, err := trackers.ParseFlag(flagValue)
		if err != nil {
			return Limits{}, err
		}
		for _, pair := range pairs {
			if _, ok := customTrackers[pair[0]]; ok {
				return Limits{}, fmt.Errorf("matcher %q for active series custom trackers is provided more than once", pair[0])
			}
			customTrackers[pair[0]] = pair[1]
		}
	}
	var err error
	l := Limits{
		NativeHistogramsIngestionEnabled:        a.NativeHistogramsIngestionEnabled,
		MaxGlobalSeriesPerUser:                  a.MaxGlobalSeriesPerUser,
		MaxGlobalExemplarsPerUser:               a.MaxGlobalExemplarsPerUser,
		MaxGlobalMetadataPerUser:                a.MaxGlobalMetadataPerUser,
		MaxGlobalMetadataPerMetric:              a.MaxGlobalMetadataPerMetric,
		IngestionPartitionsTenantShardSize:      a.IngestionPartitionsTenantShardSize,
		EarlyHeadCompactionOwnedSeriesThreshold: a.EarlyHeadCompactionOwnedSeriesThreshold,
		MaxActiveSeriesAdditionalCustomTrackers: a.MaxActiveSeriesAdditionalCustomTrackers,
		MaxCostAttributionCardinality:           a.MaxCostAttributionCardinality,
	}
	for _, d := range []struct {
		value string
		into  *int64
	}{
		{a.OutOfOrderTimeWindow, &l.OutOfOrderTimeWindowMs},
		{a.CreationGracePeriod, &l.CreationGracePeriodMs},
		{a.PastGracePeriod, &l.PastGracePeriodMs},
		{a.CostAttributionCooldown, &l.CostAttributionCooldownMs},
	} {
		if *d.into, err = ParseDurationMs(d.value); err != nil {
			return Limits{}, err
		}
	}
	if l.ActiveSeriesCustomTrackers, err = trackers.NewCustomTrackers(customTrackers); err != nil {
		return Limits{}, err
	}
	if strings.TrimSpace(a.CostAttributionTrackers) != "" {
		var value any
		if err := json.Unmarshal([]byte(a.CostAttributionTrackers), &value); err != nil {
			return Limits{}, fmt.Errorf("parse cost attribution trackers: %w", err)
		}
		if l.CostAttributionTrackers, err = trackers.CostAttributionTrackersFromValue(value); err != nil {
			return Limits{}, err
		}
	}
	return l, nil
}

// WithOverrides returns these limits with the fields present in a tenant's `overrides` entry
// replaced, the way Mimir decodes an override on top of the defaults. Fields the ingester doesn't
// use are ignored.
func (l Limits) WithOverrides(overrides map[string]any) (Limits, error) {
	for field, value := range overrides {
		var err error
		switch field {
		case "out_of_order_time_window":
			l.OutOfOrderTimeWindowMs, err = durationValue(value)
		case "creation_grace_period":
			l.CreationGracePeriodMs, err = durationValue(value)
		case "past_grace_period":
			l.PastGracePeriodMs, err = durationValue(value)
		case "native_histograms_ingestion_enabled":
			l.NativeHistogramsIngestionEnabled, err = boolValue(value)
		case "max_global_series_per_user":
			l.MaxGlobalSeriesPerUser, err = intValue(value)
		case "max_global_exemplars_per_user":
			l.MaxGlobalExemplarsPerUser, err = intValue(value)
		case "max_global_metadata_per_user":
			l.MaxGlobalMetadataPerUser, err = intValue(value)
		case "max_global_metadata_per_metric":
			l.MaxGlobalMetadataPerMetric, err = intValue(value)
		case "ingestion_partitions_tenant_shard_size":
			l.IngestionPartitionsTenantShardSize, err = intValue(value)
		case "early_head_compaction_owned_series_threshold":
			l.EarlyHeadCompactionOwnedSeriesThreshold, err = intValue(value)
		case "active_series_custom_trackers":
			l.ActiveSeriesCustomTrackers, err = trackers.CustomTrackersFromValue(value)
		case "active_series_additional_custom_trackers":
			l.ActiveSeriesAdditionalCustomTrackers, err = trackers.CustomTrackersFromValue(value)
		case "max_active_series_additional_custom_trackers":
			l.MaxActiveSeriesAdditionalCustomTrackers, err = intValue(value)
		case "cost_attribution_trackers":
			l.CostAttributionTrackers, err = trackers.CostAttributionTrackersFromValue(value)
		case "additional_cost_attribution_trackers":
			l.AdditionalCostAttributionTrackers, err = trackers.CostAttributionTrackersFromValue(value)
		case "max_cost_attribution_cardinality":
			l.MaxCostAttributionCardinality, err = intValue(value)
		case "cost_attribution_cooldown":
			l.CostAttributionCooldownMs, err = durationValue(value)
		}
		if err != nil {
			return Limits{}, fmt.Errorf("override %s: %w", field, err)
		}
	}
	if err := l.validate(); err != nil {
		return Limits{}, err
	}
	return l, nil
}

// validate fails like Mimir's `Limits.Validate`: an invalid tenant fails the whole runtime config
// load.
func (l Limits) validate() error {
	max := l.MaxActiveSeriesAdditionalCustomTrackers
	if max < 0 {
		return fmt.Errorf("active_series_additional_custom_trackers validation failed: invalid max custom trackers limit: %d", max)
	}
	count := l.ActiveSeriesAdditionalCustomTrackers.Len()
	if max > 0 && int64(count) > max {
		return fmt.Errorf("active_series_additional_custom_trackers validation failed: the number of custom trackers [%d] exceeds the configured limit [%d]", count, max)
	}
	return nil
}

// CustomTrackers returns base trackers with the additional ones on top, like
// `ActiveSeriesCustomTrackersConfig`.
func (l Limits) CustomTrackers() *trackers.CustomTrackers {
	if l.ActiveSeriesAdditionalCustomTrackers.IsEmpty() {
		if l.ActiveSeriesCustomTrackers == nil {
			empty, _ := trackers.NewCustomTrackers(nil)
			return empty
		}
		return l.ActiveSeriesCustomTrackers
	}
	if l.ActiveSeriesCustomTrackers.IsEmpty() {
		return l.ActiveSeriesAdditionalCustomTrackers
	}
	return l.ActiveSeriesCustomTrackers.Merged(l.ActiveSeriesAdditionalCustomTrackers)
}

func (l Limits) CostAttribution() trackers.CostAttributionTrackers {
	if l.AdditionalCostAttributionTrackers.IsEmpty() {
		return l.CostAttributionTrackers
	}
	return l.CostAttributionTrackers.Merged(l.AdditionalCostAttributionTrackers)
}

func intValue(value any) (int64, error) {
	switch value := value.(type) {
	// YAML writes large limits such as 4e+08 as floats.
	case int:
		return int64(value), nil
	case int64:
		return value, nil
	case uint64:
		return int64(value), nil
	case float64:
		return int64(value), nil
	case json.Number:
		if i, err := value.Int64(); err == nil {
			return i, nil
		}
		f, err := value.Float64()
		return int64(f), err
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		return int64(f), err
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("expected a number, got %v", value)
	}
}

func boolValue(value any) (bool, error) {
	switch value := value.(type) {
	case bool:
		return value, nil
	case string:
		return strconv.ParseBool(value)
	default:
		return false, fmt.Errorf("expected a boolean, got %v", value)
	}
}

func durationValue(value any) (int64, error) {
	switch value := value.(type) {
	case string:
		return ParseDurationMs(value)
	case int, int64, uint64, float64, json.Number, nil:
		return intValue(value)
	default:
		return 0, fmt.Errorf("expected a duration, got %v", value)
	}
}

// InstanceLimits are Mimir's per-ingester instance limits, from `-ingester.instance-limits.*`
// with the runtime config's `ingester_limits` on top. Only reported, like the other admission
// limits.
type InstanceLimits struct {
	MaxIngestionRate             float64
	MaxTenants                   int64
	MaxSeries                    int64
	MaxInflightPushRequests      int64
	MaxInflightPushRequestsBytes int64
}

func DefaultInstanceLimits() InstanceLimits {
	return InstanceLimits{MaxInflightPushRequests: 30_000}
}

func (l *InstanceLimits) RegisterFlags(f *flag.FlagSet) {
	f.Float64Var(&l.MaxIngestionRate, "ingester.instance-limits.max-ingestion-rate", 0, "")
	f.Int64Var(&l.MaxTenants, "ingester.instance-limits.max-tenants", 0, "")
	f.Int64Var(&l.MaxSeries, "ingester.instance-limits.max-series", 0, "")
	f.Int64Var(&l.MaxInflightPushRequests, "ingester.instance-limits.max-inflight-push-requests", 30_000, "")
	f.Int64Var(&l.MaxInflightPushRequestsBytes, "ingester.instance-limits.max-inflight-push-requests-bytes", 0, "")
}

func (l InstanceLimits) withOverrides(overrides map[string]any) (InstanceLimits, error) {
	for field, value := range overrides {
		var err error
		switch field {
		case "max_ingestion_rate":
			switch value := value.(type) {
			case float64:
				l.MaxIngestionRate = value
			default:
				var i int64
				i, err = intValue(value)
				l.MaxIngestionRate = float64(i)
			}
		case "max_tenants":
			l.MaxTenants, err = intValue(value)
		case "max_series":
			l.MaxSeries, err = intValue(value)
		case "max_inflight_push_requests":
			l.MaxInflightPushRequests, err = intValue(value)
		case "max_inflight_push_requests_bytes":
			l.MaxInflightPushRequestsBytes, err = intValue(value)
		}
		if err != nil {
			return InstanceLimits{}, fmt.Errorf("ingester_limits.%s: %w", field, err)
		}
	}
	return l, nil
}

// TenantLimits are a tenant's limits, resolved once per runtime config reload.
type TenantLimits struct {
	Limits          Limits
	CustomTrackers  *trackers.CustomTrackers
	CostAttribution trackers.CostAttributionTrackers
}

func newTenantLimits(l Limits) *TenantLimits {
	return &TenantLimits{Limits: l, CustomTrackers: l.CustomTrackers(), CostAttribution: l.CostAttribution()}
}

// Overrides are defaults from flags plus per-tenant overrides from the runtime config.
type Overrides struct {
	defaults         Limits
	instanceDefaults InstanceLimits
	instance         atomic.Pointer[InstanceLimits]
	defaultTenant    *TenantLimits
	tenants          atomic.Pointer[map[string]*TenantLimits]
	// Bumped on every change, so cached per-series tracker matches know to recompute.
	generation atomic.Uint64
	// Active partitions in the partition ring; 0 until known.
	activePartitions atomic.Uint64
	applyLock        sync.Mutex
}

func NewOverrides(defaults Limits) *Overrides {
	return NewOverridesWithInstanceLimits(defaults, DefaultInstanceLimits())
}

func NewOverridesWithInstanceLimits(defaults Limits, instance InstanceLimits) *Overrides {
	o := &Overrides{defaults: defaults, instanceDefaults: instance, defaultTenant: newTenantLimits(defaults)}
	o.instance.Store(&instance)
	o.tenants.Store(&map[string]*TenantLimits{})
	o.generation.Store(1)
	return o
}

// DefaultOverrides are overrides with Mimir's default limits.
func DefaultOverrides() *Overrides {
	return NewOverrides(DefaultLimits())
}

func (o *Overrides) Tenant(tenant string) *TenantLimits {
	if limits, ok := (*o.tenants.Load())[tenant]; ok {
		return limits
	}
	return o.defaultTenant
}

func (o *Overrides) Generation() uint64 { return o.generation.Load() }

func (o *Overrides) InstanceLimits() InstanceLimits { return *o.instance.Load() }

// ApplyRuntimeConfig replaces every tenant's overrides with the `overrides` map of a merged
// runtime config, and the instance limits with its `ingester_limits`.
func (o *Overrides) ApplyRuntimeConfig(config map[string]any) error {
	o.applyLock.Lock()
	defer o.applyLock.Unlock()
	instance := o.instanceDefaults
	if fields, ok := config["ingester_limits"].(map[string]any); ok {
		var err error
		if instance, err = o.instanceDefaults.withOverrides(fields); err != nil {
			return err
		}
	}
	tenants := map[string]*TenantLimits{}
	if raw, ok := config["overrides"]; ok {
		var overrides map[string]any
		switch raw := raw.(type) {
		case map[string]any:
			overrides = raw
		case nil:
		default:
			return fmt.Errorf("overrides must be a map, got %v", raw)
		}
		for tenant, value := range overrides {
			var l Limits
			switch value := value.(type) {
			case map[string]any:
				var err error
				if l, err = o.defaults.WithOverrides(value); err != nil {
					return fmt.Errorf("tenant %s: %w", tenant, err)
				}
			case nil:
				l = o.defaults
			default:
				return fmt.Errorf("overrides for tenant %s must be a map, got %v", tenant, value)
			}
			tenants[tenant] = newTenantLimits(l)
		}
	}
	o.tenants.Store(&tenants)
	o.instance.Store(&instance)
	o.generation.Add(1)
	return nil
}

func (o *Overrides) SetActivePartitions(partitions uint64) {
	o.activePartitions.Store(partitions)
}

// LocalLimit converts a global limit to this partition's share, like Mimir's
// `partitionRingLimiterStrategy`: 0 when the limit is disabled or no partition is known.
func (o *Overrides) LocalLimit(l *Limits, global int64) int64 {
	if global == 0 {
		return 0
	}
	active := int64(o.activePartitions.Load())
	shard := l.IngestionPartitionsTenantShardSize
	partitions := shard
	if shard <= 0 || shard > active {
		partitions = active
	}
	if partitions == 0 {
		return 0
	}
	return int64(float64(global) / float64(partitions))
}

// MaxExemplars is 0 when exemplars are disabled; an unknown local share falls back to the global
// limit.
func (o *Overrides) MaxExemplars(l *Limits) int {
	global := l.MaxGlobalExemplarsPerUser
	limit := o.LocalLimit(l, global)
	if limit <= 0 {
		limit = global
	}
	if limit < 0 {
		return 0
	}
	return int(limit)
}

// MaxSeriesPerUser is like `Limiter.maxSeriesPerUser`, 0 meaning unlimited as `math.MaxInt32`.
func (o *Overrides) MaxSeriesPerUser(l *Limits) int {
	return orUnlimited(o.LocalLimit(l, l.MaxGlobalSeriesPerUser))
}

func (o *Overrides) MaxMetadataPerUser(l *Limits) int {
	return orUnlimited(o.LocalLimit(l, l.MaxGlobalMetadataPerUser))
}

func (o *Overrides) MaxMetadataPerMetric(l *Limits) int {
	return orUnlimited(o.LocalLimit(l, l.MaxGlobalMetadataPerMetric))
}

func orUnlimited(local int64) int {
	if local <= 0 {
		return math.MaxInt32
	}
	return int(local)
}
