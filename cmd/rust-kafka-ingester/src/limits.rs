//! Per-tenant limits: command-line flags give the defaults, like Mimir's `-validation.*` and
//! `-ingester.*` flags, and the `overrides` map of the runtime config replaces them per tenant.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};

use crate::trackers::{CostAttributionTrackers, CustomTrackers};

/// Parses a Prometheus `model.Duration` such as `2h`, `1h30m`, `10000d` or `0`, in milliseconds.
pub fn parse_duration_ms(value: &str) -> Result<i64> {
    let value = value.trim();
    if value == "0" {
        return Ok(0);
    }
    if value.is_empty() {
        bail!("empty duration");
    }
    let mut total = 0_i64;
    let mut rest = value;
    while !rest.is_empty() {
        let digits = rest
            .find(|character: char| !character.is_ascii_digit())
            .with_context(|| format!("duration {value:?} is missing a unit"))?;
        if digits == 0 {
            bail!("invalid duration {value:?}");
        }
        let amount: i64 = rest[..digits].parse()?;
        rest = &rest[digits..];
        let unit_len = rest
            .find(|character: char| character.is_ascii_digit())
            .unwrap_or(rest.len());
        let unit_ms = match &rest[..unit_len] {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            "w" => 7 * 86_400_000,
            "y" => 365 * 86_400_000,
            unit => bail!("unknown unit {unit:?} in duration {value:?}"),
        };
        total = total.saturating_add(amount.saturating_mul(unit_ms));
        rest = &rest[unit_len..];
    }
    Ok(total)
}

/// The limits the Rust ingester applies, named after their Mimir YAML fields.
#[derive(Clone, Debug)]
pub struct Limits {
    pub out_of_order_time_window_ms: i64,
    pub creation_grace_period_ms: i64,
    pub past_grace_period_ms: i64,
    pub native_histograms_ingestion_enabled: bool,
    pub max_global_series_per_user: i64,
    pub max_global_exemplars_per_user: i64,
    pub max_global_metadata_per_user: i64,
    pub max_global_metadata_per_metric: i64,
    pub ingestion_partitions_tenant_shard_size: i64,
    pub active_series_custom_trackers: Arc<CustomTrackers>,
    pub active_series_additional_custom_trackers: Arc<CustomTrackers>,
    pub max_active_series_additional_custom_trackers: i64,
    pub cost_attribution_trackers: Arc<CostAttributionTrackers>,
    pub additional_cost_attribution_trackers: Arc<CostAttributionTrackers>,
    pub max_cost_attribution_cardinality: i64,
    pub cost_attribution_cooldown_ms: i64,
}

impl Default for Limits {
    // Mimir's flag defaults.
    fn default() -> Self {
        Self {
            out_of_order_time_window_ms: 0,
            creation_grace_period_ms: 10 * 60_000,
            past_grace_period_ms: 0,
            native_histograms_ingestion_enabled: true,
            max_global_series_per_user: 150_000,
            max_global_exemplars_per_user: 0,
            max_global_metadata_per_user: 0,
            max_global_metadata_per_metric: 0,
            ingestion_partitions_tenant_shard_size: 0,
            active_series_custom_trackers: Arc::default(),
            active_series_additional_custom_trackers: Arc::default(),
            max_active_series_additional_custom_trackers: 0,
            cost_attribution_trackers: Arc::default(),
            additional_cost_attribution_trackers: Arc::default(),
            max_cost_attribution_cardinality: 2000,
            cost_attribution_cooldown_ms: 0,
        }
    }
}

/// The flags behind [`Limits`], spelled like Mimir's.
#[derive(clap::Args, Clone, Debug)]
pub struct LimitsArgs {
    #[arg(long = "ingester.out-of-order-time-window", default_value = "0")]
    pub out_of_order_time_window: String,
    #[arg(long = "validation.create-grace-period", default_value = "10m")]
    pub creation_grace_period: String,
    #[arg(long = "validation.past-grace-period", default_value = "0")]
    pub past_grace_period: String,
    #[arg(
        long = "ingester.native-histograms-ingestion-enabled",
        default_value_t = true,
        action = clap::ArgAction::Set
    )]
    pub native_histograms_ingestion_enabled: bool,
    /// Only reported in `cortex_ingester_local_limits`: series limits apply before Kafka.
    #[arg(
        long = "ingester.max-global-series-per-user",
        default_value_t = 150_000
    )]
    pub max_global_series_per_user: i64,
    #[arg(long = "ingester.max-global-exemplars-per-user", default_value_t = 0)]
    pub max_global_exemplars_per_user: i64,
    #[arg(long = "ingester.max-global-metadata-per-user", default_value_t = 0)]
    pub max_global_metadata_per_user: i64,
    #[arg(long = "ingester.max-global-metadata-per-metric", default_value_t = 0)]
    pub max_global_metadata_per_metric: i64,
    #[arg(
        long = "ingest-storage.ingestion-partition-tenant-shard-size",
        default_value_t = 0
    )]
    pub ingestion_partitions_tenant_shard_size: i64,
    /// `<name>:<matcher>[;<name>:<matcher>]*`; the flag may repeat.
    #[arg(long = "ingester.active-series-custom-trackers")]
    pub active_series_custom_trackers: Vec<String>,
    /// JSON map of tracker name to `{"labels": [{"input": ..., "output": ...}], "internal": ...}`.
    #[arg(long = "validation.cost-attribution-trackers")]
    pub cost_attribution_trackers: Option<String>,
    #[arg(
        long = "validation.max-active-series-additional-custom-trackers",
        default_value_t = 0
    )]
    pub max_active_series_additional_custom_trackers: i64,
    #[arg(
        long = "validation.max-cost-attribution-cardinality",
        default_value_t = 2000
    )]
    pub max_cost_attribution_cardinality: i64,
    #[arg(long = "validation.cost-attribution-cooldown", default_value = "0")]
    pub cost_attribution_cooldown: String,
}

impl LimitsArgs {
    pub fn to_limits(&self) -> Result<Limits> {
        let mut custom_trackers = BTreeMap::new();
        for flag in &self.active_series_custom_trackers {
            for (name, matcher) in CustomTrackers::parse_flag(flag)? {
                if custom_trackers.insert(name.clone(), matcher).is_some() {
                    bail!(
                        "matcher {name:?} for active series custom trackers is provided more than once"
                    );
                }
            }
        }
        Ok(Limits {
            out_of_order_time_window_ms: parse_duration_ms(&self.out_of_order_time_window)?,
            creation_grace_period_ms: parse_duration_ms(&self.creation_grace_period)?,
            past_grace_period_ms: parse_duration_ms(&self.past_grace_period)?,
            native_histograms_ingestion_enabled: self.native_histograms_ingestion_enabled,
            max_global_series_per_user: self.max_global_series_per_user,
            max_global_exemplars_per_user: self.max_global_exemplars_per_user,
            max_global_metadata_per_user: self.max_global_metadata_per_user,
            max_global_metadata_per_metric: self.max_global_metadata_per_metric,
            ingestion_partitions_tenant_shard_size: self.ingestion_partitions_tenant_shard_size,
            active_series_custom_trackers: Arc::new(CustomTrackers::new(custom_trackers)?),
            active_series_additional_custom_trackers: Arc::default(),
            max_active_series_additional_custom_trackers: self
                .max_active_series_additional_custom_trackers,
            cost_attribution_trackers: Arc::new(match &self.cost_attribution_trackers {
                Some(json) if !json.trim().is_empty() => CostAttributionTrackers::from_value(
                    &serde_json::from_str(json).context("parse cost attribution trackers")?,
                )?,
                _ => CostAttributionTrackers::default(),
            }),
            additional_cost_attribution_trackers: Arc::default(),
            max_cost_attribution_cardinality: self.max_cost_attribution_cardinality,
            cost_attribution_cooldown_ms: parse_duration_ms(&self.cost_attribution_cooldown)?,
        })
    }
}

impl Limits {
    /// Returns these limits with the fields present in a tenant's `overrides` entry replaced, the
    /// way Mimir decodes an override on top of the defaults. Fields the Rust ingester does not use
    /// are ignored.
    pub fn with_overrides(&self, overrides: &Map<String, Value>) -> Result<Self> {
        let mut limits = self.clone();
        for (field, value) in overrides {
            let context = || format!("override {field}");
            match field.as_str() {
                "out_of_order_time_window" => {
                    limits.out_of_order_time_window_ms =
                        duration_value(value).with_context(context)?
                }
                "creation_grace_period" => {
                    limits.creation_grace_period_ms = duration_value(value).with_context(context)?
                }
                "past_grace_period" => {
                    limits.past_grace_period_ms = duration_value(value).with_context(context)?
                }
                "native_histograms_ingestion_enabled" => {
                    limits.native_histograms_ingestion_enabled =
                        bool_value(value).with_context(context)?
                }
                "max_global_series_per_user" => {
                    limits.max_global_series_per_user = int_value(value).with_context(context)?
                }
                "max_global_exemplars_per_user" => {
                    limits.max_global_exemplars_per_user = int_value(value).with_context(context)?
                }
                "max_global_metadata_per_user" => {
                    limits.max_global_metadata_per_user = int_value(value).with_context(context)?
                }
                "max_global_metadata_per_metric" => {
                    limits.max_global_metadata_per_metric =
                        int_value(value).with_context(context)?
                }
                "ingestion_partitions_tenant_shard_size" => {
                    limits.ingestion_partitions_tenant_shard_size =
                        int_value(value).with_context(context)?
                }
                "active_series_custom_trackers" => {
                    limits.active_series_custom_trackers =
                        Arc::new(CustomTrackers::from_value(value).with_context(context)?)
                }
                "active_series_additional_custom_trackers" => {
                    limits.active_series_additional_custom_trackers =
                        Arc::new(CustomTrackers::from_value(value).with_context(context)?)
                }
                "max_active_series_additional_custom_trackers" => {
                    limits.max_active_series_additional_custom_trackers =
                        int_value(value).with_context(context)?
                }
                "cost_attribution_trackers" => {
                    limits.cost_attribution_trackers =
                        Arc::new(CostAttributionTrackers::from_value(value).with_context(context)?)
                }
                "additional_cost_attribution_trackers" => {
                    limits.additional_cost_attribution_trackers =
                        Arc::new(CostAttributionTrackers::from_value(value).with_context(context)?)
                }
                "max_cost_attribution_cardinality" => {
                    limits.max_cost_attribution_cardinality =
                        int_value(value).with_context(context)?
                }
                "cost_attribution_cooldown" => {
                    limits.cost_attribution_cooldown_ms =
                        duration_value(value).with_context(context)?
                }
                _ => {}
            }
        }
        limits.validate()?;
        Ok(limits)
    }

    // Like Mimir's `Limits.Validate`, an invalid tenant fails the whole runtime config load.
    fn validate(&self) -> Result<()> {
        let max = self.max_active_series_additional_custom_trackers;
        if max < 0 {
            bail!(
                "active_series_additional_custom_trackers validation failed: invalid max custom trackers limit: {max}"
            );
        }
        let count = self.active_series_additional_custom_trackers.len();
        if max > 0 && count as i64 > max {
            bail!(
                "active_series_additional_custom_trackers validation failed: the number of custom trackers [{count}] exceeds the configured limit [{max}]"
            );
        }
        Ok(())
    }

    /// Base trackers with the additional ones on top, like `ActiveSeriesCustomTrackersConfig`.
    pub fn custom_trackers(&self) -> Arc<CustomTrackers> {
        if self.active_series_additional_custom_trackers.is_empty() {
            return Arc::clone(&self.active_series_custom_trackers);
        }
        if self.active_series_custom_trackers.is_empty() {
            return Arc::clone(&self.active_series_additional_custom_trackers);
        }
        Arc::new(
            self.active_series_custom_trackers
                .merged(&self.active_series_additional_custom_trackers),
        )
    }

    pub fn cost_attribution(&self) -> Arc<CostAttributionTrackers> {
        if self.additional_cost_attribution_trackers.is_empty() {
            return Arc::clone(&self.cost_attribution_trackers);
        }
        Arc::new(
            self.cost_attribution_trackers
                .merged(&self.additional_cost_attribution_trackers),
        )
    }
}

fn int_value(value: &Value) -> Result<i64> {
    match value {
        // YAML writes large limits such as 4e+08 as floats.
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_f64().map(|float| float as i64))
            .context("not a number"),
        Value::String(text) => Ok(text.trim().parse::<f64>()? as i64),
        Value::Null => Ok(0),
        other => bail!("expected a number, got {other}"),
    }
}

fn bool_value(value: &Value) -> Result<bool> {
    match value {
        Value::Bool(value) => Ok(*value),
        Value::String(text) => Ok(text.parse()?),
        other => bail!("expected a boolean, got {other}"),
    }
}

fn duration_value(value: &Value) -> Result<i64> {
    match value {
        Value::String(text) => parse_duration_ms(text),
        Value::Number(_) | Value::Null => int_value(value),
        other => bail!("expected a duration, got {other}"),
    }
}

/// Mimir's per-ingester instance limits, from `-ingester.instance-limits.*` with the runtime
/// config's `ingester_limits` on top. Only reported, like the other admission limits.
#[derive(Clone, Debug, PartialEq)]
pub struct InstanceLimits {
    pub max_ingestion_rate: f64,
    pub max_tenants: i64,
    pub max_series: i64,
    pub max_inflight_push_requests: i64,
    pub max_inflight_push_requests_bytes: i64,
}

impl Default for InstanceLimits {
    fn default() -> Self {
        Self {
            max_ingestion_rate: 0.0,
            max_tenants: 0,
            max_series: 0,
            max_inflight_push_requests: 30_000,
            max_inflight_push_requests_bytes: 0,
        }
    }
}

#[derive(clap::Args, Clone, Debug)]
pub struct InstanceLimitsArgs {
    #[arg(
        long = "ingester.instance-limits.max-ingestion-rate",
        default_value_t = 0.0
    )]
    pub max_ingestion_rate: f64,
    #[arg(long = "ingester.instance-limits.max-tenants", default_value_t = 0)]
    pub max_tenants: i64,
    #[arg(long = "ingester.instance-limits.max-series", default_value_t = 0)]
    pub max_series: i64,
    #[arg(
        long = "ingester.instance-limits.max-inflight-push-requests",
        default_value_t = 30_000
    )]
    pub max_inflight_push_requests: i64,
    #[arg(
        long = "ingester.instance-limits.max-inflight-push-requests-bytes",
        default_value_t = 0
    )]
    pub max_inflight_push_requests_bytes: i64,
}

impl InstanceLimitsArgs {
    pub fn to_limits(&self) -> InstanceLimits {
        InstanceLimits {
            max_ingestion_rate: self.max_ingestion_rate,
            max_tenants: self.max_tenants,
            max_series: self.max_series,
            max_inflight_push_requests: self.max_inflight_push_requests,
            max_inflight_push_requests_bytes: self.max_inflight_push_requests_bytes,
        }
    }
}

impl InstanceLimits {
    fn with_overrides(&self, overrides: &Map<String, Value>) -> Result<Self> {
        let mut limits = self.clone();
        for (field, value) in overrides {
            let context = || format!("ingester_limits.{field}");
            match field.as_str() {
                "max_ingestion_rate" => {
                    limits.max_ingestion_rate = match value {
                        Value::Number(number) => number.as_f64().context("not a number")?,
                        other => int_value(other)? as f64,
                    }
                }
                "max_tenants" => limits.max_tenants = int_value(value).with_context(context)?,
                "max_series" => limits.max_series = int_value(value).with_context(context)?,
                "max_inflight_push_requests" => {
                    limits.max_inflight_push_requests = int_value(value).with_context(context)?
                }
                "max_inflight_push_requests_bytes" => {
                    limits.max_inflight_push_requests_bytes =
                        int_value(value).with_context(context)?
                }
                _ => {}
            }
        }
        Ok(limits)
    }
}

/// A tenant's limits, resolved once per runtime config reload.
pub struct TenantLimits {
    pub limits: Limits,
    pub custom_trackers: Arc<CustomTrackers>,
    pub cost_attribution: Arc<CostAttributionTrackers>,
}

impl TenantLimits {
    fn new(limits: Limits) -> Self {
        Self {
            custom_trackers: limits.custom_trackers(),
            cost_attribution: limits.cost_attribution(),
            limits,
        }
    }
}

/// Defaults from flags plus per-tenant overrides from the runtime config.
pub struct Overrides {
    defaults: Limits,
    instance_defaults: InstanceLimits,
    instance: RwLock<InstanceLimits>,
    default_tenant: Arc<TenantLimits>,
    tenants: RwLock<Arc<HashMap<String, Arc<TenantLimits>>>>,
    // Bumped on every change, so cached per-series tracker matches know to recompute.
    generation: AtomicU64,
    // Active partitions in the partition ring; 0 until known.
    active_partitions: AtomicU64,
}

impl Default for Overrides {
    fn default() -> Self {
        Self::new(Limits::default())
    }
}

impl Overrides {
    pub fn new(defaults: Limits) -> Self {
        Self::with_instance_limits(defaults, InstanceLimits::default())
    }

    pub fn with_instance_limits(defaults: Limits, instance: InstanceLimits) -> Self {
        Self {
            default_tenant: Arc::new(TenantLimits::new(defaults.clone())),
            defaults,
            instance: RwLock::new(instance.clone()),
            instance_defaults: instance,
            tenants: RwLock::new(Arc::default()),
            generation: AtomicU64::new(1),
            active_partitions: AtomicU64::new(0),
        }
    }

    pub fn tenant(&self, tenant: &str) -> Arc<TenantLimits> {
        let tenants = self.tenants.read().expect("overrides lock poisoned");
        tenants
            .get(tenant)
            .map_or_else(|| Arc::clone(&self.default_tenant), Arc::clone)
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn instance_limits(&self) -> InstanceLimits {
        self.instance
            .read()
            .expect("overrides lock poisoned")
            .clone()
    }

    /// Replaces every tenant's overrides with the `overrides` map of a merged runtime config, and
    /// the instance limits with its `ingester_limits`.
    pub fn apply_runtime_config(&self, config: &Map<String, Value>) -> Result<()> {
        let instance = match config.get("ingester_limits") {
            Some(Value::Object(fields)) => self.instance_defaults.with_overrides(fields)?,
            _ => self.instance_defaults.clone(),
        };
        let mut tenants = HashMap::new();
        if let Some(overrides) = config.get("overrides") {
            let overrides = match overrides {
                Value::Object(overrides) => overrides,
                Value::Null => &Map::new(),
                other => bail!("overrides must be a map, got {other}"),
            };
            for (tenant, value) in overrides {
                let limits = match value {
                    Value::Object(fields) => self
                        .defaults
                        .with_overrides(fields)
                        .with_context(|| format!("tenant {tenant}"))?,
                    Value::Null => self.defaults.clone(),
                    other => bail!("overrides for tenant {tenant} must be a map, got {other}"),
                };
                tenants.insert(tenant.clone(), Arc::new(TenantLimits::new(limits)));
            }
        }
        *self.tenants.write().expect("overrides lock poisoned") = Arc::new(tenants);
        *self.instance.write().expect("overrides lock poisoned") = instance;
        self.generation.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub fn set_active_partitions(&self, partitions: u64) {
        self.active_partitions.store(partitions, Ordering::Relaxed);
    }

    /// Converts a global limit to this partition's share, like Mimir's
    /// `partitionRingLimiterStrategy`: 0 when the limit is disabled or no partition is known.
    pub fn local_limit(&self, limits: &Limits, global: i64) -> i64 {
        if global == 0 {
            return 0;
        }
        let active = self.active_partitions.load(Ordering::Relaxed) as i64;
        let shard = limits.ingestion_partitions_tenant_shard_size;
        let partitions = if shard <= 0 || shard > active {
            active
        } else {
            shard
        };
        if partitions == 0 {
            return 0;
        }
        (global as f64 / partitions as f64) as i64
    }

    /// 0 disables exemplars; an unknown local share falls back to the global limit.
    pub fn max_exemplars(&self, limits: &Limits) -> usize {
        let global = limits.max_global_exemplars_per_user;
        let local = self.local_limit(limits, global);
        let limit = if local > 0 { local } else { global };
        usize::try_from(limit).unwrap_or(0)
    }

    /// Like `Limiter.maxSeriesPerUser`, 0 meaning unlimited as `math.MaxInt32`.
    pub fn max_series_per_user(&self, limits: &Limits) -> usize {
        or_unlimited(self.local_limit(limits, limits.max_global_series_per_user))
    }

    pub fn max_metadata_per_user(&self, limits: &Limits) -> usize {
        or_unlimited(self.local_limit(limits, limits.max_global_metadata_per_user))
    }

    pub fn max_metadata_per_metric(&self, limits: &Limits) -> usize {
        or_unlimited(self.local_limit(limits, limits.max_global_metadata_per_metric))
    }
}

fn or_unlimited(local: i64) -> usize {
    if local <= 0 {
        i32::MAX as usize
    } else {
        local as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_prometheus_durations() {
        assert_eq!(parse_duration_ms("0").unwrap(), 0);
        assert_eq!(parse_duration_ms("2h").unwrap(), 7_200_000);
        assert_eq!(parse_duration_ms("1h30m").unwrap(), 5_400_000);
        assert_eq!(parse_duration_ms("10000d").unwrap(), 10_000 * 86_400_000);
        assert_eq!(parse_duration_ms("250ms").unwrap(), 250);
        assert!(parse_duration_ms("5").is_err());
        assert!(parse_duration_ms("h").is_err());
    }

    fn overrides_yaml(yaml: &str) -> Map<String, Value> {
        let value: Value = serde_yaml::from_str(yaml).unwrap();
        value.as_object().unwrap().clone()
    }

    #[test]
    fn tenant_overrides_replace_only_their_fields() {
        let defaults = LimitsArgs {
            out_of_order_time_window: "2h".into(),
            creation_grace_period: "10m".into(),
            past_grace_period: "0".into(),
            native_histograms_ingestion_enabled: false,
            max_global_series_per_user: 150_000,
            max_global_exemplars_per_user: 100_000,
            max_global_metadata_per_user: 30_000,
            max_global_metadata_per_metric: 10,
            ingestion_partitions_tenant_shard_size: 1,
            active_series_custom_trackers: vec![r#"a:{job="a"};b:{job="b"}"#.into()],
            cost_attribution_trackers: None,
            max_active_series_additional_custom_trackers: 0,
            max_cost_attribution_cardinality: 2000,
            cost_attribution_cooldown: "0".into(),
        }
        .to_limits()
        .unwrap();
        let overrides = Overrides::new(defaults);
        // Large YAML limits are floats, and numeric tenant IDs are map keys.
        overrides
            .apply_runtime_config(&overrides_yaml(
                r#"
ingester_limits:
  max_series: 3e+06
overrides:
  "18657":
    max_global_exemplars_per_user: 2.5e+06
    native_histograms_ingestion_enabled: true
    ingestion_partitions_tenant_shard_size: 3091
    active_series_additional_custom_trackers:
      c: '{job="c"}'
  9960:
    out_of_order_time_window: 30m
"#,
            ))
            .unwrap();
        let big = overrides.tenant("18657");
        assert_eq!(big.limits.max_global_exemplars_per_user, 2_500_000);
        assert!(big.limits.native_histograms_ingestion_enabled);
        assert_eq!(big.limits.out_of_order_time_window_ms, 7_200_000);
        assert_eq!(big.limits.max_global_metadata_per_user, 30_000);
        assert_eq!(big.custom_trackers.names(), ["a", "b", "c"]);
        let small = overrides.tenant("9960");
        assert_eq!(small.limits.out_of_order_time_window_ms, 30 * 60_000);
        assert!(!small.limits.native_histograms_ingestion_enabled);
        let other = overrides.tenant("other");
        assert_eq!(other.limits.max_global_exemplars_per_user, 100_000);
        assert_eq!(other.custom_trackers.names(), ["a", "b"]);
    }

    #[test]
    fn reload_drops_removed_tenants() {
        let overrides = Overrides::default();
        let before = overrides.generation();
        overrides
            .apply_runtime_config(&overrides_yaml(
                "overrides:\n  t:\n    max_global_exemplars_per_user: 5\n",
            ))
            .unwrap();
        assert_eq!(
            overrides.tenant("t").limits.max_global_exemplars_per_user,
            5
        );
        assert!(overrides.generation() > before);
        overrides
            .apply_runtime_config(&overrides_yaml("overrides: {}\n"))
            .unwrap();
        assert_eq!(
            overrides.tenant("t").limits.max_global_exemplars_per_user,
            0
        );
    }

    #[test]
    fn rejects_invalid_override_values() {
        let overrides = Overrides::default();
        assert!(
            overrides
                .apply_runtime_config(&overrides_yaml(
                    "overrides:\n  t:\n    out_of_order_time_window: soon\n",
                ))
                .is_err()
        );
    }

    #[test]
    fn converts_global_limits_like_the_partition_ring_strategy() {
        let overrides = Overrides::default();
        let mut limits = Limits {
            max_global_exemplars_per_user: 100,
            max_global_metadata_per_user: 30,
            ..Limits::default()
        };
        // No known partitions: exemplars fall back to the global limit, metadata is unlimited.
        assert_eq!(overrides.max_exemplars(&limits), 100);
        assert_eq!(overrides.max_metadata_per_user(&limits), i32::MAX as usize);
        overrides.set_active_partitions(10);
        assert_eq!(overrides.max_exemplars(&limits), 10);
        assert_eq!(overrides.max_metadata_per_user(&limits), 3);
        limits.ingestion_partitions_tenant_shard_size = 3;
        assert_eq!(overrides.max_exemplars(&limits), 33);
        // A shard size above the active partitions uses all of them.
        limits.ingestion_partitions_tenant_shard_size = 3091;
        assert_eq!(overrides.max_exemplars(&limits), 10);
        // 0 means unlimited metadata and disabled exemplars.
        limits.max_global_exemplars_per_user = 0;
        limits.max_global_metadata_per_metric = 0;
        assert_eq!(overrides.max_exemplars(&limits), 0);
        assert_eq!(
            overrides.max_metadata_per_metric(&limits),
            i32::MAX as usize
        );
    }

    #[test]
    fn instance_limits_come_from_flags_and_the_runtime_config() {
        let overrides = Overrides::with_instance_limits(
            Limits::default(),
            InstanceLimits {
                max_tenants: 7,
                ..InstanceLimits::default()
            },
        );
        overrides
            .apply_runtime_config(&overrides_yaml(
                "ingester_limits:\n  max_series: 3e+06\n  max_tenants: 500\n",
            ))
            .unwrap();
        let limits = overrides.instance_limits();
        assert_eq!(limits.max_series, 3_000_000);
        assert_eq!(limits.max_tenants, 500);
        assert_eq!(limits.max_inflight_push_requests, 30_000);
        overrides.apply_runtime_config(&Map::new()).unwrap();
        assert_eq!(overrides.instance_limits().max_tenants, 7);
    }

    #[test]
    fn rejects_more_additional_trackers_than_allowed() {
        let overrides = Overrides::new(Limits {
            max_active_series_additional_custom_trackers: 1,
            ..Limits::default()
        });
        let yaml = "overrides:\n  t:\n    active_series_additional_custom_trackers:\n      a: '{x=\"1\"}'\n      b: '{x=\"2\"}'\n";
        let error = overrides
            .apply_runtime_config(&overrides_yaml(yaml))
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("exceeds the configured limit [1]"),
            "{error:#}"
        );
        overrides
            .apply_runtime_config(&overrides_yaml(&yaml.replace("      b: '{x=\"2\"}'\n", "")))
            .unwrap();
    }
}
