//! Histograms with Prometheus native (exponential) buckets next to classic ones, like
//! client_golang's with a `NativeHistogramBucketFactor`. The Go ingester's request durations are
//! such histograms, and dashboards read their native buckets: most requests take under the first
//! classic bucket, 5 ms, where classic buckets tell nothing.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use prometheus::core::{Collector, Desc};
use prometheus::proto;

/// client_golang's `DefNativeHistogramZeroThreshold`, 2^-128.
pub const ZERO_THRESHOLD: f64 = 2.938735877055719e-39;

// client_golang's `nativeHistogramBounds`: the fractions of a power of two where each schema's
// buckets start. Copied rather than computed, so keys match Go's to the last bit.
const BOUNDS: [&[f64]; 4] = [
    &[0.5],
    &[0.5, 0.7071067811865475],
    &[
        0.5,
        0.5946035575013605,
        0.7071067811865475,
        0.8408964152537144,
    ],
    &[
        0.5,
        0.5452538663326288,
        0.5946035575013605,
        0.6484197773255048,
        0.7071067811865475,
        0.7711054127039704,
        0.8408964152537144,
        0.9170040432046711,
    ],
];
const MIN_SCHEMA: i32 = -4;

/// client_golang's `pickSchema`: the coarsest schema whose buckets grow by at most `factor`.
fn pick_schema(factor: f64) -> i32 {
    assert!(factor > 1.0, "bucket factor {factor} is <= 1");
    let floor = factor.log2().log2().floor();
    let schema = (-floor).clamp(f64::from(MIN_SCHEMA), 8.0) as i32;
    assert!(
        schema < BOUNDS.len() as i32,
        "bucket factor {factor} needs schema {schema}"
    );
    schema
}

/// Go's `math.Frexp` for finite values: `value = frac * 2^exp` with `frac` in [0.5, 1).
fn frexp(value: f64) -> (f64, i32) {
    if value == 0.0 || !value.is_finite() {
        return (value, 0);
    }
    let (value, adjust) = if value.abs() < f64::MIN_POSITIVE {
        (value * (1_u64 << 54) as f64, -54)
    } else {
        (value, 0)
    };
    let bits = value.to_bits();
    let exp = ((bits >> 52) & 0x7ff) as i32 - 1022 + adjust;
    let frac = f64::from_bits((bits & !(0x7ff << 52)) | (1022 << 52));
    (frac, exp)
}

/// The native bucket of `value`, like client_golang's `histogramCounts.observe`: bucket `key`
/// holds values in (2^((key-1)/2^schema), 2^(key/2^schema)].
fn bucket_key(value: f64, schema: i32) -> i32 {
    let infinite = value.is_infinite();
    let value = if infinite {
        f64::MAX.copysign(value)
    } else {
        value
    };
    let (frac, exp) = frexp(value.abs());
    let mut key = if schema > 0 {
        let bounds = BOUNDS[schema as usize];
        bounds.partition_point(|bound| *bound < frac) as i32 + (exp - 1) * bounds.len() as i32
    } else {
        let key = if frac == 0.5 { exp - 1 } else { exp };
        let offset = (1 << -schema) - 1;
        (key + offset) >> -schema
    };
    if infinite {
        key += 1;
    }
    key
}

/// Bucket spans and count deltas, like client_golang's `makeBuckets`: gaps of up to two empty
/// buckets stay within a span.
pub fn spans_and_deltas(buckets: &BTreeMap<i32, u64>) -> (Vec<(i32, u32)>, Vec<i64>) {
    let (mut spans, mut deltas) = (Vec::<(i32, u32)>::new(), Vec::new());
    let (mut previous, mut next) = (0_i64, 0_i32);
    for (&key, &count) in buckets {
        let gap = key - next;
        if spans.is_empty() || gap > 2 {
            spans.push((gap, 0));
        } else {
            for _ in 0..gap {
                spans.last_mut().expect("span").1 += 1;
                deltas.push(-previous);
                previous = 0;
            }
        }
        spans.last_mut().expect("span").1 += 1;
        deltas.push(count as i64 - previous);
        previous = count as i64;
        next = key + 1;
    }
    (spans, deltas)
}

struct Config {
    classic: Vec<f64>,
    schema: i32,
    max_buckets: usize,
    min_reset: Duration,
}

struct State {
    // Per classic bucket, not cumulative.
    classic: Vec<u64>,
    count: u64,
    sum: f64,
    schema: i32,
    zero_count: u64,
    positive: BTreeMap<i32, u64>,
    negative: BTreeMap<i32, u64>,
    last_reset: Instant,
    // When the resolution was reduced before `min_reset` had passed, like client_golang's
    // scheduled reset: the full resolution comes back then.
    reset_at: Option<Instant>,
}

impl State {
    fn new(config: &Config, now: Instant) -> Self {
        Self {
            classic: vec![0; config.classic.len()],
            count: 0,
            sum: 0.0,
            schema: config.schema,
            zero_count: 0,
            positive: BTreeMap::new(),
            negative: BTreeMap::new(),
            last_reset: now,
            reset_at: None,
        }
    }

    fn add(&mut self, config: &Config, value: f64) {
        let bucket = config.classic.partition_point(|bound| *bound < value);
        if let Some(count) = self.classic.get_mut(bucket) {
            *count += 1;
        }
        self.count += 1;
        self.sum += value;
        if value.is_nan() {
            return;
        }
        let key = bucket_key(value, self.schema);
        if value > ZERO_THRESHOLD {
            *self.positive.entry(key).or_default() += 1;
        } else if value < -ZERO_THRESHOLD {
            *self.negative.entry(key).or_default() += 1;
        } else {
            self.zero_count += 1;
        }
    }

    /// client_golang's `doubleBucketWidth`: halves the resolution, merging bucket pairs.
    fn double_width(&mut self) {
        if self.schema == MIN_SCHEMA {
            return;
        }
        self.schema -= 1;
        for buckets in [&mut self.positive, &mut self.negative] {
            let mut merged = BTreeMap::new();
            for (key, count) in std::mem::take(buckets) {
                // Go's integer division, which truncates toward zero.
                let key = if key > 0 { key + 1 } else { key } / 2;
                *merged.entry(key).or_default() += count;
            }
            *buckets = merged;
        }
    }
}

pub struct NativeHistogram {
    config: Arc<Config>,
    state: Mutex<State>,
}

impl NativeHistogram {
    pub fn observe(&self, value: f64) {
        self.observe_at(value, Instant::now());
    }

    fn observe_at(&self, value: f64, now: Instant) {
        let config = &self.config;
        let mut state = self.state.lock().expect("histogram lock poisoned");
        if state.reset_at.is_some_and(|at| now >= at) {
            *state = State::new(config, now);
        }
        state.add(config, value);
        if state.positive.len() + state.negative.len() <= config.max_buckets {
            return;
        }
        // client_golang's `limitBuckets`: reset when the last reset is old enough, otherwise
        // reduce the resolution until a reset is due. Mimir sets no max zero threshold, so the
        // zero bucket never widens.
        if state.reset_at.is_none() && now.duration_since(state.last_reset) >= config.min_reset {
            *state = State::new(config, now);
            state.add(config, value);
            return;
        }
        if state.reset_at.is_none() {
            state.reset_at = Some(state.last_reset + config.min_reset);
        }
        state.double_width();
    }

    pub fn get_sample_count(&self) -> u64 {
        self.state.lock().expect("histogram lock poisoned").count
    }

    pub fn snapshot(&self) -> Snapshot {
        let state = self.state.lock().expect("histogram lock poisoned");
        let mut cumulative = 0;
        Snapshot {
            count: state.count,
            sum: state.sum,
            classic: self
                .config
                .classic
                .iter()
                .zip(&state.classic)
                .map(|(bound, count)| {
                    cumulative += count;
                    (*bound, cumulative)
                })
                .collect(),
            schema: state.schema,
            zero_count: state.zero_count,
            positive: spans_and_deltas(&state.positive),
            negative: spans_and_deltas(&state.negative),
        }
    }
}

/// A series' label pairs, sorted by name, with its snapshot.
pub type SeriesSnapshot = (Vec<(String, String)>, Snapshot);

/// What a scrape exposes of a histogram.
pub struct Snapshot {
    pub count: u64,
    pub sum: f64,
    /// Upper bound and cumulative count of each classic bucket.
    pub classic: Vec<(f64, u64)>,
    pub schema: i32,
    pub zero_count: u64,
    pub positive: (Vec<(i32, u32)>, Vec<i64>),
    pub negative: (Vec<(i32, u32)>, Vec<i64>),
}

struct Inner {
    desc: Desc,
    config: Arc<Config>,
    series: Mutex<HashMap<Vec<String>, Arc<NativeHistogram>>>,
}

/// A histogram family by label values. As a `Collector` it exports the classic buckets for the
/// text format; `snapshots` also gives the native buckets for the protobuf format.
#[derive(Clone)]
pub struct NativeHistogramVec(Arc<Inner>);

// Every family with native buckets, which the protobuf exposition looks up by name.
static FAMILIES: LazyLock<Mutex<Vec<NativeHistogramVec>>> = LazyLock::new(Default::default);

impl NativeHistogramVec {
    /// Like client_golang's `HistogramOpts` with `Buckets`, `NativeHistogramBucketFactor`,
    /// `NativeHistogramMaxBucketNumber` and `NativeHistogramMinResetDuration`.
    pub fn new(
        name: &str,
        help: &str,
        classic: Vec<f64>,
        labels: &[&str],
        bucket_factor: f64,
        max_buckets: usize,
        min_reset: Duration,
    ) -> prometheus::Result<Self> {
        let desc = Desc::new(
            name.to_owned(),
            help.to_owned(),
            labels.iter().map(|label| (*label).to_owned()).collect(),
            HashMap::new(),
        )?;
        let family = Self(Arc::new(Inner {
            desc,
            config: Arc::new(Config {
                classic,
                schema: pick_schema(bucket_factor),
                max_buckets,
                min_reset,
            }),
            series: Mutex::default(),
        }));
        FAMILIES
            .lock()
            .expect("families lock poisoned")
            .push(family.clone());
        Ok(family)
    }

    pub fn with_label_values(&self, values: &[&str]) -> Arc<NativeHistogram> {
        assert_eq!(
            values.len(),
            self.0.desc.variable_labels.len(),
            "label values"
        );
        let key = values
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>();
        let mut series = self.0.series.lock().expect("series lock poisoned");
        Arc::clone(series.entry(key).or_insert_with(|| {
            Arc::new(NativeHistogram {
                config: Arc::clone(&self.0.config),
                state: Mutex::new(State::new(&self.0.config, Instant::now())),
            })
        }))
    }

    /// Each series' label pairs, sorted by name like client_golang's, with its snapshot.
    fn snapshots(&self) -> Vec<SeriesSnapshot> {
        let series = self
            .0
            .series
            .lock()
            .expect("series lock poisoned")
            .iter()
            .map(|(values, histogram)| (values.clone(), Arc::clone(histogram)))
            .collect::<Vec<_>>();
        let mut snapshots = series
            .into_iter()
            .map(|(values, histogram)| {
                let mut labels = self
                    .0
                    .desc
                    .variable_labels
                    .iter()
                    .cloned()
                    .zip(values)
                    .collect::<Vec<_>>();
                labels.sort();
                (labels, histogram.snapshot())
            })
            .collect::<Vec<_>>();
        snapshots.sort_by(|a, b| a.0.cmp(&b.0));
        snapshots
    }
}

/// The native histogram families' snapshots by name, for the protobuf exposition.
pub fn snapshots(name: &str) -> Option<Vec<SeriesSnapshot>> {
    FAMILIES
        .lock()
        .expect("families lock poisoned")
        .iter()
        .find(|family| family.0.desc.fq_name == name)
        .cloned()
        .map(|family| family.snapshots())
}

impl Collector for NativeHistogramVec {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.0.desc]
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let metrics = self
            .snapshots()
            .into_iter()
            .map(|(labels, snapshot)| {
                let mut histogram = proto::Histogram::default();
                histogram.set_sample_count(snapshot.count);
                histogram.set_sample_sum(snapshot.sum);
                histogram.set_bucket(
                    snapshot
                        .classic
                        .iter()
                        .map(|(bound, count)| {
                            let mut bucket = proto::Bucket::default();
                            bucket.set_upper_bound(*bound);
                            bucket.set_cumulative_count(*count);
                            bucket
                        })
                        .collect(),
                );
                let mut metric = proto::Metric::default();
                metric.set_label(
                    labels
                        .into_iter()
                        .map(|(name, value)| {
                            let mut pair = proto::LabelPair::default();
                            pair.set_name(name);
                            pair.set_value(value);
                            pair
                        })
                        .collect(),
                );
                metric.set_histogram(histogram);
                metric
            })
            .collect();
        let mut family = proto::MetricFamily::default();
        family.set_name(self.0.desc.fq_name.clone());
        family.set_help(self.0.desc.help.clone());
        family.set_field_type(proto::MetricType::HISTOGRAM);
        family.set_metric(metrics);
        vec![family]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn histogram() -> NativeHistogram {
        let config = Arc::new(Config {
            classic: vec![0.005, 0.01, 0.1, 1.0],
            schema: pick_schema(1.1),
            max_buckets: 100,
            min_reset: Duration::from_secs(3600),
        });
        NativeHistogram {
            state: Mutex::new(State::new(&config, Instant::now())),
            config,
        }
    }

    fn latencies() -> Vec<f64> {
        let mut values = (0..400)
            .map(|i| 0.0003 * 1.037_f64.powf(f64::from(i)))
            .collect::<Vec<_>>();
        values.extend([
            0.0,
            1.0,
            0.5,
            2.0,
            1.0905077326652577,
            0.0010000001,
            7e-7,
            3e6,
        ]);
        values
    }

    #[test]
    fn buckets_like_client_golang() {
        assert_eq!(pick_schema(1.1), 3);
        // client_golang's output for the same observations with Mimir's options: 408 values over
        // more than 100 buckets halve the resolution once.
        let histogram = histogram();
        for value in latencies() {
            histogram.observe(value);
        }
        let snapshot = histogram.snapshot();
        assert_eq!(
            (snapshot.count, snapshot.schema, snapshot.zero_count),
            (408, 2, 1)
        );
        assert_eq!(snapshot.positive.0, [(-81, 1), (34, 84), (49, 1)]);
        assert_eq!(
            snapshot.positive.1,
            [
                1, 3, 1, 0, 0, -1, 1, 0, 1, -1, -1, 1, 0, 0, -1, 1, 0, 0, -1, 1, 0, 0, 0, -1, 1, 0,
                0, -1, 1, 0, 0, -1, 1, 0, 0, 0, -1, 1, 0, 0, -1, 1, 0, 1, -2, 1, 0, 1, 0, -2, 1, 1,
                -1, -1, 1, 0, 0, -1, 1, 0, 0, 0, -1, 1, 0, 0, -1, 1, 0, 0, -1, 1, 0, 0, 0, -1, 1,
                0, 0, -1, 1, 0, 0, -1, 1, -4
            ]
        );
        assert!(snapshot.negative.0.is_empty());
        // Classic buckets stay exact alongside.
        assert_eq!(snapshot.classic.last(), Some(&(1.0, 229)));
        // Upper bounds are inclusive, like Go's.
        assert_eq!(bucket_key(1.0, 3), 0);
        assert_eq!(bucket_key(1.0905077326652577, 3), 1);
        assert_eq!(
            bucket_key(f64::from_bits(1.0905077326652577_f64.to_bits() + 1), 3),
            2
        );
        assert_eq!(bucket_key(0.001, 3), -79);
    }

    #[test]
    fn full_resolution_returns_after_the_min_reset_duration() {
        let histogram = histogram();
        let start = Instant::now();
        for value in latencies() {
            histogram.observe_at(value, start);
        }
        assert_eq!(histogram.snapshot().schema, 2);
        // The reduction scheduled a reset one min reset duration after the last one.
        let later = start + Duration::from_secs(3601);
        histogram.observe_at(0.002, later);
        let snapshot = histogram.snapshot();
        assert_eq!((snapshot.count, snapshot.schema), (1, 3));
        assert_eq!(
            snapshot.positive,
            (vec![(bucket_key(0.002, 3), 1)], vec![1])
        );
    }
}
