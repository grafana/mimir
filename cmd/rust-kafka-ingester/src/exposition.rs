//! The Prometheus protobuf exposition format, `io.prometheus.client.MetricFamily` messages each
//! prefixed by its length. Scrapers only read native histograms from it.

use prost::Message;

use crate::native_histogram;

pub const PROTOBUF_CONTENT_TYPE: &str =
    "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited";

// The messages of Prometheus's `io/prometheus/client/metrics.proto`, a proto2 file: fields have
// presence and repeated scalars are not packed.
#[derive(Clone, PartialEq, Message)]
pub struct LabelPair {
    #[prost(string, optional, tag = "1")]
    pub name: Option<String>,
    #[prost(string, optional, tag = "2")]
    pub value: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Gauge {
    #[prost(double, optional, tag = "1")]
    pub value: Option<f64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Counter {
    #[prost(double, optional, tag = "1")]
    pub value: Option<f64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Quantile {
    #[prost(double, optional, tag = "1")]
    pub quantile: Option<f64>,
    #[prost(double, optional, tag = "2")]
    pub value: Option<f64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Summary {
    #[prost(uint64, optional, tag = "1")]
    pub sample_count: Option<u64>,
    #[prost(double, optional, tag = "2")]
    pub sample_sum: Option<f64>,
    #[prost(message, repeated, tag = "3")]
    pub quantile: Vec<Quantile>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Untyped {
    #[prost(double, optional, tag = "1")]
    pub value: Option<f64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Bucket {
    #[prost(uint64, optional, tag = "1")]
    pub cumulative_count: Option<u64>,
    #[prost(double, optional, tag = "2")]
    pub upper_bound: Option<f64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct BucketSpan {
    #[prost(sint32, optional, tag = "1")]
    pub offset: Option<i32>,
    #[prost(uint32, optional, tag = "2")]
    pub length: Option<u32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Histogram {
    #[prost(uint64, optional, tag = "1")]
    pub sample_count: Option<u64>,
    #[prost(double, optional, tag = "2")]
    pub sample_sum: Option<f64>,
    #[prost(message, repeated, tag = "3")]
    pub bucket: Vec<Bucket>,
    #[prost(sint32, optional, tag = "5")]
    pub schema: Option<i32>,
    #[prost(double, optional, tag = "6")]
    pub zero_threshold: Option<f64>,
    #[prost(uint64, optional, tag = "7")]
    pub zero_count: Option<u64>,
    #[prost(message, repeated, tag = "9")]
    pub negative_span: Vec<BucketSpan>,
    #[prost(sint64, repeated, packed = "false", tag = "10")]
    pub negative_delta: Vec<i64>,
    #[prost(message, repeated, tag = "12")]
    pub positive_span: Vec<BucketSpan>,
    #[prost(sint64, repeated, packed = "false", tag = "13")]
    pub positive_delta: Vec<i64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Metric {
    #[prost(message, repeated, tag = "1")]
    pub label: Vec<LabelPair>,
    #[prost(message, optional, tag = "2")]
    pub gauge: Option<Gauge>,
    #[prost(message, optional, tag = "3")]
    pub counter: Option<Counter>,
    #[prost(message, optional, tag = "4")]
    pub summary: Option<Summary>,
    #[prost(message, optional, tag = "5")]
    pub untyped: Option<Untyped>,
    #[prost(int64, optional, tag = "6")]
    pub timestamp_ms: Option<i64>,
    #[prost(message, optional, tag = "7")]
    pub histogram: Option<Histogram>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum MetricType {
    Counter = 0,
    Gauge = 1,
    Summary = 2,
    Untyped = 3,
    Histogram = 4,
}

#[derive(Clone, PartialEq, Message)]
pub struct MetricFamily {
    #[prost(string, optional, tag = "1")]
    pub name: Option<String>,
    #[prost(string, optional, tag = "2")]
    pub help: Option<String>,
    #[prost(enumeration = "MetricType", optional, tag = "3")]
    pub r#type: Option<i32>,
    #[prost(message, repeated, tag = "4")]
    pub metric: Vec<Metric>,
}

/// Whether an `Accept` header prefers the protobuf format, weighing media ranges by `q` like
/// Prometheus's negotiation; ties go to the first.
pub fn accepts_protobuf(accept: &str) -> bool {
    let mut best: Option<(f64, bool)> = None;
    for range in accept.split(',') {
        let mut parts = range.split(';').map(str::trim);
        let media = parts.next().unwrap_or("");
        let (mut q, mut proto, mut delimited) = (1.0, false, false);
        for parameter in parts {
            match parameter.split_once('=').map(|(k, v)| (k.trim(), v.trim())) {
                Some(("q", value)) => q = value.parse().unwrap_or(0.0),
                Some(("proto", value)) => proto = value == "io.prometheus.client.MetricFamily",
                Some(("encoding", value)) => delimited = value == "delimited",
                _ => {}
            }
        }
        let protobuf = media == "application/vnd.google.protobuf" && proto && delimited;
        if q > 0.0 && best.is_none_or(|(best, _)| q > best) {
            best = Some((q, protobuf));
        }
    }
    best.is_some_and(|(_, protobuf)| protobuf)
}

fn label_pairs(labels: impl IntoIterator<Item = (String, String)>) -> Vec<LabelPair> {
    labels
        .into_iter()
        .map(|(name, value)| LabelPair {
            name: Some(name),
            value: Some(value),
        })
        .collect()
}

fn spans(spans: Vec<(i32, u32)>) -> Vec<BucketSpan> {
    spans
        .into_iter()
        .map(|(offset, length)| BucketSpan {
            offset: Some(offset),
            length: Some(length),
        })
        .collect()
}

fn native_family(family: &prometheus::proto::MetricFamily) -> Option<MetricFamily> {
    let snapshots = native_histogram::snapshots(family.name())?;
    let metric = snapshots
        .into_iter()
        .map(|(labels, snapshot)| Metric {
            label: label_pairs(labels),
            histogram: Some(Histogram {
                sample_count: Some(snapshot.count),
                sample_sum: Some(snapshot.sum),
                bucket: snapshot
                    .classic
                    .into_iter()
                    .map(|(bound, count)| Bucket {
                        cumulative_count: Some(count),
                        upper_bound: Some(bound),
                    })
                    .collect(),
                schema: Some(snapshot.schema),
                zero_threshold: Some(native_histogram::ZERO_THRESHOLD),
                zero_count: Some(snapshot.zero_count),
                negative_span: spans(snapshot.negative.0),
                negative_delta: snapshot.negative.1,
                positive_span: spans(snapshot.positive.0),
                positive_delta: snapshot.positive.1,
            }),
            ..Metric::default()
        })
        .collect();
    Some(MetricFamily {
        name: Some(family.name().to_owned()),
        help: Some(family.help().to_owned()),
        r#type: Some(MetricType::Histogram as i32),
        metric,
    })
}

fn convert(family: &prometheus::proto::MetricFamily) -> MetricFamily {
    use prometheus::proto::MetricType as Type;
    if let Some(native) = native_family(family) {
        return native;
    }
    let r#type = match family.get_field_type() {
        Type::COUNTER => MetricType::Counter,
        Type::GAUGE => MetricType::Gauge,
        Type::SUMMARY => MetricType::Summary,
        Type::UNTYPED => MetricType::Untyped,
        Type::HISTOGRAM => MetricType::Histogram,
    };
    let metric = family
        .get_metric()
        .iter()
        .map(|metric| {
            let mut converted = Metric {
                label: label_pairs(
                    metric
                        .get_label()
                        .iter()
                        .map(|pair| (pair.name().to_owned(), pair.value().to_owned())),
                ),
                timestamp_ms: (metric.timestamp_ms() != 0).then(|| metric.timestamp_ms()),
                ..Metric::default()
            };
            match r#type {
                MetricType::Counter => {
                    converted.counter = Some(Counter {
                        value: Some(metric.get_counter().get_value()),
                    })
                }
                MetricType::Gauge => {
                    converted.gauge = Some(Gauge {
                        value: Some(metric.get_gauge().get_value()),
                    })
                }
                // The crate deprecates untyped values without a replacement, but still gathers them.
                #[allow(deprecated)]
                MetricType::Untyped => {
                    converted.untyped = Some(Untyped {
                        value: Some(metric.get_untyped().get_value()),
                    })
                }
                MetricType::Summary => {
                    let summary = metric.get_summary();
                    converted.summary = Some(Summary {
                        sample_count: Some(summary.sample_count()),
                        sample_sum: Some(summary.sample_sum()),
                        quantile: summary
                            .get_quantile()
                            .iter()
                            .map(|quantile| Quantile {
                                quantile: Some(quantile.quantile()),
                                value: Some(quantile.value()),
                            })
                            .collect(),
                    })
                }
                MetricType::Histogram => {
                    let histogram = metric.get_histogram();
                    converted.histogram = Some(Histogram {
                        sample_count: Some(histogram.get_sample_count()),
                        sample_sum: Some(histogram.get_sample_sum()),
                        bucket: histogram
                            .get_bucket()
                            .iter()
                            .map(|bucket| Bucket {
                                cumulative_count: Some(bucket.cumulative_count()),
                                upper_bound: Some(bucket.upper_bound()),
                            })
                            .collect(),
                        ..Histogram::default()
                    })
                }
            }
            converted
        })
        .collect();
    MetricFamily {
        name: Some(family.name().to_owned()),
        help: Some(family.help().to_owned()),
        r#type: Some(r#type as i32),
        metric,
    }
}

/// `families` in the protobuf format, with native buckets for the native histogram families.
pub fn encode(families: &[prometheus::proto::MetricFamily]) -> Vec<u8> {
    let mut buffer = Vec::new();
    for family in families {
        convert(family)
            .encode_length_delimited(&mut buffer)
            .expect("encode metric family");
    }
    buffer
}

/// Length-prefixed metric families from another process, without the ones `drop` names. Kept as
/// they came, so fields this module doesn't model, such as exemplars, pass through.
pub fn filter_families(mut data: &[u8], drop: impl Fn(&str) -> bool) -> Option<Vec<u8>> {
    let mut output = Vec::with_capacity(data.len());
    while !data.is_empty() {
        let start = data;
        let length = prost::encoding::decode_varint(&mut data).ok()? as usize;
        let message = data.get(..length)?;
        data = &data[length..];
        let name = family_name(message)?;
        if !drop(&name) {
            output.extend_from_slice(&start[..start.len() - data.len()]);
        }
    }
    Some(output)
}

fn family_name(mut message: &[u8]) -> Option<String> {
    use prost::encoding::{DecodeContext, WireType, decode_key, skip_field};
    while !message.is_empty() {
        let (tag, wire_type) = decode_key(&mut message).ok()?;
        if tag == 1 && wire_type == WireType::LengthDelimited {
            let length = prost::encoding::decode_varint(&mut message).ok()? as usize;
            return String::from_utf8(message.get(..length)?.to_vec()).ok();
        }
        skip_field(wire_type, tag, &mut message, DecodeContext::default()).ok()?;
    }
    Some(String::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiates_like_prometheus() {
        // Prometheus with native histograms enabled, then without.
        assert!(accepts_protobuf(
            "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=0.5,application/openmetrics-text;version=1.0.0;q=0.4,text/plain;version=0.0.4;q=0.3,*/*;q=0.2"
        ));
        assert!(!accepts_protobuf(
            "application/openmetrics-text;version=1.0.0;q=0.5,text/plain;version=0.0.4;q=0.3,*/*;q=0.2"
        ));
        assert!(!accepts_protobuf(
            "text/plain;q=0.9,application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=0.5"
        ));
        assert!(!accepts_protobuf(
            "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily"
        ));
        assert!(!accepts_protobuf(""));
    }

    #[test]
    fn exposes_native_buckets_and_other_families() {
        let registry = prometheus::Registry::new();
        let durations = native_histogram::NativeHistogramVec::new(
            "exposition_test_duration_seconds",
            "Test durations.",
            vec![0.005, 0.01],
            &["route"],
            1.1,
            100,
            std::time::Duration::from_secs(3600),
        )
        .unwrap();
        registry.register(Box::new(durations.clone())).unwrap();
        let counter =
            prometheus::IntCounter::new("exposition_test_total", "Test counter.").unwrap();
        registry.register(Box::new(counter.clone())).unwrap();
        counter.inc_by(3);
        for value in [0.001, 0.001, 0.007] {
            durations.with_label_values(&["/a"]).observe(value);
        }
        let mut data = &encode(&registry.gather())[..];
        let mut families = Vec::new();
        while !data.is_empty() {
            families.push(MetricFamily::decode_length_delimited(&mut data).unwrap());
        }
        let family = |name: &str| {
            families
                .iter()
                .find(|family| family.name.as_deref() == Some(name))
                .unwrap()
        };
        assert_eq!(
            family("exposition_test_total").metric[0].counter,
            Some(Counter { value: Some(3.0) })
        );
        let histogram = family("exposition_test_duration_seconds").metric[0]
            .histogram
            .clone()
            .unwrap();
        assert_eq!(histogram.sample_count, Some(3));
        assert_eq!(histogram.schema, Some(3));
        assert_eq!(
            histogram.zero_threshold,
            Some(native_histogram::ZERO_THRESHOLD)
        );
        // 0.001 and 0.007 are 22 buckets apart at schema 3: two spans.
        assert_eq!(histogram.positive_span.len(), 2);
        assert_eq!(histogram.positive_delta, [2, -1]);
        assert_eq!(
            histogram
                .bucket
                .iter()
                .map(|b| b.cumulative_count)
                .collect::<Vec<_>>(),
            [Some(2), Some(3)]
        );
        // The text format keeps the classic buckets.
        use prometheus::Encoder as _;
        let mut text = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&registry.gather(), &mut text)
            .unwrap();
        let text = String::from_utf8(text).unwrap();
        assert!(
            text.contains("exposition_test_duration_seconds_bucket{route=\"/a\",le=\"0.005\"} 2")
        );
        assert!(text.contains("exposition_test_duration_seconds_count{route=\"/a\"} 3"));
    }

    #[test]
    fn filters_families_as_they_came() {
        let mut data = Vec::new();
        for name in [
            "process_cpu_seconds_total",
            "memberlist_members",
            "process_open_fds",
        ] {
            MetricFamily {
                name: Some(name.into()),
                help: Some("help".into()),
                r#type: Some(MetricType::Gauge as i32),
                metric: vec![Metric {
                    gauge: Some(Gauge { value: Some(1.0) }),
                    ..Metric::default()
                }],
            }
            .encode_length_delimited(&mut data)
            .unwrap();
        }
        let kept = filter_families(&data, |name| name.starts_with("process_")).unwrap();
        let family = MetricFamily::decode_length_delimited(&kept[..]).unwrap();
        assert_eq!(family.name.as_deref(), Some("memberlist_members"));
        assert_eq!(kept.len(), family.encoded_len() + 1);
        assert_eq!(
            filter_families(&data[..data.len() - 1], |_| false),
            None,
            "truncated"
        );
    }
}
