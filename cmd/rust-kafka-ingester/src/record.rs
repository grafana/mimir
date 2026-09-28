use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use prost::Message;

use crate::proto::cortexpb;

const SYMBOL_OFFSET: usize = 64;
const COMMON_SYMBOLS: &[&str] = &[
    "",
    "__name__",
    "__aggregation__",
    "<aggregated>",
    "le",
    "component",
    "cortex_request_duration_seconds_bucket",
    "storage_operation_duration_seconds_bucket",
    "grafana",
    "asserts_env",
    "asserts_request_context",
    "asserts_source",
    "asserts_entity_type",
    "asserts_request_type",
    "name",
    "image",
    "cluster",
    "namespace",
    "pod",
    "job",
    "instance",
    "container",
    "replicaset",
    "interface",
    "status_code",
    "resource",
    "operation",
    "method",
    "kube-system",
    "kube-system/cadvisor",
    "node-exporter",
    "node-exporter/node-exporter",
    "kube-system/kubelet",
    "kube-system/node-local-dns",
    "kube-state-metrics/kube-state-metrics",
    "default/kubernetes",
];

/// A label name or value from a record, sharing the record's buffer: records carry every label
/// of every series, and most belong to series the store already has, so copying each one into
/// its own allocation was most of the decoding cost.
#[derive(Clone, Default)]
pub struct LabelStr(Bytes);

impl LabelStr {
    /// Invalid UTF-8 is replaced like `String::from_utf8_lossy`.
    pub fn from_utf8_lossy(bytes: Bytes) -> Self {
        match std::str::from_utf8(&bytes) {
            Ok(_) => Self(bytes),
            Err(_) => Self::from(String::from_utf8_lossy(&bytes).into_owned()),
        }
    }

    pub fn as_str(&self) -> &str {
        // SAFETY: built only from valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}

impl Deref for LabelStr {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for LabelStr {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for LabelStr {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<String> for LabelStr {
    fn from(value: String) -> Self {
        Self(Bytes::from(value))
    }
}

impl From<&str> for LabelStr {
    fn from(value: &str) -> Self {
        Self(Bytes::copy_from_slice(value.as_bytes()))
    }
}

impl From<&String> for LabelStr {
    fn from(value: &String) -> Self {
        Self::from(value.as_str())
    }
}

impl From<LabelStr> for String {
    fn from(value: LabelStr) -> Self {
        value.as_str().to_owned()
    }
}

impl PartialEq for LabelStr {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for LabelStr {}

impl PartialEq<str> for LabelStr {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for LabelStr {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for LabelStr {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl PartialOrd for LabelStr {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LabelStr {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl Hash for LabelStr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl fmt::Debug for LabelStr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), formatter)
    }
}

impl fmt::Display for LabelStr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.as_str(), formatter)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DecodedSeries {
    pub labels: Vec<(LabelStr, LabelStr)>,
    pub samples: Vec<cortexpb::Sample>,
    pub histograms: Vec<cortexpb::Histogram>,
    pub exemplars: Vec<cortexpb::Exemplar>,
    pub created_timestamp: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DecodedRequest {
    pub source: i32,
    pub series: Vec<DecodedSeries>,
    pub metadata: Vec<cortexpb::MetricMetadata>,
}

pub fn decode_record(version: u32, bytes: &[u8]) -> Result<DecodedRequest> {
    if version > 2 {
        bail!("unsupported ingest-storage record version {version}");
    }
    // Decoding from `Bytes` makes label fields slices of one copy of the record.
    let request = cortexpb::WriteRequest::decode(Bytes::copy_from_slice(bytes))
        .context("decode WriteRequest")?;
    if version < 2 {
        return decode_v1(request);
    }
    decode_v2(request)
}

fn decode_v1(request: cortexpb::WriteRequest) -> Result<DecodedRequest> {
    let series = request
        .timeseries
        .into_iter()
        .map(|ts| DecodedSeries {
            labels: decode_pairs(ts.labels),
            samples: ts.samples,
            histograms: ts.histograms,
            exemplars: ts.exemplars,
            created_timestamp: ts.created_timestamp,
        })
        .collect();
    let metadata = request
        .metadata
        .into_iter()
        .filter_map(|mut item| {
            item.metric_family_name =
                normalize_metadata_name(&item.metric_family_name, item.r#type);
            (!item.metric_family_name.is_empty()
                && (item.r#type != 0 || !item.help.is_empty() || !item.unit.is_empty()))
            .then_some(item)
        })
        .collect();
    Ok(DecodedRequest {
        source: request.source,
        series,
        metadata,
    })
}

fn normalize_metadata_name(name: &str, metric_type: i32) -> String {
    let suffixes: &[&str] = match metric_type {
        5 => &["_count", "_sum"],
        3 => &["_bucket", "_count", "_sum"],
        4 => &["_bucket", "_gcount", "_gsum"],
        _ => &[],
    };
    suffixes
        .iter()
        .find_map(|suffix| name.strip_suffix(suffix))
        .unwrap_or(name)
        .to_owned()
}

fn decode_v2(request: cortexpb::WriteRequest) -> Result<DecodedRequest> {
    // Each symbol is converted once, and labels share it.
    let symbols = request
        .symbols_rw2
        .into_iter()
        .map(LabelStr::from)
        .collect::<Vec<_>>();
    let symbol = |reference: u32| -> Result<LabelStr> {
        let reference = reference as usize;
        if reference < COMMON_SYMBOLS.len() {
            return Ok(LabelStr(Bytes::from_static(
                COMMON_SYMBOLS[reference].as_bytes(),
            )));
        }
        if reference < SYMBOL_OFFSET {
            bail!("reserved RW2 symbol reference {reference}");
        }
        symbols
            .get(reference - SYMBOL_OFFSET)
            .cloned()
            .with_context(|| format!("RW2 symbol reference {reference} out of range"))
    };

    let mut metadata = Vec::new();
    let mut series = Vec::with_capacity(request.timeseries_rw2.len());
    for ts in request.timeseries_rw2 {
        if ts.labels_refs.len() % 2 != 0 {
            bail!("odd number of RW2 label references");
        }
        let mut labels = Vec::with_capacity(ts.labels_refs.len() / 2);
        for pair in ts.labels_refs.as_chunks::<2>().0 {
            labels.push((symbol(pair[0])?, symbol(pair[1])?));
        }
        let exemplars = ts
            .exemplars
            .into_iter()
            .map(|e| -> Result<cortexpb::Exemplar> {
                if e.labels_refs.len() % 2 != 0 {
                    bail!("odd number of RW2 exemplar label references");
                }
                let mut pairs = Vec::with_capacity(e.labels_refs.len() / 2);
                for pair in e.labels_refs.as_chunks::<2>().0 {
                    pairs.push(cortexpb::LabelPair {
                        name: symbol(pair[0])?.0,
                        value: symbol(pair[1])?.0,
                    });
                }
                Ok(cortexpb::Exemplar {
                    labels: pairs,
                    value: e.value,
                    timestamp_ms: e.timestamp,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if let Some(meta) = ts.metadata {
            let metric_name = labels
                .iter()
                .find(|(name, _)| name == "__name__")
                .map(|(_, value)| String::from(value.clone()))
                .unwrap_or_default();
            let help = String::from(symbol(meta.help_ref)?);
            let unit = String::from(symbol(meta.unit_ref)?);
            // Like Mimir's RW2 unmarshalling, every series carries a metadata field, and only
            // one that says something about a named metric becomes metadata.
            if !metric_name.is_empty() && (meta.r#type != 0 || !help.is_empty() || !unit.is_empty())
            {
                metadata.push(cortexpb::MetricMetadata {
                    r#type: meta.r#type,
                    metric_family_name: metric_name,
                    help,
                    unit,
                });
            }
        }
        series.push(DecodedSeries {
            labels,
            samples: ts.samples,
            histograms: ts.histograms,
            exemplars,
            created_timestamp: ts.created_timestamp,
        });
    }
    Ok(DecodedRequest {
        source: request.source,
        series,
        metadata,
    })
}

fn decode_pairs(pairs: Vec<cortexpb::LabelPair>) -> Vec<(LabelStr, LabelStr)> {
    pairs
        .into_iter()
        .map(|pair| {
            (
                LabelStr::from_utf8_lossy(pair.name),
                LabelStr::from_utf8_lossy(pair.value),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_share_one_copy_of_the_record_and_replace_invalid_utf8() {
        let pair = |name: &[u8], value: &[u8]| cortexpb::LabelPair {
            name: Bytes::copy_from_slice(name),
            value: Bytes::copy_from_slice(value),
        };
        let request = cortexpb::WriteRequest {
            timeseries: (0..20)
                .map(|index| cortexpb::TimeSeries {
                    labels: vec![
                        pair(b"__name__", format!("metric_{index}").as_bytes()),
                        pair(b"job", b"api"),
                    ],
                    ..Default::default()
                })
                .chain([cortexpb::TimeSeries {
                    labels: vec![pair(b"bad", b"a\xffb")],
                    ..Default::default()
                }])
                .collect(),
            ..Default::default()
        };
        let encoded = request.encode_to_vec();
        let decoded = decode_record(1, &encoded).unwrap();
        assert_eq!(
            decoded.series[3].labels[0],
            ("__name__".into(), "metric_3".into())
        );
        assert_eq!(decoded.series[20].labels[0].1, "a\u{fffd}b");
        let addresses = decoded.series[..20]
            .iter()
            .flat_map(|series| &series.labels)
            .flat_map(|(name, value)| [name.as_ptr() as usize, value.as_ptr() as usize])
            .collect::<Vec<_>>();
        let span = addresses.iter().max().unwrap() - addresses.iter().min().unwrap();
        assert!(span < encoded.len(), "labels were copied one by one");
    }
}
