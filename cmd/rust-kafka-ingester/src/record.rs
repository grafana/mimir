use anyhow::{Context, Result, bail};
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

#[derive(Clone, Debug, PartialEq)]
pub struct DecodedSeries {
    pub labels: Vec<(String, String)>,
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
    let request = cortexpb::WriteRequest::decode(bytes).context("decode WriteRequest")?;
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
    let symbol = |reference: u32| -> Result<String> {
        let reference = reference as usize;
        if reference < COMMON_SYMBOLS.len() {
            return Ok(COMMON_SYMBOLS[reference].to_owned());
        }
        if reference < SYMBOL_OFFSET {
            bail!("reserved RW2 symbol reference {reference}");
        }
        request
            .symbols_rw2
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
                        name: symbol(pair[0])?.into_bytes().into(),
                        value: symbol(pair[1])?.into_bytes().into(),
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
                .map(|(_, value)| value.clone())
                .unwrap_or_default();
            metadata.push(cortexpb::MetricMetadata {
                r#type: meta.r#type,
                metric_family_name: metric_name,
                help: symbol(meta.help_ref)?,
                unit: symbol(meta.unit_ref)?,
            });
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

fn decode_pairs(pairs: Vec<cortexpb::LabelPair>) -> Vec<(String, String)> {
    pairs
        .into_iter()
        .map(|pair| {
            (
                String::from_utf8_lossy(&pair.name).into_owned(),
                String::from_utf8_lossy(&pair.value).into_owned(),
            )
        })
        .collect()
}
