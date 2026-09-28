use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use prost::Message;
use prost::encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field};

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
        // Label names and values are nearly always ASCII, which is checked inline a word at a
        // time.
        if bytes.is_ascii() || std::str::from_utf8(&bytes).is_ok() {
            return Self(bytes);
        }
        Self::from(String::from_utf8_lossy(&bytes).into_owned())
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
    decode_record_bytes(version, Bytes::copy_from_slice(bytes))
}

/// Like `decode_record`, with label fields as slices of `bytes`.
pub fn decode_record_bytes(version: u32, bytes: Bytes) -> Result<DecodedRequest> {
    if version > 2 {
        bail!("unsupported ingest-storage record version {version}");
    }
    if version < 2 {
        return decode_v1(&bytes).context("decode WriteRequest");
    }
    let request = cortexpb::WriteRequest::decode(bytes).context("decode WriteRequest")?;
    decode_v2(request)
}

// Records are mostly series labels, and building prost's messages for them and converting those
// cost several times reading the fields directly. Decodes like `cortexpb::WriteRequest::decode`,
// with the rarer messages left to prost.
fn decode_v1(record: &Bytes) -> Result<DecodedRequest> {
    let ctx = DecodeContext::default();
    let mut buf = &record[..];
    let mut source = 0;
    let mut series = Vec::new();
    let mut metadata = Vec::new();
    while !buf.is_empty() {
        let (tag, wire_type) = decode_key(&mut buf)?;
        match tag {
            1 => series.push(decode_series(
                record,
                length_delimited(wire_type, &mut buf)?,
            )?),
            2 => source = varint(wire_type, &mut buf)? as i32,
            3 => metadata.push(cortexpb::MetricMetadata::decode(length_delimited(
                wire_type, &mut buf,
            )?)?),
            // Remote write 2 fields, which a version 1 record does not use but prost validates.
            4 => {
                String::from_utf8(length_delimited(wire_type, &mut buf)?.to_vec())
                    .context("invalid string value: data is not UTF-8 encoded")?;
            }
            5 => {
                cortexpb::TimeSeriesRw2::decode(length_delimited(wire_type, &mut buf)?)?;
            }
            _ => skip_field(wire_type, tag, &mut buf, ctx.clone())?,
        }
    }
    Ok(DecodedRequest {
        source,
        series,
        metadata: v1_metadata(metadata),
    })
}

fn v1_metadata(metadata: Vec<cortexpb::MetricMetadata>) -> Vec<cortexpb::MetricMetadata> {
    metadata
        .into_iter()
        .filter_map(|mut item| {
            item.metric_family_name =
                normalize_metadata_name(&item.metric_family_name, item.r#type);
            (!item.metric_family_name.is_empty()
                && (item.r#type != 0 || !item.help.is_empty() || !item.unit.is_empty()))
            .then_some(item)
        })
        .collect()
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

fn decode_series(record: &Bytes, mut buf: &[u8]) -> Result<DecodedSeries> {
    let ctx = DecodeContext::default();
    let mut series = DecodedSeries {
        labels: Vec::with_capacity(20),
        samples: Vec::new(),
        histograms: Vec::new(),
        exemplars: Vec::new(),
        created_timestamp: 0,
    };
    while !buf.is_empty() {
        let (tag, wire_type) = decode_key(&mut buf)?;
        match tag {
            1 => {
                let mut pair = length_delimited(wire_type, &mut buf)?;
                let (mut name, mut value): (&[u8], &[u8]) = (&[], &[]);
                while !pair.is_empty() {
                    let (tag, wire_type) = decode_key(&mut pair)?;
                    match tag {
                        1 => name = length_delimited(wire_type, &mut pair)?,
                        2 => value = length_delimited(wire_type, &mut pair)?,
                        _ => skip_field(wire_type, tag, &mut pair, ctx.clone())?,
                    }
                }
                series
                    .labels
                    .push((label(record, name), label(record, value)));
            }
            2 => {
                let mut sample_buf = length_delimited(wire_type, &mut buf)?;
                let mut sample = cortexpb::Sample::default();
                while !sample_buf.is_empty() {
                    let (tag, wire_type) = decode_key(&mut sample_buf)?;
                    match tag {
                        1 => {
                            check_wire_type(WireType::SixtyFourBit, wire_type)?;
                            let (bits, rest) = sample_buf
                                .split_first_chunk::<8>()
                                .context("buffer underflow")?;
                            sample.value = f64::from_le_bytes(*bits);
                            sample_buf = rest;
                        }
                        2 => sample.timestamp_ms = varint(wire_type, &mut sample_buf)? as i64,
                        _ => skip_field(wire_type, tag, &mut sample_buf, ctx.clone())?,
                    }
                }
                series.samples.push(sample);
            }
            3 => {
                let exemplar = length_delimited(wire_type, &mut buf)?;
                series
                    .exemplars
                    .push(cortexpb::Exemplar::decode(record.slice_ref(exemplar))?);
            }
            4 => series
                .histograms
                .push(cortexpb::Histogram::decode(length_delimited(
                    wire_type, &mut buf,
                )?)?),
            6 => series.created_timestamp = varint(wire_type, &mut buf)? as i64,
            _ => skip_field(wire_type, tag, &mut buf, ctx.clone())?,
        }
    }
    Ok(series)
}

fn label(record: &Bytes, bytes: &[u8]) -> LabelStr {
    if bytes.is_empty() {
        return LabelStr::default();
    }
    LabelStr::from_utf8_lossy(record.slice_ref(bytes))
}

fn check_wire_type(expected: WireType, actual: WireType) -> Result<()> {
    if expected != actual {
        bail!("invalid wire type: {actual:?} (expected {expected:?})");
    }
    Ok(())
}

fn varint(wire_type: WireType, buf: &mut &[u8]) -> Result<u64> {
    check_wire_type(WireType::Varint, wire_type)?;
    Ok(decode_varint(buf)?)
}

fn length_delimited<'a>(wire_type: WireType, buf: &mut &'a [u8]) -> Result<&'a [u8]> {
    check_wire_type(WireType::LengthDelimited, wire_type)?;
    let length = decode_varint(buf)?;
    if length > buf.len() as u64 {
        bail!("buffer underflow");
    }
    let (field, rest) = buf.split_at(length as usize);
    *buf = rest;
    Ok(field)
}

#[cfg(test)]
mod tests {
    use super::*;

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

    // What `decode_v1` must match: prost's decoding, then the same conversions.
    fn decode_v1_with_prost(record: &Bytes) -> Result<DecodedRequest> {
        let request = cortexpb::WriteRequest::decode(record.clone())?;
        Ok(DecodedRequest {
            source: request.source,
            series: request
                .timeseries
                .into_iter()
                .map(|ts| DecodedSeries {
                    labels: decode_pairs(ts.labels),
                    samples: ts.samples,
                    histograms: ts.histograms,
                    exemplars: ts.exemplars,
                    created_timestamp: ts.created_timestamp,
                })
                .collect(),
            metadata: v1_metadata(request.metadata),
        })
    }

    struct Random(u64);

    impl Random {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, bound: u64) -> u64 {
            self.next() % bound
        }

        fn bytes(&mut self) -> Vec<u8> {
            let alphabet: [&[u8]; 6] = [b"a", b"_", b"9", "\u{e9}".as_bytes(), b"\xff", b""];
            (0..self.below(6))
                .flat_map(|_| alphabet[self.below(6) as usize].to_vec())
                .collect()
        }
    }

    fn put_field(record: &mut Vec<u8>, tag: u32, field: &[u8]) {
        prost::encoding::encode_key(tag, WireType::LengthDelimited, record);
        prost::encoding::encode_varint(field.len() as u64, record);
        record.extend_from_slice(field);
    }

    fn random_record(random: &mut Random) -> Vec<u8> {
        let mut record = Vec::new();
        for _ in 0..random.below(4) {
            let series = cortexpb::TimeSeries {
                labels: (0..random.below(5))
                    .map(|_| cortexpb::LabelPair {
                        name: random.bytes().into(),
                        value: random.bytes().into(),
                    })
                    .collect(),
                samples: (0..random.below(3))
                    .map(|_| cortexpb::Sample {
                        value: random.below(1000) as f64 - 500.5,
                        timestamp_ms: random.next() as i64,
                    })
                    .collect(),
                exemplars: (0..random.below(2))
                    .map(|_| cortexpb::Exemplar {
                        labels: vec![cortexpb::LabelPair {
                            name: random.bytes().into(),
                            value: random.bytes().into(),
                        }],
                        value: 1.5,
                        timestamp_ms: random.below(100) as i64,
                    })
                    .collect(),
                histograms: (0..random.below(2))
                    .map(|_| cortexpb::Histogram {
                        sum: 2.0,
                        schema: 3,
                        positive_deltas: vec![1, -1, random.below(9) as i64],
                        timestamp: random.below(100) as i64,
                        count: Some(cortexpb::histogram::Count::CountInt(random.below(9))),
                        ..Default::default()
                    })
                    .collect(),
                created_timestamp: random.below(3) as i64,
            };
            let mut encoded = series.encode_to_vec();
            if random.below(4) == 0 {
                // A field the decoder does not know.
                prost::encoding::encode_key(7, WireType::Varint, &mut encoded);
                prost::encoding::encode_varint(random.next(), &mut encoded);
            }
            put_field(&mut record, 1, &encoded);
        }
        if random.below(2) == 0 {
            prost::encoding::encode_key(2, WireType::Varint, &mut record);
            prost::encoding::encode_varint(random.below(3), &mut record);
        }
        for _ in 0..random.below(3) {
            let metadata = cortexpb::MetricMetadata {
                r#type: random.below(6) as i32,
                metric_family_name: ["", "up", "rpc_count", "latency_bucket"]
                    [random.below(4) as usize]
                    .into(),
                help: ["", "help"][random.below(2) as usize].into(),
                unit: String::new(),
            };
            put_field(&mut record, 3, &metadata.encode_to_vec());
        }
        if random.below(4) == 0 {
            put_field(&mut record, 99, b"unknown");
        }
        record
    }

    #[test]
    fn version_1_records_decode_like_prost() {
        let mut random = Random(0x9e37_79b9_7f4a_7c15);
        let mut compared = 0;
        for _ in 0..3_000 {
            let record = random_record(&mut random);
            let mut candidates = vec![record.clone()];
            // Truncated and corrupted records must fail, or decode the same, like prost.
            for _ in 0..3 {
                if !record.is_empty() {
                    candidates.push(record[..random.below(record.len() as u64) as usize].to_vec());
                    let mut corrupted = record.clone();
                    let position = random.below(record.len() as u64) as usize;
                    corrupted[position] ^= 1 << random.below(8);
                    candidates.push(corrupted);
                }
            }
            for candidate in candidates {
                let candidate = Bytes::from(candidate);
                let expected = decode_v1_with_prost(&candidate);
                let decoded = decode_v1(&candidate);
                match (&expected, &decoded) {
                    // Corrupted floats can be NaN, which only compare equal printed.
                    (Ok(expected), Ok(decoded)) => {
                        assert_eq!(format!("{decoded:?}"), format!("{expected:?}"))
                    }
                    (Err(_), Err(_)) => {}
                    _ => panic!(
                        "prost: {expected:?}, decode_v1: {decoded:?} for {:?}",
                        &candidate[..]
                    ),
                }
                compared += 1;
            }
        }
        assert!(compared > 10_000);
    }

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
