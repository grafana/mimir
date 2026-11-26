use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::{Deref, Range};

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

/// Short labels travel inline so each reference to a remote-write symbol needs no
/// allocation or atomic reference count. Long labels share the original bytes.
#[derive(Clone, Default)]
pub struct LabelStr(LabelStorage);

const INLINE_LABEL_BYTES: usize = 23;

#[derive(Clone)]
enum LabelStorage {
    Shared(Bytes),
    Inline {
        len: u8,
        bytes: [u8; INLINE_LABEL_BYTES],
    },
}

impl Default for LabelStorage {
    fn default() -> Self {
        Self::Shared(Bytes::new())
    }
}

impl LabelStr {
    /// Invalid UTF-8 is replaced like `String::from_utf8_lossy`.
    pub fn from_utf8_lossy(bytes: Bytes) -> Self {
        // Label names and values are nearly always ASCII, which is checked inline a word at a
        // time.
        if bytes.is_ascii() || std::str::from_utf8(&bytes).is_ok() {
            return Self::inline_short(&bytes).unwrap_or_else(|| Self(LabelStorage::Shared(bytes)));
        }
        Self::from(String::from_utf8_lossy(&bytes).into_owned())
    }

    fn from_record_lossy(record: &Bytes, field: &[u8]) -> Self {
        // Short ASCII labels need no shared slice of the record.
        if field.len() <= INLINE_LABEL_BYTES && field.is_ascii() {
            return Self::inline_short(field).expect("short label");
        }
        Self::from_utf8_lossy(record.slice_ref(field))
    }

    fn inline_short(value: &[u8]) -> Option<Self> {
        let mut bytes = [0; INLINE_LABEL_BYTES];
        bytes.get_mut(..value.len())?.copy_from_slice(value);
        Some(Self(LabelStorage::Inline {
            len: value.len() as u8,
            bytes,
        }))
    }

    fn into_bytes(self) -> Bytes {
        match self.0 {
            LabelStorage::Shared(bytes) => bytes,
            LabelStorage::Inline { len, bytes } => {
                Bytes::copy_from_slice(&bytes[..usize::from(len)])
            }
        }
    }

    pub fn from_static(value: &'static str) -> Self {
        Self(LabelStorage::Shared(Bytes::from_static(value.as_bytes())))
    }

    pub fn as_str(&self) -> &str {
        let bytes = match &self.0 {
            LabelStorage::Shared(bytes) => &bytes[..],
            LabelStorage::Inline { len, bytes } => &bytes[..usize::from(*len)],
        };
        // SAFETY: built only from valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(bytes) }
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
        Self::inline_short(value.as_bytes())
            .unwrap_or_else(|| Self(LabelStorage::Shared(Bytes::from(value))))
    }
}

impl From<&str> for LabelStr {
    fn from(value: &str) -> Self {
        Self::inline_short(value.as_bytes()).unwrap_or_else(|| {
            Self(LabelStorage::Shared(Bytes::copy_from_slice(
                value.as_bytes(),
            )))
        })
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
        return decode_v1(&bytes, None).context("decode WriteRequest");
    }
    decode_v2(&bytes).context("decode WriteRequest")
}

/// Like `decode_record_bytes`, with where each series' encoded labels are in `bytes`, when they
/// are one run of fields. The same bytes always decode to the same labels.
pub fn decode_record_with_label_spans(
    version: u32,
    bytes: Bytes,
) -> Result<(DecodedRequest, Vec<Option<Range<usize>>>)> {
    if version != 1 && version != 0 {
        let request = decode_record_bytes(version, bytes)?;
        let spans = vec![None; request.series.len()];
        return Ok((request, spans));
    }
    let mut spans = Vec::new();
    let request = decode_v1(&bytes, Some(&mut spans)).context("decode WriteRequest")?;
    Ok((request, spans))
}

// Records are mostly series labels, and building prost's messages for them and converting those
// cost several times reading the fields directly. Decodes like `cortexpb::WriteRequest::decode`,
// with the rarer messages left to prost.
fn decode_v1(
    record: &Bytes,
    mut label_spans: Option<&mut Vec<Option<Range<usize>>>>,
) -> Result<DecodedRequest> {
    let ctx = DecodeContext::default();
    let mut buf = &record[..];
    let mut source = 0;
    let mut series = Vec::new();
    let mut metadata = Vec::new();
    while !buf.is_empty() {
        let (tag, wire_type) = decode_key(&mut buf)?;
        match tag {
            1 => {
                let (decoded, span) =
                    decode_series(record, length_delimited(wire_type, &mut buf)?)?;
                series.push(decoded);
                if let Some(spans) = label_spans.as_deref_mut() {
                    spans.push(span);
                }
            }
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

// Like `decode_v1`, version 2 records are read field by field: prost's messages cost an allocation
// for every symbol and series, and most of decoding them. Decodes like
// `cortexpb::WriteRequest::decode` then `decode_v2_request`.
fn decode_v2(record: &Bytes) -> Result<DecodedRequest> {
    let ctx = DecodeContext::default();
    let mut buf = &record[..];
    let mut source = 0;
    let mut symbol_fields = Vec::new();
    let mut series_fields = Vec::new();
    while !buf.is_empty() {
        let (tag, wire_type) = decode_key(&mut buf)?;
        match tag {
            // Version 1 fields, which a version 2 record does not use but prost validates.
            1 => {
                cortexpb::TimeSeries::decode(length_delimited(wire_type, &mut buf)?)?;
            }
            2 => source = varint(wire_type, &mut buf)? as i32,
            3 => {
                cortexpb::MetricMetadata::decode(length_delimited(wire_type, &mut buf)?)?;
            }
            4 => {
                let field = length_delimited(wire_type, &mut buf)?;
                std::str::from_utf8(field)
                    .context("invalid string value: data is not UTF-8 encoded")?;
                symbol_fields.push(field);
            }
            // Symbols can follow the series, so series are read once all fields are.
            5 => series_fields.push(length_delimited(wire_type, &mut buf)?),
            _ => skip_field(wire_type, tag, &mut buf, ctx.clone())?,
        }
    }
    // Each symbol is converted once, and labels share it.
    let symbols = symbol_fields
        .into_iter()
        .map(|field| {
            if field.len() <= INLINE_LABEL_BYTES {
                LabelStr::inline_short(field).expect("short label")
            } else {
                LabelStr(LabelStorage::Shared(record.slice_ref(field)))
            }
        })
        .collect::<Vec<_>>();
    let symbol = |reference: u32| -> Result<&LabelStr> {
        let reference = reference as usize;
        if reference < COMMON_SYMBOLS.len() {
            return Ok(&COMMON_LABELS[reference]);
        }
        if reference < SYMBOL_OFFSET {
            bail!("reserved RW2 symbol reference {reference}");
        }
        symbols
            .get(reference - SYMBOL_OFFSET)
            .with_context(|| format!("RW2 symbol reference {reference} out of range"))
    };
    // Each symbol's interned name, once a series uses it as a name.
    let mut interned_names: Vec<Option<Option<&'static str>>> = vec![None; symbols.len()];
    let mut metadata = Vec::new();
    let mut series = Vec::with_capacity(series_fields.len());
    let mut labels_refs = Vec::new();
    for field in series_fields {
        let fields = decode_series_v2(record, field, &mut labels_refs)?;
        if labels_refs.len() % 2 != 0 {
            bail!("odd number of RW2 label references");
        }
        let mut labels = Vec::with_capacity(labels_refs.len() / 2);
        for pair in labels_refs.as_chunks::<2>().0 {
            let reference = pair[0] as usize;
            let known = reference
                .checked_sub(SYMBOL_OFFSET)
                .and_then(|index| Some((index, interned_names.get_mut(index)?)))
                .and_then(|(index, known)| {
                    // Reuse names the store already holds without copying the symbol for every
                    // occurrence or interning rejected input.
                    *known.get_or_insert_with(|| {
                        crate::labels::names::lookup(symbols[index].as_str())
                            .map(crate::labels::names::name)
                    })
                });
            let name = match known {
                Some(name) => LabelStr::from_static(name),
                None => symbol(pair[0])?.clone(),
            };
            labels.push((name, symbol(pair[1])?.clone()));
        }
        let exemplars = fields
            .exemplars
            .into_iter()
            .map(|e| -> Result<cortexpb::Exemplar> {
                if e.labels_refs.len() % 2 != 0 {
                    bail!("odd number of RW2 exemplar label references");
                }
                let mut pairs = Vec::with_capacity(e.labels_refs.len() / 2);
                for pair in e.labels_refs.as_chunks::<2>().0 {
                    pairs.push(cortexpb::LabelPair {
                        name: symbol(pair[0])?.clone().into_bytes(),
                        value: symbol(pair[1])?.clone().into_bytes(),
                    });
                }
                Ok(cortexpb::Exemplar {
                    labels: pairs,
                    value: e.value,
                    timestamp_ms: e.timestamp,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if let Some(meta) = fields.metadata {
            let help = symbol(meta.help_ref)?;
            let unit = symbol(meta.unit_ref)?;
            // Like Mimir's RW2 unmarshalling, every series carries a metadata field, and only
            // one that says something about a named metric becomes metadata.
            if meta.r#type != 0 || !help.is_empty() || !unit.is_empty() {
                let metric_name = labels
                    .iter()
                    .find(|(name, _)| name == "__name__")
                    .map_or("", |(_, value)| value.as_str());
                if !metric_name.is_empty() {
                    metadata.push(cortexpb::MetricMetadata {
                        r#type: meta.r#type,
                        metric_family_name: metric_name.to_owned(),
                        help: help.as_str().to_owned(),
                        unit: unit.as_str().to_owned(),
                    });
                }
            }
        }
        series.push(DecodedSeries {
            labels,
            samples: fields.samples,
            histograms: fields.histograms,
            exemplars,
            created_timestamp: fields.created_timestamp,
        });
    }
    Ok(DecodedRequest {
        source,
        series,
        metadata,
    })
}

static COMMON_LABELS: std::sync::LazyLock<Vec<LabelStr>> = std::sync::LazyLock::new(|| {
    COMMON_SYMBOLS
        .iter()
        .map(|symbol| LabelStr::from_static(symbol))
        .collect()
});

// A version 2 series' fields but its labels, which go to `labels_refs`.
struct SeriesV2Fields {
    samples: Vec<cortexpb::Sample>,
    histograms: Vec<cortexpb::Histogram>,
    exemplars: Vec<cortexpb::ExemplarRw2>,
    metadata: Option<cortexpb::MetadataRw2>,
    created_timestamp: i64,
}

// Decodes like prost's `TimeSeriesRw2`: repeated scalars packed or not, and a repeated message
// field merges into the one before.
fn decode_series_v2(
    record: &Bytes,
    mut buf: &[u8],
    labels_refs: &mut Vec<u32>,
) -> Result<SeriesV2Fields> {
    let ctx = DecodeContext::default();
    labels_refs.clear();
    let mut series = SeriesV2Fields {
        samples: Vec::new(),
        histograms: Vec::new(),
        exemplars: Vec::new(),
        metadata: None,
        created_timestamp: 0,
    };
    while !buf.is_empty() {
        let (tag, wire_type) = decode_key(&mut buf)?;
        match tag {
            1 => {
                if wire_type == WireType::LengthDelimited {
                    let mut packed = length_delimited(wire_type, &mut buf)?;
                    while !packed.is_empty() {
                        labels_refs.push(decode_varint(&mut packed)? as u32);
                    }
                } else {
                    labels_refs.push(varint(wire_type, &mut buf)? as u32);
                }
            }
            2 => series
                .samples
                .push(decode_sample(length_delimited(wire_type, &mut buf)?)?),
            3 => series
                .histograms
                .push(cortexpb::Histogram::decode(length_delimited(
                    wire_type, &mut buf,
                )?)?),
            4 => {
                let exemplar = length_delimited(wire_type, &mut buf)?;
                series
                    .exemplars
                    .push(cortexpb::ExemplarRw2::decode(record.slice_ref(exemplar))?);
            }
            5 => {
                let mut fields = length_delimited(wire_type, &mut buf)?;
                let metadata = series.metadata.get_or_insert_default();
                while !fields.is_empty() {
                    let (tag, wire_type) = decode_key(&mut fields)?;
                    match tag {
                        1 => metadata.r#type = varint(wire_type, &mut fields)? as i32,
                        3 => metadata.help_ref = varint(wire_type, &mut fields)? as u32,
                        4 => metadata.unit_ref = varint(wire_type, &mut fields)? as u32,
                        _ => skip_field(wire_type, tag, &mut fields, ctx.clone())?,
                    }
                }
            }
            6 => series.created_timestamp = varint(wire_type, &mut buf)? as i64,
            _ => skip_field(wire_type, tag, &mut buf, ctx.clone())?,
        }
    }
    Ok(series)
}

fn decode_sample(mut buf: &[u8]) -> Result<cortexpb::Sample> {
    let ctx = DecodeContext::default();
    let mut sample = cortexpb::Sample::default();
    while !buf.is_empty() {
        let (tag, wire_type) = decode_key(&mut buf)?;
        match tag {
            1 => {
                check_wire_type(WireType::SixtyFourBit, wire_type)?;
                let (bits, rest) = buf.split_first_chunk::<8>().context("buffer underflow")?;
                sample.value = f64::from_le_bytes(*bits);
                buf = rest;
            }
            2 => sample.timestamp_ms = varint(wire_type, &mut buf)? as i64,
            _ => skip_field(wire_type, tag, &mut buf, ctx.clone())?,
        }
    }
    Ok(sample)
}

fn decode_series(record: &Bytes, mut buf: &[u8]) -> Result<(DecodedSeries, Option<Range<usize>>)> {
    let ctx = DecodeContext::default();
    let offset = |bytes: &[u8]| bytes.as_ptr() as usize - record.as_ptr() as usize;
    // The labels' fields, while no other field came between them.
    let mut labels_span: Option<Range<usize>> = None;
    let mut labels_contiguous = true;
    let mut after_labels = false;
    let mut series = DecodedSeries {
        labels: Vec::with_capacity(20),
        samples: Vec::new(),
        histograms: Vec::new(),
        exemplars: Vec::new(),
        created_timestamp: 0,
    };
    while !buf.is_empty() {
        let field_start = offset(buf);
        let (tag, wire_type) = decode_key(&mut buf)?;
        if tag == 1 {
            labels_contiguous &= !after_labels;
        } else if labels_span.is_some() {
            after_labels = true;
        }
        match tag {
            1 => {
                let mut pair = length_delimited(wire_type, &mut buf)?;
                let start = labels_span.as_ref().map_or(field_start, |span| span.start);
                labels_span = Some(start..offset(buf));
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
            2 => series
                .samples
                .push(decode_sample(length_delimited(wire_type, &mut buf)?)?),
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
    Ok((series, labels_span.filter(|_| labels_contiguous)))
}

fn label(record: &Bytes, bytes: &[u8]) -> LabelStr {
    if bytes.is_empty() {
        return LabelStr::default();
    }
    LabelStr::from_record_lossy(record, bytes)
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

    #[test]
    fn rw2_known_label_names_decode_like_uninterned_names() {
        let name = "rust_rw2_symbol_reuse_test_name";
        let record = cortexpb::WriteRequest {
            symbols_rw2: vec![name.into(), "value".into()],
            timeseries_rw2: vec![cortexpb::TimeSeriesRw2 {
                labels_refs: vec![64, 65],
                ..Default::default()
            }],
            ..Default::default()
        };
        let encoded = Bytes::from(record.encode_to_vec());
        let original = decode_record_bytes(2, encoded.clone()).unwrap();
        crate::labels::names::intern(name);
        let interned = decode_record_bytes(2, encoded).unwrap();
        assert_eq!(interned, original);
        assert_eq!(interned.series[0].labels[0].0.as_str(), name);
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

    // What `decode_v2` must match: prost's decoding, then the same conversions.
    fn decode_v2_with_prost(request: cortexpb::WriteRequest) -> Result<DecodedRequest> {
        // Each symbol is converted once, and labels share it.
        let symbols = request
            .symbols_rw2
            .into_iter()
            .map(LabelStr::from)
            .collect::<Vec<_>>();
        let symbol = |reference: u32| -> Result<LabelStr> {
            let reference = reference as usize;
            if reference < COMMON_SYMBOLS.len() {
                return Ok(LabelStr::from_static(COMMON_SYMBOLS[reference]));
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
        let mut interned_names = hashbrown::HashMap::new();
        for ts in request.timeseries_rw2 {
            if ts.labels_refs.len() % 2 != 0 {
                bail!("odd number of RW2 label references");
            }
            let mut labels = Vec::with_capacity(ts.labels_refs.len() / 2);
            for pair in ts.labels_refs.as_chunks::<2>().0 {
                let reference = pair[0] as usize;
                let name = if reference >= SYMBOL_OFFSET {
                    // Reuse names the store already holds without incrementing the record's
                    // shared refcount for every occurrence or interning rejected input.
                    let known = interned_names.entry(reference).or_insert_with(|| {
                        symbols
                            .get(reference - SYMBOL_OFFSET)
                            .and_then(|name| crate::labels::names::lookup(name.as_str()))
                            .map(crate::labels::names::name)
                    });
                    match *known {
                        Some(name) => LabelStr::from_static(name),
                        None => symbol(pair[0])?,
                    }
                } else {
                    symbol(pair[0])?
                };
                labels.push((name, symbol(pair[1])?));
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
                            name: symbol(pair[0])?.into_bytes(),
                            value: symbol(pair[1])?.into_bytes(),
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
                if !metric_name.is_empty()
                    && (meta.r#type != 0 || !help.is_empty() || !unit.is_empty())
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

    fn random_rw2_record(random: &mut Random) -> Vec<u8> {
        let mut record = Vec::new();
        let symbols = random.below(6);
        let reference = |random: &mut Random| -> u32 {
            match random.below(8) {
                // Common, reserved, or past the last symbol.
                0 => random.below(SYMBOL_OFFSET as u64 + 2) as u32,
                1 => (SYMBOL_OFFSET as u64 + symbols + random.below(2)) as u32,
                _ => (SYMBOL_OFFSET as u64 + random.below(symbols.max(1))) as u32,
            }
        };
        let mut series_fields = Vec::new();
        for _ in 0..random.below(4) {
            let mut labels_refs = (0..random.below(3) * 2)
                .map(|_| reference(random))
                .collect::<Vec<_>>();
            if random.below(8) == 0 {
                labels_refs.push(reference(random));
            }
            let series = cortexpb::TimeSeriesRw2 {
                labels_refs,
                samples: (0..random.below(3))
                    .map(|_| cortexpb::Sample {
                        value: random.below(1000) as f64 - 500.5,
                        timestamp_ms: random.next() as i64,
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
                exemplars: (0..random.below(2))
                    .map(|_| cortexpb::ExemplarRw2 {
                        labels_refs: (0..random.below(2) * 2 + random.below(4) / 3)
                            .map(|_| reference(random))
                            .collect(),
                        value: 1.5,
                        timestamp: random.below(100) as i64,
                    })
                    .collect(),
                metadata: (random.below(3) != 0).then(|| cortexpb::MetadataRw2 {
                    r#type: random.below(3) as i32,
                    help_ref: [0, reference(random)][random.below(2) as usize],
                    unit_ref: [0, reference(random)][random.below(2) as usize],
                }),
                created_timestamp: random.below(3) as i64,
            };
            let mut encoded = series.encode_to_vec();
            match random.below(6) {
                // A field the decoder does not know.
                0 => {
                    prost::encoding::encode_key(7, WireType::Varint, &mut encoded);
                    prost::encoding::encode_varint(random.next(), &mut encoded);
                }
                // An unpacked reference, and a second metadata field merging into the first.
                1 => {
                    prost::encoding::encode_key(1, WireType::Varint, &mut encoded);
                    prost::encoding::encode_varint(reference(random).into(), &mut encoded);
                    let metadata = cortexpb::MetadataRw2 {
                        help_ref: reference(random),
                        ..Default::default()
                    };
                    put_field(&mut encoded, 5, &metadata.encode_to_vec());
                }
                _ => {}
            }
            series_fields.push(encoded);
        }
        // Symbols usually come first, but can follow or interleave with the series.
        let symbols_first = random.below(4) != 0;
        if !symbols_first {
            for series in &series_fields {
                put_field(&mut record, 5, series);
            }
        }
        for index in 0..symbols {
            let symbol = match random.below(4) {
                0 => format!("a_long_symbol_shared_with_the_record_{index}").into_bytes(),
                _ => random.bytes(),
            };
            put_field(&mut record, 4, &symbol);
        }
        if symbols_first {
            for series in &series_fields {
                put_field(&mut record, 5, series);
            }
        }
        if random.below(2) == 0 {
            prost::encoding::encode_key(2, WireType::Varint, &mut record);
            prost::encoding::encode_varint(random.below(3), &mut record);
        }
        if random.below(8) == 0 {
            let series = cortexpb::TimeSeries {
                labels: vec![cortexpb::LabelPair {
                    name: random.bytes().into(),
                    value: random.bytes().into(),
                }],
                ..Default::default()
            };
            put_field(&mut record, 1, &series.encode_to_vec());
        }
        if random.below(4) == 0 {
            put_field(&mut record, 99, b"unknown");
        }
        record
    }

    #[test]
    fn version_2_records_decode_like_prost() {
        let mut random = Random(0x2545_f491_4f6c_dd1d);
        let (mut compared, mut decoded_ok) = (0, 0);
        for _ in 0..5_000 {
            let record = random_rw2_record(&mut random);
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
                let expected = cortexpb::WriteRequest::decode(candidate.clone())
                    .map_err(anyhow::Error::from)
                    .and_then(decode_v2_with_prost);
                let decoded = decode_v2(&candidate);
                match (&expected, &decoded) {
                    // Corrupted floats can be NaN, which only compare equal printed.
                    (Ok(expected), Ok(decoded)) => {
                        assert_eq!(format!("{decoded:?}"), format!("{expected:?}"));
                        decoded_ok += 1;
                    }
                    (Err(_), Err(_)) => {}
                    _ => panic!(
                        "prost: {expected:?}, decode_v2: {decoded:?} for {:?}",
                        &candidate[..]
                    ),
                }
                compared += 1;
            }
        }
        assert!(compared > 10_000);
        assert!(decoded_ok > 2_000, "{decoded_ok} records decoded");
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
                let mut spans = Vec::new();
                let decoded = decode_v1(&candidate, Some(&mut spans));
                if let Ok(decoded) = &decoded {
                    // A series' label bytes alone decode to its labels.
                    assert_eq!(spans.len(), decoded.series.len());
                    for (span, series) in spans.iter().zip(&decoded.series) {
                        if let Some(span) = span {
                            let alone = cortexpb::TimeSeries::decode(&candidate[span.clone()]);
                            assert_eq!(decode_pairs(alone.unwrap().labels), series.labels);
                        }
                    }
                }
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

    // A remote-write 2 record like the ones ingesters consume: a few dozen series from one
    // scrape, sharing most of their label values and each carrying its metric's metadata.
    fn synthetic_rw2_record(series: usize) -> Bytes {
        let mut symbols: Vec<String> = Vec::new();
        let mut refs = std::collections::HashMap::new();
        let mut symbol = |value: String| -> u32 {
            *refs.entry(value.clone()).or_insert_with(|| {
                symbols.push(value);
                (SYMBOL_OFFSET + symbols.len() - 1) as u32
            })
        };
        let timeseries = (0..series)
            .map(|index| {
                let metric = format!(
                    "synthetic_http_server_request_duration_seconds_{}",
                    index % 9
                );
                let mut labels_refs = vec![
                    1,
                    symbol(metric.clone()),
                    16,
                    symbol("dev-cluster-0".into()),
                    20,
                    symbol(format!("10.0.{}.{}:9090", index % 3, index % 7)),
                    19,
                    symbol("synthetic-namespace/synthetic-service".into()),
                    17,
                    symbol("synthetic-namespace".into()),
                    18,
                    symbol(format!("synthetic-service-7d9f8b6c4-{:05}", index % 3)),
                    symbol("route".into()),
                    symbol(format!("/api/v1/resource/{}", index % 11)),
                ];
                if index % 2 == 0 {
                    labels_refs.extend([4, symbol(format!("{}", index % 12))]);
                }
                cortexpb::TimeSeriesRw2 {
                    labels_refs,
                    samples: vec![cortexpb::Sample {
                        value: index as f64,
                        timestamp_ms: 1_700_000_000_000 + index as i64,
                    }],
                    metadata: Some(cortexpb::MetadataRw2 {
                        r#type: 3,
                        help_ref: symbol(format!("Help for {metric}.")),
                        unit_ref: 0,
                    }),
                    ..Default::default()
                }
            })
            .collect();
        Bytes::from(
            cortexpb::WriteRequest {
                symbols_rw2: symbols,
                timeseries_rw2: timeseries,
                ..Default::default()
            }
            .encode_to_vec(),
        )
    }

    // cargo test --release --lib record::tests::bench_rw2_decode -- --ignored --nocapture
    #[test]
    #[ignore]
    fn bench_rw2_decode() {
        let record = synthetic_rw2_record(80);
        let iterations = 200_000;
        for round in 0..5 {
            let started = std::time::Instant::now();
            for _ in 0..iterations {
                std::hint::black_box(
                    decode_record_with_label_spans(2, std::hint::black_box(record.clone()))
                        .unwrap(),
                );
            }
            let elapsed = started.elapsed();
            println!(
                "round {round}: {:.0} ns/record, {:.1} ns/series",
                elapsed.as_nanos() as f64 / iterations as f64,
                elapsed.as_nanos() as f64 / iterations as f64 / 80.0
            );
        }
    }

    #[test]
    fn labels_inline_short_strings_and_share_long_values_with_the_record() {
        let pair = |name: &[u8], value: &[u8]| cortexpb::LabelPair {
            name: Bytes::copy_from_slice(name),
            value: Bytes::copy_from_slice(value),
        };
        let request = cortexpb::WriteRequest {
            timeseries: (0..20)
                .map(|index| cortexpb::TimeSeries {
                    labels: vec![
                        pair(
                            b"__name__",
                            format!("a_long_metric_name_to_query_{index}").as_bytes(),
                        ),
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
        let encoded = Bytes::from(request.encode_to_vec());
        let decoded = decode_record_bytes(1, encoded.clone()).unwrap();
        assert_eq!(
            decoded.series[3].labels[0],
            ("__name__".into(), "a_long_metric_name_to_query_3".into())
        );
        assert_eq!(decoded.series[20].labels[0].1, "a\u{fffd}b");
        let start = encoded.as_ptr() as usize;
        let record = start..start + encoded.len();
        for series in &decoded.series[..20] {
            let (name, metric) = &series.labels[0];
            assert!(matches!(name.0, LabelStorage::Inline { .. }));
            assert!(record.contains(&(metric.as_ptr() as usize)));
            let (name, job) = &series.labels[1];
            assert!(matches!(name.0, LabelStorage::Inline { .. }));
            assert!(matches!(job.0, LabelStorage::Inline { .. }));
        }
    }
}
