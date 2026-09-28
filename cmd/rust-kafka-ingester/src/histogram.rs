use crate::proto::cortexpb;
use crate::proto::cortexpb::histogram::{Count, ZeroCount};

const CUSTOM_BUCKETS_SCHEMA: i32 = -53;
const STALE_NAN: u64 = 0x7ff0_0000_0000_0002;

pub struct EncodedHistogram {
    pub encoding: i32,
    pub data: Vec<u8>,
}

pub fn encode(histogram: &cortexpb::Histogram) -> EncodedHistogram {
    encode_sequence(std::slice::from_ref(histogram))
}

/// Prometheus's `Histogram.Validate` and `FloatHistogram.Validate`, which its head appender runs
/// before appending.
pub fn is_valid(histogram: &cortexpb::Histogram) -> bool {
    let float = is_float(histogram);
    let spans_match =
        |spans: &[cortexpb::BucketSpan], buckets: usize, first_may_be_negative: bool| {
            spans
                .iter()
                .enumerate()
                .all(|(index, span)| span.offset >= 0 || (index == 0 && first_may_be_negative))
                && span_count(spans) == buckets
        };
    let (positive_len, negative_len) = if float {
        (
            histogram.positive_counts.len(),
            histogram.negative_counts.len(),
        )
    } else {
        (
            histogram.positive_deltas.len(),
            histogram.negative_deltas.len(),
        )
    };
    if histogram.schema == CUSTOM_BUCKETS_SCHEMA {
        let bounds = &histogram.custom_values;
        let mut previous = f64::NEG_INFINITY;
        for (index, bound) in bounds.iter().enumerate() {
            if bound.is_nan() || (index > 0 && *bound <= previous) {
                return false;
            }
            previous = *bound;
        }
        if previous == f64::INFINITY || !spans_match(&histogram.positive_spans, positive_len, false)
        {
            return false;
        }
        let total_span_length: i64 = histogram
            .positive_spans
            .iter()
            .map(|span| i64::from(span.length) + i64::from(span.offset))
            .sum();
        if (bounds.len() as i64 + 1) < total_span_length
            || totals_of(histogram).1 != 0.0
            || histogram.zero_threshold != 0.0
            || !histogram.negative_spans.is_empty()
            || negative_len > 0
        {
            return false;
        }
    } else if (-4..=8).contains(&histogram.schema) {
        if !spans_match(&histogram.positive_spans, positive_len, true)
            || !spans_match(&histogram.negative_spans, negative_len, true)
            || !histogram.custom_values.is_empty()
        {
            return false;
        }
    } else {
        return false;
    }
    if float {
        let (count, zero) = totals_of(histogram);
        return zero >= 0.0
            && count >= 0.0
            && histogram
                .positive_counts
                .iter()
                .chain(&histogram.negative_counts)
                .all(|count| *count >= 0.0);
    }
    let mut observations = 0_u64;
    for deltas in [&histogram.positive_deltas, &histogram.negative_deltas] {
        let mut current = 0_i64;
        for delta in deltas {
            current = current.wrapping_add(*delta);
            if current < 0 {
                return false;
            }
            observations = observations.wrapping_add(current as u64);
        }
    }
    let observations = observations.wrapping_add(zero_count_int(histogram));
    if histogram.sum.is_nan() {
        observations <= count_int(histogram)
    } else {
        observations == count_int(histogram)
    }
}

/// Prometheus's `Histogram.Equals` and `FloatHistogram.Equals`: the same values and bucket
/// layout, whatever the reset hint or zero-length spans.
pub fn equal_values(a: &cortexpb::Histogram, b: &cortexpb::Histogram) -> bool {
    let same_count = match (&a.count, &b.count) {
        (Some(Count::CountInt(x)), Some(Count::CountInt(y))) => {
            x == y && zero_count_int(a) == zero_count_int(b)
        }
        (Some(Count::CountFloat(x)), Some(Count::CountFloat(y))) => {
            x.to_bits() == y.to_bits()
                && zero_count_float(a).to_bits() == zero_count_float(b).to_bits()
        }
        _ => false,
    };
    same_count
        && a.schema == b.schema
        && a.sum.to_bits() == b.sum.to_bits()
        && (a.schema != CUSTOM_BUCKETS_SCHEMA || a.custom_values == b.custom_values)
        && a.zero_threshold == b.zero_threshold
        && merged_spans(&a.positive_spans) == merged_spans(&b.positive_spans)
        && merged_spans(&a.negative_spans) == merged_spans(&b.negative_spans)
        && a.positive_deltas == b.positive_deltas
        && a.negative_deltas == b.negative_deltas
        && a.positive_counts
            .iter()
            .map(|value| value.to_bits())
            .eq(b.positive_counts.iter().map(|value| value.to_bits()))
        && a.negative_counts
            .iter()
            .map(|value| value.to_bits())
            .eq(b.negative_counts.iter().map(|value| value.to_bits()))
}

// Folds zero-length spans into the next span, like `spansMatch`.
fn merged_spans(spans: &[cortexpb::BucketSpan]) -> Vec<(i32, u32)> {
    let mut merged = Vec::with_capacity(spans.len());
    let mut offset = 0;
    for span in spans {
        offset += span.offset;
        if span.length > 0 {
            merged.push((offset, span.length));
            offset = 0;
        }
    }
    merged
}

/// Encodes histograms of one layout into one chunk, without the layout and counter reset
/// checks of [`HistogramAppender::append`].
pub fn encode_sequence(histograms: &[cortexpb::Histogram]) -> EncodedHistogram {
    let first = &histograms[0];
    let mut appender = HistogramAppender::empty(
        matches!(first.count, Some(Count::CountFloat(_))),
        Header::from_hint(first.reset_hint),
    );
    for histogram in histograms {
        appender.raw_append(histogram.clone());
    }
    appender.encoded()
}

/// The counter reset header of a histogram chunk, in the top two bits of its third byte.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Header {
    #[default]
    Unknown,
    CounterReset,
    NotCounterReset,
    Gauge,
}

impl Header {
    fn byte(self) -> u8 {
        match self {
            Header::Unknown => 0x00,
            Header::CounterReset => 0x80,
            Header::NotCounterReset => 0x40,
            Header::Gauge => 0xc0,
        }
    }

    fn from_byte(byte: u8) -> Self {
        match byte & 0xc0 {
            0x80 => Header::CounterReset,
            0x40 => Header::NotCounterReset,
            0xc0 => Header::Gauge,
            _ => Header::Unknown,
        }
    }

    /// From a sample's reset hint, as protobuf `ResetHint` numbers it.
    pub fn from_hint(hint: i32) -> Self {
        match hint {
            1 => Header::CounterReset,
            2 => Header::NotCounterReset,
            3 => Header::Gauge,
            _ => Header::Unknown,
        }
    }

    pub fn hint(self) -> i32 {
        match self {
            Header::Unknown => 0,
            Header::CounterReset => 1,
            Header::NotCounterReset => 2,
            Header::Gauge => 3,
        }
    }
}

const GAUGE_HINT: i32 = 3;
const COUNTER_RESET_HINT: i32 = 1;

/// Buckets to insert, like Prometheus's `Insert`: `num` buckets before position `pos` of the
/// existing buckets, starting at global bucket index `bucket_index`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Insert {
    pos: usize,
    num: usize,
    bucket_index: i64,
}

// Prometheus's `bucketIterator`: global bucket indices of a span layout.
struct BucketIterator<'a> {
    spans: &'a [cortexpb::BucketSpan],
    span: usize,
    bucket: i64,
    index: i64,
}

impl<'a> BucketIterator<'a> {
    fn new(spans: &'a [cortexpb::BucketSpan]) -> Self {
        Self {
            spans,
            span: 0,
            bucket: -1,
            index: spans.first().map_or(-1, |span| i64::from(span.offset) - 1),
        }
    }

    fn next(&mut self) -> Option<i64> {
        if self.span >= self.spans.len() {
            return None;
        }
        if self.bucket < i64::from(self.spans[self.span].length) - 1 {
            self.bucket += 1;
            self.index += 1;
            return Some(self.index);
        }
        while self.span + 1 < self.spans.len() {
            self.span += 1;
            self.index += i64::from(self.spans[self.span].offset) + 1;
            self.bucket = 0;
            if self.spans[self.span].length == 0 {
                self.index -= 1;
                continue;
            }
            return Some(self.index);
        }
        None
    }
}

// Prometheus's `expandIntSpansAndBuckets` and `expandFloatSpansAndBuckets` on absolute bucket
// counts: the inserts into `a` for buckets only `b` has, and into `b` for buckets only `a` has,
// which must be empty; None when a bucket decreased, which is a counter reset.
fn expand_spans_and_buckets<T: Copy + PartialOrd + Default>(
    a: &[cortexpb::BucketSpan],
    b: &[cortexpb::BucketSpan],
    a_counts: &[T],
    b_counts: &[T],
) -> Option<(Vec<Insert>, Vec<Insert>)> {
    let mut ai = BucketIterator::new(a);
    let mut bi = BucketIterator::new(b);
    let (mut a_inserts, mut b_inserts) = (Vec::new(), Vec::new());
    let (mut a_inter, mut b_inter) = (Insert::default(), Insert::default());
    let mut a_index = ai.next();
    let mut b_index = bi.next();
    let (mut a_count_index, mut b_count_index) = (0, 0);
    let count = |counts: &[T], index: usize| counts.get(index).copied().unwrap_or_default();
    let add_insert = |inserts: &mut Vec<Insert>, insert: &mut Insert, other_index: i64| {
        if insert.num == 0 {
            insert.bucket_index = other_index;
        } else if insert.bucket_index + insert.num as i64 != other_index {
            inserts.push(*insert);
            insert.num = 0;
            insert.bucket_index = other_index;
        }
        insert.num += 1;
    };
    loop {
        match (a_index, b_index) {
            (Some(ai_value), Some(bi_value)) => {
                if ai_value == bi_value {
                    if count(a_counts, a_count_index) > count(b_counts, b_count_index) {
                        return None;
                    }
                    if a_inter.num > 0 {
                        a_inserts.push(a_inter);
                        a_inter.num = 0;
                    }
                    a_index = ai.next();
                    a_inter.pos += 1;
                    a_count_index += 1;
                    if b_inter.num > 0 {
                        b_inserts.push(b_inter);
                        b_inter.num = 0;
                    }
                    b_index = bi.next();
                    b_inter.pos += 1;
                    b_count_index += 1;
                } else if ai_value < bi_value {
                    if count(a_counts, a_count_index) != T::default() {
                        return None;
                    }
                    add_insert(&mut b_inserts, &mut b_inter, ai_value);
                    if a_inter.num > 0 {
                        a_inserts.push(a_inter);
                        a_inter.num = 0;
                    }
                    a_index = ai.next();
                    a_inter.pos += 1;
                    a_count_index += 1;
                } else {
                    add_insert(&mut a_inserts, &mut a_inter, bi_value);
                    if b_inter.num > 0 {
                        b_inserts.push(b_inter);
                        b_inter.num = 0;
                    }
                    b_index = bi.next();
                    b_inter.pos += 1;
                    b_count_index += 1;
                }
            }
            (Some(ai_value), None) => {
                if count(a_counts, a_count_index) != T::default() {
                    return None;
                }
                add_insert(&mut b_inserts, &mut b_inter, ai_value);
                if a_inter.num > 0 {
                    a_inserts.push(a_inter);
                    a_inter.num = 0;
                }
                a_index = ai.next();
                a_inter.pos += 1;
                a_count_index += 1;
            }
            (None, Some(bi_value)) => {
                add_insert(&mut a_inserts, &mut a_inter, bi_value);
                if b_inter.num > 0 {
                    b_inserts.push(b_inter);
                    b_inter.num = 0;
                }
                b_index = bi.next();
                b_inter.pos += 1;
                b_count_index += 1;
            }
            (None, None) => {
                if a_inter.num > 0 {
                    a_inserts.push(a_inter);
                }
                if b_inter.num > 0 {
                    b_inserts.push(b_inter);
                }
                return Some((a_inserts, b_inserts));
            }
        }
    }
}

// Prometheus's `expandSpansBothWays`, for gauges: inserts both ways and the merged layout.
fn expand_spans_both_ways(
    a: &[cortexpb::BucketSpan],
    b: &[cortexpb::BucketSpan],
) -> (Vec<Insert>, Vec<Insert>, Vec<cortexpb::BucketSpan>) {
    let mut ai = BucketIterator::new(a);
    let mut bi = BucketIterator::new(b);
    let (mut forward, mut backward) = (Vec::new(), Vec::new());
    let mut merged: Vec<cortexpb::BucketSpan> = Vec::new();
    let mut last_bucket = 0_i64;
    let mut add_bucket = |merged: &mut Vec<cortexpb::BucketSpan>, bucket: i64| {
        let mut offset = bucket - last_bucket - 1;
        if offset == 0 && !merged.is_empty() {
            merged.last_mut().expect("span").length += 1;
        } else {
            if merged.is_empty() {
                offset += 1;
            }
            merged.push(cortexpb::BucketSpan {
                offset: offset as i32,
                length: 1,
            });
        }
        last_bucket = bucket;
    };
    let (mut f_inter, mut b_inter) = (Insert::default(), Insert::default());
    let mut av = ai.next();
    let mut bv = bi.next();
    loop {
        match (av, bv) {
            (Some(a_value), Some(b_value)) if a_value == b_value => {
                if f_inter.num > 0 {
                    forward.push(f_inter);
                    f_inter.num = 0;
                }
                if b_inter.num > 0 {
                    backward.push(b_inter);
                    b_inter.num = 0;
                }
                add_bucket(&mut merged, a_value);
                av = ai.next();
                bv = bi.next();
                f_inter.pos += 1;
                b_inter.pos += 1;
            }
            (Some(a_value), Some(b_value)) if a_value < b_value => {
                b_inter.num += 1;
                if f_inter.num > 0 {
                    forward.push(f_inter);
                    f_inter.num = 0;
                }
                add_bucket(&mut merged, a_value);
                f_inter.pos += 1;
                av = ai.next();
            }
            (Some(_), Some(b_value)) => {
                f_inter.num += 1;
                if b_inter.num > 0 {
                    backward.push(b_inter);
                    b_inter.num = 0;
                }
                add_bucket(&mut merged, b_value);
                b_inter.pos += 1;
                bv = bi.next();
            }
            (Some(a_value), None) => {
                b_inter.num += 1;
                add_bucket(&mut merged, a_value);
                av = ai.next();
            }
            (None, Some(b_value)) => {
                f_inter.num += 1;
                add_bucket(&mut merged, b_value);
                bv = bi.next();
            }
            (None, None) => {
                if f_inter.num > 0 {
                    forward.push(f_inter);
                }
                if b_inter.num > 0 {
                    backward.push(b_inter);
                }
                return (forward, backward, merged);
            }
        }
    }
}

// Prometheus's `insert`: `input` with zero buckets inserted; delta-encoded buckets keep the
// following buckets' absolute counts.
fn insert<T>(input: &[T], out_len: usize, inserts: &[Insert], deltas: bool) -> Vec<T>
where
    T: Copy + Default + std::ops::Add<Output = T> + std::ops::Neg<Output = T>,
{
    let mut out = vec![T::default(); out_len];
    let (mut oi, mut ii) = (0, 0);
    let mut v = T::default();
    for (i, d) in input.iter().copied().enumerate() {
        if ii >= inserts.len() || i != inserts[ii].pos {
            out[oi] = d;
            oi += 1;
            v = v + d;
            continue;
        }
        let mut first_insert = true;
        while ii < inserts.len() && i == inserts[ii].pos {
            if deltas && first_insert {
                out[oi] = -v;
                first_insert = false;
            } else {
                out[oi] = T::default();
            }
            oi += 1;
            for _ in 1..inserts[ii].num {
                out[oi] = T::default();
                oi += 1;
            }
            ii += 1;
        }
        out[oi] = if deltas { d + v } else { d };
        oi += 1;
        v = v + d;
    }
    while ii < inserts.len() {
        out[oi] = if deltas { -v } else { T::default() };
        oi += 1;
        for _ in 1..inserts[ii].num {
            out[oi] = T::default();
            oi += 1;
        }
        ii += 1;
        v = T::default();
    }
    out
}

// Prometheus's `adjustForInserts`: the layout of `spans` with the inserted buckets.
fn adjust_for_inserts(
    spans: &[cortexpb::BucketSpan],
    inserts: &[Insert],
) -> Vec<cortexpb::BucketSpan> {
    if inserts.is_empty() {
        return spans.to_vec();
    }
    let mut iterator = BucketIterator::new(spans);
    let mut merged: Vec<cortexpb::BucketSpan> = Vec::new();
    let mut last_bucket = 0_i64;
    let mut add_bucket = |merged: &mut Vec<cortexpb::BucketSpan>, bucket: i64| {
        let mut offset = bucket - last_bucket - 1;
        if offset == 0 && !merged.is_empty() {
            merged.last_mut().expect("span").length += 1;
        } else {
            if merged.is_empty() {
                offset += 1;
            }
            merged.push(cortexpb::BucketSpan {
                offset: offset as i32,
                length: 1,
            });
        }
        last_bucket = bucket;
    };
    let mut i = 0;
    let mut insert_index = inserts[0].bucket_index;
    let mut insert_num = inserts[0].num;
    let consume = |i: &mut usize, insert_index: &mut i64, insert_num: &mut usize| {
        *insert_num -= 1;
        if *insert_num == 0 {
            *i += 1;
            if *i < inserts.len() {
                *insert_index = inserts[*i].bucket_index;
                *insert_num = inserts[*i].num;
            }
        } else {
            *insert_index += 1;
        }
    };
    let mut bucket = iterator.next();
    while let Some(current) = bucket {
        if i < inserts.len() && insert_index < current {
            add_bucket(&mut merged, insert_index);
            consume(&mut i, &mut insert_index, &mut insert_num);
        } else {
            add_bucket(&mut merged, current);
            bucket = iterator.next();
        }
    }
    while i < inserts.len() {
        add_bucket(&mut merged, insert_index);
        consume(&mut i, &mut insert_index, &mut insert_num);
    }
    merged
}

fn span_count(spans: &[cortexpb::BucketSpan]) -> usize {
    spans.iter().map(|span| span.length as usize).sum()
}

// Absolute bucket counts of delta-encoded integer buckets.
fn cumulative(deltas: &[i64]) -> Vec<i64> {
    let mut total = 0_i64;
    deltas
        .iter()
        .map(|delta| {
            total = total.wrapping_add(*delta);
            total
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Default)]
struct XorValue {
    value: f64,
    leading: u8,
    trailing: u8,
}

impl XorValue {
    fn new(value: f64) -> Self {
        Self {
            value,
            leading: 0xff,
            trailing: 0,
        }
    }
}

#[derive(Clone, Debug)]
enum AppendState {
    Int {
        count: u64,
        count_delta: i64,
        zero: u64,
        zero_delta: i64,
        sum: f64,
        sum_leading: u8,
        sum_trailing: u8,
        positive: Vec<i64>,
        positive_deltas: Vec<i64>,
        negative: Vec<i64>,
        negative_deltas: Vec<i64>,
    },
    Float {
        count: XorValue,
        zero: XorValue,
        sum: XorValue,
        positive: Vec<XorValue>,
        negative: Vec<XorValue>,
    },
}

/// What [`HistogramAppender::append`] did, like Prometheus's `AppendHistogram` results.
#[derive(Debug)]
pub enum Appended {
    InChunk,
    /// The chunk was re-encoded with a wider bucket layout; the appender now holds it.
    Recoded,
    /// The sample needs a new chunk, returned with the sample in it.
    NewChunk(Box<HistogramAppender>),
}

/// An open histogram or float histogram chunk with the encoding and appending rules of
/// Prometheus's histogram appenders: bucket layouts that only gain buckets recode the chunk,
/// counter resets and incompatible layouts start a new one.
#[derive(Clone, Debug)]
pub struct HistogramAppender {
    writer: BitWriter,
    count: u16,
    header: Header,
    float: bool,
    schema: i32,
    zero_threshold: f64,
    positive_spans: Vec<cortexpb::BucketSpan>,
    negative_spans: Vec<cortexpb::BucketSpan>,
    custom_values: Vec<f64>,
    t: i64,
    t_delta: i64,
    first_timestamp: i64,
    state: AppendState,
    last: cortexpb::Histogram,
}

fn is_float(histogram: &cortexpb::Histogram) -> bool {
    matches!(histogram.count, Some(Count::CountFloat(_)))
}

fn is_stale(value: f64) -> bool {
    value.to_bits() == STALE_NAN
}

impl HistogramAppender {
    pub fn empty(float: bool, header: Header) -> Self {
        Self {
            writer: BitWriter::new(vec![0, 0, header.byte()]),
            count: 0,
            header,
            float,
            schema: 0,
            zero_threshold: 0.0,
            positive_spans: Vec::new(),
            negative_spans: Vec::new(),
            custom_values: Vec::new(),
            t: 0,
            t_delta: 0,
            first_timestamp: 0,
            state: if float {
                AppendState::Float {
                    count: XorValue::new(0.0),
                    zero: XorValue::new(0.0),
                    sum: XorValue::new(0.0),
                    positive: Vec::new(),
                    negative: Vec::new(),
                }
            } else {
                AppendState::Int {
                    count: 0,
                    count_delta: 0,
                    zero: 0,
                    zero_delta: 0,
                    sum: 0.0,
                    sum_leading: 0xff,
                    sum_trailing: 0,
                    positive: Vec::new(),
                    positive_deltas: Vec::new(),
                    negative: Vec::new(),
                    negative_deltas: Vec::new(),
                }
            },
            last: cortexpb::Histogram::default(),
        }
    }

    /// A chunk with `histogram` as its first sample. `previous` is the chunk it follows, which
    /// decides whether the new chunk starts with a counter reset.
    pub fn new(histogram: cortexpb::Histogram, previous: Option<&HistogramAppender>) -> Self {
        let header = if histogram.reset_hint == GAUGE_HINT {
            Header::Gauge
        } else if histogram.reset_hint == COUNTER_RESET_HINT {
            Header::CounterReset
        } else {
            match previous {
                Some(previous) if previous.float == is_float(&histogram) => {
                    previous.counter_reset_header(&histogram)
                }
                _ => Header::Unknown,
            }
        };
        let mut appender = Self::empty(is_float(&histogram), header);
        appender.raw_append(histogram);
        appender
    }

    // The header a chunk starting with `histogram` gets after this one.
    fn counter_reset_header(&self, histogram: &cortexpb::Histogram) -> Header {
        if self.float {
            if self.appendable(histogram).counter_reset {
                Header::CounterReset
            } else {
                Header::NotCounterReset
            }
        } else {
            self.appendable(histogram).header
        }
    }

    pub fn len(&self) -> usize {
        usize::from(self.count)
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn header(&self) -> Header {
        self.header
    }

    pub fn first_timestamp(&self) -> i64 {
        self.first_timestamp
    }

    pub fn last(&self) -> &cortexpb::Histogram {
        &self.last
    }

    pub fn encoded(&self) -> EncodedHistogram {
        let mut data = self.writer.bytes.clone();
        data[..2].copy_from_slice(&self.count.to_be_bytes());
        data[2] = (data[2] & 0x3f) | self.header.byte();
        EncodedHistogram {
            encoding: if self.float { 6 } else { 5 },
            data,
        }
    }

    pub fn encoded_len(&self) -> usize {
        self.writer.bytes.len()
    }

    /// Prometheus's `appendHistogram`/`appendFloatHistogram`: encodes `histogram` without
    /// checking that it fits the chunk.
    pub fn raw_append(&mut self, histogram: cortexpb::Histogram) {
        let stale = is_stale(histogram.sum);
        // A stale marker is written without its values or buckets.
        let values = if stale {
            cortexpb::Histogram {
                sum: histogram.sum,
                count: histogram.count.map(|count| match count {
                    Count::CountFloat(_) => Count::CountFloat(0.0),
                    Count::CountInt(_) => Count::CountInt(0),
                }),
                ..Default::default()
            }
        } else {
            histogram.clone()
        };
        let t = histogram.timestamp;
        let writer = &mut self.writer;
        if self.count == 0 {
            write_layout(
                writer,
                values.schema,
                values.zero_threshold,
                &values.positive_spans,
                &values.negative_spans,
                &values.custom_values,
            );
            self.schema = values.schema;
            self.zero_threshold = values.zero_threshold;
            self.positive_spans = values.positive_spans.clone();
            self.negative_spans = values.negative_spans.clone();
            self.custom_values = values.custom_values.clone();
            self.first_timestamp = t;
            put_varbit_int(writer, t);
            match &mut self.state {
                AppendState::Int {
                    count,
                    zero,
                    sum,
                    positive,
                    positive_deltas,
                    negative,
                    negative_deltas,
                    count_delta,
                    zero_delta,
                    ..
                } => {
                    put_varbit_uint(writer, count_int(&values));
                    put_varbit_uint(writer, zero_count_int(&values));
                    writer.write_bits(values.sum.to_bits(), 64);
                    for value in values.positive_deltas.iter().chain(&values.negative_deltas) {
                        put_varbit_int(writer, *value);
                    }
                    *count = count_int(&values);
                    *zero = zero_count_int(&values);
                    *count_delta = 0;
                    *zero_delta = 0;
                    *sum = values.sum;
                    *positive = values.positive_deltas.clone();
                    *negative = values.negative_deltas.clone();
                    *positive_deltas = vec![0; positive.len()];
                    *negative_deltas = vec![0; negative.len()];
                }
                AppendState::Float {
                    count,
                    zero,
                    sum,
                    positive,
                    negative,
                } => {
                    writer.write_bits(count_float(&values).to_bits(), 64);
                    writer.write_bits(zero_count_float(&values).to_bits(), 64);
                    writer.write_bits(values.sum.to_bits(), 64);
                    for value in values.positive_counts.iter().chain(&values.negative_counts) {
                        writer.write_bits(value.to_bits(), 64);
                    }
                    *count = XorValue::new(count_float(&values));
                    *zero = XorValue::new(zero_count_float(&values));
                    *sum = XorValue::new(values.sum);
                    *positive = values
                        .positive_counts
                        .iter()
                        .copied()
                        .map(XorValue::new)
                        .collect();
                    *negative = values
                        .negative_counts
                        .iter()
                        .copied()
                        .map(XorValue::new)
                        .collect();
                }
            }
            self.t_delta = 0;
        } else {
            let t_delta = t.wrapping_sub(self.t);
            put_varbit_int(writer, t_delta.wrapping_sub(self.t_delta));
            self.t_delta = t_delta;
            match &mut self.state {
                AppendState::Int {
                    count,
                    count_delta,
                    zero,
                    zero_delta,
                    sum,
                    sum_leading,
                    sum_trailing,
                    positive,
                    positive_deltas,
                    negative,
                    negative_deltas,
                } => {
                    let next_count_delta = (count_int(&values) as i64).wrapping_sub(*count as i64);
                    let next_zero_delta =
                        (zero_count_int(&values) as i64).wrapping_sub(*zero as i64);
                    let (count_dod, zero_dod) = if stale {
                        (0, 0)
                    } else {
                        (
                            next_count_delta.wrapping_sub(*count_delta),
                            next_zero_delta.wrapping_sub(*zero_delta),
                        )
                    };
                    put_varbit_int(writer, count_dod);
                    put_varbit_int(writer, zero_dod);
                    xor_write(writer, values.sum, *sum, sum_leading, sum_trailing);
                    for (index, bucket) in values.positive_deltas.iter().enumerate() {
                        let delta = bucket.wrapping_sub(positive[index]);
                        put_varbit_int(writer, delta.wrapping_sub(positive_deltas[index]));
                        positive_deltas[index] = delta;
                    }
                    for (index, bucket) in values.negative_deltas.iter().enumerate() {
                        let delta = bucket.wrapping_sub(negative[index]);
                        put_varbit_int(writer, delta.wrapping_sub(negative_deltas[index]));
                        negative_deltas[index] = delta;
                    }
                    *count = count_int(&values);
                    *zero = zero_count_int(&values);
                    *count_delta = next_count_delta;
                    *zero_delta = next_zero_delta;
                    for (slot, bucket) in positive.iter_mut().zip(&values.positive_deltas) {
                        *slot = *bucket;
                    }
                    for (slot, bucket) in negative.iter_mut().zip(&values.negative_deltas) {
                        *slot = *bucket;
                    }
                    *sum = values.sum;
                }
                AppendState::Float {
                    count,
                    zero,
                    sum,
                    positive,
                    negative,
                } => {
                    let mut write = |slot: &mut XorValue, value: f64| {
                        xor_write(
                            writer,
                            value,
                            slot.value,
                            &mut slot.leading,
                            &mut slot.trailing,
                        );
                        slot.value = value;
                    };
                    write(count, count_float(&values));
                    write(zero, zero_count_float(&values));
                    write(sum, values.sum);
                    for (index, value) in values.positive_counts.iter().enumerate() {
                        write(&mut positive[index], *value);
                    }
                    for (index, value) in values.negative_counts.iter().enumerate() {
                        write(&mut negative[index], *value);
                    }
                }
            }
        }
        self.t = t;
        self.count = self
            .count
            .checked_add(1)
            .expect("histogram chunk sample count exceeds u16");
        self.last = histogram;
    }

    // Prometheus's `appendable` for integer and float histograms.
    fn appendable(&self, histogram: &cortexpb::Histogram) -> Appendability {
        let mut result = Appendability {
            header: Header::NotCounterReset,
            ..Appendability::default()
        };
        if self.count > 0 && self.header == Header::Gauge {
            return result;
        }
        if histogram.reset_hint == COUNTER_RESET_HINT {
            result.header = Header::CounterReset;
            result.counter_reset = true;
            return result;
        }
        if is_stale(histogram.sum) {
            result.ok = true;
            return result;
        }
        let (count, zero, sum) = self.totals();
        if is_stale(sum) {
            result.header = Header::Unknown;
            return result;
        }
        let (next_count, next_zero) = totals_of(histogram);
        if next_count < count {
            result.header = Header::CounterReset;
            result.counter_reset = true;
            return result;
        }
        if histogram.schema != self.schema || histogram.zero_threshold != self.zero_threshold {
            result.header = Header::Unknown;
            return result;
        }
        if histogram.schema == CUSTOM_BUCKETS_SCHEMA
            && histogram.custom_values != self.custom_values
        {
            result.header = Header::CounterReset;
            result.counter_reset = true;
            return result;
        }
        if next_zero < zero {
            result.header = Header::CounterReset;
            result.counter_reset = true;
            return result;
        }
        let expanded = match &self.state {
            AppendState::Int {
                positive, negative, ..
            } => expand_spans_and_buckets(
                &self.positive_spans,
                &histogram.positive_spans,
                &cumulative(positive),
                &cumulative(&histogram.positive_deltas),
            )
            .zip(expand_spans_and_buckets(
                &self.negative_spans,
                &histogram.negative_spans,
                &cumulative(negative),
                &cumulative(&histogram.negative_deltas),
            )),
            AppendState::Float {
                positive, negative, ..
            } => expand_spans_and_buckets(
                &self.positive_spans,
                &histogram.positive_spans,
                &positive.iter().map(|value| value.value).collect::<Vec<_>>(),
                &histogram.positive_counts,
            )
            .zip(expand_spans_and_buckets(
                &self.negative_spans,
                &histogram.negative_spans,
                &negative.iter().map(|value| value.value).collect::<Vec<_>>(),
                &histogram.negative_counts,
            )),
        };
        match expanded {
            Some((
                (positive_forward, positive_backward),
                (negative_forward, negative_backward),
            )) => {
                result.positive_forward = positive_forward;
                result.positive_backward = positive_backward;
                result.negative_forward = negative_forward;
                result.negative_backward = negative_backward;
                result.ok = true;
            }
            None => {
                result.header = Header::CounterReset;
                result.counter_reset = true;
            }
        }
        result
    }

    fn totals(&self) -> (f64, f64, f64) {
        match &self.state {
            AppendState::Int {
                count, zero, sum, ..
            } => (*count as f64, *zero as f64, *sum),
            AppendState::Float {
                count, zero, sum, ..
            } => (count.value, zero.value, sum.value),
        }
    }

    // Prometheus's `appendableGauge`.
    fn appendable_gauge(&self, histogram: &cortexpb::Histogram) -> Option<GaugeExpansion> {
        if self.count > 0 && self.header != Header::Gauge {
            return None;
        }
        if is_stale(histogram.sum) {
            return Some(GaugeExpansion::default());
        }
        if is_stale(self.totals().2) {
            return None;
        }
        if histogram.schema != self.schema || histogram.zero_threshold != self.zero_threshold {
            return None;
        }
        if histogram.schema == CUSTOM_BUCKETS_SCHEMA
            && histogram.custom_values != self.custom_values
        {
            return None;
        }
        let (positive_forward, positive_backward, positive_spans) =
            expand_spans_both_ways(&self.positive_spans, &histogram.positive_spans);
        let (negative_forward, negative_backward, negative_spans) =
            expand_spans_both_ways(&self.negative_spans, &histogram.negative_spans);
        Some(GaugeExpansion {
            positive_forward,
            positive_backward,
            negative_forward,
            negative_backward,
            positive_spans,
            negative_spans,
        })
    }

    /// Prometheus's `AppendHistogram`/`AppendFloatHistogram` with `appendOnly` false.
    pub fn append(&mut self, mut histogram: cortexpb::Histogram) -> Appended {
        if self.count == 0 {
            if histogram.reset_hint == GAUGE_HINT {
                self.header = Header::Gauge;
            } else if histogram.reset_hint == COUNTER_RESET_HINT {
                self.header = Header::CounterReset;
            }
            self.raw_append(histogram);
            return Appended::InChunk;
        }
        if is_float(&histogram) != self.float {
            return Appended::NewChunk(Box::new(Self::new(histogram, None)));
        }
        let (forward, spans) = if histogram.reset_hint != GAUGE_HINT {
            let appendable = self.appendable(&histogram);
            if !appendable.ok || appendable.header != Header::NotCounterReset {
                let header = if self.float {
                    if appendable.counter_reset {
                        Header::CounterReset
                    } else {
                        Header::Unknown
                    }
                } else {
                    appendable.header
                };
                let mut chunk = Self::empty(self.float, header);
                chunk.raw_append(histogram);
                return Appended::NewChunk(Box::new(chunk));
            }
            if !appendable.positive_backward.is_empty() || !appendable.negative_backward.is_empty()
            {
                if appendable.positive_forward.is_empty() && appendable.negative_forward.is_empty()
                {
                    histogram.positive_spans = self.positive_spans.clone();
                    histogram.negative_spans = self.negative_spans.clone();
                } else {
                    histogram.positive_spans = adjust_for_inserts(
                        &histogram.positive_spans,
                        &appendable.positive_backward,
                    );
                    histogram.negative_spans = adjust_for_inserts(
                        &histogram.negative_spans,
                        &appendable.negative_backward,
                    );
                }
                recode_histogram(
                    &mut histogram,
                    &appendable.positive_backward,
                    &appendable.negative_backward,
                );
            }
            (
                (appendable.positive_forward, appendable.negative_forward),
                (
                    histogram.positive_spans.clone(),
                    histogram.negative_spans.clone(),
                ),
            )
        } else {
            let Some(gauge) = self.appendable_gauge(&histogram) else {
                let mut chunk = Self::empty(self.float, Header::Gauge);
                chunk.raw_append(histogram);
                return Appended::NewChunk(Box::new(chunk));
            };
            if !gauge.positive_backward.is_empty() || !gauge.negative_backward.is_empty() {
                histogram.positive_spans = gauge.positive_spans.clone();
                histogram.negative_spans = gauge.negative_spans.clone();
                recode_histogram(
                    &mut histogram,
                    &gauge.positive_backward,
                    &gauge.negative_backward,
                );
            }
            (
                (gauge.positive_forward, gauge.negative_forward),
                (
                    histogram.positive_spans.clone(),
                    histogram.negative_spans.clone(),
                ),
            )
        };
        let ((positive_forward, negative_forward), (positive_spans, negative_spans)) =
            (forward, spans);
        if !positive_forward.is_empty() || !negative_forward.is_empty() {
            *self = self.recode(
                &positive_forward,
                &negative_forward,
                &positive_spans,
                &negative_spans,
            );
            self.raw_append(histogram);
            return Appended::Recoded;
        }
        self.raw_append(histogram);
        Appended::InChunk
    }

    // Prometheus's `recode`: every sample re-encoded with the wider layout.
    fn recode(
        &self,
        positive_inserts: &[Insert],
        negative_inserts: &[Insert],
        positive_spans: &[cortexpb::BucketSpan],
        negative_spans: &[cortexpb::BucketSpan],
    ) -> Self {
        let encoded = self.encoded();
        let samples = decode(encoded.encoding, &encoded.data).expect("decode own histogram chunk");
        let (positive_len, negative_len) = (span_count(positive_spans), span_count(negative_spans));
        let mut recoded = Self::empty(self.float, self.header);
        for mut sample in samples {
            sample.positive_spans = positive_spans.to_vec();
            sample.negative_spans = negative_spans.to_vec();
            if self.float {
                if !positive_inserts.is_empty() {
                    sample.positive_counts = insert(
                        &sample.positive_counts,
                        positive_len,
                        positive_inserts,
                        false,
                    );
                }
                if !negative_inserts.is_empty() {
                    sample.negative_counts = insert(
                        &sample.negative_counts,
                        negative_len,
                        negative_inserts,
                        false,
                    );
                }
            } else {
                if !positive_inserts.is_empty() {
                    sample.positive_deltas = insert(
                        &sample.positive_deltas,
                        positive_len,
                        positive_inserts,
                        true,
                    );
                }
                if !negative_inserts.is_empty() {
                    sample.negative_deltas = insert(
                        &sample.negative_deltas,
                        negative_len,
                        negative_inserts,
                        true,
                    );
                }
            }
            recoded.raw_append(sample);
        }
        recoded
    }
}

#[derive(Debug, Default)]
struct Appendability {
    positive_forward: Vec<Insert>,
    negative_forward: Vec<Insert>,
    positive_backward: Vec<Insert>,
    negative_backward: Vec<Insert>,
    ok: bool,
    header: Header,
    counter_reset: bool,
}

#[derive(Debug, Default)]
struct GaugeExpansion {
    positive_forward: Vec<Insert>,
    negative_forward: Vec<Insert>,
    positive_backward: Vec<Insert>,
    negative_backward: Vec<Insert>,
    positive_spans: Vec<cortexpb::BucketSpan>,
    negative_spans: Vec<cortexpb::BucketSpan>,
}

fn totals_of(histogram: &cortexpb::Histogram) -> (f64, f64) {
    match histogram.count {
        Some(Count::CountFloat(count)) => (count, zero_count_float(histogram)),
        _ => (
            count_int(histogram) as f64,
            zero_count_int(histogram) as f64,
        ),
    }
}

// Prometheus's `recodeHistogram`: the new sample with the buckets only the chunk has.
fn recode_histogram(
    histogram: &mut cortexpb::Histogram,
    positive_inserts: &[Insert],
    negative_inserts: &[Insert],
) {
    let float = is_float(histogram);
    if !positive_inserts.is_empty() {
        let len = span_count(&histogram.positive_spans);
        if float {
            histogram.positive_counts =
                insert(&histogram.positive_counts, len, positive_inserts, false);
        } else {
            histogram.positive_deltas =
                insert(&histogram.positive_deltas, len, positive_inserts, true);
        }
    }
    if !negative_inserts.is_empty() {
        let len = span_count(&histogram.negative_spans);
        if float {
            histogram.negative_counts =
                insert(&histogram.negative_counts, len, negative_inserts, false);
        } else {
            histogram.negative_deltas =
                insert(&histogram.negative_deltas, len, negative_inserts, true);
        }
    }
}

fn xor_write(
    writer: &mut BitWriter,
    next: f64,
    previous: f64,
    leading: &mut u8,
    trailing: &mut u8,
) {
    let delta = next.to_bits() ^ previous.to_bits();
    if delta == 0 {
        writer.write_bit(false);
        return;
    }
    writer.write_bit(true);
    let next_leading = (delta.leading_zeros() as u8).min(31);
    let next_trailing = delta.trailing_zeros() as u8;
    if *leading != 0xff && next_leading >= *leading && next_trailing >= *trailing {
        writer.write_bit(false);
        writer.write_bits(
            delta >> *trailing,
            64 - usize::from(*leading) - usize::from(*trailing),
        );
        return;
    }
    *leading = next_leading;
    *trailing = next_trailing;
    writer.write_bit(true);
    writer.write_bits(u64::from(next_leading), 5);
    let significant = 64 - usize::from(next_leading) - usize::from(next_trailing);
    writer.write_bits(significant as u64, 6);
    writer.write_bits(delta >> next_trailing, significant);
}

fn count_int(histogram: &cortexpb::Histogram) -> u64 {
    match histogram.count {
        Some(Count::CountInt(value)) => value,
        _ => 0,
    }
}

fn count_float(histogram: &cortexpb::Histogram) -> f64 {
    match histogram.count {
        Some(Count::CountFloat(value)) => value,
        _ => 0.0,
    }
}

fn zero_count_int(histogram: &cortexpb::Histogram) -> u64 {
    match histogram.zero_count {
        Some(ZeroCount::ZeroCountInt(value)) => value,
        _ => 0,
    }
}

fn zero_count_float(histogram: &cortexpb::Histogram) -> f64 {
    match histogram.zero_count {
        Some(ZeroCount::ZeroCountFloat(value)) => value,
        _ => 0.0,
    }
}

fn write_layout(
    writer: &mut BitWriter,
    schema: i32,
    zero_threshold: f64,
    positive_spans: &[cortexpb::BucketSpan],
    negative_spans: &[cortexpb::BucketSpan],
    custom_values: &[f64],
) {
    put_zero_threshold(writer, zero_threshold);
    put_varbit_int(writer, i64::from(schema));
    put_spans(writer, positive_spans);
    put_spans(writer, negative_spans);
    if schema == CUSTOM_BUCKETS_SCHEMA {
        put_varbit_uint(writer, custom_values.len() as u64);
        for value in custom_values {
            put_custom_bound(writer, *value);
        }
    }
}

fn put_spans(writer: &mut BitWriter, spans: &[cortexpb::BucketSpan]) {
    put_varbit_uint(writer, spans.len() as u64);
    for span in spans {
        put_varbit_uint(writer, u64::from(span.length));
        put_varbit_int(writer, i64::from(span.offset));
    }
}

fn put_zero_threshold(writer: &mut BitWriter, value: f64) {
    if value == 0.0 {
        writer.write_byte(0);
        return;
    }
    let log = value.log2();
    if value.is_sign_positive() && log.fract() == 0.0 {
        let exponent = log as i32 + 1;
        if (-242..=11).contains(&exponent) && 2f64.powi(exponent - 1) == value {
            writer.write_byte((exponent + 243) as u8);
            return;
        }
    }
    writer.write_byte(255);
    writer.write_bits(value.to_bits(), 64);
}

fn put_custom_bound(writer: &mut BitWriter, value: f64) {
    let multiplied = value * 1000.0;
    if !(0.0..=33_554_430.0).contains(&multiplied) || (multiplied.round() / 1000.0) != value {
        writer.write_bit(false);
        writer.write_bits(value.to_bits(), 64);
        return;
    }
    put_varbit_uint(writer, multiplied.round() as u64 + 1);
}

fn put_varbit_int(writer: &mut BitWriter, value: i64) {
    let ranges = [
        (3, 0b10, 2),
        (6, 0b110, 3),
        (9, 0b1110, 4),
        (12, 0b11110, 5),
        (18, 0b111110, 6),
        (25, 0b1111110, 7),
        (56, 0b11111110, 8),
    ];
    if value == 0 {
        writer.write_bit(false);
        return;
    }
    for (bits, prefix, prefix_bits) in ranges {
        let min = -(1_i128 << (bits - 1)) + 1;
        let max = 1_i128 << (bits - 1);
        if (min..=max).contains(&i128::from(value)) {
            writer.write_bits(prefix, prefix_bits);
            writer.write_bits(value as u64, bits);
            return;
        }
    }
    writer.write_bits(0xff, 8);
    writer.write_bits(value as u64, 64);
}

fn put_varbit_uint(writer: &mut BitWriter, value: u64) {
    let buckets = [
        (3, 0b10, 2),
        (6, 0b110, 3),
        (9, 0b1110, 4),
        (12, 0b11110, 5),
        (18, 0b111110, 6),
        (25, 0b1111110, 7),
        (56, 0b11111110, 8),
    ];
    if value == 0 {
        writer.write_bit(false);
        return;
    }
    for (bits, prefix, prefix_bits) in buckets {
        if value.leading_zeros() as usize >= 64 - bits {
            writer.write_bits(prefix, prefix_bits);
            writer.write_bits(value, bits);
            return;
        }
    }
    writer.write_bits(0xff, 8);
    writer.write_bits(value, 64);
}

#[derive(Clone, Debug)]
struct BitWriter {
    bytes: Vec<u8>,
    remaining: u8,
}

impl BitWriter {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            remaining: 0,
        }
    }

    fn write_bit(&mut self, bit: bool) {
        if self.remaining == 0 {
            self.bytes.push(0);
            self.remaining = 8;
        }
        if bit {
            let last = self.bytes.len() - 1;
            self.bytes[last] |= 1 << (self.remaining - 1);
        }
        self.remaining -= 1;
    }

    fn write_byte(&mut self, value: u8) {
        if self.remaining == 0 {
            self.bytes.push(value);
            return;
        }
        let last = self.bytes.len() - 1;
        self.bytes[last] |= value >> (8 - self.remaining);
        self.bytes.push(value << self.remaining);
    }

    fn write_bits(&mut self, value: u64, mut bits: usize) {
        if self.remaining == 0 && bits >= 8 && bits.is_multiple_of(8) {
            self.bytes
                .extend_from_slice(&value.to_be_bytes()[8 - bits / 8..]);
            return;
        }
        while bits > 0 {
            if self.remaining == 0 {
                self.bytes.push(0);
                self.remaining = 8;
            }
            let take = bits.min(usize::from(self.remaining));
            let chunk = ((value >> (bits - take)) & ((1_u64 << take) - 1)) as u8;
            let last = self.bytes.len() - 1;
            self.bytes[last] |= chunk << (usize::from(self.remaining) - take);
            self.remaining -= take as u8;
            bits -= take;
        }
    }
}

/// Decodes a Prometheus histogram (`encoding` 5) or float histogram (6) chunk written by
/// [`encode_sequence`] or by Prometheus, with the reset hints and stale markers the Prometheus
/// iterator reports: gauge chunks mark every sample as a gauge, other chunks leave the first
/// sample's hint unknown and mark the others as not a counter reset.
pub fn decode(encoding: i32, data: &[u8]) -> anyhow::Result<Vec<cortexpb::Histogram>> {
    use anyhow::{Context, bail};
    if data.len() < 3 {
        bail!("histogram chunk too short");
    }
    let count = usize::from(u16::from_be_bytes([data[0], data[1]]));
    let gauge = Header::from_byte(data[2]) == Header::Gauge;
    let hint = |index: usize| {
        if gauge {
            GAUGE_HINT
        } else if index == 0 {
            0
        } else {
            2
        }
    };
    let stale_marker = |timestamp: i64, sum: f64, float: bool| cortexpb::Histogram {
        timestamp,
        sum,
        count: Some(if float {
            Count::CountFloat(0.0)
        } else {
            Count::CountInt(0)
        }),
        ..Default::default()
    };
    let is_float = match encoding {
        5 => false,
        6 => true,
        other => bail!("not a histogram chunk encoding: {other}"),
    };
    let mut reader = BitReader::new(&data[3..]);
    let mut result = Vec::with_capacity(count);
    if count == 0 {
        return Ok(result);
    }
    let zero_threshold = reader.zero_threshold().context("zero threshold")?;
    let schema = reader.varbit_int().context("schema")? as i32;
    let positive_spans = reader.spans().context("positive spans")?;
    let negative_spans = reader.spans().context("negative spans")?;
    let custom_values = if schema == CUSTOM_BUCKETS_SCHEMA {
        let len = reader.varbit_uint().context("custom values")? as usize;
        (0..len)
            .map(|_| reader.custom_bound())
            .collect::<Option<Vec<_>>>()
            .context("custom value")?
    } else {
        Vec::new()
    };
    let positive_len = positive_spans
        .iter()
        .map(|span| span.length as usize)
        .sum::<usize>();
    let negative_len = negative_spans
        .iter()
        .map(|span| span.length as usize)
        .sum::<usize>();
    let buckets = positive_len + negative_len;
    let base = cortexpb::Histogram {
        schema,
        zero_threshold,
        positive_spans,
        negative_spans,
        custom_values,
        ..Default::default()
    };
    let mut timestamp = reader.varbit_int().context("timestamp")?;
    let mut t_delta = 0_i64;
    if is_float {
        let mut values = (0..3 + buckets)
            .map(|_| reader.bits(64).map(f64::from_bits))
            .collect::<Option<Vec<_>>>()
            .context("first float histogram")?;
        let mut leading = vec![0xff_u8; values.len()];
        let mut trailing = vec![0_u8; values.len()];
        for index in 0..count {
            if index > 0 {
                let dod = reader.varbit_int().context("timestamp delta")?;
                t_delta = t_delta.wrapping_add(dod);
                timestamp = timestamp.wrapping_add(t_delta);
                for slot in 0..values.len() {
                    // A stale marker is written without its buckets.
                    if slot == 3 && is_stale(values[2]) {
                        break;
                    }
                    values[slot] = reader
                        .xor(values[slot], &mut leading[slot], &mut trailing[slot])
                        .context("float histogram value")?;
                }
            }
            if is_stale(values[2]) {
                result.push(stale_marker(timestamp, values[2], true));
                continue;
            }
            let mut histogram = base.clone();
            histogram.timestamp = timestamp;
            histogram.reset_hint = hint(index);
            histogram.count = Some(Count::CountFloat(values[0]));
            histogram.zero_count = Some(ZeroCount::ZeroCountFloat(values[1]));
            histogram.sum = values[2];
            histogram.positive_counts = values[3..3 + positive_len].to_vec();
            histogram.negative_counts = values[3 + positive_len..].to_vec();
            result.push(histogram);
        }
    } else {
        let mut count_value = reader.varbit_uint().context("count")?;
        let mut zero = reader.varbit_uint().context("zero count")?;
        let mut sum = f64::from_bits(reader.bits(64).context("sum")?);
        let mut bucket_values = (0..buckets)
            .map(|_| reader.varbit_int())
            .collect::<Option<Vec<_>>>()
            .context("buckets")?;
        let (mut count_delta, mut zero_delta) = (0_i64, 0_i64);
        let mut bucket_deltas = vec![0_i64; buckets];
        let (mut sum_leading, mut sum_trailing) = (0xff_u8, 0_u8);
        for index in 0..count {
            if index > 0 {
                t_delta = t_delta.wrapping_add(reader.varbit_int().context("timestamp delta")?);
                timestamp = timestamp.wrapping_add(t_delta);
                count_delta = count_delta.wrapping_add(reader.varbit_int().context("count delta")?);
                count_value = count_value.wrapping_add(count_delta as u64);
                zero_delta = zero_delta.wrapping_add(reader.varbit_int().context("zero delta")?);
                zero = zero.wrapping_add(zero_delta as u64);
                sum = reader
                    .xor(sum, &mut sum_leading, &mut sum_trailing)
                    .context("sum")?;
                if !is_stale(sum) {
                    for (value, delta) in bucket_values.iter_mut().zip(&mut bucket_deltas) {
                        *delta = delta.wrapping_add(reader.varbit_int().context("bucket delta")?);
                        *value = value.wrapping_add(*delta);
                    }
                }
            }
            if is_stale(sum) {
                result.push(stale_marker(timestamp, sum, false));
                continue;
            }
            let mut histogram = base.clone();
            histogram.timestamp = timestamp;
            histogram.reset_hint = hint(index);
            histogram.count = Some(Count::CountInt(count_value));
            histogram.zero_count = Some(ZeroCount::ZeroCountInt(zero));
            histogram.sum = sum;
            histogram.positive_deltas = bucket_values[..positive_len].to_vec();
            histogram.negative_deltas = bucket_values[positive_len..].to_vec();
            result.push(histogram);
        }
    }
    Ok(result)
}

struct BitReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> BitReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn bit(&mut self) -> Option<bool> {
        let byte = *self.bytes.get(self.position / 8)?;
        let bit = (byte >> (7 - self.position % 8)) & 1 == 1;
        self.position += 1;
        Some(bit)
    }

    fn bits(&mut self, count: usize) -> Option<u64> {
        let mut value = 0_u64;
        for _ in 0..count {
            value = (value << 1) | u64::from(self.bit()?);
        }
        Some(value)
    }

    fn varbit_prefix(&mut self) -> Option<usize> {
        let mut ones = 0;
        while ones < 8 && self.bit()? {
            ones += 1;
        }
        Some(match ones {
            0 => 0,
            1 => 3,
            2 => 6,
            3 => 9,
            4 => 12,
            5 => 18,
            6 => 25,
            7 => 56,
            _ => 64,
        })
    }

    fn varbit_int(&mut self) -> Option<i64> {
        let bits = self.varbit_prefix()?;
        if bits == 0 {
            return Some(0);
        }
        let raw = self.bits(bits)?;
        if bits == 64 {
            return Some(raw as i64);
        }
        let value = raw as i64;
        Some(if value > 1 << (bits - 1) {
            value - (1 << bits)
        } else {
            value
        })
    }

    fn varbit_uint(&mut self) -> Option<u64> {
        let bits = self.varbit_prefix()?;
        if bits == 0 {
            return Some(0);
        }
        self.bits(bits)
    }

    fn zero_threshold(&mut self) -> Option<f64> {
        let byte = self.bits(8)? as u8;
        Some(match byte {
            0 => 0.0,
            255 => f64::from_bits(self.bits(64)?),
            byte => 2f64.powi(i32::from(byte) - 243 - 1),
        })
    }

    fn spans(&mut self) -> Option<Vec<cortexpb::BucketSpan>> {
        let len = self.varbit_uint()? as usize;
        (0..len)
            .map(|_| {
                let length = self.varbit_uint()? as u32;
                let offset = self.varbit_int()? as i32;
                Some(cortexpb::BucketSpan { offset, length })
            })
            .collect()
    }

    fn custom_bound(&mut self) -> Option<f64> {
        let value = self.varbit_uint()?;
        if value == 0 {
            return Some(f64::from_bits(self.bits(64)?));
        }
        Some((value - 1) as f64 / 1000.0)
    }

    fn xor(&mut self, previous: f64, leading: &mut u8, trailing: &mut u8) -> Option<f64> {
        if !self.bit()? {
            return Some(previous);
        }
        if self.bit()? {
            *leading = self.bits(5)? as u8;
            let mut significant = self.bits(6)? as u8;
            if significant == 0 {
                significant = 64;
            }
            *trailing = 64 - *leading - significant;
        }
        let significant = 64 - usize::from(*leading) - usize::from(*trailing);
        let delta = self.bits(significant)? << *trailing;
        Some(f64::from_bits(previous.to_bits() ^ delta))
    }
}

#[cfg(test)]
mod tests {
    use super::{BitWriter, decode, encode_sequence};
    use crate::proto::cortexpb;
    use crate::proto::cortexpb::histogram::Count;

    #[test]
    fn write_bits_matches_bitwise_reference_at_every_alignment() {
        for prefix_bits in 0..8 {
            for bits in 0..=64 {
                for value in [0, u64::MAX, 0x0123_4567_89ab_cdef] {
                    let mut actual = BitWriter::new(vec![0, 1, 0]);
                    let mut reference = BitWriter::new(vec![0, 1, 0]);
                    for _ in 0..prefix_bits {
                        actual.write_bit(true);
                        reference.write_bit(true);
                    }
                    actual.write_bits(value, bits);
                    for bit in (0..bits).rev() {
                        reference.write_bit(((value >> bit) & 1) != 0);
                    }
                    actual.write_byte(0xa5);
                    reference.write_byte(0xa5);
                    assert_eq!(
                        actual.bytes, reference.bytes,
                        "prefix={prefix_bits} bits={bits}"
                    );
                    assert_eq!(actual.remaining, reference.remaining);
                }
            }
        }
    }

    fn int_histogram(timestamp: i64, scale: i64) -> cortexpb::Histogram {
        cortexpb::Histogram {
            timestamp,
            count: Some(Count::CountInt((12 * scale) as u64)),
            zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountInt(scale as u64)),
            sum: 1.5 * scale as f64,
            schema: 3,
            zero_threshold: 1e-128,
            positive_spans: vec![
                cortexpb::BucketSpan {
                    offset: -2,
                    length: 2,
                },
                cortexpb::BucketSpan {
                    offset: 5,
                    length: 1,
                },
            ],
            negative_spans: vec![cortexpb::BucketSpan {
                offset: 1,
                length: 1,
            }],
            positive_deltas: vec![scale, 3 * scale, -scale],
            negative_deltas: vec![2 * scale],
            ..Default::default()
        }
    }

    #[test]
    fn decodes_what_it_encodes() {
        let mut ints = (1..40)
            .map(|t| int_histogram(t * 15_000 + t * t, t))
            .collect::<Vec<_>>();
        ints[0].reset_hint = 1;
        let encoded = encode_sequence(&ints);
        // Like the Prometheus iterator: the first hint is unknown, the others not a reset.
        let mut expected = ints.clone();
        expected[0].reset_hint = 0;
        for histogram in &mut expected[1..] {
            histogram.reset_hint = 2;
        }
        assert_eq!(decode(encoded.encoding, &encoded.data).unwrap(), expected);

        let floats = (0..30)
            .map(|t| cortexpb::Histogram {
                timestamp: 1_000 + t * 60_000,
                count: Some(Count::CountFloat(3.5 + t as f64)),
                zero_count: Some(cortexpb::histogram::ZeroCount::ZeroCountFloat(0.25)),
                sum: t as f64 * 1.1,
                schema: -53,
                custom_values: vec![0.5, 1.0, 2.5, 1e30],
                positive_spans: vec![cortexpb::BucketSpan {
                    offset: 0,
                    length: 3,
                }],
                positive_counts: vec![1.0, t as f64 / 3.0, 2.0],
                reset_hint: 3,
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let encoded = encode_sequence(&floats);
        let decoded = decode(encoded.encoding, &encoded.data).unwrap();
        assert_eq!(decoded.len(), floats.len());
        // Gauge chunks mark every sample as a gauge.
        assert_eq!(decoded, floats);
        assert!(decode(5, &[0, 1]).is_err());
    }

    #[test]
    fn equal_values_ignores_reset_hints_and_empty_spans() {
        let base = cortexpb::Histogram {
            timestamp: 1,
            count: Some(Count::CountInt(3)),
            sum: 2.0,
            positive_spans: vec![
                cortexpb::BucketSpan {
                    offset: 1,
                    length: 0,
                },
                cortexpb::BucketSpan {
                    offset: 2,
                    length: 2,
                },
            ],
            positive_deltas: vec![1, 1],
            ..Default::default()
        };
        let mut other = base.clone();
        other.reset_hint = 2;
        other.zero_count = Some(cortexpb::histogram::ZeroCount::ZeroCountInt(0));
        other.positive_spans = vec![cortexpb::BucketSpan {
            offset: 3,
            length: 2,
        }];
        assert!(super::equal_values(&base, &other));
        other.sum = 2.5;
        assert!(!super::equal_values(&base, &other));
    }
}
