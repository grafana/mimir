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

pub fn compatible(previous: &cortexpb::Histogram, next: &cortexpb::Histogram) -> bool {
    if previous.sum.to_bits() == STALE_NAN || next.sum.to_bits() == STALE_NAN {
        return false;
    }
    if previous.schema != next.schema
        || previous.zero_threshold.to_bits() != next.zero_threshold.to_bits()
        || previous.positive_spans != next.positive_spans
        || previous.negative_spans != next.negative_spans
        || previous.custom_values != next.custom_values
        || previous.reset_hint != next.reset_hint
        || next.reset_hint == 1
        || next.timestamp <= previous.timestamp
    {
        return false;
    }
    match (&previous.count, &next.count) {
        (Some(Count::CountInt(old)), Some(Count::CountInt(new))) => {
            previous.positive_deltas.len() == next.positive_deltas.len()
                && previous.negative_deltas.len() == next.negative_deltas.len()
                && (next.reset_hint == 3
                    || (new >= old
                        && zero_count_int(next) >= zero_count_int(previous)
                        && integer_buckets_non_decreasing(
                            &previous.positive_deltas,
                            &next.positive_deltas,
                        )
                        && integer_buckets_non_decreasing(
                            &previous.negative_deltas,
                            &next.negative_deltas,
                        )))
        }
        (Some(Count::CountFloat(old)), Some(Count::CountFloat(new))) => {
            previous.positive_counts.len() == next.positive_counts.len()
                && previous.negative_counts.len() == next.negative_counts.len()
                && (next.reset_hint == 3
                    || (new >= old
                        && zero_count_float(next) >= zero_count_float(previous)
                        && previous
                            .positive_counts
                            .iter()
                            .zip(&next.positive_counts)
                            .all(|(old, new)| new >= old)
                        && previous
                            .negative_counts
                            .iter()
                            .zip(&next.negative_counts)
                            .all(|(old, new)| new >= old)))
        }
        _ => false,
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

fn integer_buckets_non_decreasing(previous: &[i64], next: &[i64]) -> bool {
    if previous == next {
        return true;
    }
    let mut old_count = 0_i128;
    let mut new_count = 0_i128;
    previous.iter().zip(next).all(|(old, new)| {
        old_count += i128::from(*old);
        new_count += i128::from(*new);
        new_count >= old_count
    })
}

pub fn encode_sequence(histograms: &[cortexpb::Histogram]) -> EncodedHistogram {
    let mut appender = HistogramAppender::new(histograms[0].clone());
    for histogram in &histograms[1..] {
        appender.append(histogram.clone());
    }
    appender.encoded()
}

// Delta state carried between samples of a chunk, like Prometheus's histogram appenders.
#[derive(Clone, Debug)]
enum AppendState {
    Int {
        t_delta: i64,
        count: u64,
        count_delta: i64,
        zero: u64,
        zero_delta: i64,
        sum: f64,
        sum_leading: u8,
        sum_trailing: u8,
        buckets: Vec<i64>,
        bucket_deltas: Vec<i64>,
    },
    Float {
        t_delta: i64,
        values: Vec<f64>,
        leading: Vec<u8>,
        trailing: Vec<u8>,
    },
}

/// An open histogram chunk appended one sample at a time, so it keeps only its encoded bytes and
/// the last histogram. Callers only append histograms `compatible` with the last one.
#[derive(Clone, Debug)]
pub struct HistogramAppender {
    writer: BitWriter,
    count: u16,
    first_timestamp: i64,
    last: cortexpb::Histogram,
    state: AppendState,
    encoding: i32,
}

impl HistogramAppender {
    pub fn new(histogram: cortexpb::Histogram) -> Self {
        let is_float = matches!(histogram.count, Some(Count::CountFloat(_)));
        let mut writer = BitWriter::new(vec![0, 1, reset_header(histogram.reset_hint)]);
        let stale = histogram.sum.to_bits() == STALE_NAN;
        if stale {
            write_layout(&mut writer, 0, 0.0, &[], &[], &[]);
        } else {
            write_layout(
                &mut writer,
                histogram.schema,
                histogram.zero_threshold,
                &histogram.positive_spans,
                &histogram.negative_spans,
                &histogram.custom_values,
            );
        }
        put_varbit_int(&mut writer, histogram.timestamp);
        let state = if is_float {
            writer.write_bits(count_float(&histogram).to_bits(), 64);
            writer.write_bits(zero_count_float(&histogram).to_bits(), 64);
            writer.write_bits(histogram.sum.to_bits(), 64);
            if !stale {
                for value in histogram
                    .positive_counts
                    .iter()
                    .chain(&histogram.negative_counts)
                {
                    writer.write_bits(value.to_bits(), 64);
                }
            }
            let values: Vec<f64> = [
                count_float(&histogram),
                zero_count_float(&histogram),
                histogram.sum,
            ]
            .into_iter()
            .chain(
                histogram
                    .positive_counts
                    .iter()
                    .chain(&histogram.negative_counts)
                    .copied(),
            )
            .collect();
            AppendState::Float {
                t_delta: 0,
                leading: vec![0xff; values.len()],
                trailing: vec![0; values.len()],
                values,
            }
        } else {
            put_varbit_uint(&mut writer, count_int(&histogram));
            put_varbit_uint(&mut writer, zero_count_int(&histogram));
            writer.write_bits(histogram.sum.to_bits(), 64);
            if !stale {
                for value in histogram
                    .positive_deltas
                    .iter()
                    .chain(&histogram.negative_deltas)
                {
                    put_varbit_int(&mut writer, *value);
                }
            }
            let buckets: Vec<i64> = histogram
                .positive_deltas
                .iter()
                .chain(&histogram.negative_deltas)
                .copied()
                .collect();
            AppendState::Int {
                t_delta: 0,
                count: count_int(&histogram),
                count_delta: 0,
                zero: zero_count_int(&histogram),
                zero_delta: 0,
                sum: histogram.sum,
                sum_leading: 0xff,
                sum_trailing: 0,
                bucket_deltas: vec![0; buckets.len()],
                buckets,
            }
        };
        Self {
            writer,
            count: 1,
            first_timestamp: histogram.timestamp,
            encoding: if is_float { 6 } else { 5 },
            last: histogram,
            state,
        }
    }

    pub fn append(&mut self, histogram: cortexpb::Histogram) {
        let writer = &mut self.writer;
        let last_t = self.last.timestamp;
        match &mut self.state {
            AppendState::Int {
                t_delta,
                count,
                count_delta,
                zero,
                zero_delta,
                sum,
                sum_leading,
                sum_trailing,
                buckets,
                bucket_deltas,
            } => {
                let next_t_delta = histogram.timestamp.wrapping_sub(last_t);
                let next_count = count_int(&histogram);
                let next_count_delta = next_count.wrapping_sub(*count) as i64;
                let next_zero = zero_count_int(&histogram);
                let next_zero_delta = next_zero.wrapping_sub(*zero) as i64;
                put_varbit_int(writer, next_t_delta.wrapping_sub(*t_delta));
                put_varbit_int(writer, next_count_delta.wrapping_sub(*count_delta));
                put_varbit_int(writer, next_zero_delta.wrapping_sub(*zero_delta));
                xor_write(writer, histogram.sum, *sum, sum_leading, sum_trailing);
                for ((current, last), last_delta) in histogram
                    .positive_deltas
                    .iter()
                    .chain(&histogram.negative_deltas)
                    .zip(buckets.iter_mut())
                    .zip(bucket_deltas.iter_mut())
                {
                    let delta = current.wrapping_sub(*last);
                    put_varbit_int(writer, delta.wrapping_sub(*last_delta));
                    *last_delta = delta;
                    *last = *current;
                }
                *t_delta = next_t_delta;
                *count = next_count;
                *count_delta = next_count_delta;
                *zero = next_zero;
                *zero_delta = next_zero_delta;
                *sum = histogram.sum;
            }
            AppendState::Float {
                t_delta,
                values,
                leading,
                trailing,
            } => {
                let next_t_delta = histogram.timestamp.wrapping_sub(last_t);
                put_varbit_int(writer, next_t_delta.wrapping_sub(*t_delta));
                for (index, value) in [
                    count_float(&histogram),
                    zero_count_float(&histogram),
                    histogram.sum,
                ]
                .into_iter()
                .chain(
                    histogram
                        .positive_counts
                        .iter()
                        .chain(&histogram.negative_counts)
                        .copied(),
                )
                .enumerate()
                {
                    xor_write(
                        writer,
                        value,
                        values[index],
                        &mut leading[index],
                        &mut trailing[index],
                    );
                    values[index] = value;
                }
                *t_delta = next_t_delta;
            }
        }
        self.count = self
            .count
            .checked_add(1)
            .expect("histogram chunk sample count exceeds u16");
        self.last = histogram;
    }

    pub fn len(&self) -> usize {
        usize::from(self.count)
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
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
        EncodedHistogram {
            encoding: self.encoding,
            data,
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

fn reset_header(reset_hint: i32) -> u8 {
    match reset_hint {
        1 => 0x80,
        3 => 0xc0,
        _ => 0,
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
/// [`encode_sequence`] or by Prometheus. Samples after the first carry no reset hint, like the
/// Prometheus iterator reports them.
pub fn decode(encoding: i32, data: &[u8]) -> anyhow::Result<Vec<cortexpb::Histogram>> {
    use anyhow::{Context, bail};
    if data.len() < 3 {
        bail!("histogram chunk too short");
    }
    let count = usize::from(u16::from_be_bytes([data[0], data[1]]));
    let reset_hint = match data[2] & 0xc0 {
        0x80 => 1,
        0x40 => 2,
        0xc0 => 3,
        _ => 0,
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
                for (slot, value) in values.iter_mut().enumerate() {
                    *value = reader
                        .xor(*value, &mut leading[slot], &mut trailing[slot])
                        .context("float histogram value")?;
                }
            }
            let mut histogram = base.clone();
            histogram.timestamp = timestamp;
            histogram.reset_hint = if index == 0 { reset_hint } else { 0 };
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
                for (value, delta) in bucket_values.iter_mut().zip(&mut bucket_deltas) {
                    *delta = delta.wrapping_add(reader.varbit_int().context("bucket delta")?);
                    *value = value.wrapping_add(*delta);
                }
            }
            let mut histogram = base.clone();
            histogram.timestamp = timestamp;
            histogram.reset_hint = if index == 0 { reset_hint } else { 0 };
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
    use super::{BitWriter, compatible, decode, encode_sequence};
    use crate::proto::cortexpb;
    use crate::proto::cortexpb::histogram::Count;

    #[test]
    fn splits_counter_resets_and_keeps_gauge_changes() {
        let original = cortexpb::Histogram {
            timestamp: 1,
            count: Some(Count::CountInt(14)),
            positive_spans: vec![cortexpb::BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_deltas: vec![4, 6],
            ..Default::default()
        };
        let mut next = original.clone();
        next.timestamp += 1;
        next.count = Some(Count::CountInt(14));
        next.positive_deltas = vec![3, 8];
        assert!(!compatible(&original, &next));
        next.positive_deltas = vec![4, 6];
        assert!(compatible(&original, &next));
        next.reset_hint = 1;
        assert!(!compatible(&original, &next));
        let mut gauge = original;
        gauge.reset_hint = 3;
        next.reset_hint = 3;
        next.count = Some(Count::CountInt(13));
        next.positive_deltas = vec![3, 7];
        assert!(compatible(&gauge, &next));
    }

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
        let mut expected = ints.clone();
        for histogram in &mut expected[1..] {
            histogram.reset_hint = 0;
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
        assert_eq!(decoded[0], floats[0]);
        for (decoded, original) in decoded.iter().zip(&floats).skip(1) {
            let mut original = original.clone();
            original.reset_hint = 0;
            assert_eq!(decoded, &original);
        }
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
