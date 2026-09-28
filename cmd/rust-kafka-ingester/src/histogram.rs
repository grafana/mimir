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
    let histogram = &histograms[0];
    let is_float = matches!(histogram.count, Some(Count::CountFloat(_)));
    let count = u16::try_from(histograms.len()).expect("histogram chunk sample count exceeds u16");
    let mut writer = BitWriter::new(vec![
        (count >> 8) as u8,
        count as u8,
        reset_header(histogram.reset_hint),
    ]);
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
    if is_float {
        writer.write_bits(count_float(histogram).to_bits(), 64);
        writer.write_bits(zero_count_float(histogram).to_bits(), 64);
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
    } else {
        put_varbit_uint(&mut writer, count_int(histogram));
        put_varbit_uint(&mut writer, zero_count_int(histogram));
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
    }
    if is_float {
        encode_float_following(&mut writer, histograms);
    } else {
        encode_int_following(&mut writer, histograms);
    }
    EncodedHistogram {
        encoding: if is_float { 6 } else { 5 },
        data: writer.bytes,
    }
}

fn encode_int_following(writer: &mut BitWriter, histograms: &[cortexpb::Histogram]) {
    let first = &histograms[0];
    let mut last_t = first.timestamp;
    let mut last_t_delta = 0_i64;
    let mut last_count = count_int(first);
    let mut last_count_delta = 0_i64;
    let mut last_zero = zero_count_int(first);
    let mut last_zero_delta = 0_i64;
    let mut last_sum = first.sum;
    let mut sum_leading = 0xff;
    let mut sum_trailing = 0;
    let mut last_buckets: Vec<i64> = first
        .positive_deltas
        .iter()
        .chain(&first.negative_deltas)
        .copied()
        .collect();
    let mut last_bucket_deltas = vec![0_i64; last_buckets.len()];
    for histogram in &histograms[1..] {
        let t_delta = histogram.timestamp.wrapping_sub(last_t);
        let count = count_int(histogram);
        let count_delta = count.wrapping_sub(last_count) as i64;
        let zero = zero_count_int(histogram);
        let zero_delta = zero.wrapping_sub(last_zero) as i64;
        put_varbit_int(writer, t_delta.wrapping_sub(last_t_delta));
        put_varbit_int(writer, count_delta.wrapping_sub(last_count_delta));
        put_varbit_int(writer, zero_delta.wrapping_sub(last_zero_delta));
        xor_write(
            writer,
            histogram.sum,
            last_sum,
            &mut sum_leading,
            &mut sum_trailing,
        );
        for ((current, last), last_delta) in histogram
            .positive_deltas
            .iter()
            .chain(&histogram.negative_deltas)
            .zip(&mut last_buckets)
            .zip(&mut last_bucket_deltas)
        {
            let delta = current.wrapping_sub(*last);
            put_varbit_int(writer, delta.wrapping_sub(*last_delta));
            *last_delta = delta;
            *last = *current;
        }
        last_t = histogram.timestamp;
        last_t_delta = t_delta;
        last_count = count;
        last_count_delta = count_delta;
        last_zero = zero;
        last_zero_delta = zero_delta;
        last_sum = histogram.sum;
    }
}

fn encode_float_following(writer: &mut BitWriter, histograms: &[cortexpb::Histogram]) {
    let first = &histograms[0];
    let mut last_t = first.timestamp;
    let mut last_t_delta = 0_i64;
    let mut values: Vec<f64> = [count_float(first), zero_count_float(first), first.sum]
        .into_iter()
        .chain(
            first
                .positive_counts
                .iter()
                .chain(&first.negative_counts)
                .copied(),
        )
        .collect();
    let mut leading = vec![0xff; values.len()];
    let mut trailing = vec![0; values.len()];
    for histogram in &histograms[1..] {
        let t_delta = histogram.timestamp.wrapping_sub(last_t);
        put_varbit_int(writer, t_delta.wrapping_sub(last_t_delta));
        for (index, value) in [
            count_float(histogram),
            zero_count_float(histogram),
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
        last_t = histogram.timestamp;
        last_t_delta = t_delta;
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
}
