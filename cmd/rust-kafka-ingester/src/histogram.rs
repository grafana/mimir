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

#[cfg(test)]
mod tests {
    use super::{BitWriter, compatible};
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
}
