#[derive(Debug)]
struct BitWriter {
    bytes: Vec<u8>,
    free: u8,
}

impl BitWriter {
    fn new() -> Self {
        Self {
            bytes: vec![0, 0],
            free: 0,
        }
    }

    fn bit(&mut self, value: bool) {
        if self.free == 0 {
            self.bytes.push(0);
            self.free = 8;
        }
        if value {
            let last = self.bytes.len() - 1;
            self.bytes[last] |= 1 << (self.free - 1);
        }
        self.free -= 1;
    }

    fn byte(&mut self, value: u8) {
        if self.free == 0 {
            self.bytes.push(value);
            return;
        }
        let last = self.bytes.len() - 1;
        self.bytes[last] |= value >> (8 - self.free);
        self.bytes.push(value << self.free);
    }

    fn bits(&mut self, value: u64, count: usize) {
        for shift in (0..count).rev() {
            self.bit(((value >> shift) & 1) != 0);
        }
    }
}

pub fn encode(samples: &[(i64, f64)]) -> Vec<u8> {
    encode_iter(samples.len(), samples.iter().copied())
}

pub fn encode_iter(samples_len: usize, samples: impl Iterator<Item = (i64, f64)>) -> Vec<u8> {
    assert!(samples_len <= u16::MAX as usize);
    let mut appender = Appender::default();
    for (timestamp, value) in samples {
        appender.append(timestamp, value);
    }
    appender.into_bytes()
}

/// Incrementally encodes a Prometheus XOR chunk so an open chunk costs about two bytes per sample.
#[derive(Debug)]
pub struct Appender {
    out: BitWriter,
    count: u16,
    previous_time: i64,
    previous_delta: u64,
    previous_value: f64,
    leading: u8,
    trailing: u8,
}

impl Default for Appender {
    fn default() -> Self {
        Self {
            out: BitWriter::new(),
            count: 0,
            previous_time: i64::MIN,
            previous_delta: 0,
            previous_value: 0.0,
            leading: u8::MAX,
            trailing: 0,
        }
    }
}

impl Appender {
    pub fn len(&self) -> usize {
        self.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn last_timestamp(&self) -> Option<i64> {
        (self.count > 0).then_some(self.previous_time)
    }

    pub fn append(&mut self, timestamp: i64, value: f64) {
        assert!(self.count < u16::MAX, "XOR chunk sample count exceeds u16");
        let out = &mut self.out;
        let mut delta = 0_u64;
        match self.count {
            0 => {
                write_varint(out, timestamp);
                out.bits(value.to_bits(), 64);
            }
            1 => {
                delta = timestamp.wrapping_sub(self.previous_time) as u64;
                write_uvarint(out, delta);
                write_value(
                    out,
                    value,
                    self.previous_value,
                    &mut self.leading,
                    &mut self.trailing,
                );
            }
            _ => {
                delta = timestamp.wrapping_sub(self.previous_time) as u64;
                let dod = delta.wrapping_sub(self.previous_delta) as i64;
                if dod == 0 {
                    out.bit(false);
                } else if bit_range(dod, 14) {
                    out.byte(0b1000_0000 | (((dod >> 8) as u8) & 0x3f));
                    out.byte(dod as u8);
                } else if bit_range(dod, 17) {
                    out.bits(0b110, 3);
                    out.bits(dod as u64, 17);
                } else if bit_range(dod, 20) {
                    out.bits(0b1110, 4);
                    out.bits(dod as u64, 20);
                } else {
                    out.bits(0b1111, 4);
                    out.bits(dod as u64, 64);
                }
                write_value(
                    out,
                    value,
                    self.previous_value,
                    &mut self.leading,
                    &mut self.trailing,
                );
            }
        }
        self.previous_time = timestamp;
        self.previous_delta = delta;
        self.previous_value = value;
        self.count += 1;
        self.out.bytes[0..2].copy_from_slice(&self.count.to_be_bytes());
    }

    pub fn bytes(&self) -> &[u8] {
        &self.out.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.out.bytes
    }
}

fn bit_range(value: i64, bits: u8) -> bool {
    -((1_i64 << (bits - 1)) - 1) <= value && value <= 1_i64 << (bits - 1)
}

fn write_value(
    out: &mut BitWriter,
    value: f64,
    previous: f64,
    leading: &mut u8,
    trailing: &mut u8,
) {
    let delta = value.to_bits() ^ previous.to_bits();
    if delta == 0 {
        out.bit(false);
        return;
    }
    out.bit(true);
    let new_leading = (delta.leading_zeros() as u8).min(31);
    let new_trailing = delta.trailing_zeros() as u8;
    if *leading != u8::MAX && new_leading >= *leading && new_trailing >= *trailing {
        out.bit(false);
        out.bits(
            delta >> *trailing,
            64 - *leading as usize - *trailing as usize,
        );
        return;
    }
    *leading = new_leading;
    *trailing = new_trailing;
    out.bit(true);
    out.bits(new_leading as u64, 5);
    let significant = 64 - new_leading - new_trailing;
    out.bits((significant & 0x3f) as u64, 6);
    out.bits(delta >> new_trailing, significant as usize);
}

fn write_uvarint(out: &mut BitWriter, mut value: u64) {
    while value >= 0x80 {
        out.byte(value as u8 | 0x80);
        value >>= 7;
    }
    out.byte(value as u8);
}

fn write_varint(out: &mut BitWriter, value: i64) {
    let encoded = if value < 0 {
        (!(value as u64)) << 1 | 1
    } else {
        (value as u64) << 1
    };
    write_uvarint(out, encoded);
}

pub fn decode(bytes: &[u8]) -> Vec<(i64, f64)> {
    struct Reader<'a> {
        bytes: &'a [u8],
        bit: usize,
    }
    impl Reader<'_> {
        fn bit(&mut self) -> bool {
            let value = self.bytes[self.bit / 8] & (0x80 >> (self.bit % 8)) != 0;
            self.bit += 1;
            value
        }
        fn bits(&mut self, count: usize) -> u64 {
            (0..count).fold(0, |value, _| value << 1 | u64::from(self.bit()))
        }
        fn uvarint(&mut self) -> u64 {
            let mut value = 0;
            for shift in (0..).step_by(7) {
                let byte = self.bits(8);
                value |= (byte & 0x7f) << shift;
                if byte < 0x80 {
                    break;
                }
            }
            value
        }
        fn signed(&mut self, count: usize) -> i64 {
            let value = self.bits(count);
            if count < 64 && value >= 1 << (count - 1) {
                value as i64 - (1 << count)
            } else {
                value as i64
            }
        }
    }
    let count = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    let mut reader = Reader { bytes, bit: 16 };
    let mut samples = Vec::with_capacity(count);
    let (mut time, mut delta, mut value) = (0_i64, 0_i64, 0_u64);
    let (mut leading, mut trailing) = (0_u32, 0_u32);
    for index in 0..count {
        match index {
            0 => {
                let encoded = reader.uvarint();
                time = (encoded >> 1) as i64 ^ -((encoded & 1) as i64);
                value = reader.bits(64);
            }
            _ => {
                if index == 1 {
                    delta = reader.uvarint() as i64;
                } else {
                    let dod = if !reader.bit() {
                        0
                    } else if !reader.bit() {
                        reader.signed(14)
                    } else if !reader.bit() {
                        reader.signed(17)
                    } else if !reader.bit() {
                        reader.signed(20)
                    } else {
                        reader.signed(64)
                    };
                    delta += dod;
                }
                time += delta;
                if reader.bit() {
                    if reader.bit() {
                        leading = reader.bits(5) as u32;
                        let significant = match reader.bits(6) {
                            0 => 64,
                            bits => bits as u32,
                        };
                        trailing = 64 - leading - significant;
                    }
                    let significant = 64 - leading - trailing;
                    value ^= reader.bits(significant as usize) << trailing;
                }
            }
        }
        samples.push((time, f64::from_bits(value)));
    }
    samples
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appender_matches_batch_encoding_and_round_trips() {
        let samples = [
            (1_000, 1.5),
            (16_000, 1.5),
            (31_000, 2.25),
            (46_001, -7.0),
            (1_000_000, f64::from_bits(0x7ff0_0000_0000_0002)),
            (1_000_015, 1e300),
        ];
        let mut appender = Appender::default();
        for (timestamp, value) in samples {
            appender.append(timestamp, value);
        }
        assert_eq!(appender.last_timestamp(), Some(1_000_015));
        assert_eq!(appender.bytes(), encode(&samples).as_slice());
        let decoded = decode(appender.bytes());
        assert_eq!(decoded.len(), samples.len());
        for ((time, value), (expected_time, expected_value)) in decoded.iter().zip(samples) {
            assert_eq!(*time, expected_time);
            assert_eq!(value.to_bits(), expected_value.to_bits());
        }
    }
}
