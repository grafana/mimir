// SPDX-License-Identifier: AGPL-3.0-only

#[derive(Clone, Debug)]
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

    fn push(&mut self, byte: u8) {
        if self.bytes.len() == self.bytes.capacity() {
            let needed = self.bytes.len() + 1;
            let capacity = [
                16, 32, 48, 64, 96, 128, 160, 192, 256, 320, 384, 512, 640, 768, 1024, 1280,
            ]
            .into_iter()
            .find(|size| *size >= needed)
            .unwrap_or(needed + needed / 4);
            self.bytes.reserve_exact(capacity - self.bytes.len());
        }
        self.bytes.push(byte);
    }

    fn bit(&mut self, value: bool) {
        if self.free == 0 {
            self.push(0);
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
            self.push(value);
            return;
        }
        let last = self.bytes.len() - 1;
        self.bytes[last] |= value >> (8 - self.free);
        self.push(value << self.free);
    }

    /// The low `count` bits of `value`, most significant first, filling the last byte's free bits
    /// at once: writing them one by one was most of what appending a sample cost.
    fn bits(&mut self, value: u64, mut count: usize) {
        while count > 0 {
            if self.free == 0 {
                self.push(0);
                self.free = 8;
            }
            let take = count.min(usize::from(self.free));
            count -= take;
            let chunk = ((value >> count) & ((1 << take) - 1)) as u8;
            let last = self.bytes.len() - 1;
            self.bytes[last] |= chunk << (usize::from(self.free) - take);
            self.free -= take as u8;
        }
    }
}

/// Like Prometheus's `MaxBytesPerXORChunkBeforeAppend`: past this, the head and merged chunks
/// start a new chunk, so no chunk exceeds 1024 bytes whatever its samples.
pub const MAX_BYTES_BEFORE_APPEND: usize = 1024 - 19;

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
#[derive(Clone, Debug)]
pub struct Appender {
    out: BitWriter,
    count: u16,
    first_time: i64,
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
            first_time: i64::MIN,
            previous_time: i64::MIN,
            previous_delta: 0,
            previous_value: 0.0,
            leading: u8::MAX,
            trailing: 0,
        }
    }
}

impl Appender {
    pub fn first_timestamp(&self) -> Option<i64> {
        (self.count > 0).then_some(self.first_time)
    }
    pub fn byte_len(&self) -> usize {
        self.out.bytes.len()
    }

    pub fn len(&self) -> usize {
        self.count as usize
    }

    /// Heap bytes the open chunk holds.
    pub fn byte_capacity(&self) -> usize {
        self.out.bytes.capacity()
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn last_timestamp(&self) -> Option<i64> {
        (self.count > 0).then_some(self.previous_time)
    }

    pub fn last_value(&self) -> Option<f64> {
        (self.count > 0).then_some(self.previous_value)
    }

    pub fn append(&mut self, timestamp: i64, value: f64) {
        if self.count == 0 {
            self.first_time = timestamp;
        }
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

    pub fn write_state(&self, writer: &mut impl std::io::Write) -> std::io::Result<()> {
        writer.write_all(&self.count.to_le_bytes())?;
        writer.write_all(&self.previous_time.to_le_bytes())?;
        writer.write_all(&self.previous_delta.to_le_bytes())?;
        writer.write_all(&self.previous_value.to_bits().to_le_bytes())?;
        writer.write_all(&[self.leading, self.trailing, self.out.free])?;
        writer.write_all(&(self.out.bytes.len() as u32).to_le_bytes())?;
        writer.write_all(&self.out.bytes)
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

    /// The next `count` bits, most significant first, a byte's worth at a time.
    fn bits(&mut self, mut count: usize) -> u64 {
        let mut value = 0_u64;
        while count > 0 {
            let available = 8 - self.bit % 8;
            let take = count.min(available);
            let byte = u64::from(self.bytes[self.bit / 8]);
            value = value << take | (byte >> (available - take)) & ((1 << take) - 1);
            self.bit += take;
            count -= take;
        }
        value
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
