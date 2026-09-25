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
    let mut out = BitWriter::new();
    let mut previous_time = i64::MIN;
    let mut previous_delta = 0_u64;
    let mut previous_value = 0_f64;
    let mut leading = u8::MAX;
    let mut trailing = 0_u8;

    for (index, (timestamp, value)) in samples.enumerate() {
        let mut delta = 0_u64;
        match index {
            0 => {
                write_varint(&mut out, timestamp);
                out.bits(value.to_bits(), 64);
            }
            1 => {
                delta = timestamp.wrapping_sub(previous_time) as u64;
                write_uvarint(&mut out, delta);
                write_value(&mut out, value, previous_value, &mut leading, &mut trailing);
            }
            _ => {
                delta = timestamp.wrapping_sub(previous_time) as u64;
                let dod = delta.wrapping_sub(previous_delta) as i64;
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
                write_value(&mut out, value, previous_value, &mut leading, &mut trailing);
            }
        }
        previous_time = timestamp;
        previous_delta = delta;
        previous_value = value;
    }
    out.bytes[0..2].copy_from_slice(&(samples_len as u16).to_be_bytes());
    out.bytes
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
