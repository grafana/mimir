// SPDX-License-Identifier: AGPL-3.0-only

//! Little-endian frames between Go and Rust: fixed-width integers, u32-prefixed byte strings.

pub type Result<T> = std::result::Result<T, String>;

pub struct Reader<'a> {
    data: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.data.len() {
            return Err("truncated frame".into());
        }
        let (head, rest) = self.data.split_at(n);
        self.data = rest;
        Ok(head)
    }

    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn bool(&mut self) -> Result<bool> {
        Ok(self.u8()? != 0)
    }
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn i64(&mut self) -> Result<i64> {
        Ok(self.u64()? as i64)
    }
    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    pub fn str(&mut self) -> Result<&'a str> {
        std::str::from_utf8(self.bytes()?).map_err(|_| "invalid UTF-8 in frame".into())
    }
    /// A u32 count, checked against what remains so a corrupt frame can't make a huge allocation.
    pub fn count(&mut self, min_item: usize) -> Result<usize> {
        let n = self.u32()? as usize;
        if n.saturating_mul(min_item.max(1)) > self.data.len() {
            return Err("invalid count in frame".into());
        }
        Ok(n)
    }
    pub fn rest(&self) -> &'a [u8] {
        self.data
    }
    pub fn skip(&mut self, n: usize) -> Result<()> {
        self.take(n).map(|_| ())
    }
    pub fn u64s(&mut self) -> Result<Vec<u64>> {
        let n = self.count(8)?;
        (0..n).map(|_| self.u64()).collect()
    }
}

#[derive(Default)]
pub struct Writer {
    pub buf: Vec<u8>,
}

impl Writer {
    pub fn with_capacity(n: usize) -> Self {
        Self {
            buf: Vec::with_capacity(n),
        }
    }
    #[inline]
    pub fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }
    #[inline]
    pub fn bool(&mut self, v: bool) {
        self.buf.push(v as u8);
    }
    #[inline]
    pub fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    pub fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    pub fn i64(&mut self, v: i64) {
        self.u64(v as u64);
    }
    #[inline]
    pub fn bytes(&mut self, v: &[u8]) {
        self.u32(v.len() as u32);
        self.buf.extend_from_slice(v);
    }
    /// Reserves a u32 to fill in later with `patch_u32`, for counts known only after writing the items.
    pub fn placeholder(&mut self) -> usize {
        let at = self.buf.len();
        self.u32(0);
        at
    }
    pub fn patch_u32(&mut self, at: usize, v: u32) {
        self.buf[at..at + 4].copy_from_slice(&v.to_le_bytes());
    }
}
