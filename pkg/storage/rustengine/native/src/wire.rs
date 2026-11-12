// SPDX-License-Identifier: AGPL-3.0-only
use super::*;

const MAGIC: u32 = 0x314d494d;
trait Wire: Sized {
    fn put(&self, w: &mut Vec<u8>);
    fn get(r: &mut &[u8]) -> Result<Self>;
}
fn take<'a>(r: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if n > r.len() {
        return Err("truncated engine frame".into());
    }
    let (v, rest) = r.split_at(n);
    *r = rest;
    Ok(v)
}
macro_rules! number {
    ($t:ty) => {
        impl Wire for $t {
            fn put(&self, w: &mut Vec<u8>) {
                w.extend_from_slice(&self.to_le_bytes());
            }
            fn get(r: &mut &[u8]) -> Result<Self> {
                Ok(Self::from_le_bytes(
                    take(r, std::mem::size_of::<Self>())?.try_into().unwrap(),
                ))
            }
        }
    };
}
number!(u8);
number!(u32);
number!(u64);
number!(i64);
impl Wire for usize {
    fn put(&self, w: &mut Vec<u8>) {
        (*self as u64).put(w)
    }
    fn get(r: &mut &[u8]) -> Result<Self> {
        usize::try_from(u64::get(r)?).map_err(io)
    }
}
impl Wire for bool {
    fn put(&self, w: &mut Vec<u8>) {
        u8::from(*self).put(w)
    }
    fn get(r: &mut &[u8]) -> Result<Self> {
        match u8::get(r)? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err("invalid engine boolean".into()),
        }
    }
}
fn count(r: &mut &[u8]) -> Result<usize> {
    let n = u32::get(r)? as usize;
    if n > r.len() {
        return Err("invalid engine collection length".into());
    }
    Ok(n)
}
impl Wire for String {
    fn put(&self, w: &mut Vec<u8>) {
        u32::try_from(self.len())
            .expect("frame string too large")
            .put(w);
        w.extend_from_slice(self.as_bytes());
    }
    fn get(r: &mut &[u8]) -> Result<Self> {
        let n = count(r)?;
        String::from_utf8(take(r, n)?.to_vec()).map_err(io)
    }
}
impl<T: Wire> Wire for Vec<T> {
    fn put(&self, w: &mut Vec<u8>) {
        u32::try_from(self.len())
            .expect("frame vector too large")
            .put(w);
        for v in self {
            v.put(w)
        }
    }
    fn get(r: &mut &[u8]) -> Result<Self> {
        let n = count(r)?;
        (0..n).map(|_| T::get(r)).collect()
    }
}
impl<T: Wire> Wire for Option<T> {
    fn put(&self, w: &mut Vec<u8>) {
        self.is_some().put(w);
        if let Some(v) = self {
            v.put(w)
        }
    }
    fn get(r: &mut &[u8]) -> Result<Self> {
        if bool::get(r)? {
            Ok(Some(T::get(r)?))
        } else {
            Ok(None)
        }
    }
}
impl<K: Wire + Ord, V: Wire> Wire for BTreeMap<K, V> {
    fn put(&self, w: &mut Vec<u8>) {
        u32::try_from(self.len())
            .expect("frame map too large")
            .put(w);
        for (k, v) in self {
            k.put(w);
            v.put(w)
        }
    }
    fn get(r: &mut &[u8]) -> Result<Self> {
        let n = count(r)?;
        (0..n).map(|_| Ok((K::get(r)?, V::get(r)?))).collect()
    }
}
macro_rules! wire {
 ($t:ty, $($f:ident),+ $(,)?) => {impl Wire for $t {
 fn put(&self,w:&mut Vec<u8>){$(self.$f.put(w);)+}
 fn get(r:&mut &[u8])->Result<Self>{Ok(Self{$($f:Wire::get(r)?,)+})}
 }};
}
wire!(Label, name, value);
wire!(Sample, t, kind, bits, data);
wire!(Exemplar, t, bits, labels);
impl Wire for Series {
    fn put(&self, w: &mut Vec<u8>) {
        self.reference.put(w);
        self.labels.put(w);
        self.samples.put(w);
        self.exemplars.put(w);
    }
    fn get(r: &mut &[u8]) -> Result<Self> {
        Ok(Self {
            reference: Wire::get(r)?,
            labels: Wire::get(r)?,
            samples: Wire::get(r)?,
            exemplars: Wire::get(r)?,
            ..Default::default()
        })
    }
}
wire!(FloatSeries, index, labels, samples);
wire!(SampleError, timestamp, error);
wire!(FloatResult, index, reference, accepted, errors);
wire!(QueryChunk, mint, maxt, data, samples);
wire!(
    Request,
    op,
    reference,
    labels,
    sample,
    exemplar,
    reject_ooo,
    window,
    max_exemplars,
    refs,
    up_to,
    mint,
    maxt,
    name,
    values,
    include_cold,
    filter,
    batch,
    limit,
    cursor,
    sorted
);
wire!(
    Response, error, reference, series, active, refs, values, results, leftovers, sample, count,
    mint, maxt, min_valid, bytes, cursor, done, chunks
);

pub(super) fn request(mut bytes: &[u8]) -> Result<Request> {
    if u32::get(&mut bytes)? != MAGIC {
        return Err("invalid engine wire version".into());
    }
    let req = Request::get(&mut bytes)?;
    if !bytes.is_empty() {
        return Err("trailing engine frame bytes".into());
    }
    Ok(req)
}
pub(super) fn response(response: &Response) -> Vec<u8> {
    let mut w = Vec::new();
    MAGIC.put(&mut w);
    response.put(&mut w);
    w
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_malformed_requests() {
        let mut good = Vec::new();
        MAGIC.put(&mut good);
        Request {
            op: "stats".into(),
            ..Default::default()
        }
        .put(&mut good);
        assert_eq!(request(&good).unwrap().op, "stats");
        for end in 0..good.len() {
            assert!(request(&good[..end]).is_err(), "{end}");
        }
        good.push(0);
        assert!(request(&good).is_err());
        let mut huge = Vec::new();
        MAGIC.put(&mut huge);
        u32::MAX.put(&mut huge);
        assert!(request(&huge).is_err());
    }
}
