// SPDX-License-Identifier: AGPL-3.0-only
use super::*;

const MAGIC: u32 = 0x344d494d;
pub(super) trait Wire: Sized {
    fn size(&self) -> usize;
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
            fn size(&self) -> usize {
                std::mem::size_of::<Self>()
            }
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
    fn size(&self) -> usize {
        8
    }
    fn put(&self, w: &mut Vec<u8>) {
        (*self as u64).put(w)
    }
    fn get(r: &mut &[u8]) -> Result<Self> {
        usize::try_from(u64::get(r)?).map_err(io)
    }
}
impl Wire for bool {
    fn size(&self) -> usize {
        1
    }
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
    fn size(&self) -> usize {
        4 + self.len()
    }
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
    fn size(&self) -> usize {
        4 + self.iter().map(Wire::size).sum::<usize>()
    }
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
        // Bound speculative allocation when rejecting a malformed collection.
        let mut values = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            values.push(T::get(r)?);
        }
        Ok(values)
    }
}
impl<T: Wire> Wire for Arc<T> {
    fn size(&self) -> usize {
        self.as_ref().size()
    }
    fn put(&self, w: &mut Vec<u8>) {
        self.as_ref().put(w)
    }
    fn get(r: &mut &[u8]) -> Result<Self> {
        Ok(Arc::new(T::get(r)?))
    }
}
impl<T: Wire> Wire for Option<T> {
    fn size(&self) -> usize {
        1 + self.as_ref().map_or(0, Wire::size)
    }
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
    fn size(&self) -> usize {
        4 + self.iter().map(|(k, v)| k.size() + v.size()).sum::<usize>()
    }
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
 fn size(&self)->usize{0 $(+self.$f.size())+}
 fn put(&self,w:&mut Vec<u8>){$(self.$f.put(w);)+}
 fn get(r:&mut &[u8])->Result<Self>{Ok(Self{$($f:Wire::get(r)?,)+})}
 }};
}
wire!(Label, name, value);
wire!(Sample, t, kind, bits, data);
wire!(Exemplar, t, bits, labels);
impl Wire for Series {
    fn size(&self) -> usize {
        self.reference.size() + self.labels.size() + self.samples.size() + self.exemplars.size()
    }
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
fn put_bytes(bytes: &[u8], w: &mut Vec<u8>) {
    u32::try_from(bytes.len())
        .expect("frame blob too large")
        .put(w);
    w.extend_from_slice(bytes);
}
fn get_bytes(r: &mut &[u8]) -> Result<Vec<u8>> {
    let n = count(r)?;
    Ok(take(r, n)?.to_vec())
}
impl Wire for FloatSeries {
    fn size(&self) -> usize {
        self.index.size() + 4 + self.key.len() + self.samples.size()
    }
    fn put(&self, w: &mut Vec<u8>) {
        self.index.put(w);
        put_bytes(&self.key, w);
        self.samples.put(w)
    }
    fn get(r: &mut &[u8]) -> Result<Self> {
        Ok(Self {
            index: Wire::get(r)?,
            key: get_bytes(r)?,
            samples: Wire::get(r)?,
        })
    }
}
wire!(SampleError, timestamp, error);
wire!(FloatResult, index, reference, accepted, errors);
impl Wire for QueryChunk {
    fn size(&self) -> usize {
        self.mint.size() + self.maxt.size() + 4 + self.data.len() + self.samples.size()
    }
    fn put(&self, w: &mut Vec<u8>) {
        self.mint.put(w);
        self.maxt.put(w);
        put_bytes(&self.data, w);
        self.samples.put(w)
    }
    fn get(r: &mut &[u8]) -> Result<Self> {
        Ok(Self {
            mint: Wire::get(r)?,
            maxt: Wire::get(r)?,
            data: get_bytes(r)?.into(),
            samples: Wire::get(r)?,
        })
    }
}
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
    sorted,
    unordered
);
wire!(
    Response,
    error,
    reference,
    series,
    active,
    refs,
    values,
    results,
    leftovers,
    sample,
    count,
    mint,
    maxt,
    min_valid,
    bytes,
    cursor,
    done,
    chunks,
    label_counts
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
    // Exact capacity avoids growth while encoding and shrinking at the C boundary.
    let mut w = Vec::with_capacity(MAGIC.size() + response.size());
    MAGIC.put(&mut w);
    response.put(&mut w);
    w
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_storage_keeps_only_its_encoded_capacity() {
        for len in [0, 1, 17, 64, 1024] {
            let labels: Vec<Label> = (0..len)
                .map(|n| Label {
                    name: format!("label_{n}"),
                    value: "value".into(),
                })
                .collect();
            let key = labels_key(&labels);
            assert_eq!(key.len(), key.capacity());
            let decoded = Vec::<Label>::get(&mut key.as_slice()).unwrap();
            assert_eq!(decoded, labels);
            assert_eq!(decoded.len(), decoded.capacity());
        }
    }

    #[test]
    fn responses_allocate_the_encoded_length() {
        for len in [0, 1, 15, 256, 1025] {
            let sample = Sample {
                t: 17,
                kind: 2,
                bits: 3,
                data: "x".repeat(len),
            };
            let response = Response {
                error: "error".into(),
                series: vec![Series {
                    reference: 9,
                    labels: Arc::new(vec![Label {
                        name: "name".into(),
                        value: "value".into(),
                    }]),
                    samples: BTreeMap::from([(17, sample.clone())]),
                    exemplars: vec![Exemplar::default()],
                    ..Default::default()
                }],
                refs: vec![1, 2],
                values: vec!["value".into()],
                results: vec![FloatResult {
                    errors: vec![SampleError::default()],
                    ..Default::default()
                }],
                leftovers: vec![3],
                sample: Some(sample.clone()),
                bytes: BTreeMap::from([("name".into(), 7)]),
                label_counts: BTreeMap::from([("zone".into(), BTreeMap::from([("a".into(), 7)]))]),
                chunks: vec![QueryChunk {
                    mint: 1,
                    maxt: 2,
                    data: vec![0; len].into(),
                    samples: vec![sample],
                }],
                ..Default::default()
            };
            let encoded = super::response(&response);
            assert_eq!(encoded.len(), MAGIC.size() + response.size());
            assert_eq!(encoded.len(), encoded.capacity());
        }
    }
    #[test]
    fn blobs_keep_the_vector_wire_format() {
        let value: Vec<u8> = (0..=255).cycle().take(8192).collect();
        let mut generic = Vec::new();
        value.put(&mut generic);
        let mut bulk = Vec::new();
        put_bytes(&value, &mut bulk);
        assert_eq!(bulk, generic);
        assert_eq!(get_bytes(&mut bulk.as_slice()).unwrap(), value);
        for end in 0..bulk.len() {
            assert!(get_bytes(&mut &bulk[..end]).is_err());
        }
    }
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
