// SPDX-License-Identifier: AGPL-3.0-only
use super::*;

const MAGIC: u32 = 0x354d494d;
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
pub(super) fn skip_string(r: &mut &[u8]) -> Result<()> {
    let n = count(r)?;
    std::str::from_utf8(take(r, n)?).map_err(io)?;
    Ok(())
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
impl Wire for Arc<str> {
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
        Ok(Arc::from(std::str::from_utf8(take(r, n)?).map_err(io)?))
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
wire!(LabelFilter, name, values, exclude, match_missing);
impl Wire for Sample {
    fn size(&self) -> usize {
        self.t.size() + self.kind.size() + self.bits.size() + self.data.size()
    }
    fn put(&self, w: &mut Vec<u8>) {
        self.t.put(w);
        self.kind.put(w);
        self.bits.put(w);
        self.data.put(w);
    }
    fn get(r: &mut &[u8]) -> Result<Self> {
        Ok(Self {
            t: Wire::get(r)?,
            kind: Wire::get(r)?,
            bits: Wire::get(r)?,
            data: Wire::get(r)?,
            ..Default::default()
        })
    }
}
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
impl Wire for FloatSeries<'_> {
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
            key: get_bytes(r)?.into(),
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
macro_rules! request_field {
    (batch, $r:expr) => {{
        let r = $r;
        let n = count(r)?;
        (0..n)
            .map(|_| {
                let index = Wire::get(r)?;
                let len = count(r)?;
                let key = Cow::Borrowed(take(r, len)?);
                Ok(FloatSeries {
                    index,
                    key,
                    samples: Wire::get(r)?,
                })
            })
            .collect::<Result<Vec<_>>>()
    }};
    ($field:ident, $r:expr) => {
        Wire::get($r)
    };
}
macro_rules! request_wire {
    ($($field:ident),+ $(,)?) => {
        wire!(Request<'_>, $($field),+);
        pub(super) fn request(mut bytes: &[u8]) -> Result<Request<'_>> {
            if u32::get(&mut bytes)? != MAGIC {
                return Err("invalid engine wire version".into());
            }
            let req = Request { $($field: request_field!($field, &mut bytes)?),+ };
            if !bytes.is_empty() {
                return Err("trailing engine frame bytes".into());
            }
            Ok(req)
        }
    };
}
request_wire!(
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
    unordered,
    filters
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
                    name: format!("label_{n}").into(),
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
                ..Default::default()
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

#[cfg(test)]
mod shared_string_wire_tests {
    use super::*;
    #[test]
    fn shared_strings_keep_the_string_format_and_own_decoded_bytes() {
        for text in ["", "a\0b", "é😀"] {
            let mut old = Vec::new();
            text.to_string().put(&mut old);
            let mut new = Vec::new();
            Arc::<str>::from(text).put(&mut new);
            assert_eq!(old, new);
            let decoded = Arc::<str>::get(&mut new.as_slice()).unwrap();
            new.fill(0);
            assert_eq!(decoded.as_ref(), text);
        }
    }
}

#[cfg(test)]
mod borrowed_frame_tests {
    use super::*;
    fn encoded(req: &Request<'_>) -> Vec<u8> {
        let mut bytes = Vec::new();
        MAGIC.put(&mut bytes);
        req.put(&mut bytes);
        bytes
    }
    fn owned_request(mut bytes: &[u8]) -> Result<Request<'_>> {
        if u32::get(&mut bytes)? != MAGIC {
            return Err("invalid engine wire version".into());
        }
        let req = Request::get(&mut bytes)?;
        if !bytes.is_empty() {
            return Err("trailing engine frame bytes".into());
        }
        Ok(req)
    }
    #[test]
    fn borrowed_and_owned_decoders_keep_every_request_field_and_reject_truncation() {
        let labels = vec![Label {
            name: "é\0".into(),
            value: "😀".into(),
        }];
        let sample = Sample {
            t: -3,
            kind: 1,
            bits: 0x7ff0000000000002,
            ..Default::default()
        };
        let req = Request {
            op: "lookup".into(),
            reference: 41,
            labels: labels.clone(),
            sample: Some(sample.clone()),
            exemplar: Some(Exemplar {
                t: -9,
                bits: 42,
                labels: labels.clone(),
            }),
            reject_ooo: true,
            window: 99,
            max_exemplars: 31,
            refs: vec![1, 9],
            up_to: -6,
            mint: -50,
            maxt: 77,
            name: "name".into(),
            values: vec!["".into(), "é\0".into()],
            include_cold: true,
            filter: true,
            batch: vec![FloatSeries {
                index: 7,
                key: labels_key(&labels).into(),
                samples: vec![sample],
            }],
            limit: 17,
            cursor: 13,
            sorted: true,
            unordered: true,
            filters: vec![LabelFilter {
                name: "a".into(),
                values: vec!["b".into()],
                exclude: true,
                match_missing: true,
            }],
        };
        let bytes = encoded(&req);
        let borrowed = request(&bytes).unwrap();
        assert!(matches!(borrowed.batch[0].key, Cow::Borrowed(_)));
        let key = borrowed.batch[0].key.as_ptr() as usize;
        assert!(
            key >= bytes.as_ptr() as usize
                && key + borrowed.batch[0].key.len() <= bytes.as_ptr() as usize + bytes.len()
        );
        assert_eq!(encoded(&borrowed), bytes);
        assert_eq!(encoded(&owned_request(&bytes).unwrap()), bytes);
        for n in 0..bytes.len() {
            assert!(request(&bytes[..n]).is_err(), "truncation {n}");
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(request(&trailing).is_err());
    }
    #[test]
    fn known_float_ingestion_does_not_retain_the_borrowed_frame() {
        let path = std::env::temp_dir().join(format!("mimir-borrow-float-{}", std::process::id()));
        let mut engine = Engine::open(&path).unwrap();
        let labels = vec![Label {
            name: "metric".into(),
            value: "counter".into(),
        }];
        engine
            .call(Request {
                op: "begin".into(),
                ..Default::default()
            })
            .unwrap();
        let reference = engine
            .call(Request {
                op: "append".into(),
                labels: labels.clone(),
                sample: Some(Sample {
                    t: 1,
                    kind: 1,
                    bits: 1.0_f64.to_bits(),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .unwrap()
            .reference;
        engine
            .call(Request {
                op: "commit".into(),
                ..Default::default()
            })
            .unwrap();
        let mut bytes = encoded(&Request {
            op: "ingest_floats".into(),
            mint: i64::MIN,
            maxt: i64::MAX,
            batch: vec![FloatSeries {
                index: 0,
                key: labels_key(&labels).into(),
                samples: vec![Sample {
                    t: 2,
                    kind: 1,
                    bits: 2.0_f64.to_bits(),
                    ..Default::default()
                }],
            }],
            ..Default::default()
        });
        assert!(engine.call(request(&bytes).unwrap()).unwrap().done);
        bytes.fill(0);
        let samples = engine.state.series[&reference].samples_in(i64::MIN, i64::MAX);
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[&2].bits, 2.0_f64.to_bits());
        assert_eq!(engine.state.series[&reference].labels.as_ref(), &labels);
        drop(engine);
        fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn mutated_float_frames_match_the_owned_decoder() {
        let labels = vec![Label {
            name: "name".into(),
            value: "value".into(),
        }];
        let template = encoded(&Request {
            op: "ingest_floats".into(),
            batch: vec![FloatSeries {
                index: 1,
                key: labels_key(&labels).into(),
                samples: vec![Sample {
                    t: 2,
                    kind: 1,
                    bits: 3,
                    ..Default::default()
                }],
            }],
            ..Default::default()
        });
        let mut seed = 0x120a56b8_u64;
        let next = |seed: &mut u64| {
            *seed ^= *seed << 13;
            *seed ^= *seed >> 7;
            *seed ^= *seed << 17;
            *seed
        };
        for n in 0..50_000 {
            let mut frame = template.clone();
            for _ in 0..1 + n % 4 {
                let index = next(&mut seed) as usize % frame.len();
                frame[index] ^= 1 << (next(&mut seed) % 8);
            }
            if n % 3 == 0 {
                let len = next(&mut seed) as usize % (frame.len() + 1);
                frame.truncate(len);
            }
            if n % 7 == 0 {
                frame.push(next(&mut seed) as u8);
            }
            match (owned_request(&frame), request(&frame)) {
                (Ok(a), Ok(b)) => assert_eq!(encoded(&a), encoded(&b)),
                (Err(a), Err(b)) => assert_eq!(a, b),
                _ => panic!("decoder disagreement at mutation {n}"),
            }
        }
    }
}
