//! Series labels in one shared buffer: the store keeps millions of series, most with the same few
//! dozen label names, so names are ids into a process-wide table and each value is stored once per
//! series, without the per-label headers of a vector of strings.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, OnceLock};

/// Label names, interned for the process: tenants use a bounded set of names, which are never
/// freed. Lookups from an id don't lock, since queries resolve every label they read.
pub mod names {
    use super::*;

    // Segment `k` holds 64 << k names, so ids never move once handed out.
    const SEGMENTS: usize = 26;
    const FIRST: usize = 64;

    static SEGMENT: [AtomicPtr<&'static str>; SEGMENTS] =
        [const { AtomicPtr::new(std::ptr::null_mut()) }; SEGMENTS];
    static LEN: AtomicU32 = AtomicU32::new(0);
    static INDEX: OnceLock<Mutex<HashMap<&'static str, u32>>> = OnceLock::new();

    thread_local! {
        static CACHE: RefCell<HashMap<Box<str>, u32>> = RefCell::new(HashMap::new());
    }

    fn position(id: u32) -> (usize, usize) {
        let index = id as usize + FIRST;
        let segment =
            (usize::BITS - 1 - index.leading_zeros()) as usize - FIRST.trailing_zeros() as usize;
        (segment, index - (FIRST << segment))
    }

    /// The id of `name`, adding it to the table when new.
    pub fn intern(name: &str) -> u32 {
        if let Some(id) = CACHE.with_borrow(|cache| cache.get(name).copied()) {
            return id;
        }
        let id = {
            let mut index = INDEX
                .get_or_init(Mutex::default)
                .lock()
                .expect("label names poisoned");
            match index.get(name) {
                Some(id) => *id,
                None => {
                    let id = LEN.load(AtomicOrdering::Relaxed);
                    let (segment, offset) = position(id);
                    let mut slots = SEGMENT[segment].load(AtomicOrdering::Acquire);
                    if slots.is_null() {
                        let allocated = vec![""; FIRST << segment].leak();
                        slots = allocated.as_mut_ptr();
                        SEGMENT[segment].store(slots, AtomicOrdering::Release);
                    }
                    let leaked: &'static str = Box::leak(name.into());
                    // SAFETY: the slot is within its segment and written once, under the lock,
                    // before LEN publishes it.
                    unsafe { slots.add(offset).write(leaked) };
                    LEN.store(id + 1, AtomicOrdering::Release);
                    index.insert(leaked, id);
                    id
                }
            }
        };
        CACHE.with_borrow_mut(|cache| cache.insert(name.into(), id));
        id
    }

    /// The id of `name` when some series has it.
    pub fn lookup(name: &str) -> Option<u32> {
        if let Some(id) = CACHE.with_borrow(|cache| cache.get(name).copied()) {
            return Some(id);
        }
        let id = INDEX
            .get_or_init(Mutex::default)
            .lock()
            .expect("label names poisoned")
            .get(name)
            .copied()?;
        CACHE.with_borrow_mut(|cache| cache.insert(name.into(), id));
        Some(id)
    }

    pub fn name(id: u32) -> &'static str {
        assert!(
            id < LEN.load(AtomicOrdering::Acquire),
            "unknown label name id {id}"
        );
        let (segment, offset) = position(id);
        // SAFETY: ids below LEN were written before LEN was published, and segments never move.
        unsafe { *SEGMENT[segment].load(AtomicOrdering::Acquire).add(offset) }
    }
}

/// A series' labels, sorted by name. Clones share the buffer.
#[derive(Clone, PartialEq, Eq)]
pub struct Labels(Arc<[u8]>);

impl Labels {
    /// `pairs` must be sorted by name, without duplicate names.
    pub fn from_sorted<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        let mut bytes = Vec::new();
        for (name, value) in pairs {
            put_varint(&mut bytes, u64::from(names::intern(name)));
            put_varint(&mut bytes, value.len() as u64);
            bytes.extend_from_slice(value.as_bytes());
        }
        Self(bytes.into())
    }

    pub fn iter(&self) -> Iter<'_> {
        Iter { bytes: &self.0 }
    }

    /// The labels as pairs with one lifetime, as functions over label pairs take them.
    pub fn pairs(&self) -> impl Iterator<Item = (&str, &str)> + Clone {
        self.iter().map(|(name, value)| (name, value))
    }

    pub fn len(&self) -> usize {
        self.iter().count()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The value of `name`, empty when absent like Prometheus.
    pub fn value(&self, name: &str) -> &str {
        names::lookup(name).map_or("", |id| self.value_of(id))
    }

    pub fn value_of(&self, id: u32) -> &str {
        let mut bytes = &self.0[..];
        while !bytes.is_empty() {
            let name = take_varint(&mut bytes) as u32;
            let len = take_varint(&mut bytes) as usize;
            if name == id {
                // SAFETY: values are only written from `&str`.
                return unsafe { std::str::from_utf8_unchecked(&bytes[..len]) };
            }
            bytes = &bytes[len..];
        }
        ""
    }

    /// Heap bytes of the buffer, with the reference counts.
    pub fn heap_size(&self) -> usize {
        16 + self.0.len()
    }
}

impl Default for Labels {
    fn default() -> Self {
        Self(Arc::from([]))
    }
}

impl Hash for Labels {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl Ord for Labels {
    fn cmp(&self, other: &Self) -> Ordering {
        self.iter().cmp(other.iter())
    }
}

impl PartialOrd for Labels {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for Labels {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}

impl<'a> IntoIterator for &'a Labels {
    type Item = (&'static str, &'a str);
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

#[derive(Clone)]
pub struct Iter<'a> {
    bytes: &'a [u8],
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'static str, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        if self.bytes.is_empty() {
            return None;
        }
        let name = names::name(take_varint(&mut self.bytes) as u32);
        let len = take_varint(&mut self.bytes) as usize;
        let (value, rest) = self.bytes.split_at(len);
        self.bytes = rest;
        // SAFETY: values are only written from `&str`.
        Some((name, unsafe { std::str::from_utf8_unchecked(value) }))
    }
}

pub(crate) fn put_varint(bytes: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        bytes.push(value as u8 | 0x80);
        value >>= 7;
    }
    bytes.push(value as u8);
}

pub(crate) fn take_varint(bytes: &mut &[u8]) -> u64 {
    let (mut value, mut shift) = (0_u64, 0);
    loop {
        let byte = bytes[0];
        *bytes = &bytes[1..];
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return value;
        }
        shift += 7;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_round_trip_and_compare_by_name_then_value() {
        let a = Labels::from_sorted([
            ("__name__", "up"),
            ("job", "api"),
            ("long", &"x".repeat(300)),
        ]);
        assert_eq!(
            a.iter().collect::<Vec<_>>(),
            [
                ("__name__", "up"),
                ("job", "api"),
                ("long", &*"x".repeat(300))
            ]
        );
        assert_eq!(a.value("job"), "api");
        assert_eq!(a.value("missing"), "");
        assert_eq!(a.len(), 3);
        let b = Labels::from_sorted([("__name__", "up"), ("job", "b")]);
        assert!(a < b);
        assert_eq!(a, Labels::from_sorted(a.pairs()));
        // 19 labels cost their bytes plus a few per label, not 40 each.
        let many = (0..19)
            .map(|index| (format!("label_{index:02}"), format!("value_{index:04}")))
            .collect::<Vec<_>>();
        let many = Labels::from_sorted(
            many.iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
        );
        assert!(many.heap_size() < 19 * 14, "{}", many.heap_size());
    }

    #[test]
    fn name_ids_are_stable_across_threads_and_segments() {
        let names = (0..500)
            .map(|index| format!("name_{index}"))
            .collect::<Vec<_>>();
        let ids = std::thread::scope(|scope| {
            let handles = (0..4)
                .map(|_| {
                    scope.spawn(|| {
                        names
                            .iter()
                            .map(|name| names::intern(name))
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(ids.windows(2).all(|pair| pair[0] == pair[1]));
        for (name, id) in names.iter().zip(&ids[0]) {
            assert_eq!(names::name(*id), name);
            assert_eq!(names::lookup(name), Some(*id));
        }
    }
}
