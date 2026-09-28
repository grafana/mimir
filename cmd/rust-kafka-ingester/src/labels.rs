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

    // A name's id, or the table length when it was not found: trackers and cost attribution
    // look up names no series has on every sample, and the index lock is shared by every
    // ingest thread.
    type Cached = Result<u32, u32>;

    // Names come from series and from configured or queried matchers; clearing past this bounds
    // what arbitrary query names can add.
    const CACHE_LIMIT: usize = 16_384;

    thread_local! {
        // Every new series looks up each of its names, and SipHash cost more than the lookup.
        static CACHE: RefCell<hashbrown::HashMap<Box<str>, Cached>> =
            RefCell::new(hashbrown::HashMap::new());
        #[cfg(test)]
        static INDEX_LOCKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }

    fn cache(name: &str, entry: Cached) {
        CACHE.with_borrow_mut(|cache| {
            if cache.len() >= CACHE_LIMIT {
                cache.clear();
            }
            cache.insert(name.into(), entry);
        });
    }

    fn index() -> std::sync::MutexGuard<'static, HashMap<&'static str, u32>> {
        #[cfg(test)]
        INDEX_LOCKS.set(INDEX_LOCKS.get() + 1);
        INDEX
            .get_or_init(Mutex::default)
            .lock()
            .expect("label names poisoned")
    }

    #[cfg(test)]
    pub(crate) fn index_locks() -> u64 {
        INDEX_LOCKS.get()
    }

    fn position(id: u32) -> (usize, usize) {
        let index = id as usize + FIRST;
        let segment =
            (usize::BITS - 1 - index.leading_zeros()) as usize - FIRST.trailing_zeros() as usize;
        (segment, index - (FIRST << segment))
    }

    /// The id of `name`, adding it to the table when new.
    pub fn intern(name: &str) -> u32 {
        if let Some(Ok(id)) = CACHE.with_borrow(|cache| cache.get(name).copied()) {
            return id;
        }
        let id = {
            let mut index = index();
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
        cache(name, Ok(id));
        id
    }

    /// The id of `name` when some series has it.
    pub fn lookup(name: &str) -> Option<u32> {
        match CACHE.with_borrow(|cache| cache.get(name).copied()) {
            Some(Ok(id)) => return Some(id),
            // Names are only added, so a miss holds until the table grows.
            Some(Err(len)) if len == LEN.load(AtomicOrdering::Acquire) => return None,
            _ => {}
        }
        // Read before the lookup, so a name added after it invalidates the miss.
        let len = LEN.load(AtomicOrdering::Acquire);
        let id = index().get(name).copied();
        cache(name, id.ok_or(len));
        id
    }

    pub fn name(id: u32) -> &'static str {
        snapshot().name(id)
    }

    /// The table as of now. Comparing a series' labels resolves every one of its names, which
    /// then synchronizes once rather than for each.
    #[derive(Clone, Copy)]
    pub struct Snapshot {
        len: u32,
    }

    pub fn snapshot() -> Snapshot {
        Snapshot {
            len: LEN.load(AtomicOrdering::Acquire),
        }
    }

    impl Snapshot {
        #[inline]
        pub fn name(self, id: u32) -> &'static str {
            assert!(id < self.len, "unknown label name id {id}");
            let (segment, offset) = position(id);
            // SAFETY: the slots and segments below `len` were written before the load of LEN
            // that made this snapshot, and segments never move.
            unsafe { *SEGMENT[segment].load(AtomicOrdering::Relaxed).add(offset) }
        }
    }
}

/// A series' labels, sorted by name. Clones share the buffer.
#[derive(Clone, PartialEq, Eq)]
pub struct Labels(Arc<[u8]>);

impl Labels {
    /// Whether these are `pairs`, in order. The store checks this for every series it appends
    /// to, on short names and values, where calling `memcmp` for each cost more than comparing.
    pub fn eq_pairs<'b>(&self, pairs: impl IntoIterator<Item = (&'b str, &'b str)>) -> bool {
        let mut stored = self.iter();
        for (name, value) in pairs {
            match stored.next() {
                Some((stored_name, stored_value))
                    if short_eq(stored_name.as_bytes(), name.as_bytes())
                        && short_eq(stored_value.as_bytes(), value.as_bytes()) => {}
                _ => return false,
            }
        }
        stored.next().is_none()
    }

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
        Iter {
            bytes: &self.0,
            names: names::snapshot(),
        }
    }

    /// The labels as pairs with one lifetime, as functions over label pairs take them.
    // The map shortens the names' `'static` to the values' lifetime.
    #[allow(clippy::map_identity)]
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
    names: names::Snapshot,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'static str, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        if self.bytes.is_empty() {
            return None;
        }
        let name = self.names.name(take_varint(&mut self.bytes) as u32);
        let len = take_varint(&mut self.bytes) as usize;
        let (value, rest) = self.bytes.split_at(len);
        self.bytes = rest;
        // SAFETY: values are only written from `&str`.
        Some((name, unsafe { std::str::from_utf8_unchecked(value) }))
    }
}

// Up to 16 bytes, two overlapping loads cover every byte.
#[inline]
fn short_eq(a: &[u8], b: &[u8]) -> bool {
    let len = a.len();
    if len != b.len() {
        return false;
    }
    let word = |bytes: &[u8], at: usize| {
        u64::from_le_bytes(bytes[at..at + 8].try_into().expect("8 bytes"))
    };
    let half = |bytes: &[u8], at: usize| {
        u32::from_le_bytes(bytes[at..at + 4].try_into().expect("4 bytes"))
    };
    match len {
        0 => true,
        1..=3 => a[0] == b[0] && a[len / 2] == b[len / 2] && a[len - 1] == b[len - 1],
        4..=7 => half(a, 0) == half(b, 0) && half(a, len - 4) == half(b, len - 4),
        8..=16 => word(a, 0) == word(b, 0) && word(a, len - 8) == word(b, len - 8),
        _ => a == b,
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
    fn short_comparisons_see_every_byte() {
        for len in 0..40 {
            let a = (0..len as u8).collect::<Vec<_>>();
            assert!(short_eq(&a, &a.clone()));
            assert!(!short_eq(&a, &[a.as_slice(), &[0]].concat()));
            for position in 0..len {
                let mut b = a.clone();
                b[position] ^= 0x10;
                assert!(!short_eq(&a, &b), "len {len}, position {position}");
            }
        }
        let labels = Labels::from_sorted([("__name__", "up"), ("job", "api")]);
        assert!(labels.eq_pairs([("__name__", "up"), ("job", "api")]));
        assert!(!labels.eq_pairs([("__name__", "up")]));
        assert!(!labels.eq_pairs([("__name__", "up"), ("job", "api"), ("x", "")]));
        assert!(!labels.eq_pairs([("__name__", "up"), ("job", "apI")]));
    }

    #[test]
    fn missing_names_do_not_lock_the_index_on_every_lookup() {
        let missing = "a_name_no_series_has";
        assert_eq!(names::lookup(missing), None);
        let locks = names::index_locks();
        for _ in 0..100 {
            assert_eq!(names::lookup(missing), None);
        }
        // Tests running in parallel add names, which rightly makes some lookups check again.
        assert!(
            names::index_locks() - locks < 50,
            "missing names locked on every lookup"
        );
        // A name another thread adds later is found.
        let id = std::thread::spawn(move || names::intern(missing))
            .join()
            .unwrap();
        assert_eq!(names::lookup(missing), Some(id));
    }

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
