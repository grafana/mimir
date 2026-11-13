// SPDX-License-Identifier: AGPL-3.0-only

//! Interned label names and values: series refer to their labels by u32 ids, so comparing, hashing and indexing
//! them never touches the strings.

use hashbrown::HashTable;
use std::hash::{BuildHasher, Hasher};

pub type Sym = u32;

#[derive(Default, Clone, Copy)]
pub struct StrHasher;

impl StrHasher {
    #[inline]
    pub fn hash(s: &[u8]) -> u64 {
        let mut h = rustc_hash::FxHasher::default();
        h.write(s);
        h.finish()
    }
}

impl BuildHasher for StrHasher {
    type Hasher = rustc_hash::FxHasher;
    fn build_hasher(&self) -> Self::Hasher {
        rustc_hash::FxHasher::default()
    }
}

/// A string the symbols own, ordered by its content: they never move nor go while the symbols live.
#[derive(Clone, Copy)]
struct Ordered(*const str);

impl Ordered {
    fn get(&self) -> &str {
        unsafe { &*self.0 }
    }
}

impl PartialEq for Ordered {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}
impl Eq for Ordered {}
impl PartialOrd for Ordered {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Ordered {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.get().cmp(other.get())
    }
}

// The pointers are to the symbols' own strings.
unsafe impl Send for Ordered {}
unsafe impl Sync for Ordered {}

#[derive(Default)]
pub struct Symbols {
    strings: Vec<Box<str>>,
    table: HashTable<Sym>,
    /// The symbols in string order, and a rank by symbol that follows it, so sorting by labels compares numbers.
    order: std::collections::BTreeMap<Ordered, Sym>,
    ranks: Vec<u64>,
}

impl Symbols {
    #[inline]
    pub fn get(&self, id: Sym) -> &str {
        &self.strings[id as usize]
    }

    pub fn len(&self) -> usize {
        self.strings.len()
    }

    #[inline]
    pub fn lookup(&self, s: &[u8]) -> Option<Sym> {
        let hash = StrHasher::hash(s);
        self.table
            .find(hash, |&id| self.strings[id as usize].as_bytes() == s)
            .copied()
    }

    pub fn intern(&mut self, s: &str) -> Sym {
        let hash = StrHasher::hash(s.as_bytes());
        let strings = &self.strings;
        if let Some(&id) = self.table.find(hash, |&id| &*strings[id as usize] == s) {
            return id;
        }
        let id = self.strings.len() as Sym;
        self.strings.push(s.into());
        let strings = &self.strings;
        self.table.insert_unique(hash, id, |&id| {
            StrHasher::hash(strings[id as usize].as_bytes())
        });
        self.rank_new(id);
        id
    }

    /// The rank of the symbol: symbols' ranks are in the order of their strings.
    #[inline]
    pub fn rank(&self, id: Sym) -> u64 {
        self.ranks[id as usize]
    }

    /// Gives a new symbol a rank between its neighbors', spreading all ranks out again when there is no room.
    fn rank_new(&mut self, id: Sym) {
        use std::ops::Bound::{Excluded, Unbounded};
        let key = Ordered(&*self.strings[id as usize] as *const str);
        let prev = self
            .order
            .range(..key)
            .next_back()
            .map_or(0, |(_, &p)| self.ranks[p as usize]);
        let next = self
            .order
            .range((Excluded(key), Unbounded))
            .next()
            .map_or(u64::MAX, |(_, &n)| self.ranks[n as usize]);
        self.order.insert(key, id);
        self.ranks.push(0);
        if next - prev >= 2 {
            self.ranks[id as usize] = prev + (next - prev) / 2;
            return;
        }
        let spacing = u64::MAX / (self.order.len() as u64 * 2 + 2);
        for (i, (_, &sym)) in self.order.iter().enumerate() {
            self.ranks[sym as usize] = (i as u64 + 1) * spacing;
        }
    }

    pub fn heap_bytes(&self) -> usize {
        self.strings.iter().map(|s| s.len()).sum::<usize>()
            + self.strings.capacity() * 16
            + self.table.capacity() * 5
            + self.ranks.capacity() * 8
            + self.order.len() * 32
    }
}
