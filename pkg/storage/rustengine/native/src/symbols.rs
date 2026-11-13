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

#[derive(Default)]
pub struct Symbols {
    strings: Vec<Box<str>>,
    table: HashTable<Sym>,
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
        id
    }

    pub fn heap_bytes(&self) -> usize {
        self.strings.iter().map(|s| s.len()).sum::<usize>()
            + self.strings.capacity() * 16
            + self.table.capacity() * 5
    }
}
