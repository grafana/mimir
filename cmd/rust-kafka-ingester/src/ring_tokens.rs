// SPDX-License-Identifier: AGPL-3.0-only

mod go_rng;

use std::collections::{BinaryHeap, HashMap, HashSet};

use anyhow::{Context, Result, ensure};

use crate::ring_snapshot::{PARTITION_ACTIVE, PARTITION_DELETED, Partition};
use go_rng::GoRng;

const RING_SIZE: u64 = 1 << 32;
const TOKENS_PER_PARTITION: usize = 512;
const ZONES: u32 = 8;

#[derive(Default)]
pub(crate) struct DerivedTokens(Vec<Vec<u32>>);

impl DerivedTokens {
    fn get(&mut self, id: i32) -> Result<&[u32]> {
        // A corrupt or unexpected descriptor must not allocate an unbounded token table.
        ensure!(
            (0..10_000).contains(&id),
            "unsupported derived partition ID {id}"
        );
        let id = id as usize;
        if id >= self.0.len() {
            self.0 = derive(id)?;
        }
        Ok(&self.0[id])
    }
}

pub(crate) struct PartitionTokens {
    active: Vec<(u32, i32)>,
    partitions: usize,
}

impl PartitionTokens {
    pub(crate) fn new(
        partitions: &HashMap<i32, Partition>,
        derived: &mut DerivedTokens,
    ) -> Result<Self> {
        if let Some(id) = partitions
            .iter()
            .filter_map(|(&id, p)| {
                (p.state != PARTITION_DELETED && p.token_scheme == 1).then_some(id)
            })
            .max()
        {
            derived.get(id)?;
        }
        let mut active = Vec::new();
        for (&id, partition) in partitions {
            if partition.state == PARTITION_DELETED {
                continue;
            }
            let tokens = match partition.token_scheme {
                0 => &partition.tokens,
                1 => derived.get(id)?,
                scheme => anyhow::bail!("unsupported partition token scheme {scheme}"),
            };
            if partition.state == PARTITION_ACTIVE {
                active.extend(tokens.iter().map(|&token| (token, id)));
            }
        }
        active.sort_unstable();
        ensure!(
            active.windows(2).all(|pair| pair[0].0 != pair[1].0),
            "duplicate active partition tokens"
        );
        Ok(Self {
            active,
            partitions: partitions
                .values()
                .filter(|p| p.state != PARTITION_DELETED)
                .count(),
        })
    }

    pub(crate) fn owned_ranges(&self, tenant: &str, size: i64, partition: i32) -> Option<Vec<u32>> {
        if self.active.is_empty() {
            return None;
        }
        let selected = if size > 0 && size < self.partitions as i64 {
            let hash = md5::compute(tenant.as_bytes());
            let mut rng = GoRng::new(i64::from_be_bytes(hash[..8].try_into().unwrap()));
            let mut selected = HashSet::new();
            for _ in 0..size {
                let key = rng.uint32();
                let start = self.active.partition_point(|&(token, _)| token <= key);
                let candidate = self.active[start..]
                    .iter()
                    .chain(&self.active[..start])
                    .find(|&&(_, id)| !selected.contains(&id));
                let Some(&(_, id)) = candidate else { break };
                selected.insert(id);
            }
            if !selected.contains(&partition) {
                return None;
            }
            Some(selected)
        } else {
            None
        };

        let in_shard = |id| selected.as_ref().is_none_or(|ids| ids.contains(&id));
        let mut tokens = self
            .active
            .iter()
            .copied()
            .filter(|&(_, id)| in_shard(id))
            .peekable();
        let &(_, first_owner) = tokens.peek()?;
        let mut previous = 0;
        let mut ranges = Vec::new();
        let add = |ranges: &mut Vec<u32>, start: u32, end: u32| {
            if start != 0 && ranges.last().is_some_and(|&last| last == start - 1) {
                *ranges.last_mut().unwrap() = end;
            } else {
                ranges.extend([start, end]);
            }
        };
        for (token, owner) in tokens {
            if owner == partition && token != 0 {
                add(&mut ranges, previous, token - 1);
            }
            previous = token;
        }
        if first_owner == partition {
            add(&mut ranges, previous, u32::MAX);
        }
        // The Go shard omits partitions without tokens. In particular, token zero
        // owns the wraparound range and never an empty interval before zero.
        (!ranges.is_empty()).then_some(ranges)
    }
}

fn derive(max_id: usize) -> Result<Vec<Vec<u32>>> {
    let first: Vec<_> = (0..TOKENS_PER_PARTITION as u32)
        .map(|i| i * (RING_SIZE / TOKENS_PER_PARTITION as u64) as u32)
        .collect();
    let mut owners = BinaryHeap::from([(RING_SIZE, 0)]);
    let mut queues = vec![BinaryHeap::from_iter(first.iter().map(|&token| {
        let width = (RING_SIZE / TOKENS_PER_PARTITION as u64) as u32;
        (width, token, token.wrapping_sub(width))
    }))];
    let mut result = vec![first];
    for id in 1..=max_id {
        let optimal = RING_SIZE as f64 / (id + 1) as f64;
        let mut owned = 0u64;
        let mut new_queue = BinaryHeap::new();
        let mut tokens = Vec::with_capacity(TOKENS_PER_PARTITION);
        let mut ignored = Vec::new();
        while tokens.len() < TOKENS_PER_PARTITION {
            let remaining = (TOKENS_PER_PARTITION - tokens.len()) as u32;
            let width = ((optimal - owned as f64) as u32 / remaining / ZONES) * ZONES;
            ensure!(
                width >= ZONES,
                "cannot derive partition {id}: token interval too small"
            );
            let (ownership, owner) = owners.pop().context("no token owners left")?;
            ensure!(
                ownership > width as u64,
                "cannot derive partition {id}: insufficient ownership"
            );
            let &(old_width, token, previous) = queues[owner].peek().context("missing token")?;
            if old_width <= width {
                ignored.push((ownership, owner));
                continue;
            }
            // Go's zone-zero generator treats the highest aligned token as
            // the wrap boundary and places the remaining offset after zero.
            let offset = (u32::MAX - (ZONES - 1)) - previous;
            let new_token = if offset < width {
                width - offset
            } else {
                previous + width
            };
            let remaining_width = token.wrapping_sub(new_token);
            let gained = old_width - remaining_width;
            queues[owner].pop();
            queues[owner].push((remaining_width, token, new_token));
            owners.push((ownership - gained as u64, owner));
            new_queue.push((new_token.wrapping_sub(previous), new_token, previous));
            tokens.push(new_token);
            owned += gained as u64;
        }
        owners.extend(ignored);
        owners.push((owned, id));
        queues.push(new_queue);
        result.push(tokens);
    }
    Ok(result)
}
