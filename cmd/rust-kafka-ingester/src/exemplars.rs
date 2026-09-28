//! Per-tenant exemplar storage with the behavior of Prometheus's `CircularExemplarStorage`: a
//! fixed number of exemplars per tenant, evicted oldest-inserted first, each series keeping its
//! exemplars sorted by timestamp.

use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};

use crate::proto::cortexpb;

/// Why an exemplar was not stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rejection {
    Disabled,
    LabelLength,
    OutOfOrder,
}

pub const MAX_LABEL_SET_LENGTH: usize = 128;

/// Prometheus's `ValidateExemplar` against a series whose newest stored exemplar is `newest`:
/// what the head appender checks when the exemplar is appended, before the commit adds it.
pub fn validate(
    capacity: usize,
    newest: Option<&cortexpb::Exemplar>,
    exemplar: &cortexpb::Exemplar,
    out_of_order_window_ms: i64,
) -> Result<(), Rejection> {
    if capacity == 0 {
        return Err(Rejection::Disabled);
    }
    let label_set_len = exemplar
        .labels
        .iter()
        .map(|label| rune_count(&label.name) + rune_count(&label.value))
        .sum::<usize>();
    if label_set_len > MAX_LABEL_SET_LENGTH {
        return Err(Rejection::LabelLength);
    }
    let Some(newest) = newest else {
        return Ok(());
    };
    if newest == exemplar {
        return Ok(());
    }
    if (exemplar.timestamp_ms < newest.timestamp_ms
        && exemplar.timestamp_ms <= newest.timestamp_ms - out_of_order_window_ms)
        || (exemplar.timestamp_ms == newest.timestamp_ms && exemplar.value < newest.value)
        || (exemplar.timestamp_ms == newest.timestamp_ms
            && exemplar.value == newest.value
            && label_hash(exemplar) < label_hash(newest))
    {
        return Err(Rejection::OutOfOrder);
    }
    Ok(())
}

struct SeriesExemplars<L> {
    labels: L,
    // (insertion sequence, exemplar), sorted by timestamp.
    exemplars: VecDeque<(u64, cortexpb::Exemplar)>,
    newest: usize,
}

pub struct TenantExemplars<L> {
    capacity: usize,
    next_sequence: u64,
    // Insertion order across series, for eviction.
    order: VecDeque<(u64, u64)>,
    series: HashMap<u64, SeriesExemplars<L>>,
}

impl<L: Clone> TenantExemplars<L> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            next_sequence: 0,
            order: VecDeque::new(),
            series: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    pub fn series_count(&self) -> usize {
        self.series.len()
    }

    /// The timestamp of the exemplar that will be evicted next, as Prometheus reports it.
    pub fn oldest_timestamp(&self) -> Option<i64> {
        let (series_id, sequence) = self.order.front()?;
        self.series
            .get(series_id)?
            .exemplars
            .iter()
            .find(|(inserted, _)| inserted == sequence)
            .map(|(_, exemplar)| exemplar.timestamp_ms)
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Changes the capacity, keeping the newest exemplars like Prometheus's `Resize`.
    pub fn resize(&mut self, capacity: usize) {
        self.capacity = capacity;
        while self.order.len() > capacity {
            self.evict_oldest();
        }
    }

    fn evict_oldest(&mut self) {
        let Some((series_id, sequence)) = self.order.pop_front() else {
            return;
        };
        if let Some(series) = self.series.get_mut(&series_id) {
            if let Some(index) = series
                .exemplars
                .iter()
                .position(|(inserted, _)| *inserted == sequence)
            {
                series.exemplars.remove(index);
                if series.newest == index {
                    series.newest = newest_index(&series.exemplars);
                } else if series.newest > index {
                    series.newest -= 1;
                }
            }
            if series.exemplars.is_empty() {
                self.series.remove(&series_id);
            }
        }
    }

    /// The series' newest exemplar, which later exemplars are validated against.
    pub fn newest(&self, series_id: u64) -> Option<&cortexpb::Exemplar> {
        self.series
            .get(&series_id)
            .map(|series| &series.exemplars[series.newest].1)
    }

    /// Adds an exemplar for the series identified by `series_id`, returning whether it was stored:
    /// a duplicate of the series' newest exemplar or of a stored timestamp is accepted without
    /// storing it again.
    pub fn add(
        &mut self,
        series_id: u64,
        labels: impl FnOnce() -> L,
        exemplar: cortexpb::Exemplar,
        out_of_order_window_ms: i64,
    ) -> Result<bool, Rejection> {
        if self.capacity == 0 {
            return Err(Rejection::Disabled);
        }
        let label_set_len = exemplar
            .labels
            .iter()
            .map(|label| rune_count(&label.name) + rune_count(&label.value))
            .sum::<usize>();
        if label_set_len > MAX_LABEL_SET_LENGTH {
            return Err(Rejection::LabelLength);
        }
        if let Some(series) = self.series.get(&series_id) {
            let newest = &series.exemplars[series.newest].1;
            if newest == &exemplar {
                return Ok(false);
            }
            if (exemplar.timestamp_ms < newest.timestamp_ms
                && exemplar.timestamp_ms <= newest.timestamp_ms - out_of_order_window_ms)
                || (exemplar.timestamp_ms == newest.timestamp_ms && exemplar.value < newest.value)
                || (exemplar.timestamp_ms == newest.timestamp_ms
                    && exemplar.value == newest.value
                    && label_hash(&exemplar) < label_hash(newest))
            {
                return Err(Rejection::OutOfOrder);
            }
            let oldest = series.exemplars[0].1.timestamp_ms;
            if exemplar.timestamp_ms >= oldest
                && exemplar.timestamp_ms < newest.timestamp_ms
                && series
                    .exemplars
                    .iter()
                    .any(|(_, stored)| stored.timestamp_ms == exemplar.timestamp_ms)
            {
                return Ok(false);
            }
        }
        if self.order.len() >= self.capacity {
            self.evict_oldest();
        }
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.order.push_back((series_id, sequence));
        let series = self
            .series
            .entry(series_id)
            .or_insert_with(|| SeriesExemplars {
                labels: labels(),
                exemplars: VecDeque::new(),
                newest: 0,
            });
        let index = series
            .exemplars
            .partition_point(|(_, stored)| stored.timestamp_ms <= exemplar.timestamp_ms);
        let is_newest = series.exemplars.is_empty()
            || exemplar.timestamp_ms >= series.exemplars[series.newest].1.timestamp_ms;
        series.exemplars.insert(index, (sequence, exemplar));
        if is_newest {
            series.newest = index;
        } else if series.newest >= index {
            series.newest += 1;
        }
        Ok(true)
    }

    /// Series with exemplars in `[start, end]`, with their exemplars in timestamp order.
    pub fn select(
        &self,
        start: i64,
        end: i64,
        mut matches: impl FnMut(&L) -> bool,
    ) -> Vec<(L, Vec<cortexpb::Exemplar>)> {
        self.series
            .values()
            .filter(|series| {
                series.exemplars[0].1.timestamp_ms <= end
                    && series.exemplars[series.newest].1.timestamp_ms >= start
            })
            .filter(|series| matches(&series.labels))
            .filter_map(|series| {
                let exemplars = series
                    .exemplars
                    .iter()
                    .map(|(_, exemplar)| exemplar)
                    .filter(|exemplar| {
                        exemplar.timestamp_ms >= start && exemplar.timestamp_ms <= end
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                (!exemplars.is_empty()).then(|| (series.labels.clone(), exemplars))
            })
            .collect()
    }

    /// Every exemplar in insertion order, for snapshots.
    pub fn in_insertion_order(&self) -> Vec<(u64, L, cortexpb::Exemplar)> {
        self.order
            .iter()
            .filter_map(|(series_id, sequence)| {
                let series = self.series.get(series_id)?;
                let (_, exemplar) = series
                    .exemplars
                    .iter()
                    .find(|(inserted, _)| inserted == sequence)?;
                Some((*series_id, series.labels.clone(), exemplar.clone()))
            })
            .collect()
    }
}

fn rune_count(bytes: &[u8]) -> usize {
    std::str::from_utf8(bytes).map_or(bytes.len(), |text| text.chars().count())
}

fn newest_index(exemplars: &VecDeque<(u64, cortexpb::Exemplar)>) -> usize {
    exemplars.len().saturating_sub(1)
}

fn label_hash(exemplar: &cortexpb::Exemplar) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for label in &exemplar.labels {
        label.name.hash(&mut hasher);
        label.value.hash(&mut hasher);
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exemplar(timestamp_ms: i64, value: f64) -> cortexpb::Exemplar {
        cortexpb::Exemplar {
            labels: vec![cortexpb::LabelPair {
                name: "trace_id".into(),
                value: format!("{timestamp_ms}").into(),
            }],
            value,
            timestamp_ms,
        }
    }

    fn timestamps(storage: &TenantExemplars<u64>, series: u64) -> Vec<i64> {
        storage
            .select(i64::MIN, i64::MAX, |labels| *labels == series)
            .into_iter()
            .flat_map(|(_, exemplars)| exemplars)
            .map(|exemplar| exemplar.timestamp_ms)
            .collect()
    }

    #[test]
    fn evicts_the_oldest_inserted_exemplar_across_series() {
        let mut storage = TenantExemplars::new(3);
        storage.add(1, || 1, exemplar(10, 1.0), 0).unwrap();
        storage.add(2, || 2, exemplar(11, 1.0), 0).unwrap();
        storage.add(1, || 1, exemplar(12, 1.0), 0).unwrap();
        storage.add(2, || 2, exemplar(13, 1.0), 0).unwrap();
        assert_eq!(storage.len(), 3);
        assert_eq!(timestamps(&storage, 1), [12]);
        assert_eq!(timestamps(&storage, 2), [11, 13]);
        storage.resize(1);
        assert_eq!(timestamps(&storage, 1), Vec::<i64>::new());
        assert_eq!(timestamps(&storage, 2), [13]);
        storage.resize(0);
        assert_eq!(
            storage.add(1, || 1, exemplar(20, 1.0), 0),
            Err(Rejection::Disabled)
        );
    }

    #[test]
    fn rejects_out_of_order_exemplars_outside_the_window() {
        let mut storage = TenantExemplars::new(10);
        storage.add(1, || 1, exemplar(100, 1.0), 0).unwrap();
        // Duplicates of the newest exemplar are accepted and not stored twice.
        storage.add(1, || 1, exemplar(100, 1.0), 0).unwrap();
        assert_eq!(storage.len(), 1);
        assert_eq!(
            storage.add(1, || 1, exemplar(90, 1.0), 0),
            Err(Rejection::OutOfOrder)
        );
        assert_eq!(
            storage.add(1, || 1, exemplar(100, 0.5), 0),
            Err(Rejection::OutOfOrder)
        );
        // With a window, older exemplars within it are stored in timestamp order.
        storage.add(1, || 1, exemplar(90, 1.0), 20).unwrap();
        storage.add(1, || 1, exemplar(95, 1.0), 20).unwrap();
        assert_eq!(
            storage.add(1, || 1, exemplar(80, 1.0), 20),
            Err(Rejection::OutOfOrder)
        );
        assert_eq!(timestamps(&storage, 1), [90, 95, 100]);
        // A timestamp already stored is skipped.
        storage.add(1, || 1, exemplar(95, 7.0), 20).unwrap();
        assert_eq!(storage.len(), 3);
        storage.add(1, || 1, exemplar(110, 1.0), 20).unwrap();
        assert_eq!(timestamps(&storage, 1), [90, 95, 100, 110]);
        assert_eq!(
            storage
                .in_insertion_order()
                .iter()
                .map(|(_, _, exemplar)| exemplar.timestamp_ms)
                .collect::<Vec<_>>(),
            [100, 90, 95, 110]
        );
    }

    #[test]
    fn rejects_long_label_sets_and_selects_by_time() {
        let mut storage = TenantExemplars::new(10);
        let mut long = exemplar(1, 1.0);
        long.labels[0].value = "x".repeat(MAX_LABEL_SET_LENGTH).into();
        assert_eq!(storage.add(1, || 1, long, 0), Err(Rejection::LabelLength));
        for timestamp in [10, 20, 30] {
            storage.add(1, || 1, exemplar(timestamp, 1.0), 0).unwrap();
        }
        let selected = storage.select(15, 30, |_| true);
        assert_eq!(
            selected[0]
                .1
                .iter()
                .map(|e| e.timestamp_ms)
                .collect::<Vec<_>>(),
            [20, 30]
        );
        assert!(storage.select(31, 40, |_| true).is_empty());
    }
}
