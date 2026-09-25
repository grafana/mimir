#[derive(Clone, Debug)]
pub(crate) struct SampleStore {
    data: SampleData,
}

#[derive(Clone, Debug)]
enum SampleData {
    Packed { base: i64, values: Vec<[u8; 12]> },
    Wide(Vec<(i64, f64)>),
}

impl Default for SampleStore {
    fn default() -> Self {
        Self {
            data: SampleData::Packed {
                base: 0,
                values: Vec::new(),
            },
        }
    }
}

impl SampleStore {
    pub(crate) fn len(&self) -> usize {
        match &self.data {
            SampleData::Packed { values, .. } => values.len(),
            SampleData::Wide(values) => values.len(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn get(&self, index: usize) -> Option<(i64, f64)> {
        match &self.data {
            SampleData::Packed { base, values } => {
                values.get(index).map(|value| unpack(*base, value))
            }
            SampleData::Wide(values) => values.get(index).copied(),
        }
    }

    pub(crate) fn last(&self) -> Option<(i64, f64)> {
        self.len().checked_sub(1).and_then(|index| self.get(index))
    }

    pub(crate) fn binary_search(&self, timestamp: i64) -> Result<usize, usize> {
        match &self.data {
            SampleData::Packed { base, values } => {
                if values
                    .last()
                    .is_some_and(|last| *base + i64::from(delta(last)) < timestamp)
                {
                    return Err(values.len());
                }
                let relative = timestamp
                    .checked_sub(*base)
                    .unwrap_or(if timestamp < *base {
                        i64::MIN
                    } else {
                        i64::MAX
                    });
                values.binary_search_by_key(&relative, |value| i64::from(delta(value)))
            }
            SampleData::Wide(values) => {
                if values.last().is_some_and(|(last, _)| *last < timestamp) {
                    return Err(values.len());
                }
                values.binary_search_by_key(&timestamp, |(time, _)| *time)
            }
        }
    }

    pub(crate) fn insert(&mut self, index: usize, timestamp: i64, value: f64) {
        self.ensure_timestamp_fits(timestamp);
        match &mut self.data {
            SampleData::Packed { base, values } => {
                if values.is_empty() {
                    *base = timestamp;
                }
                values.insert(index, pack((timestamp - *base) as i32, value));
            }
            SampleData::Wide(values) => values.insert(index, (timestamp, value)),
        }
    }

    pub(crate) fn retain_since(&mut self, cutoff: i64) {
        match &mut self.data {
            SampleData::Packed { base, values } => {
                values.retain(|value| *base + i64::from(delta(value)) >= cutoff);
                if values.is_empty() {
                    *base = 0;
                }
            }
            SampleData::Wide(values) => values.retain(|(timestamp, _)| *timestamp >= cutoff),
        }
    }

    pub(crate) fn iter(&self) -> SampleIter<'_> {
        self.iter_from(0)
    }

    pub(crate) fn iter_from(&self, index: usize) -> SampleIter<'_> {
        match &self.data {
            SampleData::Packed { base, values } => SampleIter::Packed {
                base: *base,
                values: values[index..].iter(),
            },
            SampleData::Wide(values) => SampleIter::Wide(values[index..].iter()),
        }
    }

    fn ensure_timestamp_fits(&mut self, timestamp: i64) {
        let SampleData::Packed { base, values } = &mut self.data else {
            return;
        };
        if values.is_empty() {
            *base = timestamp;
            return;
        }
        if timestamp
            .checked_sub(*base)
            .and_then(|delta| i32::try_from(delta).ok())
            .is_some()
        {
            return;
        }
        let first = *base + i64::from(delta(values.first().expect("nonempty")));
        let last = *base + i64::from(delta(values.last().expect("nonempty")));
        let new_base = first.min(timestamp);
        if last
            .max(timestamp)
            .checked_sub(new_base)
            .is_some_and(|span| span <= i64::from(i32::MAX))
        {
            for value in values.iter_mut() {
                let time = *base + i64::from(delta(value));
                value[..4].copy_from_slice(&((time - new_base) as i32).to_le_bytes());
            }
            *base = new_base;
            return;
        }
        // Long-lived series without pruning can span more than the compact timestamp range.
        self.data = SampleData::Wide(values.iter().map(|value| unpack(*base, value)).collect());
    }
}

pub(crate) enum SampleIter<'a> {
    Packed {
        base: i64,
        values: std::slice::Iter<'a, [u8; 12]>,
    },
    Wide(std::slice::Iter<'a, (i64, f64)>),
}

impl Iterator for SampleIter<'_> {
    type Item = (i64, f64);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SampleIter::Packed { base, values } => values.next().map(|value| unpack(*base, value)),
            SampleIter::Wide(values) => values.next().copied(),
        }
    }
}

fn pack(delta: i32, value: f64) -> [u8; 12] {
    let mut packed = [0; 12];
    packed[..4].copy_from_slice(&delta.to_le_bytes());
    packed[4..].copy_from_slice(&value.to_bits().to_le_bytes());
    packed
}

fn delta(packed: &[u8; 12]) -> i32 {
    i32::from_le_bytes(packed[..4].try_into().expect("four bytes"))
}

fn unpack(base: i64, packed: &[u8; 12]) -> (i64, f64) {
    (
        base + i64::from(delta(packed)),
        f64::from_bits(u64::from_le_bytes(
            packed[4..].try_into().expect("eight bytes"),
        )),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserts_out_of_order_and_prunes() {
        let mut samples = SampleStore::default();
        samples.insert(0, 100, -0.0);
        samples.insert(0, 50, 2.0);
        samples.insert(2, 150, 3.0);
        assert_eq!(samples.binary_search(100), Ok(1));
        assert_eq!(
            samples
                .iter()
                .map(|(_, value)| value.to_bits())
                .collect::<Vec<_>>(),
            vec![2.0_f64.to_bits(), (-0.0_f64).to_bits(), 3.0_f64.to_bits()]
        );
        samples.retain_since(100);
        assert_eq!(
            samples.iter().map(|(time, _)| time).collect::<Vec<_>>(),
            vec![100, 150]
        );
    }

    #[test]
    fn rebases_then_falls_back_for_wide_timestamp_spans() {
        let mut samples = SampleStore::default();
        samples.insert(0, 0, 1.0);
        samples.insert(1, i64::from(i32::MAX), 2.0);
        samples.retain_since(i64::from(i32::MAX));
        samples.insert(1, i64::from(i32::MAX) * 2, 3.0);
        assert!(matches!(samples.data, SampleData::Packed { .. }));
        samples.insert(2, i64::from(i32::MAX) * 4, 4.0);
        assert!(matches!(samples.data, SampleData::Wide(_)));
        assert_eq!(
            samples.iter().map(|(time, _)| time).collect::<Vec<_>>(),
            vec![
                i64::from(i32::MAX),
                i64::from(i32::MAX) * 2,
                i64::from(i32::MAX) * 4
            ]
        );
    }
}
