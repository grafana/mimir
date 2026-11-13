// SPDX-License-Identifier: AGPL-3.0-only
use super::wire::Wire;
use super::*;
use std::os::unix::fs::MetadataExt;

const WAL_MAGIC: u32 = 0x334c4157;
const PREVIOUS_WAL_MAGIC: u32 = 0x324c4157;
const LEGACY_WAL_MAGIC: u32 = 0x314c4157;
const COLD_MAGIC: u32 = 0x31444c43;

pub(super) fn wal_record(commit: &Commit, state: &State) -> Vec<u8> {
    let mut body = Vec::new();
    WAL_MAGIC.put(&mut body);
    commit.max_exemplars.put(&mut body);
    commit.seq.put(&mut body);
    commit.next_ref.put(&mut body);
    (commit.series.len() as u32).put(&mut body);
    for s in commit.series.values() {
        s.reference.put(&mut body);
        // Stable references make repeated label dictionaries unnecessary in the WAL.
        if state.series.contains_key(&s.reference) {
            Vec::<Label>::new().put(&mut body);
        } else {
            s.labels.put(&mut body);
        }
        s.samples.put(&mut body);
        s.exemplars.put(&mut body);
    }
    commit
        .series
        .values()
        .filter_map(|series| {
            let times: Vec<_> = series
                .samples
                .iter()
                .filter_map(|(&t, sample)| sample.out_of_order.then_some(t))
                .collect();
            (!times.is_empty()).then_some((series.reference, times))
        })
        .collect::<BTreeMap<_, _>>()
        .put(&mut body);
    let mut record = Vec::with_capacity(body.len() + 8);
    (body.len() as u64).put(&mut record);
    record.extend_from_slice(&body);
    record
}
pub(super) fn replay(bytes: &[u8], state: &mut State) -> Result<usize> {
    let mut valid = 0;
    while bytes.len() - valid >= 8 {
        let mut input = &bytes[valid..];
        let len = usize::try_from(u64::get(&mut input)?).map_err(io)?;
        if len > input.len() {
            break;
        }
        let mut body = &input[..len];
        let version = u32::get(&mut body)?;
        if version != WAL_MAGIC && version != PREVIOUS_WAL_MAGIC && version != LEGACY_WAL_MAGIC {
            return Err("invalid WAL version".into());
        }
        let max_exemplars = usize::get(&mut body)?;
        let seq = u64::get(&mut body)?;
        let next_ref = u64::get(&mut body)?;
        let mut series: Vec<Series> = Vec::get(&mut body)?;
        if version == WAL_MAGIC {
            let mut ooo: BTreeMap<u64, Vec<i64>> = Wire::get(&mut body)?;
            for series in &mut series {
                let mut times: BTreeSet<_> = ooo
                    .remove(&series.reference)
                    .unwrap_or_default()
                    .into_iter()
                    .collect();
                for (&t, sample) in &mut series.samples {
                    sample.out_of_order = times.remove(&t);
                    series.uncompacted_ooo |= sample.out_of_order;
                }
                if !times.is_empty() {
                    return Err("WAL marks unknown samples out of order".into());
                }
            }
            if !ooo.is_empty() {
                return Err("WAL marks unknown series out of order".into());
            }
        } else if version == PREVIOUS_WAL_MAGIC {
            let mut ooo: BTreeSet<u64> = Vec::<u64>::get(&mut body)?.into_iter().collect();
            for s in &mut series {
                s.uncompacted_ooo = ooo.remove(&s.reference);
                if s.uncompacted_ooo {
                    for sample in s.samples.values_mut() {
                        sample.out_of_order = true;
                    }
                }
            }
            if !ooo.is_empty() {
                return Err("WAL marks unknown series out of order".into());
            }
        }
        if !body.is_empty() {
            return Err("trailing WAL bytes".into());
        }
        if seq > state.seq {
            let commit = Commit {
                max_exemplars,
                seq,
                next_ref,
                series: series.into_iter().map(|s| (s.reference, s)).collect(),
            };
            for s in commit.series.values() {
                if s.labels.is_empty() && !state.series.contains_key(&s.reference) {
                    return Err("WAL references unknown series".into());
                }
            }
            state.apply(commit);
        }
        valid += len + 8;
    }
    Ok(valid)
}
fn put_chunk(mint: i64, maxt: i64, data: &[u8], bytes: &mut Vec<u8>) {
    mint.put(bytes);
    maxt.put(bytes);
    (data.len() as u32).put(bytes);
    bytes.extend_from_slice(data);
}
fn put_series(s: &Series, bytes: &mut Vec<u8>) {
    s.reference.put(bytes);
    // The checkpoint owns each series' dictionary; cold records only need its stable reference.
    Vec::<Label>::new().put(bytes);
    // Immutable cold files can compress short OOO tails once instead of on every read.
    let floats = s.samples.values().filter(|sample| sample.kind == 1).count();
    ((s.samples.len() - floats) as u32).put(bytes);
    for (&t, sample) in &s.samples {
        if sample.kind != 1 {
            t.put(bytes);
            sample.put(bytes);
        }
    }
    s.exemplars.put(bytes);
    ((s.float_chunks.len() + usize::from(s.open_float.is_some()) + floats.div_ceil(120)) as u32)
        .put(bytes);
    for c in &s.float_chunks {
        put_chunk(c.mint, c.maxt, &c.data, bytes)
    }
    if let Some(c) = &s.open_float {
        put_chunk(
            c.first_timestamp().unwrap(),
            c.last_timestamp().unwrap(),
            c.bytes(),
            bytes,
        )
    }
    let mut tail = Vec::with_capacity(floats.min(120));
    for sample in s.samples.values().filter(|sample| sample.kind == 1) {
        tail.push((sample.t, f64::from_bits(sample.bits)));
        if tail.len() == 120 {
            put_chunk(
                tail[0].0,
                tail.last().unwrap().0,
                &xor::encode(&tail),
                bytes,
            );
            tail.clear();
        }
    }
    if !tail.is_empty() {
        put_chunk(
            tail[0].0,
            tail.last().unwrap().0,
            &xor::encode(&tail),
            bytes,
        );
    }
}
#[cfg(test)]
fn get_series(bytes: &[u8], mint: i64, maxt: i64) -> Result<Series> {
    get_series_view(bytes, mint, maxt, None)
}
fn get_series_view(
    mut bytes: &[u8],
    mint: i64,
    maxt: i64,
    source: Option<(&Arc<ChunkOwner>, usize)>,
) -> Result<Series> {
    let record_len = bytes.len();
    if u32::get(&mut bytes)? != COLD_MAGIC {
        return Err("invalid cold file version".into());
    }
    let mut s = Series {
        reference: Wire::get(&mut bytes)?,
        labels: Wire::get(&mut bytes)?,
        ..Default::default()
    };
    let count = u32::get(&mut bytes)? as usize;
    if count > bytes.len() / 29 {
        return Err("invalid cold sample count".into());
    }
    s.samples = (0..count)
        .map(|_| -> Result<_> {
            let t = i64::get(&mut bytes)?;
            if t >= mint && t <= maxt {
                Ok(Some((t, Sample::get(&mut bytes)?)))
            } else {
                i64::get(&mut bytes)?;
                u8::get(&mut bytes)?;
                u64::get(&mut bytes)?;
                // Range pruning must retain corruption checks without allocating skipped payloads.
                wire::skip_string(&mut bytes)?;
                Ok(None)
            }
        })
        .filter_map(|result| result.transpose())
        .collect::<Result<_>>()?;
    s.exemplars = Wire::get(&mut bytes)?;
    let count = u32::get(&mut bytes)? as usize;
    if count > bytes.len() / 20 {
        return Err("invalid cold chunk count".into());
    }
    for _ in 0..count {
        let first = i64::get(&mut bytes)?;
        let last = i64::get(&mut bytes)?;
        let len = u32::get(&mut bytes)? as usize;
        if len > bytes.len() {
            return Err("truncated cold chunk".into());
        }
        let offset = record_len - bytes.len();
        let (data, rest) = bytes.split_at(len);
        bytes = rest;
        if first <= maxt && last >= mint {
            s.float_chunks.push(FloatChunk {
                mint: first,
                maxt: last,
                data: match source {
                    Some((owner, base)) => ChunkBytes {
                        owner: Some(owner.clone()),
                        start: base + offset,
                        end: base + offset + len,
                    },
                    None => data.into(),
                },
                out_of_order: false,
            });
        }
    }
    if !bytes.is_empty() {
        return Err("trailing cold file bytes".into());
    }
    Ok(s)
}
pub(super) fn cold_file(series: &[Series]) -> (Vec<u8>, BTreeMap<u64, (u64, u64)>) {
    let mut bytes = Vec::new();
    let mut locations = BTreeMap::new();
    for s in series {
        let start = bytes.len() as u64;
        COLD_MAGIC.put(&mut bytes);
        put_series(s, &mut bytes);
        locations.insert(s.reference, (start, bytes.len() as u64 - start));
    }
    (bytes, locations)
}
pub(super) fn read_cold(
    engine: &Engine,
    block: &Cold,
    selected: &BTreeSet<u64>,
    mint: i64,
    maxt: i64,
) -> Result<Vec<Series>> {
    let mut series = Vec::new();
    visit_cold(engine, block, selected, mint, maxt, |s| series.push(s))?;
    Ok(series)
}
pub(super) fn visit_cold(
    engine: &Engine,
    block: &Cold,
    selected: &BTreeSet<u64>,
    mint: i64,
    maxt: i64,
    mut visit: impl FnMut(Series),
) -> Result<()> {
    // A block's time envelope can span many disjoint series lifetimes.
    let overlaps = |reference: &&u64| {
        block
            .ranges
            .get(*reference)
            .is_none_or(|&(first, last)| first <= maxt && last >= mint)
    };
    let mut wanted = selected
        .intersection(&block.refs)
        .filter(overlaps)
        .peekable();
    if mint > maxt || wanted.peek().is_none() {
        return Ok(());
    }
    if block.locations.is_empty() {
        let series: Vec<Series> = serde_json::from_reader(std::io::BufReader::new(
            File::open(engine.dir.join(&block.file)).map_err(io)?,
        ))
        .map_err(io)?;
        for s in series
            .into_iter()
            .filter(|s| selected.contains(&s.reference) && overlaps(&&s.reference))
        {
            visit(s);
        }
        return Ok(());
    }
    let path = engine.dir.join(&block.file);
    let metadata = fs::metadata(&path).map_err(io)?;
    let size = metadata.len();
    if size == 0 {
        return Err("truncated cold series".into());
    }
    let cached = {
        let mut cache = engine.cold_mappings.borrow_mut();
        cache
            .iter()
            .position(|(file, _)| file == &block.file)
            .map(|index| {
                let entry = cache.remove(index).unwrap();
                let mapping = entry.1.clone();
                cache.push_back(entry);
                mapping
            })
    };
    let mapped = match cached {
        Some(mapping) => mapping,
        None => {
            let file = File::open(&path).map_err(io)?;
            let identity = file.metadata().map_err(io)?;
            // SAFETY: cold files stay immutable after atomic rename; chunk owners retain mappings across retention.
            let map = unsafe { memmap2::Mmap::map(&file).map_err(io)? };
            let mapping = Arc::new(ColdMapping {
                owner: Arc::new(ChunkOwner::Mapped(map)),
                device: identity.dev(),
                inode: identity.ino(),
            });
            let mut cache = engine.cold_mappings.borrow_mut();
            cache.push_back((block.file.clone(), mapping.clone()));
            while cache.len() > MAX_COLD_MAPPINGS {
                cache.pop_front();
            }
            mapping
        }
    };
    if mapped.owner.len() as u64 != size
        || mapped.device != metadata.dev()
        || mapped.inode != metadata.ino()
    {
        return Err("cold file changed after mapping".into());
    }
    for reference in wanted {
        let Some(&(offset, len)) = block.locations.get(reference) else {
            continue;
        };
        if offset > size || len > size - offset {
            return Err("truncated cold series".into());
        }
        let start = usize::try_from(offset).map_err(io)?;
        let end = start + usize::try_from(len).map_err(io)?;
        let mut series = get_series_view(
            &mapped.owner[start..end],
            mint,
            maxt,
            Some((&mapped.owner, start)),
        )?;
        if series.reference != *reference {
            return Err("cold series reference mismatch".into());
        }
        series.labels = engine
            .state
            .series
            .get(reference)
            .ok_or("cold series metadata is missing")?
            .labels
            .clone();
        visit(series);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample(t: i64) -> Sample {
        Sample {
            t,
            kind: 1,
            bits: (t as f64).to_bits(),
            data: String::new(),
            ..Default::default()
        }
    }
    #[test]
    fn wal_preserves_exact_sample_order_and_rejects_unknown_markers() {
        let mut late = sample(2);
        late.out_of_order = true;
        let commit = Commit {
            seq: 1,
            next_ref: 1,
            max_exemplars: 0,
            series: BTreeMap::from([(
                1,
                Series {
                    reference: 1,
                    labels: Arc::new(vec![Label {
                        name: "name".into(),
                        value: "value".into(),
                    }]),
                    samples: BTreeMap::from([(1, sample(1)), (2, late)]),
                    ..Default::default()
                },
            )]),
        };
        let record = wal_record(&commit, &State::default());
        let mut restored = State::default();
        assert_eq!(replay(&record, &mut restored).unwrap(), record.len());
        assert_eq!(restored.series[&1].ordered_bounds(false), Some((1, 1)));
        assert_eq!(restored.series[&1].ordered_bounds(true), Some((2, 2)));
        assert_eq!(restored.series[&1].last.as_ref().unwrap().t, 1);

        let mut body = &record[8..];
        u32::get(&mut body).unwrap();
        usize::get(&mut body).unwrap();
        u64::get(&mut body).unwrap();
        u64::get(&mut body).unwrap();
        Vec::<Series>::get(&mut body).unwrap();
        let trailer = record.len() - body.len();
        for (offset, expected) in [
            (trailer + 4, "WAL marks unknown series out of order"),
            (trailer + 16, "WAL marks unknown samples out of order"),
        ] {
            let mut invalid = record.clone();
            invalid[offset..offset + 8].copy_from_slice(&999u64.to_le_bytes());
            assert_eq!(
                replay(&invalid, &mut State::default()).unwrap_err(),
                expected
            );
        }

        let mut previous = record[..trailer].to_vec();
        Vec::from([1u64]).put(&mut previous);
        let len = (previous.len() - 8) as u64;
        previous[..8].copy_from_slice(&len.to_le_bytes());
        previous[8..12].copy_from_slice(&PREVIOUS_WAL_MAGIC.to_le_bytes());
        let mut restored = State::default();
        assert_eq!(replay(&previous, &mut restored).unwrap(), previous.len());
        assert_eq!(restored.series[&1].samples_in(0, 3).len(), 2);
        assert_eq!(restored.series[&1].ordered_bounds(true), Some((1, 2)));
    }
    #[test]
    fn wal_replays_references_and_discards_only_an_incomplete_tail() {
        let mut state = State::default();
        let commit = Commit {
            max_exemplars: 10,
            seq: 1,
            next_ref: 1,
            series: BTreeMap::from([(
                1,
                Series {
                    reference: 1,
                    labels: Arc::new(vec![Label {
                        name: "name".into(),
                        value: "value".into(),
                    }]),
                    samples: BTreeMap::from([(1, sample(1))]),
                    ..Default::default()
                },
            )]),
        };
        let first = wal_record(&commit, &state);
        state.apply(commit);
        let second = Commit {
            max_exemplars: 10,
            seq: 2,
            next_ref: 1,
            series: BTreeMap::from([(
                1,
                Series {
                    reference: 1,
                    labels: state.series[&1].labels.clone(),
                    samples: BTreeMap::from([(2, sample(2))]),
                    ..Default::default()
                },
            )]),
        };
        let bytes = wal_record(&second, &state);
        for end in 0..bytes.len() {
            let mut restored = State::default();
            let joined = [first.as_slice(), &bytes[..end]].concat();
            assert_eq!(replay(&joined, &mut restored).unwrap(), first.len());
            assert_eq!(restored.series[&1].samples_in(0, 10).len(), 1);
        }
        let mut restored = State::default();
        let joined = [first.as_slice(), bytes.as_slice()].concat();
        assert_eq!(replay(&joined, &mut restored).unwrap(), joined.len());
        assert_eq!(restored.series[&1].labels, state.series[&1].labels);
        assert_eq!(restored.series[&1].samples_in(0, 10).len(), 2);
        let mut invalid = first.clone();
        invalid[8] ^= 1;
        assert!(replay(&invalid, &mut State::default()).is_err());
        assert!(replay(&bytes, &mut State::default()).is_err());
    }
    #[test]
    fn cold_raw_ranges_skip_payloads_but_still_validate_them() {
        let samples = BTreeMap::from([
            (
                10,
                Sample {
                    t: 10,
                    kind: 2,
                    data: "histogram é".into(),
                    ..Default::default()
                },
            ),
            (20, sample(20)),
            (
                30,
                Sample {
                    t: 30,
                    kind: 3,
                    data: "float histogram".into(),
                    ..Default::default()
                },
            ),
        ]);
        let original = Series {
            reference: 7,
            samples: samples.clone(),
            ..Default::default()
        };
        // The decoder must still support cold records containing uncompressed floats.
        let mut bytes = Vec::new();
        COLD_MAGIC.put(&mut bytes);
        original.put(&mut bytes);
        0u32.put(&mut bytes);
        for (min, max) in [(i64::MIN, i64::MAX), (10, 10), (20, 30), (11, 29), (31, 40)] {
            let selected = get_series(&bytes, min, max).unwrap();
            assert_eq!(selected.reference, 7);
            assert_eq!(
                selected.samples,
                samples
                    .range(min..=max)
                    .map(|(&t, s)| (t, s.clone()))
                    .collect()
            );
            for end in 0..bytes.len() {
                assert!(
                    get_series(&bytes[..end], min, max).is_err(),
                    "end {end}, range {min}..{max}"
                );
            }
        }
        let mut invalid = bytes.clone();
        let payload = invalid
            .windows("histogram é".len())
            .position(|w| w == "histogram é".as_bytes())
            .unwrap();
        invalid[payload] = 0xff;
        assert!(
            get_series(&invalid, 20, 30)
                .err()
                .unwrap()
                .contains("invalid utf-8")
        );
        let mut invalid = bytes;
        invalid[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            get_series(&invalid, 31, 40).err().unwrap(),
            "invalid cold sample count"
        );
    }
    #[test]
    fn cold_files_compress_short_float_tails_and_preserve_mixed_samples() {
        let mut original = Series {
            reference: 7,
            ..Default::default()
        };
        for t in 300..450 {
            original.insert(sample(t));
        }
        for t in 0..239 {
            let mut sample = sample(t);
            sample.out_of_order = true;
            if t % 17 == 0 {
                sample.bits = 0x7ff0000000000002;
            }
            original.samples.insert(t, sample);
        }
        original.samples.insert(
            250,
            Sample {
                t: 250,
                kind: 2,
                data: "histogram".into(),
                ..Default::default()
            },
        );
        let expected = original.samples_in(i64::MIN, i64::MAX);
        let (bytes, _) = cold_file(&[original]);
        let restored = get_series(&bytes, i64::MIN, i64::MAX).unwrap();
        assert_eq!(restored.samples.len(), 1);
        assert!(restored.samples.values().all(|sample| sample.kind != 1));
        assert_eq!(restored.float_chunks.len(), 4);
        assert_eq!(restored.samples_in(i64::MIN, i64::MAX), expected);
        for (min, max) in [(0, 0), (100, 150), (239, 300), (451, 500)] {
            let filtered = get_series(&bytes, min, max).unwrap();
            assert_eq!(
                filtered.samples_in(min, max),
                expected
                    .range(min..=max)
                    .map(|(&t, s)| (t, s.clone()))
                    .collect()
            );
        }
    }
    #[test]
    fn cold_encoding_preserves_compressed_chunks_without_decoding() {
        let mut original = Series {
            reference: 7,
            labels: Arc::new(vec![Label {
                name: "name".into(),
                value: "metric".into(),
            }]),
            ..Default::default()
        };
        for t in 0..400 {
            original.insert(sample(t));
        }
        let (bytes, locations) = cold_file(&[original.clone()]);
        assert_eq!(locations[&7], (0, bytes.len() as u64));
        let restored = get_series(&bytes, i64::MIN, i64::MAX).unwrap();
        assert_eq!(restored.samples_in(0, 400), original.samples_in(0, 400));
        assert!(restored.labels.is_empty());
        let filtered = get_series(&bytes, 120, 239).unwrap();
        assert_eq!(filtered.float_chunks.len(), 1);
        assert_eq!(filtered.samples_in(0, 400), original.samples_in(120, 239));
        for end in 0..bytes.len() {
            assert!(
                get_series(&bytes[..end], i64::MIN, i64::MAX).is_err(),
                "{end}"
            );
        }
    }
}

#[cfg(test)]
mod buffered_tests {
    use super::*;

    #[test]
    fn mapping_cache_eviction_keeps_chunk_views_alive() {
        let dir = Directory::new();
        let mut engine = Engine::open(&dir.0).unwrap();
        append(&mut engine, 0, 0);
        engine
            .call(Request {
                op: "flush".into(),
                up_to: 0,
                ..Default::default()
            })
            .unwrap();
        let mut block = engine.state.cold[0].clone();
        let original = block.file.clone();
        let selected = BTreeSet::from([1]);
        let first = read_cold(&engine, &block, &selected, 0, 0).unwrap();
        let same = read_cold(&engine, &block, &selected, 0, 0).unwrap();
        assert!(Arc::ptr_eq(
            first[0].float_chunks[0].data.owner.as_ref().unwrap(),
            same[0].float_chunks[0].data.owner.as_ref().unwrap()
        ));
        let bytes = fs::read(dir.0.join(&original)).unwrap();
        for n in 0..MAX_COLD_MAPPINGS {
            block.file = format!("extra-{n}.bin");
            fs::write(dir.0.join(&block.file), &bytes).unwrap();
            read_cold(&engine, &block, &selected, 0, 0).unwrap();
        }
        assert_eq!(engine.cold_mappings.borrow().len(), MAX_COLD_MAPPINGS);
        block.file = original;
        let reloaded = read_cold(&engine, &block, &selected, 0, 0).unwrap();
        assert!(!Arc::ptr_eq(
            first[0].float_chunks[0].data.owner.as_ref().unwrap(),
            reloaded[0].float_chunks[0].data.owner.as_ref().unwrap()
        ));
        assert_eq!(engine.cold_mappings.borrow().len(), MAX_COLD_MAPPINGS);
        fs::remove_file(dir.0.join(&block.file)).unwrap();
        drop(engine);
        assert_eq!(first[0].samples_in(0, 0), reloaded[0].samples_in(0, 0));
        assert_eq!(first[0].samples_in(0, 0).len(), 1);
    }

    #[test]
    fn mapped_chunks_survive_retention_cursor_close_and_engine_drop() {
        let dir = Directory::new();
        let mut engine = Engine::open(&dir.0).unwrap();
        for t in 0..400 {
            append(&mut engine, if t == 0 { 0 } else { 1 }, t);
        }
        engine
            .call(Request {
                op: "flush".into(),
                up_to: 250,
                ..Default::default()
            })
            .unwrap();
        let block = engine.state.cold[0].clone();
        let cold = read_cold(&engine, &block, &BTreeSet::from([1]), 0, 399).unwrap();
        assert!(matches!(
            cold[0].float_chunks[0].data.owner.as_deref(),
            Some(ChunkOwner::Mapped(_))
        ));
        let restored: Series =
            serde_json::from_slice(&serde_json::to_vec(&cold[0]).unwrap()).unwrap();
        assert_eq!(restored.samples_in(0, 399), cold[0].samples_in(0, 399));
        let opened = engine
            .call(Request {
                op: "cursor_open_all".into(),
                mint: 0,
                maxt: 399,
                unordered: true,
                ..Default::default()
            })
            .unwrap();
        let file = dir.0.join(&block.file);
        drop(block);
        engine
            .call(Request {
                op: "retain".into(),
                up_to: i64::MAX,
                ..Default::default()
            })
            .unwrap();
        assert!(!file.exists());
        let first = engine
            .call(Request {
                op: "cursor_series_page".into(),
                cursor: opened.cursor,
                limit: 1,
                ..Default::default()
            })
            .unwrap();
        let rest = engine
            .call(Request {
                op: "cursor_chunks".into(),
                cursor: opened.cursor,
                up_to: 1,
                limit: 16,
                ..Default::default()
            })
            .unwrap();
        engine
            .call(Request {
                op: "cursor_close".into(),
                cursor: opened.cursor,
                ..Default::default()
            })
            .unwrap();
        drop(engine);
        let samples: BTreeMap<_, _> = first
            .chunks
            .iter()
            .chain(&rest.chunks)
            .flat_map(|chunk| xor::decode(&chunk.data))
            .collect();
        assert_eq!(samples, (0..400).map(|t| (t, t as f64)).collect());
        assert_eq!(cold[0].samples_in(0, 399).len(), 251);
    }

    #[test]
    fn cold_mapping_checks_file_identity_and_retries_open_failures() {
        let dir = Directory::new();
        let mut engine = Engine::open(&dir.0).unwrap();
        append(&mut engine, 0, 0);
        engine
            .call(Request {
                op: "flush".into(),
                up_to: 0,
                ..Default::default()
            })
            .unwrap();
        let block = engine.state.cold[0].clone();
        let selected = BTreeSet::from([1]);
        read_cold(&engine, &block, &selected, 0, 0).unwrap();
        let path = dir.0.join(&block.file);
        let saved = dir.0.join("saved.bin");
        fs::rename(&path, &saved).unwrap();
        fs::copy(&saved, &path).unwrap();
        assert_eq!(
            read_cold(&engine, &block, &selected, 0, 0).err().unwrap(),
            "cold file changed after mapping"
        );
        fs::remove_file(&path).unwrap();
        assert!(read_cold(&engine, &block, &selected, 0, 0).is_err());
        engine.cold_mappings.borrow_mut().clear();
        assert!(read_cold(&engine, &block, &selected, 0, 0).is_err());
        fs::write(&path, []).unwrap();
        assert_eq!(
            read_cold(&engine, &block, &selected, 0, 0).err().unwrap(),
            "truncated cold series"
        );
        fs::remove_file(&path).unwrap();
        fs::rename(&saved, &path).unwrap();
        assert_eq!(
            read_cold(&engine, &block, &selected, 0, 0).unwrap()[0]
                .samples_in(0, 0)
                .len(),
            1
        );
        assert_eq!(
            read_cold(&engine, &block, &selected, 0, 0).unwrap()[0]
                .samples_in(0, 0)
                .len(),
            1
        );
    }

    #[test]
    fn cold_reads_preserve_sparse_and_reversed_records() {
        let dir = Directory::new();
        let mut engine = Engine::open(&dir.0).unwrap();
        let mut series: Vec<_> = (1..=50)
            .map(|reference| Series {
                reference,
                labels: Arc::new(vec![Label {
                    name: "id".into(),
                    value: reference.to_string().into(),
                }]),
                samples: BTreeMap::from([(
                    100,
                    Sample {
                        t: 100,
                        kind: 2,
                        data: "x".repeat(if reference == 25 { 2 * 1024 * 1024 } else { 64 }),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            })
            .collect();
        engine.state.series = series
            .iter()
            .cloned()
            .map(|s| (s.reference, Arc::new(s)))
            .collect();
        for reverse in [false, true] {
            if reverse {
                series.reverse();
            }
            let (bytes, locations) = cold_file(&series);
            let file = if reverse {
                "cold-reversed.bin"
            } else {
                "cold-forward.bin"
            };
            fs::write(dir.0.join(file), &bytes).unwrap();
            let mut block = Cold {
                created_at: 0,
                file: file.into(),
                mint: 100,
                maxt: 100,
                refs: (1..=50).collect::<BTreeSet<_>>().into(),
                ranges: (1..=50)
                    .map(|r| (r, (100, 100)))
                    .collect::<BTreeMap<_, _>>()
                    .into(),
                locations: locations.into(),
                label_blocks: Vec::new().into(),
                evicted: false,
            };
            for selected in [
                (1..=50).collect::<BTreeSet<_>>(),
                (2..=50).step_by(2).collect(),
                BTreeSet::from([24, 25, 26]),
                BTreeSet::from([1, 50, 999]),
            ] {
                let got = read_cold(&engine, &block, &selected, 100, 100).unwrap();
                let refs: Vec<_> = got.iter().map(|s| s.reference).collect();
                assert_eq!(
                    refs,
                    selected
                        .intersection(&block.refs)
                        .copied()
                        .collect::<Vec<_>>()
                );
                for row in got {
                    let expected = &engine.state.series[&row.reference];
                    assert_eq!(row.labels, expected.labels);
                    assert_eq!(row.samples_in(100, 100), expected.samples_in(100, 100));
                }
            }
            let valid = block.locations.clone();
            Arc::make_mut(&mut block.locations).insert(2, (u64::MAX, 1));
            assert_eq!(
                read_cold(&engine, &block, &BTreeSet::from([1, 2]), 100, 100)
                    .err()
                    .unwrap(),
                "truncated cold series"
            );
            block.locations = valid.clone();
            Arc::make_mut(&mut block.locations).insert(2, (0, u64::MAX));
            assert_eq!(
                read_cold(&engine, &block, &BTreeSet::from([1, 2]), 100, 100)
                    .err()
                    .unwrap(),
                "truncated cold series"
            );
            block.locations = valid;
            let location = block.locations[&1];
            Arc::make_mut(&mut block.locations).insert(2, location);
            assert_eq!(
                read_cold(&engine, &block, &BTreeSet::from([1, 2]), 100, 100)
                    .err()
                    .unwrap(),
                "cold series reference mismatch"
            );
        }
    }

    #[test]
    fn metadata_dictionary_and_series_scans_match_the_visible_series() {
        let dir = Directory::new();
        let mut engine = Engine::open(&dir.0).unwrap();
        for reference in 1..=128 {
            let series = Series {
                reference,
                labels: Arc::new(vec![
                    Label {
                        name: "__name__".into(),
                        value: "x".into(),
                    },
                    Label {
                        name: "group".into(),
                        value: (reference % 7).to_string().into(),
                    },
                    Label {
                        name: "id".into(),
                        value: reference.to_string().into(),
                    },
                ]),
                ..Default::default()
            };
            engine.index.insert(&series);
            if reference % 3 != 0 {
                engine.head_index.insert(&series);
            }
            engine.state.series.insert(reference, series.into());
        }
        for include_cold in [false, true] {
            for selected in [
                None,
                Some(Vec::new()),
                Some(vec![10000]),
                Some(vec![1]),
                Some((20..50).collect()),
                Some((40..111).collect()),
                Some((1..=128).collect()),
                Some((1..128).chain([10000]).collect()),
            ] {
                let mut counts = BTreeMap::<String, BTreeMap<String, u64>>::new();
                for (&reference, series) in &engine.state.series {
                    if selected
                        .as_ref()
                        .is_some_and(|refs| !refs.contains(&reference))
                        || (!include_cold && !engine.head_index.all.contains(&reference))
                    {
                        continue;
                    }
                    for label in series.labels.iter() {
                        *counts
                            .entry(label.name.to_string())
                            .or_default()
                            .entry(label.value.to_string())
                            .or_default() += 1;
                    }
                }
                for limit in [0, 1, 1000] {
                    for op in [
                        "metadata_names",
                        "metadata_values",
                        "metadata_all",
                        "metadata_counts",
                    ] {
                        for name in ["group", "id", "missing"] {
                            let response = engine
                                .call(Request {
                                    op: op.into(),
                                    include_cold,
                                    filter: selected.is_some(),
                                    refs: selected.clone().unwrap_or_default(),
                                    name: name.into(),
                                    values: vec!["group".into(), "id".into(), "missing".into()],
                                    limit,
                                    ..Default::default()
                                })
                                .unwrap();
                            let limit = if limit == 0 { usize::MAX } else { limit };
                            match op {
                                "metadata_names" => assert_eq!(
                                    response.values,
                                    counts.keys().take(limit).cloned().collect::<Vec<_>>()
                                ),
                                "metadata_values" => assert_eq!(
                                    response.values,
                                    counts
                                        .get(name)
                                        .map(|values| values
                                            .keys()
                                            .take(limit)
                                            .cloned()
                                            .collect::<Vec<_>>())
                                        .unwrap_or_default()
                                ),
                                "metadata_all" => assert_eq!(response.label_counts, counts),
                                "metadata_counts" => assert_eq!(
                                    response.label_counts,
                                    counts
                                        .iter()
                                        .filter(|(name, _)| name.as_str() != "__name__")
                                        .map(|(name, values)| (name.clone(), values.clone()))
                                        .collect()
                                ),
                                _ => unreachable!(),
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn cold_reads_skip_series_outside_the_requested_time_range() {
        let dir = Directory::new();
        let mut engine = Engine::open(&dir.0).unwrap();
        let series: Vec<_> = [(1, 10), (2, 100)]
            .into_iter()
            .map(|(reference, t)| Series {
                reference,
                labels: Arc::new(vec![Label {
                    name: "__name__".into(),
                    value: reference.to_string().into(),
                }]),
                samples: BTreeMap::from([(
                    t,
                    Sample {
                        t,
                        kind: 1,
                        bits: 1.0_f64.to_bits(),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            })
            .collect();
        engine.state.series = series
            .iter()
            .cloned()
            .map(|s| (s.reference, Arc::new(s)))
            .collect();
        let (bytes, locations) = cold_file(&series);
        fs::write(dir.0.join("cold.bin"), bytes).unwrap();
        let mut block = Cold {
            created_at: 0,
            file: "cold.bin".into(),
            mint: 10,
            maxt: 100,
            refs: BTreeSet::from([1, 2]).into(),
            ranges: BTreeMap::from([(1, (10, 10)), (2, (100, 100))]).into(),
            locations: locations.into(),
            label_blocks: Vec::new().into(),
            evicted: false,
        };
        let selected = BTreeSet::from([1, 2, 3]);
        Arc::make_mut(&mut block.locations).insert(1, (u64::MAX, 1));
        let result = read_cold(&engine, &block, &selected, 100, 100).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].reference, 2);
        assert_eq!(
            read_cold(&engine, &block, &selected, 10, 10).err().unwrap(),
            "truncated cold series"
        );
        fs::remove_file(dir.0.join("cold.bin")).unwrap();
        assert!(
            read_cold(&engine, &block, &selected, 101, 200)
                .unwrap()
                .is_empty()
        );

        block.file = "legacy.json".into();
        Arc::make_mut(&mut block.locations).clear();
        fs::write(
            dir.0.join(&block.file),
            serde_json::to_vec(&series).unwrap(),
        )
        .unwrap();
        let result = read_cold(&engine, &block, &selected, 10, 10).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].reference, 1);
    }

    #[test]
    fn kafka_recovery_requires_replay_after_crash_and_restores_clean_shutdown() {
        let dir = Directory::new();
        let mut engine = Engine::open(&dir.0).unwrap();
        append(&mut engine, 0, 1);
        drop(engine);
        let mut engine = Engine::open_mode(&dir.0, true).unwrap();
        assert!(engine.restored_clean);
        assert_eq!(engine.state.series[&1].last.as_ref().unwrap().t, 1);
        assert!(engine.wal.is_none());
        append(&mut engine, 1, 2);
        assert!(engine.prepare_sync().unwrap().is_none());
        assert_eq!(fs::metadata(dir.0.join("wal.bin")).unwrap().len(), 0);
        engine
            .call(Request {
                op: "flush".into(),
                up_to: 1,
                ..Default::default()
            })
            .unwrap();
        engine
            .call(Request {
                op: "close_checkpoint".into(),
                ..Default::default()
            })
            .unwrap();
        assert!(
            engine
                .call(Request {
                    op: "begin".into(),
                    ..Default::default()
                })
                .is_err()
        );
        drop(engine);
        assert!(Engine::open(&dir.0).is_err());
        let mut engine = Engine::open_mode(&dir.0, true).unwrap();
        assert!(engine.restored_clean);
        append(&mut engine, 1, 3);
        drop(engine);
        let mut engine = Engine::open_mode(&dir.0, true).unwrap();
        assert!(!engine.restored_clean);
        assert_eq!(engine.state.series[&1].last.as_ref().unwrap().t, 2);
        append(&mut engine, 1, 3);
        let read = engine
            .call(Request {
                op: "read".into(),
                mint: 0,
                maxt: 3,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(read.series[0].samples.len(), 3);
    }

    #[test]
    fn failed_kafka_checkpoint_never_marks_the_head_clean() {
        let dir = Directory::new();
        let mut engine = Engine::open_mode(&dir.0, true).unwrap();
        append(&mut engine, 0, 1);
        fs::create_dir(dir.0.join("head.json.tmp")).unwrap();
        assert!(
            engine
                .call(Request {
                    op: "close_checkpoint".into(),
                    ..Default::default()
                })
                .is_err()
        );
        assert!(!dir.0.join("kafka.clean").exists());
        drop(engine);
        assert!(!Engine::open_mode(&dir.0, true).unwrap().restored_clean);
    }
    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            // The clock can return the same timestamp to parallel test threads.
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            Self(std::env::temp_dir().join(format!(
                    "mimir-wal-test-{}-{}-{}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos(),
                    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                )))
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            if self.0.exists() {
                fs::remove_dir_all(&self.0).unwrap();
            }
        }
    }
    fn append(e: &mut Engine, r: u64, t: i64) {
        e.call(Request {
            op: "begin".into(),
            ..Default::default()
        })
        .unwrap();
        e.call(Request {
            op: "append".into(),
            reference: r,
            labels: if r == 0 {
                vec![Label {
                    name: "name".into(),
                    value: "value".into(),
                }]
            } else {
                Vec::new()
            },
            sample: Some(Sample {
                t,
                kind: 1,
                bits: (t as f64).to_bits(),
                data: String::new(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
        e.call(Request {
            op: "commit".into(),
            ..Default::default()
        })
        .unwrap();
    }
    #[test]
    fn legacy_wal_records_still_replay() {
        let dir = Directory::new();
        let mut engine = Engine::open(&dir.0).unwrap();
        append(&mut engine, 0, 1);
        drop(engine);
        let mut record = fs::read(dir.0.join("wal.bin")).unwrap();
        assert_eq!(&record[record.len() - 4..], &[0; 4]);
        record.truncate(record.len() - 4);
        let len = (record.len() - 8) as u64;
        record[..8].copy_from_slice(&len.to_le_bytes());
        record[8..12].copy_from_slice(&LEGACY_WAL_MAGIC.to_le_bytes());
        let mut state = State::default();
        assert_eq!(replay(&record, &mut state).unwrap(), record.len());
        assert_eq!(state.series[&1].last.as_ref().unwrap().t, 1);
    }

    #[test]
    fn detached_sync_preserves_later_dirty_records_and_checkpoint() {
        let dir = Directory::new();
        let mut engine = Engine::open(&dir.0).unwrap();
        engine.buffered_wal = true;
        append(&mut engine, 0, 1);
        let first = engine.prepare_sync().unwrap().unwrap();
        assert!(!engine.wal_dirty);
        append(&mut engine, 1, 2);
        first.sync_all().unwrap();
        assert!(engine.wal_dirty);
        assert!(!engine.wal.as_ref().unwrap().buffer().is_empty());
        let second = engine.prepare_sync().unwrap().unwrap();
        engine
            .call(Request {
                op: "flush".into(),
                up_to: 2,
                ..Default::default()
            })
            .unwrap();
        append(&mut engine, 1, 3);
        second.sync_all().unwrap();
        assert!(engine.wal_dirty);
        engine.prepare_sync().unwrap().unwrap().sync_all().unwrap();
        assert!(engine.prepare_sync().unwrap().is_none());
        drop(engine);
        let mut restored = Engine::open(&dir.0).unwrap();
        let read = restored
            .call(Request {
                op: "read".into(),
                mint: 0,
                maxt: 3,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(read.series[0].samples.len(), 3);
    }

    #[test]
    fn failed_detached_sync_discards_later_buffered_records() {
        let dir = Directory::new();
        let mut engine = Engine::open(&dir.0).unwrap();
        engine.buffered_wal = true;
        append(&mut engine, 0, 1);
        let first = engine.prepare_sync().unwrap().unwrap();
        append(&mut engine, 1, 2);
        // A failed durability barrier must prevent publishing subsequent data.
        engine.poison_wal();
        drop(first);
        assert!(engine.prepare_sync().is_err());
        assert!(engine.wal.as_ref().unwrap().buffer().is_empty());
        drop(engine);
        let restored = Engine::open(&dir.0).unwrap();
        assert_eq!(restored.state.series[&1].last.as_ref().unwrap().t, 1);
    }

    #[test]
    fn sync_and_checkpoint_cover_buffered_records() {
        let dir = Directory::new();
        let mut engine = Engine::open(&dir.0).unwrap();
        engine
            .call(Request {
                op: "enable_buffered_wal".into(),
                ..Default::default()
            })
            .unwrap();
        append(&mut engine, 0, 1);
        assert_eq!(
            engine
                .wal
                .as_ref()
                .unwrap()
                .get_ref()
                .metadata()
                .unwrap()
                .len(),
            0
        );
        assert!(!engine.wal.as_ref().unwrap().buffer().is_empty());
        engine
            .call(Request {
                op: "sync".into(),
                ..Default::default()
            })
            .unwrap();
        assert!(engine.wal.as_ref().unwrap().buffer().is_empty());
        assert!(!engine.wal_dirty);
        append(&mut engine, 1, 2);
        engine
            .call(Request {
                op: "flush".into(),
                up_to: 2,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            engine
                .wal
                .as_ref()
                .unwrap()
                .get_ref()
                .metadata()
                .unwrap()
                .len(),
            0
        );
        assert!(engine.wal.as_ref().unwrap().buffer().is_empty());
        append(&mut engine, 1, 3);
        engine
            .call(Request {
                op: "sync".into(),
                ..Default::default()
            })
            .unwrap();
        drop(engine);
        let mut restored = Engine::open(&dir.0).unwrap();
        let read = restored
            .call(Request {
                op: "read".into(),
                mint: 0,
                maxt: 3,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(read.series.len(), 1);
        assert_eq!(read.series[0].samples.len(), 3);
    }
    #[test]
    fn failed_buffer_flush_poisoning_discards_unpublished_bytes() {
        let dir = Directory::new();
        let mut engine = Engine::open(&dir.0).unwrap();
        engine
            .call(Request {
                op: "enable_buffered_wal".into(),
                ..Default::default()
            })
            .unwrap();
        append(&mut engine, 0, 1);
        *engine.wal.as_mut().unwrap().get_mut() = File::open(dir.0.join("wal.bin")).unwrap();
        assert!(
            engine
                .call(Request {
                    op: "sync".into(),
                    ..Default::default()
                })
                .is_err()
        );
        assert!(engine.poisoned);
        assert!(engine.wal.as_ref().unwrap().buffer().is_empty());
        assert_eq!(
            engine
                .call(Request {
                    op: "stats".into(),
                    ..Default::default()
                })
                .err()
                .unwrap(),
            "engine requires reopen after WAL failure"
        );
        drop(engine);
        let restored = Engine::open(&dir.0).unwrap();
        assert!(restored.state.series.is_empty());
    }
    #[test]
    fn direct_commits_flush_without_waiting_for_sync() {
        let dir = Directory::new();
        let mut engine = Engine::open(&dir.0).unwrap();
        append(&mut engine, 0, 1);
        assert!(
            engine
                .wal
                .as_ref()
                .unwrap()
                .get_ref()
                .metadata()
                .unwrap()
                .len()
                > 0
        );
        assert!(engine.wal.as_ref().unwrap().buffer().is_empty());
    }
}
