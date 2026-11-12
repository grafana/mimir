// SPDX-License-Identifier: AGPL-3.0-only
use super::wire::Wire;
use super::*;
use std::io::{Read, Seek, SeekFrom};

const WAL_MAGIC: u32 = 0x314c4157;
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
        if u32::get(&mut body)? != WAL_MAGIC {
            return Err("invalid WAL version".into());
        }
        let max_exemplars = usize::get(&mut body)?;
        let seq = u64::get(&mut body)?;
        let next_ref = u64::get(&mut body)?;
        let series: Vec<Series> = Vec::get(&mut body)?;
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
    s.samples.put(bytes);
    s.exemplars.put(bytes);
    ((s.float_chunks.len() + usize::from(s.open_float.is_some())) as u32).put(bytes);
    for c in &s.float_chunks {
        put_chunk(c.mint, c.maxt, &c.data, bytes)
    }
    if let Some(c) = &s.open_float {
        put_chunk(
            c.first_timestamp().unwrap(),
            c.last_timestamp().unwrap(),
            &c.clone().into_bytes(),
            bytes,
        )
    }
}
fn get_series(mut bytes: &[u8]) -> Result<Series> {
    if u32::get(&mut bytes)? != COLD_MAGIC {
        return Err("invalid cold file version".into());
    }
    let mut s = Series::get(&mut bytes)?;
    let count = u32::get(&mut bytes)? as usize;
    if count > bytes.len() / 20 {
        return Err("invalid cold chunk count".into());
    }
    for _ in 0..count {
        let mint = i64::get(&mut bytes)?;
        let maxt = i64::get(&mut bytes)?;
        let len = u32::get(&mut bytes)? as usize;
        if len > bytes.len() {
            return Err("truncated cold chunk".into());
        }
        let (data, rest) = bytes.split_at(len);
        bytes = rest;
        s.float_chunks.push(FloatChunk {
            mint,
            maxt,
            data: data.into(),
        });
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
) -> Result<Vec<Series>> {
    if block.locations.is_empty() {
        let series: Vec<Series> = serde_json::from_reader(std::io::BufReader::new(
            File::open(engine.dir.join(&block.file)).map_err(io)?,
        ))
        .map_err(io)?;
        return Ok(series
            .into_iter()
            .filter(|s| selected.contains(&s.reference))
            .collect());
    }
    let mut file = File::open(engine.dir.join(&block.file)).map_err(io)?;
    let size = file.metadata().map_err(io)?.len();
    let mut result = Vec::new();
    let mut buffer = Vec::new();
    for r in selected {
        let Some(&(offset, len)) = block.locations.get(r) else {
            continue;
        };
        if offset > size || len > size - offset {
            return Err("truncated cold series".into());
        }
        buffer.resize(usize::try_from(len).map_err(io)?, 0);
        file.seek(SeekFrom::Start(offset)).map_err(io)?;
        file.read_exact(&mut buffer).map_err(io)?;
        let mut s = get_series(&buffer)?;
        s.labels = engine
            .state
            .series
            .get(r)
            .ok_or("cold series metadata is missing")?
            .labels
            .clone();
        if s.reference != *r {
            return Err("cold series reference mismatch".into());
        }
        result.push(s);
    }
    Ok(result)
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
        }
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
        let restored = get_series(&bytes).unwrap();
        assert_eq!(restored.samples_in(0, 400), original.samples_in(0, 400));
        assert!(restored.labels.is_empty());
        for end in 0..bytes.len() {
            assert!(get_series(&bytes[..end]).is_err(), "{end}");
        }
    }
}

#[cfg(test)]
mod buffered_tests {
    use super::*;
    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!(
                    "mimir-wal-test-{}-{}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
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
