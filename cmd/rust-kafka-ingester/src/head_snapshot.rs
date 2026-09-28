use std::fs::{self, File};
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::Path;

use super::*;
use crate::chunk_disk::FileState;

const MAGIC: &[u8; 8] = b"MIMIRHS3";
const FILE_NAME: &str = "snapshot";

/// The Kafka position a head snapshot covers for one cluster.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotOffset {
    pub offset: Option<i64>,
    pub timestamp_ms: i64,
}

pub struct Restored {
    pub store: Store,
    pub offsets: Vec<SnapshotOffset>,
}

impl Store {
    /// Like Prometheus's memory snapshot on shutdown: persists every series' open chunks and chunk
    /// references next to the chunk files so the next start skips segment replay.
    pub fn write_snapshot(&self, offsets: &[SnapshotOffset]) -> Result<()> {
        let states = self
            .shards
            .iter()
            .map(|shard| shard.read().expect("store lock poisoned"))
            .collect::<Vec<_>>();
        let directory = states[0]
            .disk
            .directory()
            .and_then(|shard| shard.parent())
            .context("head snapshots need a chunk directory")?
            .to_path_buf();
        for state in &states {
            state.disk.sync()?;
        }
        let temporary = directory.join(format!("{FILE_NAME}.tmp"));
        let file = File::create(&temporary)
            .with_context(|| format!("create head snapshot {}", temporary.display()))?;
        let mut writer = Checksummed::new(BufWriter::with_capacity(1 << 20, file));
        writer.inner.write_all(MAGIC)?;
        write_snapshot_body(&mut writer, &states, offsets)?;
        write_exemplars(
            &mut writer,
            &self.exemplars.lock().expect("exemplar lock poisoned"),
        )?;
        let checksum = writer.hasher.clone().finalize();
        let mut file = writer
            .inner
            .into_inner()
            .map_err(|error| error.into_error())
            .context("flush head snapshot")?;
        file.write_all(&checksum.to_le_bytes())?;
        file.sync_all()?;
        fs::rename(&temporary, directory.join(FILE_NAME))?;
        File::open(&directory)?.sync_all()?;
        Ok(())
    }

    /// Loads and removes the head snapshot in `chunk_dir`. Returns `None` when there is none or it
    /// is unusable; the caller then rebuilds from segments, which clears the directory.
    pub fn restore(
        active_window_ms: i64,
        retention_ms: Option<i64>,
        chunk_dir: &Path,
        threads: usize,
    ) -> Result<Option<Restored>> {
        let path = chunk_dir.join(FILE_NAME);
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        // Chunk files change as soon as ingestion resumes, so a snapshot is only valid once. The
        // open handle keeps it readable.
        fs::remove_file(&path)
            .with_context(|| format!("remove head snapshot {}", path.display()))?;
        File::open(chunk_dir)?.sync_all()?;
        match read_snapshot(file, active_window_ms, retention_ms, chunk_dir, threads) {
            Ok(restored) => Ok(Some(restored)),
            Err(error) => {
                eprintln!("phase=head_snapshot_invalid error={error:#}");
                Ok(None)
            }
        }
    }
}

fn write_snapshot_body(
    writer: &mut Checksummed<BufWriter<File>>,
    states: &[std::sync::RwLockReadGuard<'_, State>],
    offsets: &[SnapshotOffset],
) -> Result<()> {
    writer.put_len(offsets.len())?;
    for offset in offsets {
        writer.put_i64(offset.offset.unwrap_or(i64::MIN))?;
        writer.put_i64(offset.timestamp_ms)?;
    }
    writer.put_len(states.len())?;
    for state in states {
        write_shard(writer, state)?;
    }
    Ok(())
}

fn write_shard(writer: &mut Checksummed<BufWriter<File>>, state: &State) -> Result<()> {
    let (files, next_sequence) = state.disk.state();
    writer.put_u32(next_sequence)?;
    writer.put_len(files.len())?;
    for file in files {
        writer.put_u32(file.sequence)?;
        writer.put_u64(file.written)?;
        writer.put_i64(file.max_time)?;
    }
    writer.put_len(state.tenants.len())?;
    for (tenant_id, tenant) in &state.tenants {
        writer.put_bytes(tenant_id.as_bytes())?;
        writer.put_i64(tenant.max_time)?;
        writer.put_len(tenant.metadata.values().map(BTreeMap::len).sum())?;
        for (metadata, seen) in tenant.metadata.values().flat_map(BTreeMap::values) {
            writer.put_bytes(&metadata.encode_to_vec())?;
            writer.put_i64(*seen)?;
        }
        writer.put_len(tenant.series.len())?;
        for ((_, labels), series) in tenant.series.iter() {
            writer.put_len(labels.len())?;
            for (name, value) in labels.iter() {
                writer.put_bytes(name.as_bytes())?;
                writer.put_bytes(value.as_bytes())?;
            }
            writer.put_i64(series.last_ingested_ms)?;
            writer.put_u32(series.last_bucket_count)?;
            writer.put_len(series.chunks.len())?;
            for chunk in &series.chunks {
                writer.put_u64(chunk.reference)?;
                writer.put_i64(chunk.min_time)?;
                writer.put_i64(chunk.max_time)?;
                writer.put_u32(chunk.len)?;
                writer.write_all(&[chunk.encoding])?;
            }
            match &series.float_head {
                Some(head) => {
                    writer.write_all(&[1])?;
                    writer.put_i64(head.min_time)?;
                    writer.put_i64(head.next_at)?;
                    head.appender.write_state(writer)?;
                }
                None => writer.write_all(&[0])?,
            }
            writer.put_len(series.histogram_head.len())?;
            for histogram in &series.histogram_head {
                writer.put_bytes(&histogram.encode_to_vec())?;
            }
            writer.put_i64(series.histogram_next_at)?;
            writer.put_len(series.out_of_order.len())?;
            for (timestamp, value) in &series.out_of_order {
                writer.put_i64(*timestamp)?;
                writer.put_u64(value.to_bits())?;
            }
        }
    }
    Ok(())
}

// Exemplars in insertion order, so a restore evicts in the same order.
fn write_exemplars(
    writer: &mut Checksummed<BufWriter<File>>,
    exemplars: &HashMap<String, TenantExemplars<Arc<StoredLabels>>>,
) -> Result<()> {
    writer.put_len(exemplars.len())?;
    for (tenant_id, storage) in exemplars {
        writer.put_bytes(tenant_id.as_bytes())?;
        writer.put_u64(storage.capacity() as u64)?;
        let entries = storage.in_insertion_order();
        writer.put_len(entries.len())?;
        for (series_id, labels, exemplar) in entries {
            writer.put_u64(series_id)?;
            writer.put_len(labels.len())?;
            for (name, value) in labels.iter() {
                writer.put_bytes(name.as_bytes())?;
                writer.put_bytes(value.as_bytes())?;
            }
            writer.put_bytes(&exemplar.encode_to_vec())?;
        }
    }
    Ok(())
}

fn read_exemplars(
    reader: &mut Checksummed<BufReader<File>>,
) -> Result<HashMap<String, TenantExemplars<Arc<StoredLabels>>>> {
    let mut exemplars = HashMap::new();
    for _ in 0..reader.count(1_000_000)? {
        let tenant_id = reader.string()?;
        let mut storage = TenantExemplars::new(usize::try_from(reader.u64()?)?);
        let mut labels_by_series: HashMap<u64, Arc<StoredLabels>> = HashMap::new();
        for _ in 0..reader.count(100_000_000)? {
            let series_id = reader.u64()?;
            let labels = (0..reader.count(10_000)?)
                .map(|_| {
                    let name: Arc<str> = reader.string()?.into();
                    Ok((name, CompactString::from(reader.string()?)))
                })
                .collect::<Result<StoredLabels>>()?;
            let labels = Arc::clone(
                labels_by_series
                    .entry(series_id)
                    .or_insert_with(|| Arc::new(labels)),
            );
            let exemplar = cortexpb::Exemplar::decode(reader.read_bytes()?.as_slice())?;
            // Stored exemplars were valid when added; a window this wide re-adds them all.
            let _ = storage.add(series_id, || labels, exemplar, i64::MAX);
        }
        exemplars.insert(tenant_id, storage);
    }
    Ok(exemplars)
}

fn read_snapshot(
    file: File,
    active_window_ms: i64,
    retention_ms: Option<i64>,
    chunk_dir: &Path,
    threads: usize,
) -> Result<Restored> {
    let mut reader = Checksummed::new(BufReader::with_capacity(1 << 20, file));
    let mut magic = [0; 8];
    reader.inner.read_exact(&mut magic)?;
    if &magic != MAGIC {
        bail!("head snapshot has invalid magic");
    }
    let offsets = (0..reader.count(1_000)?)
        .map(|_| {
            let offset = reader.i64()?;
            Ok(SnapshotOffset {
                offset: (offset != i64::MIN).then_some(offset),
                timestamp_ms: reader.i64()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let shards = (0..reader.count(4096)?)
        .map(|_| read_shard(&mut reader))
        .collect::<Result<Vec<_>>>()?;
    let exemplars = read_exemplars(&mut reader)?;
    let expected = reader.hasher.clone().finalize();
    let mut checksum = [0; 4];
    reader.inner.read_exact(&mut checksum)?;
    if u32::from_le_bytes(checksum) != expected {
        bail!("head snapshot checksum mismatch");
    }
    if reader.inner.read(&mut [0])? != 0 {
        bail!("trailing bytes in head snapshot");
    }
    let shards = shards
        .into_iter()
        .enumerate()
        .map(|(index, (files, next_sequence, tenants))| {
            let disk = ChunkDiskMapper::reopen(shard_dir(chunk_dir, index), &files, next_sequence)?;
            Ok(RwLock::new(State { tenants, disk }))
        })
        .collect::<Result<Vec<_>>>()?;
    let store = Store::from_shards(shards, threads, active_window_ms, retention_ms)?;
    *store.exemplars.lock().expect("exemplar lock poisoned") = exemplars;
    Ok(Restored { store, offsets })
}

type ShardImage = (Vec<FileState>, u32, HashMap<String, Tenant>);

fn read_shard(reader: &mut Checksummed<BufReader<File>>) -> Result<ShardImage> {
    let next_sequence = reader.u32()?;
    let files = (0..reader.count(1_000_000)?)
        .map(|_| {
            Ok(FileState {
                sequence: reader.u32()?,
                written: reader.u64()?,
                max_time: reader.i64()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut tenants = HashMap::new();
    for _ in 0..reader.count(1_000_000)? {
        let tenant_id = reader.string()?;
        let mut tenant = Tenant {
            max_time: reader.i64()?,
            ..Tenant::default()
        };
        for _ in 0..reader.count(10_000_000)? {
            let metadata = cortexpb::MetricMetadata::decode(reader.read_bytes()?.as_slice())?;
            let seen = reader.i64()?;
            tenant
                .metadata
                .entry(metadata.metric_family_name.clone())
                .or_default()
                .insert(
                    (
                        metadata.r#type,
                        metadata.help.clone(),
                        metadata.unit.clone(),
                    ),
                    (metadata, seen),
                );
        }
        for _ in 0..reader.count(100_000_000)? {
            let labels = (0..reader.count(10_000)?)
                .map(|_| {
                    let name = reader.string()?;
                    let name = match tenant.label_names.get(name.as_str()) {
                        Some(stored) => Arc::clone(stored),
                        None => {
                            let stored: Arc<str> = name.into();
                            tenant.label_names.insert(Arc::clone(&stored));
                            stored
                        }
                    };
                    Ok((name, CompactString::from(reader.string()?)))
                })
                .collect::<Result<StoredLabels>>()?;
            let mut series = Series {
                last_ingested_ms: reader.i64()?,
                last_bucket_count: reader.u32()?,
                ..Series::default()
            };
            series.chunks = (0..reader.count(1_000_000)?)
                .map(|_| {
                    Ok(ChunkMeta {
                        reference: reader.u64()?,
                        min_time: reader.i64()?,
                        max_time: reader.i64()?,
                        len: reader.u32()?,
                        encoding: reader.u8()?,
                    })
                })
                .collect::<Result<_>>()?;
            if reader.u8()? == 1 {
                series.float_head = Some(FloatHead {
                    min_time: reader.i64()?,
                    next_at: reader.i64()?,
                    appender: xor::Appender::read_state(reader)?,
                });
            }
            series.histogram_head = (0..reader.count(1_000_000)?)
                .map(|_| {
                    Ok(cortexpb::Histogram::decode(
                        reader.read_bytes()?.as_slice(),
                    )?)
                })
                .collect::<Result<_>>()?;
            series.histogram_next_at = reader.i64()?;
            series.out_of_order = (0..reader.count(1_000_000)?)
                .map(|_| Ok((reader.i64()?, f64::from_bits(reader.u64()?))))
                .collect::<Result<_>>()?;
            if !tenant.series.insert(series_key(labels), series) {
                bail!("duplicate series in head snapshot");
            }
        }
        if tenants.insert(tenant_id, tenant).is_some() {
            bail!("duplicate tenant in head snapshot");
        }
    }
    Ok((files, next_sequence, tenants))
}

struct Checksummed<T> {
    inner: T,
    hasher: crc32fast::Hasher,
}

impl<T> Checksummed<T> {
    fn new(inner: T) -> Self {
        Self {
            inner,
            hasher: crc32fast::Hasher::new(),
        }
    }
}

impl<T: Write> Write for Checksummed<T> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<T: Write> Checksummed<T> {
    fn put_u32(&mut self, value: u32) -> Result<()> {
        Ok(self.write_all(&value.to_le_bytes())?)
    }

    fn put_u64(&mut self, value: u64) -> Result<()> {
        Ok(self.write_all(&value.to_le_bytes())?)
    }

    fn put_i64(&mut self, value: i64) -> Result<()> {
        Ok(self.write_all(&value.to_le_bytes())?)
    }

    fn put_len(&mut self, len: usize) -> Result<()> {
        self.put_u32(u32::try_from(len).context("head snapshot collection exceeds u32")?)
    }

    fn put_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.put_len(bytes.len())?;
        Ok(self.write_all(bytes)?)
    }
}

impl<T: Read> Read for Checksummed<T> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.hasher.update(&buffer[..read]);
        Ok(read)
    }
}

impl<T: Read> Checksummed<T> {
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut bytes = [0; N];
        self.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    fn count(&mut self, maximum: u32) -> Result<usize> {
        let count = self.u32()?;
        if count > maximum {
            bail!("head snapshot collection exceeds {maximum} entries");
        }
        Ok(count as usize)
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.count(64 * 1024 * 1024)?;
        let mut bytes = vec![0; len];
        self.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn string(&mut self) -> Result<String> {
        Ok(String::from_utf8(self.read_bytes()?)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restores_series_heads_and_chunks_and_removes_the_snapshot() {
        let directory =
            std::env::temp_dir().join(format!("mimir-rust-head-snapshot-{}", std::process::id()));
        let overrides = Arc::new(Overrides::new(crate::limits::Limits {
            out_of_order_time_window_ms: 3_600_000,
            max_global_exemplars_per_user: 100,
            ..Default::default()
        }));
        let store = Store::new(20 * 60 * 1000, None, Some(directory.clone()))
            .unwrap()
            .with_overrides(Arc::clone(&overrides));
        let request = |samples: Vec<(i64, f64)>, histograms: Vec<i64>| DecodedRequest {
            source: 0,
            series: vec![DecodedSeries {
                labels: vec![
                    ("__name__".into(), "metric".into()),
                    ("job".into(), "api".into()),
                ],
                samples: samples
                    .into_iter()
                    .map(|(timestamp_ms, value)| cortexpb::Sample {
                        timestamp_ms,
                        value,
                    })
                    .collect(),
                histograms: histograms
                    .into_iter()
                    .map(|timestamp| cortexpb::Histogram {
                        timestamp,
                        count: Some(cortexpb::histogram::Count::CountInt(1)),
                        positive_spans: vec![cortexpb::BucketSpan {
                            offset: 0,
                            length: 1,
                        }],
                        positive_deltas: vec![1],
                        ..Default::default()
                    })
                    .collect(),
                exemplars: vec![cortexpb::Exemplar {
                    value: 1.0,
                    timestamp_ms: 5,
                    ..Default::default()
                }],
                created_timestamp: 0,
            }],
            metadata: vec![cortexpb::MetricMetadata {
                r#type: 1,
                metric_family_name: "metric".into(),
                help: "help".into(),
                unit: String::new(),
            }],
        };
        store
            .ingest(
                "tenant",
                request(
                    (0..500).map(|t| (t * 15_000, t as f64)).collect(),
                    Vec::new(),
                ),
            )
            .unwrap();
        store
            .ingest(
                "tenant",
                request(vec![(7, 7.0)], (1..300).map(|t| t * 7_000 + 3).collect()),
            )
            .unwrap();
        let before = query_everything(&store);
        let exemplars_before = store
            .select_exemplars("tenant", i64::MIN, i64::MAX, &[])
            .unwrap();
        assert_eq!(exemplars_before[0].exemplars.len(), 1);
        let offsets = [SnapshotOffset {
            offset: Some(41),
            timestamp_ms: 99,
        }];
        store.write_snapshot(&offsets).unwrap();
        drop(store);

        let mut restored = Store::restore(20 * 60 * 1000, None, &directory, 2)
            .unwrap()
            .unwrap();
        restored.store = restored.store.with_overrides(overrides);
        assert_eq!(restored.offsets, offsets);
        assert!(!directory.join(FILE_NAME).exists());
        assert_eq!(query_everything(&restored.store), before);
        assert_eq!(restored.store.metadata("tenant").len(), 1);
        assert_eq!(
            restored
                .store
                .select_exemplars("tenant", i64::MIN, i64::MAX, &[])
                .unwrap()
                .into_iter()
                .map(|series| (series.labels, series.exemplars))
                .collect::<Vec<_>>(),
            exemplars_before
                .into_iter()
                .map(|series| (series.labels, series.exemplars))
                .collect::<Vec<_>>()
        );
        // The head's max time survives: a sample over an hour older than it is too old.
        let mut old = request(vec![(0, 1.0)], Vec::new());
        old.series[0].labels[1].1 = "other".into();
        old.series[0].exemplars.clear();
        restored.store.ingest("tenant", old).unwrap();
        assert_eq!(restored.store.num_series("tenant"), 2);
        assert_eq!(
            restored
                .store
                .select_labels(
                    "tenant",
                    i64::MIN,
                    i64::MAX,
                    &[cortex::LabelMatcher {
                        r#type: 0,
                        name: "job".into(),
                        value: "other".into(),
                    }]
                )
                .unwrap()
                .len(),
            0
        );
        restored
            .store
            .ingest("tenant", request(vec![(500 * 15_000, 1.0)], Vec::new()))
            .unwrap();
        assert!(query_everything(&restored.store).len() >= before.len());
        drop(restored);

        assert!(
            Store::restore(20 * 60 * 1000, None, &directory, 2)
                .unwrap()
                .is_none()
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_corrupt_snapshot_and_removes_it() {
        let directory =
            std::env::temp_dir().join(format!("mimir-rust-head-corrupt-{}", std::process::id()));
        let store = Store::new(20 * 60 * 1000, None, Some(directory.clone())).unwrap();
        store.write_snapshot(&[]).unwrap();
        drop(store);
        let path = directory.join(FILE_NAME);
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        fs::write(&path, bytes).unwrap();
        assert!(
            Store::restore(20 * 60 * 1000, None, &directory, 2)
                .unwrap()
                .is_none()
        );
        assert!(!path.exists());
        fs::remove_dir_all(directory).unwrap();
    }

    fn query_everything(store: &Store) -> Vec<(i64, i64, Bytes)> {
        store
            .select_chunks("tenant", i64::MIN, i64::MAX, &[])
            .unwrap()
            .iter()
            .flat_map(|view| {
                view.chunks[view.chunk_start..view.chunk_end]
                    .iter()
                    .map(|chunk| {
                        (
                            chunk.start_timestamp_ms,
                            chunk.end_timestamp_ms,
                            chunk.wire.clone(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }
}
