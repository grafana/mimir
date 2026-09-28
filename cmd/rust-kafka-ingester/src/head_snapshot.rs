use std::fs::{self, File};
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::Path;

use super::*;
use crate::chunk_disk::FileState;

const MAGIC: &[u8; 8] = b"MIMIRHS1";
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
        let state = self.state.read().expect("store lock poisoned");
        let directory = state
            .disk
            .directory()
            .context("head snapshots need a chunk directory")?
            .clone();
        state.disk.sync()?;
        let temporary = directory.join(format!("{FILE_NAME}.tmp"));
        let file = File::create(&temporary)
            .with_context(|| format!("create head snapshot {}", temporary.display()))?;
        let mut writer = Checksummed::new(BufWriter::with_capacity(1 << 20, file));
        writer.inner.write_all(MAGIC)?;
        write_snapshot_body(&mut writer, &state, offsets)?;
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
        match read_snapshot(file, active_window_ms, retention_ms, chunk_dir) {
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
    state: &State,
    offsets: &[SnapshotOffset],
) -> Result<()> {
    writer.put_len(offsets.len())?;
    for offset in offsets {
        writer.put_i64(offset.offset.unwrap_or(i64::MIN))?;
        writer.put_i64(offset.timestamp_ms)?;
    }
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
        writer.put_len(tenant.metadata.len())?;
        for metadata in tenant.metadata.values() {
            writer.put_bytes(&metadata.encode_to_vec())?;
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
            writer.put_len(series.exemplars.len())?;
            for exemplar in &series.exemplars {
                writer.put_bytes(&exemplar.encode_to_vec())?;
            }
        }
    }
    Ok(())
}

fn read_snapshot(
    file: File,
    active_window_ms: i64,
    retention_ms: Option<i64>,
    chunk_dir: &Path,
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
        let mut tenant = Tenant::default();
        for _ in 0..reader.count(10_000_000)? {
            let metadata = cortexpb::MetricMetadata::decode(reader.read_bytes()?.as_slice())?;
            let key = (
                metadata.metric_family_name.clone(),
                metadata.r#type,
                metadata.help.clone(),
                metadata.unit.clone(),
            );
            tenant.metadata.insert(key, metadata);
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
                    appender: xor::Appender::read_state(&mut reader)?,
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
            series.exemplars = (0..reader.count(10_000_000)?)
                .map(|_| Ok(cortexpb::Exemplar::decode(reader.read_bytes()?.as_slice())?))
                .collect::<Result<_>>()?;
            if !tenant.series.insert(series_key(labels), series) {
                bail!("duplicate series in head snapshot");
            }
        }
        if tenants.insert(tenant_id, tenant).is_some() {
            bail!("duplicate tenant in head snapshot");
        }
    }
    let expected = reader.hasher.clone().finalize();
    let mut checksum = [0; 4];
    reader.inner.read_exact(&mut checksum)?;
    if u32::from_le_bytes(checksum) != expected {
        bail!("head snapshot checksum mismatch");
    }
    if reader.inner.read(&mut [0])? != 0 {
        bail!("trailing bytes in head snapshot");
    }
    let disk = ChunkDiskMapper::reopen(chunk_dir.to_path_buf(), &files, next_sequence)?;
    Ok(Restored {
        store: Store {
            state: RwLock::new(State { tenants, disk }),
            active_window_ms,
            retention_ms,
        },
        offsets,
    })
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
        let store = Store::new(20 * 60 * 1000, None, Some(directory.clone())).unwrap();
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
        let offsets = [SnapshotOffset {
            offset: Some(41),
            timestamp_ms: 99,
        }];
        store.write_snapshot(&offsets).unwrap();
        drop(store);

        let restored = Store::restore(20 * 60 * 1000, None, &directory)
            .unwrap()
            .unwrap();
        assert_eq!(restored.offsets, offsets);
        assert!(!directory.join(FILE_NAME).exists());
        assert_eq!(query_everything(&restored.store), before);
        assert_eq!(restored.store.metadata("tenant").len(), 1);
        restored
            .store
            .ingest("tenant", request(vec![(500 * 15_000, 1.0)], Vec::new()))
            .unwrap();
        assert!(query_everything(&restored.store).len() >= before.len());
        drop(restored);

        assert!(
            Store::restore(20 * 60 * 1000, None, &directory)
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
            Store::restore(20 * 60 * 1000, None, &directory)
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
