use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use anyhow::{Context, Result, bail};
use crc32fast::Hasher;

use crate::store::Store;

const MAGIC: &[u8; 8] = b"MIMIRS01";
const VERSION: u32 = 1;
const HEADER_LEN: usize = 40;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Encoding {
    Raw,
    Zstd,
    Lz4,
}

#[derive(Clone, Copy, Debug)]
pub struct Identity<'a> {
    pub cluster: u32,
    pub topic: &'a str,
    pub partition: i32,
}

pub struct Snapshot {
    pub offset: i64,
    pub store: Store,
}

pub fn write(
    path: &Path,
    identity: Identity<'_>,
    offset: i64,
    store: &Store,
    encoding: Encoding,
    include_chunks: bool,
) -> Result<usize> {
    let temporary = path.with_extension("tmp");
    let previous = path.with_extension("previous");
    let mut file = File::create(&temporary)
        .with_context(|| format!("create recovery image {}", temporary.display()))?;
    let mut header = Vec::with_capacity(HEADER_LEN);
    header.extend_from_slice(MAGIC);
    header.extend_from_slice(&VERSION.to_le_bytes());
    header.extend_from_slice(&identity.cluster.to_le_bytes());
    header.extend_from_slice(&identity.partition.to_le_bytes());
    header.extend_from_slice(&crc32fast::hash(identity.topic.as_bytes()).to_le_bytes());
    header.extend_from_slice(&offset.to_le_bytes());
    header.push(match encoding {
        Encoding::Raw => 0,
        Encoding::Zstd => 1,
        Encoding::Lz4 => 2,
    });
    header.push(u8::from(include_chunks));
    header.extend_from_slice(&[0, 0]);
    header.extend_from_slice(&crc32fast::hash(&header).to_le_bytes());
    debug_assert_eq!(header.len(), HEADER_LEN);
    file.write_all(&header)?;

    let (file, series) = match encoding {
        Encoding::Raw => {
            let mut writer = BufWriter::new(file);
            let (series, checksum) = write_body(&mut writer, store, include_chunks)?;
            writer.write_all(&checksum.to_le_bytes())?;
            writer.flush()?;
            (writer.into_inner()?, series)
        }
        Encoding::Zstd => {
            let mut encoder = zstd::stream::write::Encoder::new(BufWriter::new(file), 1)?;
            encoder.include_checksum(true)?;
            let (series, checksum) = write_body(&mut encoder, store, include_chunks)?;
            encoder.write_all(&checksum.to_le_bytes())?;
            let mut writer = encoder.finish()?;
            writer.flush()?;
            (writer.into_inner()?, series)
        }
        Encoding::Lz4 => {
            let mut encoder = lz4_flex::frame::FrameEncoder::new(BufWriter::new(file));
            let (series, checksum) = write_body(&mut encoder, store, include_chunks)?;
            encoder.write_all(&checksum.to_le_bytes())?;
            let mut writer = encoder.finish()?;
            writer.flush()?;
            (writer.into_inner()?, series)
        }
    };
    file.sync_all()?;
    if path.exists() {
        fs::rename(path, &previous)?;
    }
    fs::rename(&temporary, path)?;
    File::open(path.parent().context("recovery image has no parent")?)?.sync_all()?;
    Ok(series)
}

pub fn read(
    path: &Path,
    identity: Identity<'_>,
    active_window_ms: i64,
    retention_ms: Option<i64>,
) -> Result<Snapshot> {
    let previous = path.with_extension("previous");
    match read_one(path, identity, active_window_ms, retention_ms) {
        Ok(snapshot) => Ok(snapshot),
        Err(current_error) if previous.exists() => {
            read_one(&previous, identity, active_window_ms, retention_ms).with_context(|| {
                format!(
                    "current recovery image {} failed: {current_error:#}",
                    path.display()
                )
            })
        }
        Err(error) => Err(error),
    }
}

fn write_body(
    writer: &mut impl Write,
    store: &Store,
    include_chunks: bool,
) -> Result<(usize, u32)> {
    let mut checksummed = ChecksummedWriter {
        writer,
        hasher: Hasher::new(),
    };
    let series = store.write_image(&mut checksummed, include_chunks)?;
    Ok((series, checksummed.hasher.finalize()))
}

fn read_one(
    path: &Path,
    identity: Identity<'_>,
    active_window_ms: i64,
    retention_ms: Option<i64>,
) -> Result<Snapshot> {
    let mut file =
        File::open(path).with_context(|| format!("open recovery image {}", path.display()))?;
    let mut header = [0; HEADER_LEN];
    file.read_exact(&mut header)?;
    if &header[..8] != MAGIC || u32::from_le_bytes(header[8..12].try_into().unwrap()) != VERSION {
        bail!("invalid recovery image version");
    }
    if crc32fast::hash(&header[..HEADER_LEN - 4])
        != u32::from_le_bytes(header[HEADER_LEN - 4..].try_into().unwrap())
    {
        bail!("recovery image header checksum mismatch");
    }
    let cluster = u32::from_le_bytes(header[12..16].try_into().unwrap());
    let partition = i32::from_le_bytes(header[16..20].try_into().unwrap());
    let topic_hash = u32::from_le_bytes(header[20..24].try_into().unwrap());
    if cluster != identity.cluster
        || partition != identity.partition
        || topic_hash != crc32fast::hash(identity.topic.as_bytes())
    {
        bail!("recovery image belongs to another Kafka source");
    }
    let offset = i64::from_le_bytes(header[24..32].try_into().unwrap());
    let encoding = header[32];
    let include_chunks = match header[33] {
        0 => false,
        1 => true,
        _ => bail!("invalid recovery image chunk flag"),
    };
    if header[34..36] != [0, 0] {
        bail!("invalid recovery image reserved bytes");
    }
    let store = match encoding {
        0 => read_body(
            BufReader::new(file),
            active_window_ms,
            retention_ms,
            include_chunks,
        )?,
        1 => read_body(
            zstd::stream::read::Decoder::new(file)?,
            active_window_ms,
            retention_ms,
            include_chunks,
        )?,
        2 => read_body(
            lz4_flex::frame::FrameDecoder::new(file),
            active_window_ms,
            retention_ms,
            include_chunks,
        )?,
        _ => bail!("invalid recovery image encoding"),
    };
    Ok(Snapshot { offset, store })
}

fn read_body(
    reader: impl Read,
    active_window_ms: i64,
    retention_ms: Option<i64>,
    include_chunks: bool,
) -> Result<Store> {
    let mut checksummed = ChecksummedReader {
        reader,
        hasher: Hasher::new(),
    };
    let store = Store::from_image(
        &mut checksummed,
        active_window_ms,
        retention_ms,
        include_chunks,
    )?;
    let checksum = checksummed.hasher.finalize();
    let mut reader = checksummed.reader;
    let mut expected = [0; 4];
    reader.read_exact(&mut expected)?;
    if u32::from_le_bytes(expected) != checksum {
        bail!("recovery image body checksum mismatch");
    }
    let mut extra = [0];
    if reader.read(&mut extra)? != 0 {
        bail!("trailing recovery image data");
    }
    Ok(store)
}

struct ChecksummedWriter<W> {
    writer: W,
    hasher: Hasher,
}

impl<W: Write> Write for ChecksummedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.writer.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

struct ChecksummedReader<R> {
    reader: R,
    hasher: Hasher,
}

impl<R: Read> Read for ChecksummedReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let read = self.reader.read(bytes)?;
        self.hasher.update(&bytes[..read]);
        Ok(read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::cortexpb;
    use crate::record::{DecodedRequest, DecodedSeries};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn request(timestamp: i64, value: f64) -> DecodedRequest {
        DecodedRequest {
            source: 0,
            series: vec![DecodedSeries {
                labels: vec![
                    ("job".into(), "snapshot-test".into()),
                    ("__name__".into(), "snapshot_metric".into()),
                ],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: timestamp,
                    value,
                }],
                histograms: Vec::new(),
                exemplars: Vec::new(),
                created_timestamp: 0,
            }],
            metadata: Vec::new(),
        }
    }

    fn fixture_dir() -> std::path::PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "mimir-recovery-image-test-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn restores_queries_and_accepts_new_samples() {
        for encoding in [Encoding::Raw, Encoding::Zstd, Encoding::Lz4] {
            for include_chunks in [false, true] {
                let directory = fixture_dir();
                let path = directory.join("image");
                let identity = Identity {
                    cluster: 0,
                    topic: "ingest",
                    partition: 2,
                };
                let store = Store::default();
                store.ingest("tenant", request(1000, 1.5)).unwrap();
                let expected = store.select_chunks("tenant", 0, 2000, &[]).unwrap();
                assert_eq!(
                    write(&path, identity, 42, &store, encoding, include_chunks).unwrap(),
                    1
                );
                let recovered = read(&path, identity, 20 * 60 * 1000, None).unwrap();
                assert_eq!(recovered.offset, 42);
                let actual = recovered
                    .store
                    .select_chunks("tenant", 0, 2000, &[])
                    .unwrap();
                assert_eq!(actual.len(), expected.len());
                assert_eq!(actual[0].encoded_labels, expected[0].encoded_labels);
                assert_eq!(actual[0].chunks[0].wire, expected[0].chunks[0].wire);
                assert!(
                    read(
                        &path,
                        Identity {
                            partition: 3,
                            ..identity
                        },
                        20 * 60 * 1000,
                        None,
                    )
                    .is_err()
                );
                recovered
                    .store
                    .ingest("tenant", request(2000, 2.5))
                    .unwrap();
                let updated = recovered
                    .store
                    .select_chunks("tenant", 0, 3000, &[])
                    .unwrap();
                assert_eq!(updated.len(), 1);
                assert_ne!(updated[0].chunks[0].wire, expected[0].chunks[0].wire);
                fs::remove_dir_all(directory).unwrap();
            }
        }
    }

    #[test]
    fn falls_back_to_previous_valid_image() {
        let directory = fixture_dir();
        let path = directory.join("image");
        let identity = Identity {
            cluster: 0,
            topic: "ingest",
            partition: 0,
        };
        let store = Store::default();
        store.ingest("tenant", request(1000, 1.5)).unwrap();
        write(&path, identity, 10, &store, Encoding::Zstd, true).unwrap();
        store.ingest("tenant", request(2000, 2.5)).unwrap();
        write(&path, identity, 20, &store, Encoding::Zstd, true).unwrap();
        fs::write(&path, b"torn").unwrap();
        let recovered = read(&path, identity, 20 * 60 * 1000, None).unwrap();
        assert_eq!(recovered.offset, 10);
        assert_eq!(
            recovered
                .store
                .select_chunks("tenant", 0, 3000, &[])
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            recovered
                .store
                .select_chunks("tenant", 2000, 3000, &[])
                .unwrap()
                .len(),
            0
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_corrupt_body_without_fallback() {
        let directory = fixture_dir();
        let path = directory.join("image");
        let identity = Identity {
            cluster: 0,
            topic: "ingest",
            partition: 0,
        };
        let store = Store::default();
        store.ingest("tenant", request(1000, 1.5)).unwrap();
        write(&path, identity, 10, &store, Encoding::Raw, true).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        bytes[HEADER_LEN + 8] ^= 0xff;
        fs::write(&path, bytes).unwrap();
        assert!(read(&path, identity, 20 * 60 * 1000, None).is_err());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn preserves_histograms_exemplars_and_metadata() {
        let directory = fixture_dir();
        let path = directory.join("image");
        let identity = Identity {
            cluster: 0,
            topic: "ingest",
            partition: 0,
        };
        let histogram = cortexpb::Histogram {
            timestamp: 1000,
            count: Some(cortexpb::histogram::Count::CountInt(3)),
            positive_spans: vec![cortexpb::BucketSpan {
                offset: 0,
                length: 3,
            }],
            positive_deltas: vec![1, 1, 1],
            ..Default::default()
        };
        let exemplar = cortexpb::Exemplar {
            value: 3.0,
            timestamp_ms: 1000,
            ..Default::default()
        };
        let metadata = cortexpb::MetricMetadata {
            metric_family_name: "snapshot_metric".into(),
            help: "histogram fixture".into(),
            ..Default::default()
        };
        let store = Store::default();
        store
            .ingest(
                "tenant",
                DecodedRequest {
                    source: 0,
                    series: vec![DecodedSeries {
                        labels: vec![("__name__".into(), "snapshot_metric".into())],
                        samples: Vec::new(),
                        histograms: vec![histogram],
                        exemplars: vec![exemplar],
                        created_timestamp: 0,
                    }],
                    metadata: vec![metadata],
                },
            )
            .unwrap();
        let before_chunks = store.select_chunks("tenant", 0, 2000, &[]).unwrap();
        let before_exemplars = store.select_exemplars("tenant", 0, 2000, &[]).unwrap();
        let before_metadata = store.metadata("tenant");
        for encoding in [Encoding::Raw, Encoding::Zstd, Encoding::Lz4] {
            for include_chunks in [false, true] {
                write(&path, identity, 7, &store, encoding, include_chunks).unwrap();
                let recovered = read(&path, identity, 20 * 60 * 1000, None).unwrap();
                let after_chunks = recovered
                    .store
                    .select_chunks("tenant", 0, 2000, &[])
                    .unwrap();
                assert_eq!(
                    after_chunks[0].chunks[0].wire,
                    before_chunks[0].chunks[0].wire
                );
                let after_exemplars = recovered
                    .store
                    .select_exemplars("tenant", 0, 2000, &[])
                    .unwrap();
                assert_eq!(after_exemplars[0].exemplars, before_exemplars[0].exemplars);
                assert_eq!(recovered.store.metadata("tenant"), before_metadata);
            }
        }
        fs::remove_dir_all(directory).unwrap();
    }
}
