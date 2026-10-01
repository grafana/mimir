use std::fs::{self, File, OpenOptions};
use std::hash::BuildHasher;
use std::io::{BufReader, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use prost::Message;
use rayon::ThreadPoolBuilder;
use rayon::prelude::*;

use crate::proto::cortexpb;
use crate::record::{DecodedRequest, DecodedSeries, LabelStr};

const FILE_MAGIC: &[u8; 8] = b"MIMIRH01";
const CHECKPOINT_MAGIC: &[u8; 8] = b"MIMIRCP1";
const LEGACY_FILE_VERSION: u32 = 1;
// Every frame holds its series' labels.
const COMPRESSED_FILE_VERSION: u32 = 2;
// Like the Prometheus WAL, a file holds a series' labels once and refers to them afterwards.
const SERIES_ID_FILE_VERSION: u32 = 3;
// A file may hold a dictionary trained on its first frames, which the frames after it use.
const DICTIONARY_FILE_VERSION: u32 = 4;
// Like series, metadata is defined once per file; lengths and counts are varints and sample
// timestamps are deltas. Frames of series the file already has are then mostly samples, which
// halves what zstd compresses.
const FILE_FORMAT_VERSION: u32 = 5;
const CHECKPOINT_VERSION: u32 = 1;
const FILE_HEADER_LEN: usize = 28;
const FRAME_HEADER_LEN: usize = 8;
const FRAME_PREFIX_LEN: usize = 24;
const HOUR_MS: i64 = 60 * 60 * 1000;
// zstd's fastest regular level: with a file's dictionary, frames are still smaller than level 1
// gives without one, and compress twice as fast.
const COMPRESSION_LEVEL: i32 = -1;
// A frame holds one Kafka record of a few KB, too little for zstd to learn from; a dictionary
// trained on a file's first frames gives the rest of the file what they have in common.
const DICTIONARY_FRAMES: usize = 2000;
const DICTIONARY_SAMPLE_BYTES: usize = 8 << 20;
const DICTIONARY_BYTES: usize = 112 * 1024;
// What a dictionary frame records as its offset: no Kafka record has it.
const DICTIONARY_OFFSET: i64 = -1;

pub struct RecoveredRecord {
    pub offset: i64,
    pub kafka_timestamp_ms: i64,
    pub ingested_ms: i64,
    pub tenant: String,
    pub request: DecodedRequest,
}

struct CurrentFile {
    hour: i64,
    sequence: u32,
    file: File,
    // Which log instance's file this is, so a frame is only written to the file it was encoded
    // for.
    id: u64,
    // The id of each series the file holds, in the order the file defined them, by the low 64 bits
    // of its `SeriesKey`, with the next 32 bits to tell apart keys that only share those: an
    // hour's file holds every series of the partition, so a slot half the size of a whole key
    // keeps lookups from missing the caches as often.
    series: hashbrown::HashMap<u64, (u32, u32), std::hash::BuildHasherDefault<KeyHasher>>,
    // The series the file defined, some of them twice when their keys shared 64 bits.
    defined_series: u32,
    // The id of each metadata entry the file defined, by its key.
    metadata: hashbrown::HashMap<SeriesKey, u32, std::hash::BuildHasherDefault<KeyHasher>>,
    dictionary: Dictionary,
    // Whether the dictionary frame is in the file, which it must be before a frame that uses it.
    dictionary_written: bool,
}

/// A file's dictionary, from its first frames' payloads to the compressor of the frames after.
enum Dictionary {
    Sampling {
        samples: Vec<u8>,
        sizes: Vec<usize>,
    },
    // Trained on another thread, so ingestion doesn't wait.
    Training(std::sync::mpsc::Receiver<Option<Vec<u8>>>),
    Ready {
        bytes: std::sync::Arc<[u8]>,
        compressor: zstd::bulk::Compressor<'static>,
    },
    // Training failed, or the file started without samples to spare; frames go without one.
    Unavailable,
}

impl Dictionary {
    fn new() -> Self {
        Self::Sampling {
            samples: Vec::new(),
            sizes: Vec::new(),
        }
    }

    /// Takes a trained dictionary once it is there.
    fn poll(&mut self) {
        let Self::Training(trained) = self else {
            return;
        };
        let bytes = match trained.try_recv() {
            Ok(bytes) => bytes,
            Err(std::sync::mpsc::TryRecvError::Empty) => return,
            Err(std::sync::mpsc::TryRecvError::Disconnected) => None,
        };
        *self = bytes
            .and_then(|bytes| {
                let compressor =
                    zstd::bulk::Compressor::with_dictionary(COMPRESSION_LEVEL, &bytes).ok()?;
                Some(Self::Ready {
                    bytes: bytes.into(),
                    compressor,
                })
            })
            .unwrap_or(Self::Unavailable);
    }

    /// Keeps `payload` to train on, and starts training once there are enough.
    fn sample(&mut self, payload: &[u8], frames: usize) {
        let Self::Sampling { samples, sizes } = self else {
            return;
        };
        samples.extend_from_slice(payload);
        sizes.push(payload.len());
        if sizes.len() < frames && samples.len() < DICTIONARY_SAMPLE_BYTES {
            return;
        }
        let (samples, sizes) = (std::mem::take(samples), std::mem::take(sizes));
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let spawned = std::thread::Builder::new()
            .name("segment-dictionary".into())
            .spawn(move || {
                let _ = sender.send(train_dictionary(&samples, &sizes));
            });
        *self = match spawned {
            Ok(_) => Self::Training(receiver),
            Err(_) => Self::Unavailable,
        };
    }
}

/// A dictionary for the payloads in `samples`, or None when zstd can't train one from them. zstd
/// wants samples many times the dictionary's size, and frames can only name a dictionary by id.
fn train_dictionary(samples: &[u8], sizes: &[usize]) -> Option<Vec<u8>> {
    let size = DICTIONARY_BYTES.min(samples.len() / 20);
    if size < 1024 {
        return None;
    }
    let dictionary = zstd::dict::from_continuous(samples, sizes, size).ok()?;
    zstd::zstd_safe::get_dict_id_from_dict(&dictionary)?;
    Some(dictionary)
}

/// A series' identity within a segment file: its tenant and labels, hashed with a seed chosen by
/// the process, so crafted labels cannot make two series share an id. Files are only written by
/// the process that created them, so keys never outlive their seed.
pub type SeriesKey = u128;

// Keys are already uniform hashes.
#[derive(Default)]
struct KeyHasher(u64);

impl std::hash::Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut word = [0; 8];
            word[..chunk.len()].copy_from_slice(chunk);
            self.0 ^= u64::from_le_bytes(word);
        }
    }

    fn write_u128(&mut self, value: u128) {
        self.0 = value as u64;
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

fn key_seed() -> u64 {
    static SEED: OnceLock<u64> = OnceLock::new();
    *SEED.get_or_init(|| std::hash::RandomState::new().hash_one("segment series keys"))
}

/// The `SeriesKey` of each series of `request`.
pub fn series_keys(tenant: &str, request: &DecodedRequest) -> Vec<SeriesKey> {
    thread_local! {
        static BUFFER: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
    }
    BUFFER.with_borrow_mut(|buffer| {
        request
            .series
            .iter()
            .map(|series| {
                buffer.clear();
                put_key_part(buffer, tenant.as_bytes());
                for (name, value) in &series.labels {
                    put_key_part(buffer, name.as_bytes());
                    put_key_part(buffer, value.as_bytes());
                }
                twox_hash::XxHash3_128::oneshot_with_seed(key_seed(), buffer)
            })
            .collect()
    })
}

/// Like `series_keys`, hashing a series' encoded labels in `record` where `label_spans` has them,
/// rather than copying its labels. Those keys differ from `series_keys`', so a series whose
/// labels come either way is defined twice in a file, which only costs its labels.
pub fn series_keys_with_label_bytes(
    tenant: &str,
    request: &DecodedRequest,
    record: &[u8],
    label_spans: &[Option<std::ops::Range<usize>>],
) -> Vec<SeriesKey> {
    let fallback = || series_keys(tenant, request);
    if label_spans.len() != request.series.len() || label_spans.iter().any(Option::is_none) {
        return fallback();
    }
    // The tenant picks the seed, so series of different tenants never share a key.
    let seed =
        twox_hash::XxHash3_128::oneshot_with_seed(key_seed() ^ ENCODED_LABELS, tenant.as_bytes())
            as u64;
    label_spans
        .iter()
        .map(|span| {
            let span = span.clone().expect("checked");
            twox_hash::XxHash3_128::oneshot_with_seed(seed, &record[span])
        })
        .collect()
}

// Keeps keys of encoded labels apart from keys of copied labels.
const ENCODED_LABELS: u64 = 0x9e37_79b9_7f4a_7c15;

fn put_key_part(buffer: &mut Vec<u8>, bytes: &[u8]) {
    buffer.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buffer.extend_from_slice(bytes);
}

pub struct SegmentLog {
    directory: PathBuf,
    cluster: u32,
    partition: i32,
    retention_ms: Option<i64>,
    current: Option<CurrentFile>,
    next_file_id: u64,
    last_offset: Option<i64>,
    pending_offset: Option<i64>,
    payload: Vec<u8>,
    // Frames a file's dictionary is trained on.
    dictionary_frames: usize,
}

thread_local! {
    // Frames hold one Kafka record, so setting up a context per frame cost more than compressing
    // it.
    static COMPRESSOR: std::cell::RefCell<zstd::bulk::Compressor<'static>> = std::cell::RefCell::new(
        zstd::bulk::Compressor::new(COMPRESSION_LEVEL).expect("zstd compressor"),
    );
}

/// A record encoded for the current segment file, written once the store has applied it.
pub struct CompressedFrame {
    offset: i64,
    file: u64,
    // The frame's header, filled in when written, then its body: the file gets it in one write.
    frame: Vec<u8>,
    // The dictionary the frame was compressed with, which the file must hold before it.
    dictionary: Option<std::sync::Arc<[u8]>>,
}

impl CompressedFrame {
    pub fn offset(&self) -> i64 {
        self.offset
    }
}

impl SegmentLog {
    pub fn open(
        root: &Path,
        cluster: usize,
        topic: &str,
        partition: i32,
        retention_ms: Option<i64>,
    ) -> Result<(Self, Vec<RecoveredRecord>)> {
        let mut records = Vec::new();
        let log =
            Self::open_replaying(root, cluster, topic, partition, retention_ms, 2, |record| {
                records.push(record);
                Ok(())
            })?;
        Ok((log, records))
    }

    pub fn open_replaying(
        root: &Path,
        cluster: usize,
        topic: &str,
        partition: i32,
        retention_ms: Option<i64>,
        decode_threads: usize,
        mut replay: impl FnMut(RecoveredRecord) -> Result<()>,
    ) -> Result<Self> {
        let cluster = u32::try_from(cluster).context("Kafka cluster index exceeds u32")?;
        let directory = root.join(format!(
            "cluster-{cluster}-partition-{partition}-topic-{}",
            hex(topic.as_bytes())
        ));
        fs::create_dir_all(&directory)
            .with_context(|| format!("create segment directory {}", directory.display()))?;

        let mut paths = segment_paths(&directory)?;
        paths.sort_by_key(|path| {
            let order = match path.extension().and_then(|value| value.to_str()) {
                Some("legacy") => 0,
                Some("segment") => 1,
                _ => 2,
            };
            (segment_id(path), order)
        });
        let mut segment_offset = None;
        let mut replayed_offset = None;
        let replay_cutoff = retention_ms.map(|retention| now_ms().saturating_sub(retention));
        for path in &paths {
            let mutable = path.extension().is_some_and(|value| value == "open");
            let offset = read_segment(
                path,
                cluster,
                partition,
                mutable,
                replay_cutoff,
                decode_threads,
                &mut |record| {
                    if replayed_offset.is_none_or(|last| record.offset > last) {
                        replayed_offset = Some(record.offset);
                        replay(record)?;
                    }
                    Ok(())
                },
            )?;
            segment_offset = max_offset(segment_offset, offset);
        }

        let checkpoint_offset = read_checkpoint(&directory)?;
        if segment_offset.is_some()
            && checkpoint_offset
                .zip(segment_offset)
                .is_some_and(|(checkpoint, segment)| checkpoint > segment)
        {
            bail!(
                "checkpoint in {} is ahead of durable segment data",
                directory.display()
            );
        }
        let last_offset = max_offset(segment_offset, checkpoint_offset);
        let log = Self {
            directory,
            cluster,
            partition,
            retention_ms,
            current: None,
            next_file_id: 0,
            last_offset,
            pending_offset: None,
            payload: Vec::new(),
            dictionary_frames: DICTIONARY_FRAMES,
        };
        log.remove_expired()?;
        Ok(log)
    }

    /// Opens the log without replaying it when its checkpoint equals `expected`, which a head
    /// snapshot written after the final flush guarantees; otherwise returns `None`.
    pub fn open_at_checkpoint(
        root: &Path,
        cluster: usize,
        topic: &str,
        partition: i32,
        retention_ms: Option<i64>,
        expected: Option<i64>,
    ) -> Result<Option<Self>> {
        let cluster = u32::try_from(cluster).context("Kafka cluster index exceeds u32")?;
        let directory = root.join(format!(
            "cluster-{cluster}-partition-{partition}-topic-{}",
            hex(topic.as_bytes())
        ));
        fs::create_dir_all(&directory)
            .with_context(|| format!("create segment directory {}", directory.display()))?;
        let checkpoint = read_checkpoint(&directory)?;
        if checkpoint != expected {
            return Ok(None);
        }
        let log = Self {
            directory,
            cluster,
            partition,
            retention_ms,
            current: None,
            next_file_id: 0,
            last_offset: checkpoint,
            pending_offset: None,
            payload: Vec::new(),
            dictionary_frames: DICTIONARY_FRAMES,
        };
        log.remove_expired()?;
        Ok(Some(log))
    }

    pub fn last_offset(&self) -> Option<i64> {
        self.last_offset
    }

    pub fn append(
        &mut self,
        offset: i64,
        timestamp_ms: i64,
        tenant: &str,
        request: &DecodedRequest,
    ) -> Result<()> {
        self.append_at(offset, timestamp_ms, tenant, request, now_ms())
    }

    fn append_at(
        &mut self,
        offset: i64,
        timestamp_ms: i64,
        tenant: &str,
        request: &DecodedRequest,
        ingested_ms: i64,
    ) -> Result<()> {
        self.check_offset(offset)?;
        self.begin_batch(ingested_ms)?;
        let keys = series_keys(tenant, request);
        let frame = self.encode(offset, timestamp_ms, ingested_ms, tenant, request, &keys)?;
        self.append_compressed(frame)
    }

    /// Starts the frames of one batch: they all go to the file of `ingested_ms`'s hour, since a
    /// frame refers to series its file defined before it.
    pub fn begin_batch(&mut self, ingested_ms: i64) -> Result<()> {
        self.rotate(hour_start(ingested_ms))
    }

    /// Encodes a record for the current file, with a series' labels the first time the file has
    /// it and its id in the file afterwards. `keys` are the `series_keys` of `request`.
    pub fn encode(
        &mut self,
        offset: i64,
        timestamp_ms: i64,
        ingested_ms: i64,
        tenant: &str,
        request: &DecodedRequest,
        keys: &[SeriesKey],
    ) -> Result<CompressedFrame> {
        if keys.len() != request.series.len() {
            bail!("segment frame needs one key per series");
        }
        let current = self.current.as_mut().context("segment file is not open")?;
        let payload = &mut self.payload;
        payload.clear();
        put_varint_string(payload, tenant);
        put_varint(payload, u64::from(request.source as u32));
        put_varint(payload, request.metadata.len() as u64);
        for metadata in &request.metadata {
            let defined = current.metadata.len() as u32;
            match current.metadata.entry(metadata_key(metadata)) {
                hashbrown::hash_map::Entry::Occupied(entry) => {
                    put_varint(payload, u64::from(*entry.get()) + 1);
                }
                hashbrown::hash_map::Entry::Vacant(entry) => {
                    entry.insert(defined);
                    put_varint(payload, 0);
                    put_varint(payload, u64::from(metadata.r#type as u32));
                    put_varint_string(payload, &metadata.metric_family_name);
                    put_varint_string(payload, &metadata.help);
                    put_varint_string(payload, &metadata.unit);
                }
            }
        }
        put_varint(payload, request.series.len() as u64);
        for (series, key) in request.series.iter().zip(keys) {
            let check = (*key >> 64) as u32;
            let id = match current.series.entry(*key as u64) {
                hashbrown::hash_map::Entry::Occupied(entry) => {
                    let (id, known) = *entry.get();
                    // Another series with the same low 64 bits: it gets its labels every time.
                    (known == check).then_some(id)
                }
                hashbrown::hash_map::Entry::Vacant(entry) => {
                    entry.insert((current.defined_series, check));
                    None
                }
            };
            match id {
                Some(id) => put_varint(payload, u64::from(id) + 1),
                None => {
                    current.defined_series += 1;
                    put_varint(payload, 0);
                    put_varint(payload, series.labels.len() as u64);
                    for (name, value) in &series.labels {
                        put_varint_string(payload, name);
                        put_varint_string(payload, value);
                    }
                }
            }
            put_compact_series_data(payload, series, timestamp_ms);
        }
        current.dictionary.poll();
        let (compressed, dictionary) = match &mut current.dictionary {
            Dictionary::Ready { bytes, compressor } => {
                (compressor.compress(payload), Some(bytes.clone()))
            }
            _ => (
                COMPRESSOR.with_borrow_mut(|compressor| compressor.compress(payload)),
                None,
            ),
        };
        let compressed = compressed.context("compress segment frame")?;
        current.dictionary.sample(payload, self.dictionary_frames);
        let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + FRAME_PREFIX_LEN + compressed.len());
        frame.resize(FRAME_HEADER_LEN, 0);
        put_i64(&mut frame, offset);
        put_i64(&mut frame, timestamp_ms);
        put_i64(&mut frame, ingested_ms);
        frame.extend_from_slice(&compressed);
        if frame.len() - FRAME_HEADER_LEN > 128 * 1024 * 1024 {
            bail!("compressed segment frame exceeds 128 MiB");
        }
        Ok(CompressedFrame {
            offset,
            file: current.id,
            frame,
            dictionary,
        })
    }

    pub fn check_offset(&self, offset: i64) -> Result<()> {
        if self.last_offset.is_some_and(|last| offset <= last) {
            bail!(
                "Kafka offset {offset} is not after persisted offset {:?}",
                self.last_offset
            );
        }
        Ok(())
    }

    pub fn append_compressed(&mut self, frame: CompressedFrame) -> Result<()> {
        self.check_offset(frame.offset)?;
        let mut bytes = frame.frame;
        fill_frame_header(&mut bytes)?;
        let current = self.current.as_mut().context("segment file is not open")?;
        if current.id != frame.file {
            bail!("segment frame was encoded for another file");
        }
        if let Some(dictionary) = frame.dictionary.filter(|_| !current.dictionary_written) {
            let mut dictionary_frame =
                Vec::with_capacity(FRAME_HEADER_LEN + FRAME_PREFIX_LEN + dictionary.len());
            dictionary_frame.resize(FRAME_HEADER_LEN, 0);
            put_i64(&mut dictionary_frame, DICTIONARY_OFFSET);
            put_i64(&mut dictionary_frame, 0);
            put_i64(&mut dictionary_frame, 0);
            dictionary_frame.extend_from_slice(&dictionary);
            fill_frame_header(&mut dictionary_frame)?;
            current.file.write_all(&dictionary_frame)?;
            current.dictionary_written = true;
        }
        current.file.write_all(&bytes)?;
        self.last_offset = Some(frame.offset);
        self.pending_offset = Some(frame.offset);
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        let Some(offset) = self.pending_offset else {
            return Ok(());
        };
        self.current
            .as_mut()
            .context("segment file is not open")?
            .file
            .sync_data()?;
        write_checkpoint(&self.directory, offset)?;
        self.pending_offset = None;
        Ok(())
    }

    pub fn maintain(&mut self) -> Result<()> {
        self.flush()?;
        if self
            .current
            .as_ref()
            .is_some_and(|current| current.hour < hour_start(now_ms()))
        {
            self.seal_current()?;
        }
        self.remove_expired()
    }

    fn rotate(&mut self, hour: i64) -> Result<()> {
        if self
            .current
            .as_ref()
            .is_some_and(|current| current.hour == hour)
        {
            return Ok(());
        }
        // The next hour's file defines about as many series, so its map starts at that size
        // rather than rehashing them all as it grows.
        let expected_series = self
            .current
            .as_ref()
            .map_or(0, |current| current.series.len());
        self.flush()?;
        self.seal_current()?;
        // A file left open by an earlier process is sealed rather than appended to: the series
        // ids it defined went with that process.
        let paths = segment_paths(&self.directory)?;
        let mut next_sequence = std::collections::HashMap::<i64, u32>::new();
        for (path_hour, path_sequence) in paths.iter().filter_map(|path| segment_id(path)) {
            let next = next_sequence.entry(path_hour).or_default();
            *next = (*next).max(path_sequence + 1);
        }
        for path in paths {
            if !path.extension().is_some_and(|value| value == "open") {
                continue;
            }
            let mut to = path.with_extension("segment");
            if to.exists() {
                // The log before sequences could reopen an hour it had sealed, when a record
                // prepared before the hour ended was appended after, leaving both files.
                let (path_hour, _) = segment_id(&path).context("segment name has no hour")?;
                let next = next_sequence.entry(path_hour).or_default();
                to = self
                    .directory
                    .join(segment_name(path_hour, *next, "segment"));
                *next += 1;
            }
            if to.exists() {
                bail!("refusing to overwrite sealed segment {}", to.display());
            }
            fs::rename(&path, &to)
                .with_context(|| format!("seal recovered segment {}", path.display()))?;
            sync_directory(&self.directory)?;
        }
        self.remove_expired()?;
        let sequence = next_sequence.get(&hour).copied().unwrap_or(0);
        let path = self.directory.join(segment_name(hour, sequence, "open"));
        let mut file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .read(true)
            .open(&path)
            .with_context(|| format!("open segment {}", path.display()))?;
        write_file_header(&mut file, self.cluster, self.partition, hour)?;
        file.sync_all()?;
        sync_directory(&self.directory)?;
        self.current = Some(CurrentFile {
            hour,
            sequence,
            file,
            id: self.next_file_id,
            series: hashbrown::HashMap::with_capacity_and_hasher(
                expected_series,
                Default::default(),
            ),
            defined_series: 0,
            metadata: Default::default(),
            dictionary: Dictionary::new(),
            dictionary_written: false,
        });
        self.next_file_id += 1;
        Ok(())
    }

    fn seal_current(&mut self) -> Result<()> {
        if let Some(current) = self.current.take() {
            current.file.sync_all()?;
            let from = self
                .directory
                .join(segment_name(current.hour, current.sequence, "open"));
            let to = from.with_extension("segment");
            fs::rename(&from, &to).with_context(|| format!("seal segment {}", from.display()))?;
            sync_directory(&self.directory)?;
        }
        Ok(())
    }

    fn remove_expired(&self) -> Result<()> {
        let Some(retention_ms) = self.retention_ms else {
            return Ok(());
        };
        let cutoff = now_ms().saturating_sub(retention_ms);
        let current_hour = self.current.as_ref().map(|current| current.hour);
        for path in segment_paths(&self.directory)? {
            let Some((hour, _)) = segment_id(&path) else {
                continue;
            };
            if Some(hour) != current_hour && hour.saturating_add(HOUR_MS) < cutoff {
                fs::remove_file(&path)
                    .with_context(|| format!("remove expired segment {}", path.display()))?;
            }
        }
        Ok(())
    }
}

impl Drop for SegmentLog {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

/// Writes the length and checksum of the body after it into a frame's header.
fn fill_frame_header(frame: &mut [u8]) -> Result<()> {
    let (header, body) = frame.split_at_mut(FRAME_HEADER_LEN);
    let length = u32::try_from(body.len()).context("segment frame exceeds 4 GiB")?;
    header[..4].copy_from_slice(&length.to_le_bytes());
    header[4..].copy_from_slice(&crc32fast::hash(body).to_le_bytes());
    Ok(())
}

fn read_segment(
    path: &Path,
    expected_cluster: u32,
    expected_partition: i32,
    mutable: bool,
    replay_cutoff: Option<i64>,
    decode_threads: usize,
    replay: &mut impl FnMut(RecoveredRecord) -> Result<()>,
) -> Result<Option<i64>> {
    let file = OpenOptions::new()
        .read(true)
        .write(mutable)
        .open(path)
        .with_context(|| format!("open segment {}", path.display()))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut header = [0; FILE_HEADER_LEN];
    reader
        .read_exact(&mut header)
        .with_context(|| format!("segment {} has a truncated header", path.display()))?;
    let mut cursor = Cursor::new(&header);
    if cursor.take(8)? != FILE_MAGIC {
        bail!("segment {} has invalid magic", path.display());
    }
    let version = cursor.u32()?;
    if !(LEGACY_FILE_VERSION..=FILE_FORMAT_VERSION).contains(&version) {
        bail!("segment {} has unsupported format version", path.display());
    }
    if cursor.u32()? != expected_cluster || cursor.i32()? != expected_partition {
        bail!("segment {} belongs to another Kafka source", path.display());
    }
    let header_hour = cursor.i64()?;
    if segment_id(path).map(|(hour, _)| hour) != Some(header_hour) {
        bail!(
            "segment {} hour does not match its filename",
            path.display()
        );
    }

    // Recovery shares the process CPU budget with Store ingest and startup work.
    let decode_pool = ThreadPoolBuilder::new()
        .num_threads(decode_threads.max(1))
        .build()
        .context("create segment decode pool")?;
    let mut last_offset = None;
    let mut valid_len = FILE_HEADER_LEN as u64;
    let mut frames = Vec::new();
    let mut spare = Vec::new();
    let mut batch_decoded_bytes = 0_u64;
    let mut replay_state = ReplayState {
        version,
        cutoff: replay_cutoff,
        series: Vec::new(),
        metadata: Vec::new(),
        dictionary: None,
    };
    loop {
        let mut frame_header = [0; FRAME_HEADER_LEN];
        match reader.read(&mut frame_header[..1]) {
            Ok(0) => break,
            Ok(_) => {}
            Err(error) => {
                replay_batch(
                    &decode_pool,
                    &mut replay_state,
                    path,
                    &mut frames,
                    &mut spare,
                    replay,
                    &mut last_offset,
                )?;
                return Err(error.into());
            }
        }
        if let Err(error) = reader.read_exact(&mut frame_header[1..]) {
            replay_batch(
                &decode_pool,
                &mut replay_state,
                path,
                &mut frames,
                &mut spare,
                replay,
                &mut last_offset,
            )?;
            if error.kind() == ErrorKind::UnexpectedEof {
                return finish_torn(path, reader, mutable, last_offset, valid_len);
            }
            return Err(error.into());
        }
        let length = u32::from_le_bytes(frame_header[..4].try_into().unwrap()) as usize;
        let checksum = u32::from_le_bytes(frame_header[4..].try_into().unwrap());
        // A crash can leave a zero-filled tail up to the next block; its length 0 and CRC 0 would
        // otherwise pass the checksum because crc32 of an empty body is 0.
        if length < FRAME_PREFIX_LEN || length > 128 * 1024 * 1024 {
            replay_batch(
                &decode_pool,
                &mut replay_state,
                path,
                &mut frames,
                &mut spare,
                replay,
                &mut last_offset,
            )?;
            return finish_torn(path, reader, mutable, last_offset, valid_len);
        }
        let mut body = spare.pop().unwrap_or_default();
        body.resize(length, 0);
        if let Err(error) = reader.read_exact(&mut body) {
            replay_batch(
                &decode_pool,
                &mut replay_state,
                path,
                &mut frames,
                &mut spare,
                replay,
                &mut last_offset,
            )?;
            if error.kind() == ErrorKind::UnexpectedEof {
                return finish_torn(path, reader, mutable, last_offset, valid_len);
            }
            return Err(error.into());
        }
        if crc32fast::hash(&body) != checksum {
            replay_batch(
                &decode_pool,
                &mut replay_state,
                path,
                &mut frames,
                &mut spare,
                replay,
                &mut last_offset,
            )?;
            return finish_torn(path, reader, mutable, last_offset, valid_len);
        }
        // The frames after it are decoded with the file's dictionary.
        if version >= DICTIONARY_FILE_VERSION
            && i64::from_le_bytes(body[..8].try_into().unwrap()) == DICTIONARY_OFFSET
        {
            replay_batch(
                &decode_pool,
                &mut replay_state,
                path,
                &mut frames,
                &mut spare,
                replay,
                &mut last_offset,
            )?;
            batch_decoded_bytes = 0;
            replay_state.dictionary = Some(zstd::dict::DecoderDictionary::copy(
                &body[FRAME_PREFIX_LEN..],
            ));
            valid_len += FRAME_HEADER_LEN as u64 + length as u64;
            spare.push(body);
            continue;
        }
        // Expired frames of a file with series ids still define series that later frames use.
        if let Some(cutoff) = replay_cutoff.filter(|_| version < SERIES_ID_FILE_VERSION) {
            if body.len() >= 24 {
                let ingested_ms = i64::from_le_bytes(body[16..24].try_into().unwrap());
                if ingested_ms < cutoff {
                    replay_batch(
                        &decode_pool,
                        &mut replay_state,
                        path,
                        &mut frames,
                        &mut spare,
                        replay,
                        &mut last_offset,
                    )?;
                    batch_decoded_bytes = 0;
                    last_offset = Some(i64::from_le_bytes(body[..8].try_into().unwrap()));
                    valid_len += FRAME_HEADER_LEN as u64 + length as u64;
                    spare.push(body);
                    continue;
                }
            }
        }
        let decoded_size = if version >= COMPRESSED_FILE_VERSION {
            body.get(24..)
                .and_then(|compressed| zstd::zstd_safe::get_frame_content_size(compressed).ok())
                .flatten()
                .unwrap_or(128 * 1024 * 1024)
        } else {
            length as u64
        };
        if !frames.is_empty() && batch_decoded_bytes.saturating_add(decoded_size) > 16 * 1024 * 1024
        {
            replay_batch(
                &decode_pool,
                &mut replay_state,
                path,
                &mut frames,
                &mut spare,
                replay,
                &mut last_offset,
            )?;
            batch_decoded_bytes = 0;
        }
        batch_decoded_bytes = batch_decoded_bytes.saturating_add(decoded_size);
        frames.push((valid_len, body));
        valid_len += FRAME_HEADER_LEN as u64 + length as u64;
        // Bound decoded data so highly compressible records cannot grow startup memory without limit.
        if frames.len() >= 32 || batch_decoded_bytes >= 16 * 1024 * 1024 {
            replay_batch(
                &decode_pool,
                &mut replay_state,
                path,
                &mut frames,
                &mut spare,
                replay,
                &mut last_offset,
            )?;
            batch_decoded_bytes = 0;
        }
    }
    replay_batch(
        &decode_pool,
        &mut replay_state,
        path,
        &mut frames,
        &mut spare,
        replay,
        &mut last_offset,
    )?;
    Ok(last_offset)
}

// What replaying a file carries from one batch of frames to the next.
struct ReplayState {
    version: u32,
    cutoff: Option<i64>,
    // The labels of the series the file defined, by id.
    series: Vec<DefinedLabels>,
    // The metadata entries the file defined, by id.
    metadata: Vec<cortexpb::MetricMetadata>,
    // The file's dictionary, once replay read it.
    dictionary: Option<zstd::dict::DecoderDictionary<'static>>,
}

enum DecodedFrame {
    Record(RecoveredRecord),
    // Its series refer to the file's series by id.
    WithSeriesIds(FrameWithSeriesIds),
}

fn replay_batch(
    decode_pool: &rayon::ThreadPool,
    state: &mut ReplayState,
    path: &Path,
    frames: &mut Vec<(u64, Vec<u8>)>,
    spare: &mut Vec<Vec<u8>>,
    replay: &mut impl FnMut(RecoveredRecord) -> Result<()>,
    last_offset: &mut Option<i64>,
) -> Result<()> {
    let version = state.version;
    let dictionary = state.dictionary.as_ref();
    let decoded: Vec<_> = decode_pool.install(|| {
        frames
            .par_iter()
            .map_init(
                || dictionary.map(zstd::bulk::Decompressor::with_prepared_dictionary),
                |with_dictionary, (at, body)| {
                    let context =
                        || format!("decode segment frame at byte {at} in {}", path.display());
                    if version == LEGACY_FILE_VERSION {
                        return decode_frame(body)
                            .map(DecodedFrame::Record)
                            .with_context(context);
                    }
                    if body.len() < FRAME_PREFIX_LEN {
                        bail!(
                            "compressed segment frame is too short in {}",
                            path.display()
                        );
                    }
                    let compressed = &body[FRAME_PREFIX_LEN..];
                    // Frames compressed with the file's dictionary name it; those before it don't.
                    let payload = match zstd::zstd_safe::get_dict_id_from_frame(compressed) {
                        None => zstd::bulk::decompress(compressed, 128 * 1024 * 1024),
                        Some(_) => match with_dictionary {
                            Some(Ok(decompressor)) => {
                                decompressor.decompress(compressed, 128 * 1024 * 1024)
                            }
                            Some(Err(error)) => {
                                bail!("load the dictionary of {}: {error}", path.display())
                            }
                            None => bail!(
                                "segment frame needs a dictionary {} doesn't hold before it",
                                path.display()
                            ),
                        },
                    }
                    .with_context(|| format!("decompress segment frame in {}", path.display()))?;
                    if version == COMPRESSED_FILE_VERSION {
                        let decoded = [&body[..FRAME_PREFIX_LEN], &payload].concat();
                        return decode_frame(&decoded)
                            .map(DecodedFrame::Record)
                            .with_context(context);
                    }
                    if version >= FILE_FORMAT_VERSION {
                        return decode_compact_frame(&body[..FRAME_PREFIX_LEN], payload.into())
                            .map(DecodedFrame::WithSeriesIds)
                            .with_context(context);
                    }
                    decode_frame_with_series_ids(&body[..FRAME_PREFIX_LEN], payload.into())
                        .map(DecodedFrame::WithSeriesIds)
                        .with_context(context)
                },
            )
            .collect()
    });
    // Replaying in file order preserves offset and duplicate handling across batches, and defines
    // series before frames use them.
    for ((_, body), frame) in frames.drain(..).zip(decoded) {
        spare.push(body);
        let record = match frame? {
            DecodedFrame::Record(record) => record,
            DecodedFrame::WithSeriesIds(frame) => {
                let expired = state
                    .cutoff
                    .is_some_and(|cutoff| frame.ingested_ms < cutoff);
                let record = frame
                    .resolve(&mut state.series, &mut state.metadata)
                    .with_context(|| format!("resolve series in {}", path.display()))?;
                if expired {
                    *last_offset = Some(record.offset);
                    continue;
                }
                record
            }
        };
        *last_offset = Some(record.offset);
        replay(record)?;
    }
    Ok(())
}

fn finish_torn(
    path: &Path,
    mut reader: BufReader<File>,
    mutable: bool,
    last_offset: Option<i64>,
    valid_len: u64,
) -> Result<Option<i64>> {
    if !mutable {
        bail!("sealed segment {} is corrupt", path.display());
    }
    let file = reader.get_mut();
    file.set_len(valid_len)?;
    file.seek(SeekFrom::End(0))?;
    file.sync_all()?;
    Ok(last_offset)
}

fn decode_frame(bytes: &[u8]) -> Result<RecoveredRecord> {
    let mut cursor = Cursor::new(bytes);
    let offset = cursor.i64()?;
    let kafka_timestamp_ms = cursor.i64()?;
    let ingested_ms = cursor.i64()?;
    let tenant = cursor.string()?;
    let request = decode_request(&mut cursor)?;
    if cursor.remaining() != 0 {
        bail!("trailing bytes in segment frame");
    }
    Ok(RecoveredRecord {
        offset,
        kafka_timestamp_ms,
        ingested_ms,
        tenant,
        request,
    })
}

/// A series a file defined, while replaying it: an hour's file defines every series it holds, so
/// they are kept as interned names and values, without the frames that defined them.
struct DefinedLabels(Bytes);

impl DefinedLabels {
    fn new(labels: &[(LabelStr, LabelStr)]) -> Self {
        let mut bytes = Vec::new();
        for (name, value) in labels {
            crate::labels::put_varint(&mut bytes, u64::from(crate::labels::names::intern(name)));
            crate::labels::put_varint(&mut bytes, value.len() as u64);
            bytes.extend_from_slice(value.as_bytes());
        }
        // Without the spare capacity a growing vector leaves.
        Self(bytes.into_boxed_slice().into())
    }

    fn labels(&self) -> Vec<(LabelStr, LabelStr)> {
        let names = crate::labels::names::snapshot();
        let mut labels = Vec::new();
        let mut bytes = &self.0[..];
        while !bytes.is_empty() {
            let name = names.name(crate::labels::take_varint(&mut bytes) as u32);
            let length = crate::labels::take_varint(&mut bytes) as usize;
            let (value, rest) = bytes.split_at(length);
            bytes = rest;
            let value = if value.is_empty() {
                LabelStr::default()
            } else {
                LabelStr::from_utf8_lossy(self.0.slice_ref(value))
            };
            labels.push((LabelStr::from_static(name), value));
        }
        labels
    }
}

struct FrameWithSeriesIds {
    offset: i64,
    kafka_timestamp_ms: i64,
    ingested_ms: i64,
    tenant: String,
    source: i32,
    metadata: Vec<FrameMetadata>,
    series: Vec<(FrameLabels, DecodedSeries)>,
}

// A metadata entry when the frame defines it, or its id.
type FrameMetadata = Result<cortexpb::MetricMetadata, u32>;

// A series' labels when the frame defines it, or its id.
type FrameLabels = Result<Vec<(LabelStr, LabelStr)>, u32>;

impl FrameWithSeriesIds {
    fn resolve(
        self,
        defined: &mut Vec<DefinedLabels>,
        defined_metadata: &mut Vec<cortexpb::MetricMetadata>,
    ) -> Result<RecoveredRecord> {
        let metadata = self
            .metadata
            .into_iter()
            .map(|metadata| match metadata {
                Ok(metadata) => {
                    defined_metadata.push(metadata.clone());
                    Ok(metadata)
                }
                Err(id) => defined_metadata
                    .get(id as usize)
                    .cloned()
                    .with_context(|| format!("metadata {id} is not defined before its use")),
            })
            .collect::<Result<_>>()?;
        let series = self
            .series
            .into_iter()
            .map(|(labels, mut series)| {
                series.labels = match labels {
                    Ok(labels) => {
                        defined.push(DefinedLabels::new(&labels));
                        labels
                    }
                    Err(id) => defined
                        .get(id as usize)
                        .with_context(|| format!("series {id} is not defined before its use"))?
                        .labels(),
                };
                Ok(series)
            })
            .collect::<Result<_>>()?;
        Ok(RecoveredRecord {
            offset: self.offset,
            kafka_timestamp_ms: self.kafka_timestamp_ms,
            ingested_ms: self.ingested_ms,
            tenant: self.tenant,
            request: DecodedRequest {
                source: self.source,
                series,
                metadata,
            },
        })
    }
}

fn decode_compact_frame(prefix: &[u8], payload: Bytes) -> Result<FrameWithSeriesIds> {
    let mut cursor = Cursor::new(prefix);
    let offset = cursor.i64()?;
    let kafka_timestamp_ms = cursor.i64()?;
    let ingested_ms = cursor.i64()?;
    let mut cursor = Cursor::new(&payload);
    let tenant = cursor.varint_string()?;
    let source = u32::try_from(cursor.varint()?).context("source exceeds u32")? as i32;
    let metadata = cursor.varint_items(|cursor| match cursor.varint()? {
        0 => Ok(Ok(cortexpb::MetricMetadata {
            r#type: u32::try_from(cursor.varint()?).context("metadata type exceeds u32")? as i32,
            metric_family_name: cursor.varint_string()?,
            help: cursor.varint_string()?,
            unit: cursor.varint_string()?,
        })),
        id => Ok(Err(
            u32::try_from(id - 1).context("metadata id exceeds u32")?
        )),
    })?;
    let series = cursor.varint_items(|cursor| {
        let labels = match cursor.varint()? {
            0 => Ok(cursor.varint_items(|cursor| {
                Ok((
                    cursor.varint_label(&payload)?,
                    cursor.varint_label(&payload)?,
                ))
            })?),
            id => Err(u32::try_from(id - 1).context("series id exceeds u32")?),
        };
        Ok((
            labels,
            decode_compact_series_data(cursor, kafka_timestamp_ms)?,
        ))
    })?;
    if cursor.remaining() != 0 {
        bail!("trailing bytes in segment frame");
    }
    Ok(FrameWithSeriesIds {
        offset,
        kafka_timestamp_ms,
        ingested_ms,
        tenant,
        source,
        metadata,
        series,
    })
}

fn decode_frame_with_series_ids(prefix: &[u8], payload: Bytes) -> Result<FrameWithSeriesIds> {
    let mut cursor = Cursor::new(prefix);
    let offset = cursor.i64()?;
    let kafka_timestamp_ms = cursor.i64()?;
    let ingested_ms = cursor.i64()?;
    let mut cursor = Cursor::new(&payload);
    let tenant = cursor.string()?;
    let source = cursor.i32()?;
    // Version 3 and 4 frames hold their metadata entries.
    let metadata = decode_metadata(&mut cursor)?.into_iter().map(Ok).collect();
    let series = cursor.items(|cursor| {
        let labels = match cursor.varint()? {
            0 => Ok(cursor.items(|cursor| Ok((cursor.label(&payload)?, cursor.label(&payload)?)))?),
            id => Err(u32::try_from(id - 1).context("series id exceeds u32")?),
        };
        Ok((labels, decode_series_data(cursor, Vec::new())?))
    })?;
    if cursor.remaining() != 0 {
        bail!("trailing bytes in segment frame");
    }
    Ok(FrameWithSeriesIds {
        offset,
        kafka_timestamp_ms,
        ingested_ms,
        tenant,
        source,
        metadata,
        series,
    })
}

#[cfg(test)]
fn put_metadata(bytes: &mut Vec<u8>, metadata: &[cortexpb::MetricMetadata]) -> Result<()> {
    put_len(bytes, metadata.len())?;
    for metadata in metadata {
        put_i32(bytes, metadata.r#type);
        put_string(bytes, &metadata.metric_family_name)?;
        put_string(bytes, &metadata.help)?;
        put_string(bytes, &metadata.unit)?;
    }
    Ok(())
}

fn decode_metadata(cursor: &mut Cursor<'_>) -> Result<Vec<cortexpb::MetricMetadata>> {
    cursor.items(|cursor| {
        Ok(cortexpb::MetricMetadata {
            r#type: cursor.i32()?,
            metric_family_name: cursor.string()?,
            help: cursor.string()?,
            unit: cursor.string()?,
        })
    })
}

// Everything of a series but its labels.
#[cfg(test)]
fn put_series_data(bytes: &mut Vec<u8>, series: &DecodedSeries) -> Result<()> {
    put_i64(bytes, series.created_timestamp);
    put_len(bytes, series.samples.len())?;
    for sample in &series.samples {
        put_i64(bytes, sample.timestamp_ms);
        put_u64(bytes, sample.value.to_bits());
    }
    put_messages(bytes, &series.histograms)?;
    put_messages(bytes, &series.exemplars)
}

/// `put_series_data` for version 5 files: varints, and each sample's timestamp as a delta from the
/// previous one's, the first from the frame's timestamp.
fn put_compact_series_data(bytes: &mut Vec<u8>, series: &DecodedSeries, timestamp_ms: i64) {
    put_varint(bytes, zigzag(series.created_timestamp));
    put_varint(bytes, series.samples.len() as u64);
    let mut previous = timestamp_ms;
    for sample in &series.samples {
        put_varint(bytes, zigzag(sample.timestamp_ms.wrapping_sub(previous)));
        previous = sample.timestamp_ms;
        put_u64(bytes, sample.value.to_bits());
    }
    put_varint_messages(bytes, &series.histograms);
    put_varint_messages(bytes, &series.exemplars);
}

fn decode_compact_series_data(cursor: &mut Cursor<'_>, timestamp_ms: i64) -> Result<DecodedSeries> {
    let created_timestamp = unzigzag(cursor.varint()?);
    let mut previous = timestamp_ms;
    let samples = cursor.varint_items(|cursor| {
        previous = previous.wrapping_add(unzigzag(cursor.varint()?));
        Ok(cortexpb::Sample {
            timestamp_ms: previous,
            value: f64::from_bits(cursor.u64()?),
        })
    })?;
    let histograms = cursor.varint_messages::<cortexpb::Histogram>()?;
    let exemplars = cursor.varint_messages::<cortexpb::Exemplar>()?;
    Ok(DecodedSeries {
        labels: Vec::new(),
        samples,
        histograms,
        exemplars,
        created_timestamp,
    })
}

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn unzigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

/// A metadata entry's identity within a file, like `SeriesKey` for series.
fn metadata_key(metadata: &cortexpb::MetricMetadata) -> SeriesKey {
    thread_local! {
        static BUFFER: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
    }
    BUFFER.with_borrow_mut(|buffer| {
        buffer.clear();
        buffer.extend_from_slice(&metadata.r#type.to_le_bytes());
        put_key_part(buffer, metadata.metric_family_name.as_bytes());
        put_key_part(buffer, metadata.help.as_bytes());
        put_key_part(buffer, metadata.unit.as_bytes());
        twox_hash::XxHash3_128::oneshot_with_seed(key_seed() ^ METADATA_KEYS, buffer)
    })
}

// Keeps metadata keys apart from series keys.
const METADATA_KEYS: u64 = 0x2545_f491_4f6c_dd1d;

fn put_varint_string(bytes: &mut Vec<u8>, value: &str) {
    put_varint(bytes, value.len() as u64);
    bytes.extend_from_slice(value.as_bytes());
}

fn put_varint_messages<M: Message>(bytes: &mut Vec<u8>, messages: &[M]) {
    put_varint(bytes, messages.len() as u64);
    for message in messages {
        put_varint(bytes, message.encoded_len() as u64);
        message
            .encode(bytes)
            .expect("a vector grows to fit the message");
    }
}

fn decode_series_data(
    cursor: &mut Cursor<'_>,
    labels: Vec<(LabelStr, LabelStr)>,
) -> Result<DecodedSeries> {
    let created_timestamp = cursor.i64()?;
    let samples = cursor.items(|cursor| {
        Ok(cortexpb::Sample {
            timestamp_ms: cursor.i64()?,
            value: f64::from_bits(cursor.u64()?),
        })
    })?;
    let histograms = cursor.messages::<cortexpb::Histogram>()?;
    let exemplars = cursor.messages::<cortexpb::Exemplar>()?;
    Ok(DecodedSeries {
        labels,
        samples,
        histograms,
        exemplars,
        created_timestamp,
    })
}

// Version 1 and 2 frames, where every series has its labels.
#[cfg(test)]
fn encode_request(bytes: &mut Vec<u8>, request: &DecodedRequest) -> Result<()> {
    put_i32(bytes, request.source);
    put_metadata(bytes, &request.metadata)?;
    put_len(bytes, request.series.len())?;
    for series in &request.series {
        put_len(bytes, series.labels.len())?;
        for (name, value) in &series.labels {
            put_string(bytes, name)?;
            put_string(bytes, value)?;
        }
        put_series_data(bytes, series)?;
    }
    Ok(())
}

fn decode_request(cursor: &mut Cursor<'_>) -> Result<DecodedRequest> {
    let source = cursor.i32()?;
    let metadata = decode_metadata(cursor)?;
    let series = cursor.items(|cursor| {
        let labels =
            cursor.items(|cursor| Ok((cursor.string()?.into(), cursor.string()?.into())))?;
        decode_series_data(cursor, labels)
    })?;
    Ok(DecodedRequest {
        source,
        series,
        metadata,
    })
}

#[cfg(test)]
fn put_messages<M: Message>(bytes: &mut Vec<u8>, messages: &[M]) -> Result<()> {
    put_len(bytes, messages.len())?;
    for message in messages {
        put_bytes(bytes, &message.encode_to_vec())?;
    }
    Ok(())
}

fn write_file_header(file: &mut File, cluster: u32, partition: i32, hour: i64) -> Result<()> {
    file.write_all(FILE_MAGIC)?;
    file.write_all(&FILE_FORMAT_VERSION.to_le_bytes())?;
    file.write_all(&cluster.to_le_bytes())?;
    file.write_all(&partition.to_le_bytes())?;
    file.write_all(&hour.to_le_bytes())?;
    Ok(())
}

fn write_checkpoint(directory: &Path, offset: i64) -> Result<()> {
    let mut bytes = Vec::with_capacity(24);
    bytes.extend_from_slice(CHECKPOINT_MAGIC);
    bytes.extend_from_slice(&CHECKPOINT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&offset.to_le_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    let temporary = directory.join("checkpoint.tmp");
    let destination = directory.join("checkpoint");
    let mut file = File::create(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, &destination)?;
    sync_directory(directory)
}

fn read_checkpoint(directory: &Path) -> Result<Option<i64>> {
    let path = directory.join("checkpoint");
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if bytes.len() != 24 || &bytes[..8] != CHECKPOINT_MAGIC {
        bail!("checkpoint {} is corrupt", path.display());
    }
    let expected = u32::from_le_bytes(bytes[20..24].try_into().expect("length checked"));
    if crc32fast::hash(&bytes[..20]) != expected {
        bail!("checkpoint {} checksum mismatch", path.display());
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().expect("length checked"));
    if version != CHECKPOINT_VERSION {
        bail!("checkpoint {} has unsupported version", path.display());
    }
    Ok(Some(i64::from_le_bytes(
        bytes[12..20].try_into().expect("length checked"),
    )))
}

fn segment_paths(directory: &Path) -> Result<Vec<PathBuf>> {
    Ok(fs::read_dir(directory)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.extension().is_some_and(|extension| {
                extension == "open" || extension == "segment" || extension == "legacy"
            })
        })
        .collect())
}

fn segment_name(hour: i64, sequence: u32, extension: &str) -> String {
    format!("{hour:020}-{sequence:04}.{extension}")
}

/// The hour and sequence of a segment file; files from before sequences are the first of their
/// hour.
fn segment_id(path: &Path) -> Option<(i64, u32)> {
    let stem = path.file_stem()?.to_str()?;
    match stem.split_once('-') {
        Some((hour, sequence)) => Some((hour.parse().ok()?, sequence.parse().ok()?)),
        None => Some((stem.parse().ok()?, 0)),
    }
}

fn sync_directory(directory: &Path) -> Result<()> {
    File::open(directory)?.sync_all()?;
    Ok(())
}

fn max_offset(first: Option<i64>, second: Option<i64>) -> Option<i64> {
    match (first, second) {
        (Some(first), Some(second)) => Some(first.max(second)),
        (first, second) => first.or(second),
    }
}

fn hour_start(timestamp_ms: i64) -> i64 {
    timestamp_ms.div_euclid(HOUR_MS) * HOUR_MS
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as i64)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 0xf) as usize] as char);
    }
    result
}

#[cfg(test)]
fn put_len(bytes: &mut Vec<u8>, length: usize) -> Result<()> {
    let length = u32::try_from(length).context("segment collection exceeds u32")?;
    bytes.extend_from_slice(&length.to_le_bytes());
    Ok(())
}

#[cfg(test)]
fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    put_len(bytes, value.len())?;
    bytes.extend_from_slice(value);
    Ok(())
}

#[cfg(test)]
fn put_string(bytes: &mut Vec<u8>, value: &str) -> Result<()> {
    put_bytes(bytes, value.as_bytes())
}

fn put_varint(bytes: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        bytes.push(value as u8 | 0x80);
        value >>= 7;
    }
    bytes.push(value as u8);
}

#[cfg(test)]
fn put_i32(bytes: &mut Vec<u8>, value: i32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_i64(bytes: &mut Vec<u8>, value: i64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .context("segment length overflow")?;
        if end > self.bytes.len() {
            bail!("truncated segment data");
        }
        let value = &self.bytes[self.position..end];
        self.position = end;
        Ok(value)
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("length checked"),
        ))
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_le_bytes(
            self.take(4)?.try_into().expect("length checked"),
        ))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("length checked"),
        ))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(
            self.take(8)?.try_into().expect("length checked"),
        ))
    }

    fn varint(&mut self) -> Result<u64> {
        let mut value = 0_u64;
        for shift in (0..64).step_by(7) {
            let byte = self.take(1)?[0];
            value |= u64::from(byte & 0x7f) << shift;
            if byte < 0x80 {
                return Ok(value);
            }
        }
        bail!("segment varint overflows u64")
    }

    /// A string of `payload`, which this cursor reads, sharing its buffer.
    fn label(&mut self, payload: &Bytes) -> Result<LabelStr> {
        let length = self.u32()? as usize;
        let bytes = self.take(length)?;
        if bytes.is_empty() {
            return Ok(LabelStr::default());
        }
        Ok(LabelStr::from_utf8_lossy(payload.slice_ref(bytes)))
    }

    fn string(&mut self) -> Result<String> {
        let length = self.u32()? as usize;
        Ok(std::str::from_utf8(self.take(length)?)?.to_owned())
    }

    fn items<T>(&mut self, mut decode: impl FnMut(&mut Self) -> Result<T>) -> Result<Vec<T>> {
        let length = self.u32()? as usize;
        (0..length).map(|_| decode(self)).collect()
    }

    fn varint_len(&mut self) -> Result<usize> {
        usize::try_from(self.varint()?).context("segment length exceeds usize")
    }

    fn varint_label(&mut self, payload: &Bytes) -> Result<LabelStr> {
        let length = self.varint_len()?;
        let bytes = self.take(length)?;
        if bytes.is_empty() {
            return Ok(LabelStr::default());
        }
        Ok(LabelStr::from_utf8_lossy(payload.slice_ref(bytes)))
    }

    fn varint_string(&mut self) -> Result<String> {
        let length = self.varint_len()?;
        Ok(std::str::from_utf8(self.take(length)?)?.to_owned())
    }

    fn varint_items<T>(
        &mut self,
        mut decode: impl FnMut(&mut Self) -> Result<T>,
    ) -> Result<Vec<T>> {
        let length = self.varint_len()?;
        // Bounded by what's left, so a corrupt count can't reserve without limit.
        let mut items = Vec::with_capacity(length.min(self.remaining()));
        for _ in 0..length {
            items.push(decode(self)?);
        }
        Ok(items)
    }

    fn varint_messages<M: Message + Default>(&mut self) -> Result<Vec<M>> {
        self.varint_items(|cursor| {
            let length = cursor.varint_len()?;
            Ok(M::decode(cursor.take(length)?)?)
        })
    }

    fn messages<M: Message + Default>(&mut self) -> Result<Vec<M>> {
        self.items(|cursor| {
            let length = cursor.u32()? as usize;
            Ok(M::decode(cursor.take(length)?)?)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Instant;

    #[test]
    fn frames_record_their_size_and_define_each_series_once_per_file() {
        let root = temporary_directory("series-ids");
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        let now = now_ms();
        log.begin_batch(now).unwrap();
        let mut other_tenant = request();
        other_tenant.series[0].samples[0].timestamp_ms = 200;
        let mut sizes = Vec::new();
        for (offset, tenant, request) in [
            (1, "tenant", request()),
            (2, "tenant", request()),
            (3, "other", other_tenant),
        ] {
            let keys = series_keys(tenant, &request);
            let frame = log.encode(offset, 1, now, tenant, &request, &keys).unwrap();
            // Recovery batches frames by the size each one records.
            let size = zstd::zstd_safe::get_frame_content_size(
                &frame.frame[FRAME_HEADER_LEN + FRAME_PREFIX_LEN..],
            )
            .unwrap()
            .unwrap();
            sizes.push(size);
            log.append_compressed(frame).unwrap();
        }
        // The second frame refers to the series and metadata the first defined; another tenant's
        // series with the same labels is its own, while the metadata is the file's.
        assert!(sizes[1] < sizes[2] && sizes[2] < sizes[0], "{sizes:?}");
        // A new file defines its series again.
        log.begin_batch(now + HOUR_MS).unwrap();
        let keys = series_keys("tenant", &request());
        let frame = log
            .encode(4, 1, now + HOUR_MS, "tenant", &request(), &keys)
            .unwrap();
        log.append_compressed(frame).unwrap();
        drop(log);
        let (_, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(recovered.len(), 4);
        for record in &recovered {
            let expected = request();
            assert_eq!(record.request.series[0].labels, expected.series[0].labels);
            assert_eq!(
                record.request.series[0].exemplars,
                expected.series[0].exemplars
            );
            assert_eq!(record.request.metadata, expected.metadata);
            assert_eq!(record.request.source, expected.source);
        }
        assert_eq!(recovered[2].tenant, "other");
        assert_eq!(recovered[2].request.series[0].samples[0].timestamp_ms, 200);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn keys_of_encoded_labels_follow_the_labels_and_the_tenant() {
        let record = |value: &str, timestamp_ms: i64| -> Bytes {
            cortexpb::WriteRequest {
                timeseries: vec![cortexpb::TimeSeries {
                    labels: vec![cortexpb::LabelPair {
                        name: b"job".to_vec().into(),
                        value: value.as_bytes().to_vec().into(),
                    }],
                    samples: vec![cortexpb::Sample {
                        timestamp_ms,
                        value: 1.0,
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }
            .encode_to_vec()
            .into()
        };
        let key = |tenant: &str, record: Bytes| {
            let (request, spans) =
                crate::record::decode_record_with_label_spans(1, record.clone()).unwrap();
            assert!(spans[0].is_some());
            series_keys_with_label_bytes(tenant, &request, &record, &spans)[0]
        };
        let api = key("tenant", record("api", 1));
        assert_eq!(
            key("tenant", record("api", 2)),
            api,
            "samples are not labels"
        );
        assert_ne!(key("other", record("api", 1)), api);
        assert_ne!(key("tenant", record("apj", 1)), api);
    }

    #[test]
    fn frames_are_only_written_to_the_file_they_were_encoded_for() {
        let root = temporary_directory("frame-file");
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        log.begin_batch(HOUR_MS).unwrap();
        let keys = series_keys("tenant", &request());
        let frame = log
            .encode(1, 1, HOUR_MS, "tenant", &request(), &keys)
            .unwrap();
        log.begin_batch(2 * HOUR_MS).unwrap();
        assert!(log.append_compressed(frame).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn expired_frames_still_define_the_series_later_frames_use() {
        let root = temporary_directory("expired-definitions");
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        let hour = hour_start(now_ms());
        // Both frames go to the current hour's file, the first ingested before the retention.
        log.begin_batch(hour).unwrap();
        for (offset, ingested_ms) in [(1, hour - 2 * HOUR_MS), (2, now_ms())] {
            let keys = series_keys("tenant", &request());
            let frame = log
                .encode(offset, 1, ingested_ms, "tenant", &request(), &keys)
                .unwrap();
            log.append_compressed(frame).unwrap();
        }
        drop(log);
        let mut recovered = Vec::new();
        let log = SegmentLog::open_replaying(&root, 0, "topic", 0, Some(HOUR_MS), 2, |record| {
            recovered.push(record);
            Ok(())
        })
        .unwrap();
        assert_eq!(log.last_offset(), Some(2));
        assert_eq!(recovered.len(), 1);
        assert_eq!(
            recovered[0].request.series[0].labels,
            request().series[0].labels
        );
        drop(log);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reads_files_where_every_frame_has_its_labels() {
        let root = temporary_directory("version-2");
        let (log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        let hour = hour_start(now_ms());
        let path = log.directory.join(format!("{hour:020}.segment"));
        let mut file = File::create(&path).unwrap();
        file.write_all(FILE_MAGIC).unwrap();
        file.write_all(&COMPRESSED_FILE_VERSION.to_le_bytes())
            .unwrap();
        file.write_all(&0_u32.to_le_bytes()).unwrap();
        file.write_all(&0_i32.to_le_bytes()).unwrap();
        file.write_all(&hour.to_le_bytes()).unwrap();
        for offset in [1, 2] {
            let body = uncompressed_frame(offset, 100, "tenant", &request(), now_ms());
            let mut compressed = body[..FRAME_PREFIX_LEN].to_vec();
            compressed
                .extend_from_slice(&zstd::bulk::compress(&body[FRAME_PREFIX_LEN..], 1).unwrap());
            file.write_all(&(compressed.len() as u32).to_le_bytes())
                .unwrap();
            file.write_all(&crc32fast::hash(&compressed).to_le_bytes())
                .unwrap();
            file.write_all(&compressed).unwrap();
        }
        drop(file);
        drop(log);
        let (mut log, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(
            recovered[1].request.series[0].labels,
            request().series[0].labels
        );
        // New frames of the same hour go to a new file.
        log.append(3, 101, "tenant", &request()).unwrap();
        drop(log);
        let (_, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(
            recovered
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn seals_an_hour_left_both_sealed_and_open_without_losing_records() {
        let root = temporary_directory("sealed-and-open");
        let (log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        let hour = hour_start(now_ms());
        // Like the log before sequences, after appending a record prepared before the hour
        // ended to the hour it had sealed.
        for (offset, extension) in [(1, "segment"), (2, "open")] {
            let path = log.directory.join(format!("{hour:020}.{extension}"));
            let mut file = File::create(&path).unwrap();
            file.write_all(FILE_MAGIC).unwrap();
            file.write_all(&COMPRESSED_FILE_VERSION.to_le_bytes())
                .unwrap();
            file.write_all(&0_u32.to_le_bytes()).unwrap();
            file.write_all(&0_i32.to_le_bytes()).unwrap();
            file.write_all(&hour.to_le_bytes()).unwrap();
            let body = uncompressed_frame(offset, 100, "tenant", &request(), now_ms());
            let mut compressed = body[..FRAME_PREFIX_LEN].to_vec();
            compressed
                .extend_from_slice(&zstd::bulk::compress(&body[FRAME_PREFIX_LEN..], 1).unwrap());
            file.write_all(&(compressed.len() as u32).to_le_bytes())
                .unwrap();
            file.write_all(&crc32fast::hash(&compressed).to_le_bytes())
                .unwrap();
            file.write_all(&compressed).unwrap();
        }
        drop(log);
        let (mut log, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(recovered.len(), 2);
        log.append(3, 101, "tenant", &request()).unwrap();
        drop(log);
        let (_, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(
            recovered
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        fs::remove_dir_all(root).unwrap();
    }

    // A frame of the first two versions, before compression.
    fn uncompressed_frame(
        offset: i64,
        timestamp_ms: i64,
        tenant: &str,
        request: &DecodedRequest,
        ingested_ms: i64,
    ) -> Vec<u8> {
        let mut body = Vec::new();
        put_i64(&mut body, offset);
        put_i64(&mut body, timestamp_ms);
        put_i64(&mut body, ingested_ms);
        put_string(&mut body, tenant).unwrap();
        encode_request(&mut body, request).unwrap();
        body
    }

    fn request() -> DecodedRequest {
        DecodedRequest {
            source: 2,
            metadata: vec![cortexpb::MetricMetadata {
                r#type: 1,
                metric_family_name: "requests".into(),
                help: "help".into(),
                unit: "seconds".into(),
            }],
            series: vec![DecodedSeries {
                labels: vec![
                    ("__name__".into(), "requests".into()),
                    ("job".into(), "api".into()),
                ],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: 123,
                    value: f64::from_bits(0x7ff0_0000_0000_0002),
                }],
                histograms: vec![cortexpb::Histogram {
                    count: Some(cortexpb::histogram::Count::CountInt(3)),
                    sum: 4.5,
                    timestamp: 124,
                    ..Default::default()
                }],
                exemplars: vec![cortexpb::Exemplar {
                    labels: vec![cortexpb::LabelPair {
                        name: b"trace_id".to_vec().into(),
                        value: b"abc".to_vec().into(),
                    }],
                    value: 1.5,
                    timestamp_ms: 122,
                }],
                created_timestamp: 100,
            }],
        }
    }

    fn temporary_directory(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "mimir-rust-segment-{name}-{}-{}",
            std::process::id(),
            now_ms()
        ));
        fs::create_dir_all(&path).expect("create test directory");
        path
    }

    fn benchmark_request(record: u64) -> DecodedRequest {
        let mut series = Vec::with_capacity(350);
        for index in 0..350_u64 {
            let id = record * 350 + index;
            let mut labels = Vec::with_capacity(19);
            labels.push((
                "__name__".into(),
                format!("metric_{:016x}{:017x}", mix(id), mix(id + 1)),
            ));
            for label in 1..19_u64 {
                labels.push((
                    format!("label_{:04x}", (id * 19 + label) % 2500),
                    format!(
                        "value_{:011x}",
                        mix(id * 19 + label) & 0x0000_00ff_ffff_ffff
                    ),
                ));
            }
            series.push(DecodedSeries {
                labels: labels
                    .into_iter()
                    .map(|(name, value)| (name.into(), value.into()))
                    .collect(),
                samples: vec![
                    cortexpb::Sample {
                        timestamp_ms: 1_800_000_000_000 + record as i64,
                        value: f64::from_bits(
                            0x3ff0_0000_0000_0000 | (mix(id) & 0x000f_ffff_ffff_ffff),
                        ),
                    },
                    cortexpb::Sample {
                        timestamp_ms: 1_800_000_000_001 + record as i64,
                        value: f64::from_bits(
                            0x3ff0_0000_0000_0000 | (mix(id + 1) & 0x000f_ffff_ffff_ffff),
                        ),
                    },
                ],
                histograms: Vec::new(),
                exemplars: Vec::new(),
                created_timestamp: 0,
            });
        }
        DecodedRequest {
            source: 2,
            series,
            metadata: Vec::new(),
        }
    }

    fn mix(mut value: u64) -> u64 {
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn process_cpu_seconds() -> f64 {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        assert_eq!(
            unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
            0
        );
        let usage = unsafe { usage.assume_init() };
        (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) as f64
            + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1_000_000.0
    }

    #[test]
    #[ignore = "manual disk benchmark; run with --ignored --nocapture"]
    fn benchmark_high_cardinality_segments() {
        const RECORDS: u64 = 100;
        let requests: Vec<_> = (0..RECORDS).map(benchmark_request).collect();
        let root = temporary_directory("disk-benchmark");
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        let start = Instant::now();
        let write_cpu = process_cpu_seconds();
        for (offset, request) in requests.iter().enumerate() {
            log.append(offset as i64, 1_800_000_000_000, "tenant", request)
                .unwrap();
        }
        log.flush().unwrap();
        let elapsed = start.elapsed();
        let write_cpu = process_cpu_seconds() - write_cpu;
        let bytes: u64 = segment_paths(&log.directory)
            .unwrap()
            .iter()
            .map(|path| fs::metadata(path).unwrap().len())
            .sum();
        println!(
            "records={RECORDS} series={} bytes={bytes} bytes_per_record={} write_ms={} write_cpu_ms={:.1} records_per_second={:.1}",
            RECORDS * 350,
            bytes / RECORDS,
            elapsed.as_millis(),
            write_cpu * 1000.0,
            RECORDS as f64 / elapsed.as_secs_f64()
        );
        drop(log);
        let recovery_start = Instant::now();
        let recovery_cpu = process_cpu_seconds();
        let (reopened, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        println!(
            "recovery_ms={} recovery_cpu_ms={:.1}",
            recovery_start.elapsed().as_millis(),
            (process_cpu_seconds() - recovery_cpu) * 1000.0
        );
        assert_eq!(reopened.last_offset(), Some(RECORDS as i64 - 1));
        assert_eq!(recovered.len(), RECORDS as usize);
        for (original, replayed) in requests.iter().zip(recovered.iter()) {
            assert_eq!(&replayed.request, original);
        }
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }

    // Records that vary like real ones, so zstd can train a dictionary on a few of them.
    fn varied_request(record: i64) -> DecodedRequest {
        DecodedRequest {
            source: 0,
            series: (0..20)
                .map(|index| DecodedSeries {
                    labels: [
                        ("__name__", format!("metric_{}", index % 7)),
                        ("job", format!("job-{}", (record + index) % 13)),
                        ("pod", format!("pod-{}-{index}", record % 50)),
                    ]
                    .into_iter()
                    .map(|(name, value)| (name.into(), value.into()))
                    .collect(),
                    samples: vec![cortexpb::Sample {
                        timestamp_ms: 1_800_000_000_000 + record * 15_000,
                        value: (record * 31 + index) as f64 / 7.0,
                    }],
                    histograms: Vec::new(),
                    exemplars: Vec::new(),
                    created_timestamp: 0,
                })
                .collect(),
            metadata: Vec::new(),
        }
    }

    fn request_bytes(request: &DecodedRequest) -> Vec<u8> {
        let mut bytes = Vec::new();
        encode_request(&mut bytes, request).unwrap();
        bytes
    }

    /// Each frame of a segment file: its offset and the dictionary id its payload names.
    fn frames_of(path: &Path) -> Vec<(i64, Option<u32>)> {
        let data = fs::read(path).unwrap();
        let mut frames = Vec::new();
        let mut at = FILE_HEADER_LEN;
        while at + FRAME_HEADER_LEN <= data.len() {
            let length = u32::from_le_bytes(data[at..at + 4].try_into().unwrap()) as usize;
            let body = &data[at + FRAME_HEADER_LEN..at + FRAME_HEADER_LEN + length];
            let offset = i64::from_le_bytes(body[..8].try_into().unwrap());
            let dictionary = (offset != DICTIONARY_OFFSET)
                .then(|| zstd::zstd_safe::get_dict_id_from_frame(&body[FRAME_PREFIX_LEN..]))
                .flatten()
                .map(u32::from);
            frames.push((offset, dictionary));
            at += FRAME_HEADER_LEN + length;
        }
        frames
    }

    /// Appends varied records from `from` until the current file compresses with its dictionary,
    /// then `after` more, and returns the next offset.
    fn append_past_dictionary(
        log: &mut SegmentLog,
        from: i64,
        after: i64,
        ingested_ms: i64,
    ) -> i64 {
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        let mut offset = from;
        loop {
            let ready = matches!(
                log.current.as_ref().map(|current| &current.dictionary),
                Some(Dictionary::Ready { .. })
            );
            if ready {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "no dictionary after {offset} records"
            );
            log.append_at(
                offset,
                offset,
                "tenant",
                &varied_request(offset),
                ingested_ms,
            )
            .unwrap();
            offset += 1;
        }
        for _ in 0..after {
            log.append_at(
                offset,
                offset,
                "tenant",
                &varied_request(offset),
                ingested_ms,
            )
            .unwrap();
            offset += 1;
        }
        offset
    }

    fn open_segment(directory: &Path) -> PathBuf {
        segment_paths(directory)
            .unwrap()
            .into_iter()
            .find(|path| path.extension().is_some_and(|value| value == "open"))
            .unwrap()
    }

    #[test]
    fn frames_after_a_files_dictionary_replay_as_written() {
        let root = temporary_directory("dictionary");
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        log.dictionary_frames = 50;
        let now = now_ms();
        let end = append_past_dictionary(&mut log, 1, 100, now);
        let path = open_segment(&log.directory);
        drop(log);
        let frames = frames_of(&path);
        // One dictionary frame, before every frame that names it, and those after all do.
        let at = frames
            .iter()
            .position(|(offset, _)| *offset == DICTIONARY_OFFSET)
            .expect("dictionary frame");
        assert_eq!(
            frames
                .iter()
                .filter(|(offset, _)| *offset == DICTIONARY_OFFSET)
                .count(),
            1
        );
        assert!(
            frames[..at]
                .iter()
                .all(|(_, dictionary)| dictionary.is_none())
        );
        assert!(at >= 50, "trained on the first 50 frames");
        let id = frames[at + 1]
            .1
            .expect("frames after it use the dictionary");
        assert!(
            frames[at + 1..]
                .iter()
                .all(|(_, dictionary)| *dictionary == Some(id))
        );
        let (mut log, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(
            recovered
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            (1..end).collect::<Vec<_>>()
        );
        for record in &recovered {
            assert_eq!(record.kafka_timestamp_ms, record.offset);
            assert_eq!(
                request_bytes(&record.request),
                request_bytes(&varied_request(record.offset))
            );
        }
        // A restart continues in a new file, which trains its own dictionary.
        log.dictionary_frames = 50;
        let next = append_past_dictionary(&mut log, end, 10, now);
        drop(log);
        let (_, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(
            recovered
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            (1..next).collect::<Vec<_>>()
        );
        fs::remove_dir_all(root).unwrap();
    }

    // The small records the Rust ingester wrote the Go port's version 4 fixture with.
    fn tiny_request(offset: i64) -> DecodedRequest {
        DecodedRequest {
            source: (offset % 3) as i32,
            series: vec![DecodedSeries {
                labels: vec![
                    ("__name__".into(), format!("metric_{}", offset % 7).into()),
                    ("job".into(), format!("job-{}", offset % 13).into()),
                    ("pod".into(), format!("pod-{}", offset % 50).into()),
                ],
                samples: vec![cortexpb::Sample {
                    timestamp_ms: 1_800_000_000_000 + offset * 15_000,
                    value: offset as f64 / 7.0,
                }],
                histograms: if offset % 100 == 0 {
                    vec![cortexpb::Histogram {
                        count: Some(cortexpb::histogram::Count::CountInt(3)),
                        sum: 4.5,
                        timestamp: offset,
                        ..Default::default()
                    }]
                } else {
                    Vec::new()
                },
                exemplars: Vec::new(),
                created_timestamp: 0,
            }],
            metadata: if offset % 500 == 0 {
                vec![cortexpb::MetricMetadata {
                    r#type: 1,
                    metric_family_name: "metric_0".into(),
                    help: "help".into(),
                    unit: String::new(),
                }]
            } else {
                Vec::new()
            },
        }
    }

    #[test]
    fn restarts_from_version_4_files() {
        let root = temporary_directory("version-4");
        let directory = root.join(format!("cluster-0-partition-0-topic-{}", hex(b"topic")));
        fs::create_dir_all(&directory).unwrap();
        // Written by the version 4 log, with a dictionary it trained after 2000 frames.
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join(
            "../../pkg/storage/seriesstore/segment/testdata/rust-v4-00000001790780400000-0000.segment",
        );
        fs::copy(
            &fixture,
            directory.join("00000001790780400000-0000.segment"),
        )
        .unwrap();
        let (mut log, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(
            recovered
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            (1..=2050).collect::<Vec<_>>()
        );
        for record in &recovered {
            assert_eq!(record.kafka_timestamp_ms, record.offset);
            assert_eq!(record.tenant, "tenant");
            assert_eq!(
                request_bytes(&record.request),
                request_bytes(&tiny_request(record.offset))
            );
        }
        // The new log writes version 5 files next to it, which replay after it.
        let now = now_ms();
        log.begin_batch(now).unwrap();
        for offset in 2051..2060 {
            log.append_at(offset, offset, "tenant", &tiny_request(offset), now)
                .unwrap();
        }
        drop(log);
        let (_, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(recovered.len(), 2059);
        assert_eq!(
            request_bytes(&recovered[2055].request),
            request_bytes(&tiny_request(2056))
        );
        fs::remove_dir_all(root).unwrap();
    }

    // Version 5 frames refer to what their file already defined: series and metadata entries,
    // whatever tenant and hour they come from, and timestamps are deltas; replay gives back every
    // record exactly.
    #[test]
    fn version_5_files_replay_every_record_exactly() {
        let root = temporary_directory("version-5");
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        log.dictionary_frames = 40;
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let now = now_ms();
        let mut written = Vec::new();
        for offset in 1..400_i64 {
            let tenant = ["tenant", "other", "third"][(next() % 3) as usize];
            let metadata = (0..next() % 3)
                .map(|_| {
                    let family = next() % 5;
                    cortexpb::MetricMetadata {
                        r#type: (family % 4) as i32,
                        metric_family_name: format!("family_{family}"),
                        help: format!("help of {family}"),
                        unit: if family % 2 == 0 {
                            "bytes".into()
                        } else {
                            String::new()
                        },
                    }
                })
                .collect();
            let series = (0..next() % 6)
                .map(|_| {
                    let id = next() % 40;
                    let base = [0_i64, 1_800_000_000_000, -5, i64::MAX / 2][(next() % 4) as usize];
                    DecodedSeries {
                        labels: vec![
                            ("__name__".into(), format!("metric_{}", id % 7).into()),
                            ("id".into(), id.to_string().into()),
                        ],
                        samples: (0..next() % 4)
                            .map(|index| cortexpb::Sample {
                                timestamp_ms: base
                                    .wrapping_add(index as i64 * 15_000)
                                    .wrapping_sub((next() % 2_000) as i64),
                                value: f64::from_bits(next()),
                            })
                            .collect(),
                        histograms: (0..next() % 2)
                            .map(|_| cortexpb::Histogram {
                                count: Some(cortexpb::histogram::Count::CountInt(next() % 9)),
                                sum: 4.5,
                                timestamp: base,
                                ..Default::default()
                            })
                            .collect(),
                        exemplars: (0..next() % 2)
                            .map(|_| cortexpb::Exemplar {
                                labels: vec![cortexpb::LabelPair {
                                    name: b"trace_id".to_vec().into(),
                                    value: next().to_string().into_bytes().into(),
                                }],
                                value: 1.5,
                                timestamp_ms: base,
                            })
                            .collect(),
                        created_timestamp: [0, base, -(next() as i64 % 1000)]
                            [(next() % 3) as usize],
                    }
                })
                .collect();
            let request = DecodedRequest {
                source: [0, 1, 2, i32::MAX, i32::MIN][(next() % 5) as usize],
                series,
                metadata,
            };
            // Files of three hours, each defining again what the one before did.
            let ingested = now + (offset / 150) * HOUR_MS;
            let timestamp = [offset, 0, -offset, 1_800_000_000_000][(next() % 4) as usize];
            log.append_at(offset, timestamp, tenant, &request, ingested)
                .unwrap();
            written.push((offset, timestamp, tenant, request));
        }
        drop(log);
        let (_, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(recovered.len(), written.len());
        for (record, (offset, timestamp, tenant, request)) in recovered.iter().zip(&written) {
            assert_eq!(record.offset, *offset);
            assert_eq!(record.kafka_timestamp_ms, *timestamp);
            assert_eq!(record.tenant, *tenant);
            assert_eq!(
                request_bytes(&record.request),
                request_bytes(request),
                "offset {offset}"
            );
        }
        fs::remove_dir_all(root).unwrap();
    }

    // A series whose key shares its low 64 bits with another's gets its labels in every frame,
    // rather than the other's id.
    #[test]
    fn series_sharing_the_low_bits_of_their_keys_keep_their_own_labels() {
        let root = temporary_directory("key-collision");
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        let now = now_ms();
        log.begin_batch(now).unwrap();
        let series = |name: &str| DecodedSeries {
            labels: vec![("__name__".into(), name.into())],
            samples: vec![cortexpb::Sample {
                timestamp_ms: 1,
                value: 1.0,
            }],
            histograms: Vec::new(),
            exemplars: Vec::new(),
            created_timestamp: 0,
        };
        let request = DecodedRequest {
            source: 0,
            series: vec![series("first"), series("second"), series("third")],
            metadata: Vec::new(),
        };
        let keys = [(1_u128 << 64) | 7, (2_u128 << 64) | 7, 3];
        for offset in 1..4 {
            let frame = log
                .encode(offset, 1, now, "tenant", &request, &keys)
                .unwrap();
            log.append_compressed(frame).unwrap();
        }
        drop(log);
        let (_, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(recovered.len(), 3);
        for record in &recovered {
            let names = record
                .request
                .series
                .iter()
                .map(|series| series.labels[0].1.as_str().to_owned())
                .collect::<Vec<_>>();
            assert_eq!(names, ["first", "second", "third"]);
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn restarts_from_version_3_files() {
        let root = temporary_directory("version-3");
        let directory = root.join(format!("cluster-0-partition-0-topic-{}", hex(b"topic")));
        fs::create_dir_all(&directory).unwrap();
        // Written by the version 3 log: 20 records of `request()`, each with its own sample.
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("testdata/segment-v3/00000000000018000000-0000.segment");
        fs::copy(&fixture, directory.join(fixture.file_name().unwrap())).unwrap();
        let expected = |offset: i64| {
            let mut request = request();
            request.series[0].samples[0].timestamp_ms = offset * 1000;
            request.series[0].samples[0].value = offset as f64;
            request
        };
        let (mut log, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(log.last_offset(), Some(20));
        assert_eq!(recovered.len(), 20);
        for record in &recovered {
            assert_eq!(record.kafka_timestamp_ms, 1000 + record.offset);
            assert_eq!(record.ingested_ms, 5 * HOUR_MS + record.offset);
            assert_eq!(
                request_bytes(&record.request),
                request_bytes(&expected(record.offset))
            );
        }
        // The new log writes version 5 files next to it.
        log.dictionary_frames = 50;
        let end = append_past_dictionary(&mut log, 21, 5, now_ms());
        drop(log);
        let (_, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(
            recovered
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            (1..end).collect::<Vec<_>>()
        );
        assert_eq!(
            request_bytes(&recovered[4].request),
            request_bytes(&expected(5))
        );
        assert_eq!(
            request_bytes(&recovered[30].request),
            request_bytes(&varied_request(31))
        );
        fs::remove_dir_all(root).unwrap();
    }

    /// Where the dictionary frame starts in the file, and the offset of the frame before it.
    fn dictionary_frame(path: &Path) -> (usize, i64) {
        let data = fs::read(path).unwrap();
        let (mut at, mut previous) = (FILE_HEADER_LEN, None);
        loop {
            let length = u32::from_le_bytes(data[at..at + 4].try_into().unwrap()) as usize;
            let body = at + FRAME_HEADER_LEN;
            let offset = i64::from_le_bytes(data[body..body + 8].try_into().unwrap());
            if offset == DICTIONARY_OFFSET {
                return (at, previous.expect("frames before the dictionary"));
            }
            previous = Some(offset);
            at = body + length;
        }
    }

    #[test]
    fn truncates_a_torn_dictionary_frame_and_resumes() {
        let root = temporary_directory("torn-dictionary");
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        log.dictionary_frames = 50;
        let now = now_ms();
        let end = append_past_dictionary(&mut log, 1, 3, now);
        let path = open_segment(&log.directory);
        drop(log);
        // A crash in the middle of the dictionary frame loses it and every frame after, which
        // were written after the last flush.
        let (at, before) = dictionary_frame(&path);
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(at as u64 + 100)
            .unwrap();
        write_checkpoint(path.parent().unwrap(), before).unwrap();
        let (mut log, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(
            recovered
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            (1..=before).collect::<Vec<_>>()
        );
        assert_eq!(fs::metadata(&path).unwrap().len(), at as u64);
        // Consumption resumes after what the log holds.
        assert_eq!(log.last_offset(), Some(before));
        for offset in before + 1..end {
            log.append_at(offset, offset, "tenant", &varied_request(offset), now)
                .unwrap();
        }
        drop(log);
        let (_, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(
            recovered
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            (1..end).collect::<Vec<_>>()
        );
        for record in &recovered {
            assert_eq!(
                request_bytes(&record.request),
                request_bytes(&varied_request(record.offset))
            );
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_frames_whose_dictionary_the_file_lacks() {
        let root = temporary_directory("missing-dictionary");
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        log.dictionary_frames = 50;
        let hour = hour_start(now_ms());
        append_past_dictionary(&mut log, 1, 5, hour);
        log.seal_current().unwrap();
        drop(log);
        let path = root
            .join(format!("cluster-0-partition-0-topic-{}", hex(b"topic")))
            .join(segment_name(hour, 0, "segment"));
        let data = fs::read(&path).unwrap();
        // Cut the dictionary frame out: the frames around it are well formed, so only its
        // absence shows.
        let (start, _) = dictionary_frame(&path);
        let length = u32::from_le_bytes(data[start..start + 4].try_into().unwrap()) as usize;
        let end = start + FRAME_HEADER_LEN + length;
        fs::write(&path, [&data[..start], &data[end..]].concat()).unwrap();
        let error = SegmentLog::open(&root, 0, "topic", 0, None)
            .err()
            .expect("frames without their dictionary must fail");
        assert!(
            format!("{error:#}").contains("needs a dictionary"),
            "{error:#}"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    #[ignore = "manual benchmark of a real segment file; run with SEGMENT_FILE=<path> --ignored --nocapture"]
    fn benchmark_real_segment() {
        let source = PathBuf::from(std::env::var("SEGMENT_FILE").expect("SEGMENT_FILE"));
        let input = temporary_directory("real-segment-input");
        let directory = input.join(format!("cluster-0-partition-0-topic-{}", hex(b"ingest")));
        fs::create_dir_all(&directory).unwrap();
        // As an open file, a copy cut short mid-frame replays up to the cut.
        let name = source
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .replace(".segment", ".open");
        fs::copy(&source, directory.join(name)).unwrap();
        let (_, records) = SegmentLog::open(&input, 0, "ingest", 0, None).unwrap();
        let output = temporary_directory("real-segment-output");
        let (mut log, _) = SegmentLog::open(&output, 0, "ingest", 0, None).unwrap();
        // The applier gets keys from each record's encoded labels, on the prepare threads.
        let keys = records
            .iter()
            .map(|record| series_keys(&record.tenant, &record.request))
            .collect::<Vec<_>>();
        let cpu = process_cpu_seconds();
        let started = Instant::now();
        for (record, keys) in records.iter().zip(&keys) {
            log.begin_batch(record.ingested_ms).unwrap();
            let frame = log
                .encode(
                    record.offset,
                    record.kafka_timestamp_ms,
                    record.ingested_ms,
                    &record.tenant,
                    &record.request,
                    keys,
                )
                .unwrap();
            log.append_compressed(frame).unwrap();
        }
        log.flush().unwrap();
        let (write_cpu, write_wall) = (process_cpu_seconds() - cpu, started.elapsed());
        let bytes: u64 = segment_paths(&log.directory)
            .unwrap()
            .iter()
            .map(|path| fs::metadata(path).unwrap().len())
            .sum();
        drop(log);
        let cpu = process_cpu_seconds();
        let started = Instant::now();
        let (_, replayed) = SegmentLog::open(&output, 0, "ingest", 0, None).unwrap();
        let (replay_cpu, replay_wall) = (process_cpu_seconds() - cpu, started.elapsed());
        assert_eq!(replayed.len(), records.len());
        for (replayed, record) in replayed.iter().zip(&records) {
            assert_eq!(replayed.offset, record.offset);
            // As bytes: stale markers are NaN, which never equal themselves.
            let bytes = |request: &DecodedRequest| {
                let mut bytes = Vec::new();
                encode_request(&mut bytes, request).unwrap();
                bytes
            };
            assert_eq!(bytes(&replayed.request), bytes(&record.request));
        }
        println!(
            "records={} bytes={bytes} write_cpu_s={write_cpu:.2} write_s={:.2} replay_cpu_s={replay_cpu:.2} replay_s={:.2}",
            records.len(),
            write_wall.as_secs_f64(),
            replay_wall.as_secs_f64(),
        );
        fs::remove_dir_all(input).unwrap();
        fs::remove_dir_all(output).unwrap();
    }

    #[test]
    fn round_trip_and_resume_offset() {
        let root = temporary_directory("round-trip");
        let (mut log, recovered) = SegmentLog::open(&root, 0, "topic", 7, None).unwrap();
        assert!(recovered.is_empty());
        log.append(41, 500, "tenant-a", &request()).unwrap();
        drop(log);

        let (log, recovered) = SegmentLog::open(&root, 0, "topic", 7, None).unwrap();
        assert_eq!(log.last_offset(), Some(41));
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].offset, 41);
        assert_eq!(recovered[0].kafka_timestamp_ms, 500);
        assert!(recovered[0].ingested_ms > 0);
        assert_eq!(recovered[0].tenant, "tenant-a");
        let expected = request();
        assert_eq!(recovered[0].request.source, expected.source);
        assert_eq!(recovered[0].request.metadata, expected.metadata);
        assert_eq!(
            recovered[0].request.series[0].labels,
            expected.series[0].labels
        );
        assert_eq!(
            recovered[0].request.series[0].samples[0].value.to_bits(),
            expected.series[0].samples[0].value.to_bits()
        );
        assert_eq!(
            recovered[0].request.series[0].histograms,
            expected.series[0].histograms
        );
        assert_eq!(
            recovered[0].request.series[0].exemplars,
            expected.series[0].exemplars
        );
        drop(log);
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 7, None).unwrap();
        log.append(42, 501, "tenant-a", &request()).unwrap();
        drop(log);
        let (_, recovered) = SegmentLog::open(&root, 0, "topic", 7, None).unwrap();
        assert_eq!(
            recovered
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![41, 42]
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn replays_records_incrementally_after_restart() {
        let root = temporary_directory("streaming-recovery");
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        for offset in 0..100 {
            log.append(offset, offset, "tenant", &request()).unwrap();
        }
        drop(log);

        let mut offsets = Vec::new();
        let recovered = SegmentLog::open_replaying(&root, 0, "topic", 0, None, 2, |record| {
            offsets.push(record.offset);
            Ok(())
        })
        .unwrap();
        assert_eq!(recovered.last_offset(), Some(99));
        assert_eq!(offsets, (0..100).collect::<Vec<_>>());
        drop(recovered);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn skips_expired_frames_without_losing_resume_offset() {
        let root = temporary_directory("expired-recovery");
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        log.append_at(1, 100, "tenant", &request(), now_ms() - 2 * HOUR_MS)
            .unwrap();
        log.append_at(2, 200, "tenant", &request(), now_ms())
            .unwrap();
        drop(log);

        let mut offsets = Vec::new();
        let recovered =
            SegmentLog::open_replaying(&root, 0, "topic", 0, Some(HOUR_MS), 2, |record| {
                offsets.push(record.offset);
                Ok(())
            })
            .unwrap();
        assert_eq!(offsets, vec![2]);
        assert_eq!(recovered.last_offset(), Some(2));
        drop(recovered);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn upgrades_open_legacy_segment_without_losing_records() {
        let root = temporary_directory("legacy-upgrade");
        let (log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        let now = now_ms();
        let hour = hour_start(now);
        let body = uncompressed_frame(1, 100, "tenant", &request(), now);
        let path = log.directory.join(format!("{hour:020}.open"));
        let mut file = File::create(&path).unwrap();
        file.write_all(FILE_MAGIC).unwrap();
        file.write_all(&LEGACY_FILE_VERSION.to_le_bytes()).unwrap();
        file.write_all(&0_u32.to_le_bytes()).unwrap();
        file.write_all(&0_i32.to_le_bytes()).unwrap();
        file.write_all(&hour.to_le_bytes()).unwrap();
        file.write_all(&(body.len() as u32).to_le_bytes()).unwrap();
        file.write_all(&crc32fast::hash(&body).to_le_bytes())
            .unwrap();
        file.write_all(&body).unwrap();
        drop(file);
        drop(log);

        let (mut log, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(recovered.len(), 1);
        log.append_at(2, 200, "tenant", &request(), now).unwrap();
        assert!(log.directory.join(format!("{hour:020}.segment")).exists());
        drop(log);

        let (log, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(log.last_offset(), Some(2));
        assert_eq!(
            recovered
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        drop(log);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn seals_completed_hours() {
        let root = temporary_directory("rotate");
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        log.append_at(1, 100, "tenant", &request(), HOUR_MS)
            .unwrap();
        log.append_at(2, 200, "tenant", &request(), 2 * HOUR_MS)
            .unwrap();
        let paths = segment_paths(&log.directory).unwrap();
        assert!(paths.iter().any(|path| {
            segment_id(path).map(|(hour, _)| hour) == Some(HOUR_MS)
                && path.extension().is_some_and(|value| value == "segment")
        }));
        assert!(paths.iter().any(|path| {
            segment_id(path).map(|(hour, _)| hour) == Some(2 * HOUR_MS)
                && path.extension().is_some_and(|value| value == "open")
        }));
        drop(log);
        let (_, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(recovered.len(), 2);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn truncates_torn_open_frame() {
        let root = temporary_directory("torn");
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        log.append(1, 100, "tenant", &request()).unwrap();
        let open_path = segment_paths(&log.directory)
            .unwrap()
            .into_iter()
            .find(|path| path.extension().is_some_and(|value| value == "open"))
            .unwrap();
        drop(log);
        let valid_len = fs::metadata(&open_path).unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&open_path).unwrap();
        file.write_all(&100_u32.to_le_bytes()).unwrap();
        file.write_all(&0_u32.to_le_bytes()).unwrap();
        file.write_all(b"partial").unwrap();
        drop(file);

        let (_, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(fs::metadata(open_path).unwrap().len(), valid_len);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn truncates_zero_filled_open_tail_and_resumes() {
        let root = temporary_directory("zero-tail");
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        log.append(1, 100, "tenant", &request()).unwrap();
        log.append(2, 101, "tenant", &request()).unwrap();
        let open_path = segment_paths(&log.directory)
            .unwrap()
            .into_iter()
            .find(|path| path.extension().is_some_and(|value| value == "open"))
            .unwrap();
        drop(log);
        let valid_len = fs::metadata(&open_path).unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&open_path).unwrap();
        file.write_all(&[0; 1458]).unwrap();
        drop(file);

        let (mut log, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(log.last_offset(), Some(2));
        assert_eq!(fs::metadata(&open_path).unwrap().len(), valid_len);
        log.append(3, 102, "tenant", &request()).unwrap();
        drop(log);

        let (log, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(
            recovered
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        drop(log);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_zero_filled_sealed_tail() {
        let root = temporary_directory("zero-tail-sealed");
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        log.append_at(1, 100, "tenant", &request(), HOUR_MS)
            .unwrap();
        log.seal_current().unwrap();
        let sealed = log.directory.join(segment_name(HOUR_MS, 0, "segment"));
        drop(log);
        let mut file = OpenOptions::new().append(true).open(&sealed).unwrap();
        file.write_all(&[0; 64]).unwrap();
        drop(file);

        let error = SegmentLog::open(&root, 0, "topic", 0, None)
            .err()
            .expect("sealed zero tail must fail");
        assert!(error.to_string().contains("is corrupt"), "{error}");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn opens_at_matching_checkpoint_without_replay() {
        let root = temporary_directory("checkpoint-open");
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        log.append(5, 100, "tenant", &request()).unwrap();
        drop(log);
        assert!(
            SegmentLog::open_at_checkpoint(&root, 0, "topic", 0, None, Some(4))
                .unwrap()
                .is_none()
        );
        let mut log = SegmentLog::open_at_checkpoint(&root, 0, "topic", 0, None, Some(5))
            .unwrap()
            .unwrap();
        assert_eq!(log.last_offset(), Some(5));
        log.append(6, 101, "tenant", &request()).unwrap();
        drop(log);
        let (log, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(log.last_offset(), Some(6));
        assert_eq!(
            recovered
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![5, 6]
        );
        drop(log);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn removes_expired_segment_without_new_records() {
        let root = temporary_directory("idle-retention");
        let (mut log, _) = SegmentLog::open(&root, 0, "topic", 0, Some(HOUR_MS)).unwrap();
        log.append_at(1, 100, "tenant", &request(), HOUR_MS)
            .unwrap();
        assert_eq!(segment_paths(&log.directory).unwrap().len(), 1);

        log.maintain().unwrap();
        assert!(segment_paths(&log.directory).unwrap().is_empty());
        assert_eq!(log.last_offset(), Some(1));
        drop(log);

        let (log, recovered) = SegmentLog::open(&root, 0, "topic", 0, Some(HOUR_MS)).unwrap();
        assert_eq!(log.last_offset(), Some(1));
        assert!(recovered.is_empty());
        drop(log);
        fs::remove_dir_all(root).unwrap();
    }
}
