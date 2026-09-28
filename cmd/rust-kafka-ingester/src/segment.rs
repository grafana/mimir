use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use prost::Message;
use rayon::ThreadPoolBuilder;
use rayon::prelude::*;

use crate::proto::cortexpb;
use crate::record::{DecodedRequest, DecodedSeries};

const FILE_MAGIC: &[u8; 8] = b"MIMIRH01";
const CHECKPOINT_MAGIC: &[u8; 8] = b"MIMIRCP1";
const LEGACY_FILE_VERSION: u32 = 1;
const FILE_FORMAT_VERSION: u32 = 2;
const CHECKPOINT_VERSION: u32 = 1;
const FILE_HEADER_LEN: usize = 28;
const FRAME_HEADER_LEN: usize = 8;
const FRAME_PREFIX_LEN: usize = 24;
const HOUR_MS: i64 = 60 * 60 * 1000;

pub struct RecoveredRecord {
    pub offset: i64,
    pub kafka_timestamp_ms: i64,
    pub ingested_ms: i64,
    pub tenant: String,
    pub request: DecodedRequest,
}

struct CurrentFile {
    hour: i64,
    file: File,
}

pub struct SegmentLog {
    directory: PathBuf,
    cluster: u32,
    partition: i32,
    retention_ms: Option<i64>,
    current: Option<CurrentFile>,
    last_offset: Option<i64>,
    pending_offset: Option<i64>,
}

pub struct PreparedFrame {
    offset: i64,
    ingested_ms: i64,
    body: Vec<u8>,
}

thread_local! {
    // Frames hold one Kafka record of a few KiB, so setting up a context per frame cost more
    // than compressing it.
    static COMPRESSOR: std::cell::RefCell<zstd::bulk::Compressor<'static>> =
        std::cell::RefCell::new(zstd::bulk::Compressor::new(1).expect("zstd compressor"));
}

/// A frame whose payload is already zstd-compressed, so compression can run off the thread that
/// owns the log.
pub struct CompressedFrame {
    offset: i64,
    ingested_ms: i64,
    body: Vec<u8>,
}

impl CompressedFrame {
    pub fn offset(&self) -> i64 {
        self.offset
    }
}

impl PreparedFrame {
    pub fn uncompressed_body(&self) -> &[u8] {
        &self.body
    }

    pub fn compress(self) -> Result<CompressedFrame> {
        let raw = self.body;
        if raw.len() < 24 || raw.len() > 128 * 1024 * 1024 {
            bail!("segment frame has invalid uncompressed size");
        }
        let compressed = COMPRESSOR
            .with_borrow_mut(|compressor| compressor.compress(&raw[24..]))
            .context("compress segment frame")?;
        let mut body = Vec::with_capacity(24 + compressed.len());
        body.extend_from_slice(&raw[..24]);
        body.extend_from_slice(&compressed);
        if body.len() > 128 * 1024 * 1024 {
            bail!("compressed segment frame exceeds 128 MiB");
        }
        Ok(CompressedFrame {
            offset: self.offset,
            ingested_ms: self.ingested_ms,
            body,
        })
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
            (segment_hour(path), order)
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
            last_offset,
            pending_offset: None,
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
            last_offset: checkpoint,
            pending_offset: None,
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
        let frame = self.prepare_at(offset, timestamp_ms, tenant, request, now_ms())?;
        self.append_prepared(frame)
    }

    fn prepare_at(
        &self,
        offset: i64,
        timestamp_ms: i64,
        tenant: &str,
        request: &DecodedRequest,
        ingested_ms: i64,
    ) -> Result<PreparedFrame> {
        if self.last_offset.is_some_and(|last| offset <= last) {
            bail!(
                "Kafka offset {offset} is not after persisted offset {:?}",
                self.last_offset
            );
        }
        Self::encode_frame(offset, timestamp_ms, tenant, request, ingested_ms)
    }

    /// Encodes a frame without access to the log; `append_compressed` checks its offset.
    pub fn frame(
        offset: i64,
        timestamp_ms: i64,
        tenant: &str,
        request: &DecodedRequest,
    ) -> Result<PreparedFrame> {
        Self::encode_frame(offset, timestamp_ms, tenant, request, now_ms())
    }

    fn encode_frame(
        offset: i64,
        timestamp_ms: i64,
        tenant: &str,
        request: &DecodedRequest,
        ingested_ms: i64,
    ) -> Result<PreparedFrame> {
        let mut body = Vec::new();
        put_i64(&mut body, offset);
        put_i64(&mut body, timestamp_ms);
        put_i64(&mut body, ingested_ms);
        put_string(&mut body, tenant)?;
        encode_request(&mut body, request)?;
        Ok(PreparedFrame {
            offset,
            ingested_ms,
            body,
        })
    }

    pub fn append_prepared(&mut self, frame: PreparedFrame) -> Result<()> {
        self.append_compressed(frame.compress()?)
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
        let hour = hour_start(frame.ingested_ms);
        self.rotate(hour)?;
        let body = frame.body;
        let length = u32::try_from(body.len()).context("segment frame exceeds 4 GiB")?;
        let checksum = crc32fast::hash(&body);
        let current = self.current.as_mut().context("segment file is not open")?;
        current.file.write_all(&length.to_le_bytes())?;
        current.file.write_all(&checksum.to_le_bytes())?;
        current.file.write_all(&body)?;
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
        self.flush()?;
        self.seal_current()?;
        let mut recovered_current = None;
        for path in segment_paths(&self.directory)? {
            if path.extension().is_some_and(|value| value == "open") {
                if segment_hour(&path) == Some(hour) {
                    if read_file_version(&path)? == LEGACY_FILE_VERSION {
                        let legacy = path.with_extension("legacy");
                        if legacy.exists() {
                            bail!("refusing to overwrite legacy segment {}", legacy.display());
                        }
                        fs::rename(&path, &legacy)
                            .with_context(|| format!("seal legacy segment {}", path.display()))?;
                        sync_directory(&self.directory)?;
                    } else {
                        recovered_current = Some(path);
                    }
                    continue;
                }
                let to = path.with_extension("segment");
                if to.exists() {
                    bail!("refusing to overwrite sealed segment {}", to.display());
                }
                fs::rename(&path, &to)
                    .with_context(|| format!("seal recovered segment {}", path.display()))?;
            }
        }
        self.remove_expired()?;
        let path =
            recovered_current.unwrap_or_else(|| self.directory.join(segment_name(hour, "open")));
        let new_file = !path.exists();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .with_context(|| format!("open segment {}", path.display()))?;
        if new_file {
            write_file_header(&mut file, self.cluster, self.partition, hour)?;
            file.sync_all()?;
            sync_directory(&self.directory)?;
        }
        self.current = Some(CurrentFile { hour, file });
        Ok(())
    }

    fn seal_current(&mut self) -> Result<()> {
        if let Some(current) = self.current.take() {
            current.file.sync_all()?;
            let from = self.directory.join(segment_name(current.hour, "open"));
            let to = self.directory.join(segment_name(current.hour, "segment"));
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
            let Some(hour) = segment_hour(&path) else {
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
    if version != LEGACY_FILE_VERSION && version != FILE_FORMAT_VERSION {
        bail!("segment {} has unsupported format version", path.display());
    }
    if cursor.u32()? != expected_cluster || cursor.i32()? != expected_partition {
        bail!("segment {} belongs to another Kafka source", path.display());
    }
    let header_hour = cursor.i64()?;
    if segment_hour(path) != Some(header_hour) {
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
    loop {
        let mut frame_header = [0; FRAME_HEADER_LEN];
        match reader.read(&mut frame_header[..1]) {
            Ok(0) => break,
            Ok(_) => {}
            Err(error) => {
                replay_batch(
                    &decode_pool,
                    version,
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
                version,
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
                version,
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
                version,
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
                version,
                path,
                &mut frames,
                &mut spare,
                replay,
                &mut last_offset,
            )?;
            return finish_torn(path, reader, mutable, last_offset, valid_len);
        }
        if let Some(cutoff) = replay_cutoff {
            if body.len() >= 24 {
                let ingested_ms = i64::from_le_bytes(body[16..24].try_into().unwrap());
                if ingested_ms < cutoff {
                    replay_batch(
                        &decode_pool,
                        version,
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
        let decoded_size = if version == FILE_FORMAT_VERSION {
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
                version,
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
                version,
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
        version,
        path,
        &mut frames,
        &mut spare,
        replay,
        &mut last_offset,
    )?;
    Ok(last_offset)
}

fn replay_batch(
    decode_pool: &rayon::ThreadPool,
    version: u32,
    path: &Path,
    frames: &mut Vec<(u64, Vec<u8>)>,
    spare: &mut Vec<Vec<u8>>,
    replay: &mut impl FnMut(RecoveredRecord) -> Result<()>,
    last_offset: &mut Option<i64>,
) -> Result<()> {
    let decoded: Vec<_> = decode_pool.install(|| {
        frames
            .par_iter()
            .map(|(at, body)| {
                let decoded;
                let bytes = if version == FILE_FORMAT_VERSION {
                    if body.len() < 24 {
                        bail!(
                            "compressed segment frame is too short in {}",
                            path.display()
                        );
                    }
                    let payload = zstd::bulk::decompress(&body[24..], 128 * 1024 * 1024)
                        .with_context(|| {
                            format!("decompress segment frame in {}", path.display())
                        })?;
                    decoded = [&body[..24], &payload].concat();
                    decoded.as_slice()
                } else {
                    body.as_slice()
                };
                decode_frame(bytes).with_context(|| {
                    format!("decode segment frame at byte {at} in {}", path.display())
                })
            })
            .collect()
    });
    // Replaying in file order preserves offset and duplicate handling across batches.
    for ((_, body), record) in frames.drain(..).zip(decoded) {
        spare.push(body);
        let record = record?;
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

fn encode_request(bytes: &mut Vec<u8>, request: &DecodedRequest) -> Result<()> {
    put_i32(bytes, request.source);
    put_len(bytes, request.metadata.len())?;
    for metadata in &request.metadata {
        put_i32(bytes, metadata.r#type);
        put_string(bytes, &metadata.metric_family_name)?;
        put_string(bytes, &metadata.help)?;
        put_string(bytes, &metadata.unit)?;
    }
    put_len(bytes, request.series.len())?;
    for series in &request.series {
        put_len(bytes, series.labels.len())?;
        for (name, value) in &series.labels {
            put_string(bytes, name)?;
            put_string(bytes, value)?;
        }
        put_i64(bytes, series.created_timestamp);
        put_len(bytes, series.samples.len())?;
        for sample in &series.samples {
            put_i64(bytes, sample.timestamp_ms);
            put_u64(bytes, sample.value.to_bits());
        }
        put_messages(bytes, &series.histograms)?;
        put_messages(bytes, &series.exemplars)?;
    }
    Ok(())
}

fn decode_request(cursor: &mut Cursor<'_>) -> Result<DecodedRequest> {
    let source = cursor.i32()?;
    let metadata = cursor.items(|cursor| {
        Ok(cortexpb::MetricMetadata {
            r#type: cursor.i32()?,
            metric_family_name: cursor.string()?,
            help: cursor.string()?,
            unit: cursor.string()?,
        })
    })?;
    let series = cursor.items(|cursor| {
        let labels = cursor.items(|cursor| Ok((cursor.string()?, cursor.string()?)))?;
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
    })?;
    Ok(DecodedRequest {
        source,
        series,
        metadata,
    })
}

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

fn read_file_version(path: &Path) -> Result<u32> {
    let mut header = [0; 12];
    File::open(path)?.read_exact(&mut header)?;
    if &header[..8] != FILE_MAGIC {
        bail!("segment {} has invalid magic", path.display());
    }
    Ok(u32::from_le_bytes(header[8..12].try_into().unwrap()))
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

fn segment_name(hour: i64, extension: &str) -> String {
    format!("{hour:020}.{extension}")
}

fn segment_hour(path: &Path) -> Option<i64> {
    path.file_stem()?.to_str()?.parse().ok()
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

fn put_len(bytes: &mut Vec<u8>, length: usize) -> Result<()> {
    let length = u32::try_from(length).context("segment collection exceeds u32")?;
    bytes.extend_from_slice(&length.to_le_bytes());
    Ok(())
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    put_len(bytes, value.len())?;
    bytes.extend_from_slice(value);
    Ok(())
}

fn put_string(bytes: &mut Vec<u8>, value: &str) -> Result<()> {
    put_bytes(bytes, value.as_bytes())
}

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

    fn string(&mut self) -> Result<String> {
        let length = self.u32()? as usize;
        Ok(std::str::from_utf8(self.take(length)?)?.to_owned())
    }

    fn items<T>(&mut self, mut decode: impl FnMut(&mut Self) -> Result<T>) -> Result<Vec<T>> {
        let length = self.u32()? as usize;
        (0..length).map(|_| decode(self)).collect()
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
    fn frames_compressed_on_one_thread_record_their_size_and_decode() {
        // Recovery batches frames by the size each one records.
        for _ in 0..3 {
            let frame = SegmentLog::frame(7, 1, "tenant", &request()).unwrap();
            let raw = frame.uncompressed_body()[24..].to_vec();
            let compressed = frame.compress().unwrap();
            assert_eq!(
                zstd::zstd_safe::get_frame_content_size(&compressed.body[24..]).unwrap(),
                Some(raw.len() as u64)
            );
            assert_eq!(
                zstd::bulk::decompress(&compressed.body[24..], raw.len()).unwrap(),
                raw
            );
        }
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
                labels,
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
        let expired = log
            .prepare_at(1, 100, "tenant", &request(), now_ms() - 2 * HOUR_MS)
            .unwrap();
        log.append_prepared(expired).unwrap();
        let fresh = log
            .prepare_at(2, 200, "tenant", &request(), now_ms())
            .unwrap();
        log.append_prepared(fresh).unwrap();
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
        let frame = log.prepare_at(1, 100, "tenant", &request(), now).unwrap();
        let path = log.directory.join(segment_name(hour, "open"));
        let mut file = File::create(&path).unwrap();
        file.write_all(FILE_MAGIC).unwrap();
        file.write_all(&LEGACY_FILE_VERSION.to_le_bytes()).unwrap();
        file.write_all(&0_u32.to_le_bytes()).unwrap();
        file.write_all(&0_i32.to_le_bytes()).unwrap();
        file.write_all(&hour.to_le_bytes()).unwrap();
        file.write_all(&(frame.body.len() as u32).to_le_bytes())
            .unwrap();
        file.write_all(&crc32fast::hash(&frame.body).to_le_bytes())
            .unwrap();
        file.write_all(&frame.body).unwrap();
        drop(file);
        drop(log);

        let (mut log, recovered) = SegmentLog::open(&root, 0, "topic", 0, None).unwrap();
        assert_eq!(recovered.len(), 1);
        let second = log.prepare_at(2, 200, "tenant", &request(), now).unwrap();
        log.append_prepared(second).unwrap();
        assert!(log.directory.join(segment_name(hour, "legacy")).exists());
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
        let first = log
            .prepare_at(1, 100, "tenant", &request(), HOUR_MS)
            .unwrap();
        log.append_prepared(first).unwrap();
        let second = log
            .prepare_at(2, 200, "tenant", &request(), 2 * HOUR_MS)
            .unwrap();
        log.append_prepared(second).unwrap();
        let paths = segment_paths(&log.directory).unwrap();
        assert!(paths.iter().any(|path| {
            segment_hour(path) == Some(HOUR_MS)
                && path.extension().is_some_and(|value| value == "segment")
        }));
        assert!(paths.iter().any(|path| {
            segment_hour(path) == Some(2 * HOUR_MS)
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
        let frame = log
            .prepare_at(1, 100, "tenant", &request(), HOUR_MS)
            .unwrap();
        log.append_prepared(frame).unwrap();
        log.seal_current().unwrap();
        let sealed = log.directory.join(segment_name(HOUR_MS, "segment"));
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
        let frame = log
            .prepare_at(1, 100, "tenant", &request(), HOUR_MS)
            .unwrap();
        log.append_prepared(frame).unwrap();
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
