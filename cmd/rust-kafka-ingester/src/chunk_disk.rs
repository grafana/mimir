use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use memmap2::MmapMut;

const FILE_SIZE: usize = 128 * 1024 * 1024;

/// Written length and newest chunk time of one chunk file, as recorded in a head snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileState {
    pub sequence: u32,
    pub written: u64,
    pub max_time: i64,
}

/// Location of a completed chunk: file sequence in the high 32 bits, byte offset in the low 32 bits.
pub type ChunkRef = u64;

struct ChunkFile {
    map: MmapMut,
    written: usize,
    max_time: i64,
}

/// Keeps completed chunks in memory-mapped files, like the Prometheus head's `chunks_head`, so only
/// open chunks stay on the heap. The files are a cache rebuilt from the segment log on startup.
pub struct ChunkDiskMapper {
    directory: Option<PathBuf>,
    files: BTreeMap<u32, ChunkFile>,
    next_sequence: u32,
}

impl ChunkDiskMapper {
    /// Removes any previous files in `directory`. Without a directory, chunks use anonymous mappings.
    pub fn open(directory: Option<PathBuf>) -> Result<Self> {
        if let Some(directory) = &directory {
            if directory.exists() {
                fs::remove_dir_all(directory)
                    .with_context(|| format!("remove chunk directory {}", directory.display()))?;
            }
            fs::create_dir_all(directory)
                .with_context(|| format!("create chunk directory {}", directory.display()))?;
        }
        Ok(Self {
            directory,
            files: BTreeMap::new(),
            next_sequence: 0,
        })
    }

    /// Maps the files described by a head snapshot and removes anything else in `directory`.
    pub fn reopen(directory: PathBuf, files: &[FileState], next_sequence: u32) -> Result<Self> {
        let mut mapped = BTreeMap::new();
        for state in files {
            let path = directory.join(file_name(state.sequence));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .with_context(|| format!("open chunk file {}", path.display()))?;
            if file.metadata()?.len() != FILE_SIZE as u64 || state.written > FILE_SIZE as u64 {
                bail!(
                    "chunk file {} does not match the head snapshot",
                    path.display()
                );
            }
            // SAFETY: the file is private to this process and only accessed through this mapping.
            let map = unsafe { MmapMut::map_mut(&file) }
                .with_context(|| format!("map chunk file {}", path.display()))?;
            mapped.insert(
                state.sequence,
                ChunkFile {
                    map,
                    written: state.written as usize,
                    max_time: state.max_time,
                },
            );
        }
        for entry in fs::read_dir(&directory)? {
            let path = entry?.path();
            let known = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.parse::<u32>().ok())
                .is_some_and(|sequence| mapped.contains_key(&sequence));
            if !known {
                fs::remove_file(&path)
                    .with_context(|| format!("remove stale chunk file {}", path.display()))?;
            }
        }
        Ok(Self {
            directory: Some(directory),
            files: mapped,
            next_sequence,
        })
    }

    pub fn directory(&self) -> Option<&PathBuf> {
        self.directory.as_ref()
    }

    pub fn state(&self) -> (Vec<FileState>, u32) {
        let files = self
            .files
            .iter()
            .map(|(sequence, file)| FileState {
                sequence: *sequence,
                written: file.written as u64,
                max_time: file.max_time,
            })
            .collect();
        (files, self.next_sequence)
    }

    /// Writes dirty mapped pages so a head snapshot never references chunk bytes that are not on disk.
    pub fn sync(&self) -> Result<()> {
        for file in self.files.values() {
            file.map.flush().context("sync chunk file")?;
        }
        Ok(())
    }

    pub fn write(&mut self, data: &[u8], max_time: i64) -> Result<ChunkRef> {
        assert!(data.len() <= FILE_SIZE, "chunk exceeds chunk file size");
        let needs_file = self
            .files
            .values()
            .next_back()
            .is_none_or(|file| file.written + data.len() > FILE_SIZE);
        if needs_file {
            self.create_file()?;
        }
        let (&sequence, file) = self
            .files
            .iter_mut()
            .next_back()
            .expect("chunk file exists");
        let offset = file.written;
        file.map[offset..offset + data.len()].copy_from_slice(data);
        file.written += data.len();
        file.max_time = file.max_time.max(max_time);
        Ok((u64::from(sequence) << 32) | offset as u64)
    }

    pub fn read(&self, reference: ChunkRef, len: u32) -> &[u8] {
        let file = &self.files[&((reference >> 32) as u32)];
        let offset = (reference & u64::from(u32::MAX)) as usize;
        &file.map[offset..offset + len as usize]
    }

    /// Deletes files whose chunks all end before `cutoff`; callers must drop references to them first.
    pub fn truncate_before(&mut self, cutoff: i64) -> Result<()> {
        let current = self.files.keys().next_back().copied();
        let expired = self
            .files
            .iter()
            .filter(|(sequence, file)| Some(**sequence) != current && file.max_time < cutoff)
            .map(|(sequence, _)| *sequence)
            .collect::<Vec<_>>();
        for sequence in expired {
            self.files.remove(&sequence);
            if let Some(directory) = &self.directory {
                let path = directory.join(file_name(sequence));
                fs::remove_file(&path)
                    .with_context(|| format!("remove chunk file {}", path.display()))?;
            }
        }
        Ok(())
    }

    fn create_file(&mut self) -> Result<()> {
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        let map = match &self.directory {
            Some(directory) => {
                let path = directory.join(file_name(sequence));
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&path)
                    .with_context(|| format!("create chunk file {}", path.display()))?;
                file.set_len(FILE_SIZE as u64)?;
                // SAFETY: the file is private to this process and only accessed through this mapping.
                unsafe { MmapMut::map_mut(&file) }
                    .with_context(|| format!("map chunk file {}", path.display()))?
            }
            None => MmapMut::map_anon(FILE_SIZE).context("map anonymous chunk file")?,
        };
        self.files.insert(
            sequence,
            ChunkFile {
                map,
                written: 0,
                max_time: i64::MIN,
            },
        );
        Ok(())
    }
}

fn file_name(sequence: u32) -> String {
    format!("{sequence:08}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_written_chunks_and_deletes_expired_files() {
        let directory =
            std::env::temp_dir().join(format!("mimir-rust-chunks-{}", std::process::id()));
        let mut mapper = ChunkDiskMapper::open(Some(directory.clone())).unwrap();
        let first = mapper.write(b"first", 10).unwrap();
        let second = mapper.write(b"second", 20).unwrap();
        assert_eq!(mapper.read(first, 5), b"first");
        assert_eq!(mapper.read(second, 6), b"second");

        let big = vec![7; FILE_SIZE - 10];
        let third = mapper.write(&big, 30).unwrap();
        assert_eq!(third >> 32, 1);
        assert!(directory.join(file_name(1)).exists());

        mapper.truncate_before(25).unwrap();
        assert!(!directory.join(file_name(0)).exists());
        assert_eq!(mapper.read(third, 3), &[7, 7, 7]);
        mapper.truncate_before(i64::MAX).unwrap();
        assert!(
            directory.join(file_name(1)).exists(),
            "current file is kept"
        );

        drop(mapper);
        ChunkDiskMapper::open(Some(directory.clone())).unwrap();
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
        fs::remove_dir_all(directory).unwrap();
    }
}
