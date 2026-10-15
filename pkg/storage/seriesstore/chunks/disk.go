// SPDX-License-Identifier: AGPL-3.0-only

// Package chunks holds the ingester's chunks: the memory-mapped files completed chunks live in, the
// appenders of open chunks, and the merging of overlapping in-order and out-of-order chunks.
package chunks

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"unsafe"

	"golang.org/x/sys/unix"
)

// FileSize is the size of every chunk file, which is mapped whole. Tests that need many files make it smaller.
var FileSize = 128 * 1024 * 1024

// FileState is the written length and newest chunk time of one chunk file, as a head snapshot
// records it.
type FileState struct {
	Sequence uint32
	Written  uint64
	MaxTime  int64
}

// Ref locates a completed chunk: file sequence in the high 32 bits, byte offset in the low 32 bits.
type Ref = uint64

type chunkFile struct {
	data    []byte
	written int
	maxTime int64
}

// DiskMapper keeps completed chunks in memory-mapped files, like the Prometheus head's
// `chunks_head`, so only open chunks stay on the heap, which the garbage collector never scans.
// The files are a cache rebuilt from the segment log on startup.
type DiskMapper struct {
	directory string
	// By sequence, nil once removed: queries resolve a file per chunk read, and sequences only grow.
	files        []*chunkFile
	first        uint32
	nextSequence uint32
	// What runs the unmaps of the files TruncateBefore removes, when they wait for their readers.
	deferUnmap func(unmap func())
}

// OpenDiskMapper removes any previous files in directory. Without a directory, chunks use
// anonymous mappings.
func OpenDiskMapper(directory string) (*DiskMapper, error) {
	if directory != "" {
		if err := os.RemoveAll(directory); err != nil {
			return nil, fmt.Errorf("remove chunk directory %s: %w", directory, err)
		}
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return nil, fmt.Errorf("create chunk directory %s: %w", directory, err)
		}
	}
	return &DiskMapper{directory: directory}, nil
}

// ReopenDiskMapper maps the files a head snapshot describes and removes anything else in directory.
func ReopenDiskMapper(directory string, files []FileState, nextSequence uint32) (*DiskMapper, error) {
	m := &DiskMapper{directory: directory, nextSequence: nextSequence}
	known := make(map[uint32]bool, len(files))
	for _, state := range files {
		path := filepath.Join(directory, fileName(state.Sequence))
		data, err := mapFile(path, false)
		if err != nil {
			_ = m.Close()
			return nil, err
		}
		if state.Written > uint64(FileSize) {
			_ = unix.Munmap(data)
			_ = m.Close()
			return nil, fmt.Errorf("chunk file %s does not match the head snapshot", path)
		}
		m.put(state.Sequence, &chunkFile{data: data, written: int(state.Written), maxTime: state.MaxTime})
		known[state.Sequence] = true
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		_ = m.Close()
		return nil, err
	}
	for _, entry := range entries {
		sequence, err := strconv.ParseUint(entry.Name(), 10, 32)
		if err == nil && known[uint32(sequence)] {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			_ = m.Close()
			return nil, fmt.Errorf("remove stale chunk file %s: %w", path, err)
		}
	}
	return m, nil
}

// Directory is where the files are, empty for anonymous mappings.
func (m *DiskMapper) Directory() string {
	return m.directory
}

// State describes the files for a head snapshot, with the next file's sequence.
func (m *DiskMapper) State() ([]FileState, uint32) {
	var files []FileState
	for index, file := range m.files {
		if file != nil {
			files = append(files, FileState{
				Sequence: m.first + uint32(index),
				Written:  uint64(file.written),
				MaxTime:  file.maxTime,
			})
		}
	}
	return files, m.nextSequence
}

// Sync writes dirty mapped pages so a head snapshot never references chunk bytes that are not on
// disk.
func (m *DiskMapper) Sync() error {
	for _, file := range m.files {
		if file == nil {
			continue
		}
		if err := unix.Msync(file.data, unix.MS_SYNC); err != nil {
			return fmt.Errorf("sync chunk file: %w", err)
		}
	}
	return nil
}

// Write appends a completed chunk ending at maxTime.
func (m *DiskMapper) Write(data []byte, maxTime int64) (Ref, error) {
	if len(data) > FileSize {
		panic("chunk exceeds chunk file size")
	}
	current := m.current()
	if current == nil || current.written+len(data) > FileSize {
		if err := m.createFile(); err != nil {
			return 0, err
		}
		current = m.current()
	}
	offset := current.written
	copy(current.data[offset:], data)
	current.written += len(data)
	current.maxTime = max(current.maxTime, maxTime)
	sequence := m.first + uint32(len(m.files)-1)
	return uint64(sequence)<<32 | uint64(offset), nil
}

// Read returns a chunk's bytes, which stay valid until TruncateBefore removes its file.
func (m *DiskMapper) Read(ref Ref, length uint32) []byte {
	file := m.files[uint32(ref>>32)-m.first]
	offset := int(ref & math.MaxUint32)
	return file.data[offset : offset+int(length) : offset+int(length)]
}

// DeferUnmaps makes the files TruncateBefore removes stay mapped until release runs what it is given: the slices Read
// returned for them stay valid for the readers that have them, which then don't copy them.
func (m *DiskMapper) DeferUnmaps(release func(unmap func())) {
	m.deferUnmap = release
}

// Holds reports whether data is in one of the mapper's files, as what Read returns is.
func (m *DiskMapper) Holds(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	at := uintptr(unsafe.Pointer(unsafe.SliceData(data)))
	for _, file := range m.files {
		if file == nil || len(file.data) == 0 {
			continue
		}
		base := uintptr(unsafe.Pointer(unsafe.SliceData(file.data)))
		if at >= base && at < base+uintptr(len(file.data)) {
			return true
		}
	}
	return false
}

// TruncateBefore deletes files whose chunks all end before cutoff; callers must drop references
// to them first, unless DeferUnmaps keeps them mapped for them.
func (m *DiskMapper) TruncateBefore(cutoff int64) error {
	var errs []error
	for index := 0; index < len(m.files)-1; index++ {
		file := m.files[index]
		if file == nil || file.maxTime >= cutoff {
			continue
		}
		m.files[index] = nil
		if m.deferUnmap != nil {
			// Removing the file is fine: the mapping keeps it until it is unmapped.
			data := file.data
			m.deferUnmap(func() { _ = unix.Munmap(data) })
		} else {
			errs = append(errs, unix.Munmap(file.data))
		}
		if m.directory != "" {
			path := filepath.Join(m.directory, fileName(m.first+uint32(index)))
			if err := os.Remove(path); err != nil {
				errs = append(errs, fmt.Errorf("remove chunk file %s: %w", path, err))
			}
		}
	}
	// Files leave from the front as they age, so the slice doesn't grow with the process lifetime.
	for len(m.files) > 1 && m.files[0] == nil {
		m.files = m.files[1:]
		m.first++
	}
	return errors.Join(errs...)
}

// Close unmaps every file, which invalidates every slice Read returned.
func (m *DiskMapper) Close() error {
	var errs []error
	for index, file := range m.files {
		if file != nil {
			errs = append(errs, unix.Munmap(file.data))
			m.files[index] = nil
		}
	}
	return errors.Join(errs...)
}

func (m *DiskMapper) current() *chunkFile {
	if len(m.files) == 0 {
		return nil
	}
	return m.files[len(m.files)-1]
}

func (m *DiskMapper) put(sequence uint32, file *chunkFile) {
	if len(m.files) == 0 {
		m.first = sequence
	}
	for m.first+uint32(len(m.files)) <= sequence {
		m.files = append(m.files, nil)
	}
	m.files[sequence-m.first] = file
}

func (m *DiskMapper) createFile() error {
	sequence := m.nextSequence
	var data []byte
	var err error
	if m.directory != "" {
		data, err = mapFile(filepath.Join(m.directory, fileName(sequence)), true)
	} else {
		data, err = unix.Mmap(-1, 0, FileSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
		if err != nil {
			err = fmt.Errorf("map anonymous chunk file: %w", err)
		}
	}
	if err != nil {
		return err
	}
	m.nextSequence++
	m.put(sequence, &chunkFile{data: data, maxTime: math.MinInt64})
	return nil
}

func mapFile(path string, create bool) ([]byte, error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE | os.O_EXCL
	}
	file, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open chunk file %s: %w", path, err)
	}
	defer file.Close()
	if create {
		if err := file.Truncate(int64(FileSize)); err != nil {
			return nil, fmt.Errorf("size chunk file %s: %w", path, err)
		}
	} else {
		info, err := file.Stat()
		if err != nil {
			return nil, err
		}
		if info.Size() != int64(FileSize) {
			return nil, fmt.Errorf("chunk file %s does not match the head snapshot", path)
		}
	}
	// The mapping outlives the descriptor.
	data, err := unix.Mmap(int(file.Fd()), 0, FileSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("map chunk file %s: %w", path, err)
	}
	return data, nil
}

func fileName(sequence uint32) string {
	return fmt.Sprintf("%08d", sequence)
}
