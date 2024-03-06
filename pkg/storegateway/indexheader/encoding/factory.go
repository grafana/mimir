// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var ErrPoolStopped = errors.New("file handle pool is stopped")

type DecbufFactoryMetrics struct {
	openCount        prometheus.Counter
	pooledOpenCount  prometheus.Counter
	closeCount       prometheus.Counter
	pooledCloseCount prometheus.Counter
}

func NewDecbufFactoryMetrics(reg prometheus.Registerer) *DecbufFactoryMetrics {
	return &DecbufFactoryMetrics{
		openCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "indexheader_stream_unpooled_open_total",
			Help: "Total number of times index-header file has been opened instead of using a pooled handle.",
		}),
		pooledOpenCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "indexheader_stream_pooled_open_total",
			Help: "Total number of times a pooled index-header file handle has been used instead of opened.",
		}),
		closeCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "indexheader_stream_unpooled_close_total",
			Help: "Total number of times index-header file has been closed instead of returning the handle to the pool.",
		}),
		pooledCloseCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "indexheader_stream_pooled_close_total",
			Help: "Total number of times pooled index-header file handle has been returned to the pool instead of closed.",
		}),
	}
}

// DecbufFactory creates new file-backed decoding buffer instances for a specific index-header file.
type DecbufFactory struct {
	files *filePool
}

func NewDecbufFactory(path string, maxIdleFileHandles uint, metrics *DecbufFactoryMetrics) *DecbufFactory {
	return &DecbufFactory{
		files: newFilePool(
			path,
			maxIdleFileHandles,
			metrics.openCount,
			metrics.pooledOpenCount,
			metrics.closeCount,
			metrics.pooledCloseCount,
		),
	}
}

// NewDecbufAtChecked returns a new file-backed decoding buffer positioned at offset + 4 bytes.
// It expects the first 4 bytes after offset to hold the big endian encoded content length, followed
// by the contents and the expected checksum. This method checks the CRC of the content and will
// return an error Decbuf if it does not match the expected CRC.
func (df *DecbufFactory) NewDecbufAtChecked(offset int, table *crc32.Table) Decbuf {
	f, err := df.files.get()
	if err != nil {
		return Decbuf{E: errors.Wrap(err, "open file for decbuf")}
	}

	// If we return early and don't include a Reader for our Decbuf, we are responsible
	// for putting the file handle back in the pool.
	closeFile := true
	defer func() {
		if closeFile {
			_ = df.files.put(f)
		}
	}()

	// TODO: A particular index-header only has symbols and posting offsets. We should only need to read
	//  the length of each of those a single time per index-header (DecbufFactory). Should the factory
	//  cache the length? Should the table of contents be passed to the factory?
	lengthBytes := make([]byte, 4)
	n, err := f.ReadAt(lengthBytes, int64(offset))
	if err != nil {
		return Decbuf{E: err}
	}
	if n != 4 {
		return Decbuf{E: errors.Wrapf(ErrInvalidSize, "insufficient bytes read for size (got %d, wanted %d)", n, 4)}
	}

	contentLength := int(binary.BigEndian.Uint32(lengthBytes))
	bufferLength := len(lengthBytes) + contentLength + crc32.Size
	r, err := newFileReader(f, offset, bufferLength, df.files)
	if err != nil {
		return Decbuf{E: errors.Wrap(err, "create file reader")}
	}

	closeFile = false
	d := Decbuf{r: r}

	if d.ResetAt(4); d.Err() != nil {
		return d
	}

	if table != nil {
		if d.CheckCrc32(table); d.Err() != nil {
			return d
		}

		// reset to the beginning of the content after reading it all for the CRC.
		d.ResetAt(4)
	}

	return d
}

// NewDecbufAtUnchecked returns a new file-backed decoding buffer positioned at offset + 4 bytes.
// It expects the first 4 bytes after offset to hold the big endian encoded content length, followed
// by the contents and the expected checksum. This method does NOT compute the CRC of the content.
// To check the CRC of the content, use NewDecbufAtChecked.
func (df *DecbufFactory) NewDecbufAtUnchecked(offset int) Decbuf {
	return df.NewDecbufAtChecked(offset, nil)
}

// NewRawDecbuf returns a new file-backed decoding buffer positioned at the beginning of the file,
// spanning the entire length of the file. It does not make any assumptions about the contents of the
// file, nor does it perform any form of integrity check. To create a decoding buffer for some subset
// of the file or perform integrity checks use NewDecbufAtUnchecked or NewDecbufAtChecked.
func (df *DecbufFactory) NewRawDecbuf() Decbuf {
	f, err := df.files.get()
	if err != nil {
		return Decbuf{E: errors.Wrap(err, "open file for decbuf")}
	}

	// If we return early and don't include a Reader for our Decbuf, we are responsible
	// for putting the file handle back in the pool.
	closeFile := true
	defer func() {
		if closeFile {
			_ = df.files.put(f)
		}
	}()

	stat, err := f.Stat()
	if err != nil {
		return Decbuf{E: errors.Wrap(err, "stat file for decbuf")}
	}

	fileSize := stat.Size()
	reader, err := newFileReader(f, 0, int(fileSize), df.files)
	if err != nil {
		return Decbuf{E: errors.Wrap(err, "file reader for decbuf")}
	}

	closeFile = false
	return Decbuf{r: reader}
}

// Stop cleans up resources associated with this DecbufFactory
func (df *DecbufFactory) Stop() {
	df.files.stop()
}

// filePool maintains a pool of file handles up to a maximum number, creating
// new handles and closing them when required. get and put operations on this
// pool never block. If there are no available file handles, one will be created
// on get. If the pool is full, the file handle is closed on put.
type filePool struct {
	path    string
	handles chan *os.File
	mtx     sync.RWMutex
	stopped bool

	opens        prometheus.Counter
	pooledOpens  prometheus.Counter
	closes       prometheus.Counter
	pooledCloses prometheus.Counter
}

// newFilePool creates a new file pool for path with cap capacity. If cap is 0,
// get always opens new file handles and put always closes them immediately.
func newFilePool(path string, cap uint, opens prometheus.Counter, pooledOpens prometheus.Counter, closes prometheus.Counter, pooledCloses prometheus.Counter) *filePool {
	return &filePool{
		path: path,
		// We don't care if cap is 0 which means the channel will be unbuffered. Because
		// we have default cases for reads and writes to the channel, we will always open
		// new files and close file handles immediately if the channel is unbuffered.
		handles: make(chan *os.File, cap),

		opens:        opens,
		pooledOpens:  pooledOpens,
		closes:       closes,
		pooledCloses: pooledCloses,
	}
}

// get returns a pooled file handle if available or opens a new one if there
// are no pooled handles available. If this pool has been stopped, an error
// is returned.
func (p *filePool) get() (*os.File, error) {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	if p.stopped {
		return nil, ErrPoolStopped
	}

	select {
	case f := <-p.handles:
		p.pooledOpens.Inc()
		return f, nil
	default:
		p.opens.Inc()
		return os.Open(p.path)
	}
}

// put returns a file handle to the pool if there is space available or closes
// the file handle if there is not. If this pool has been stopped, the file handle
// is closed immediately.
func (p *filePool) put(f *os.File) error {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	if p.stopped {
		return f.Close()
	}

	select {
	case p.handles <- f:
		p.pooledCloses.Inc()
		return nil
	default:
		p.closes.Inc()
		return f.Close()
	}
}

// stop closes all pooled file handles. After this method is called, subsequent
// get calls will return an error and put calls will immediately close the file
// handle.
func (p *filePool) stop() {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.stopped = true

	for {
		select {
		case f := <-p.handles:
			_ = f.Close()
		default:
			return
		}
	}
}
