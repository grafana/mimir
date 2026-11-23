// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

var ErrFileDecbufFactoryClosed = errors.New("file decbuf factory is closed")

const fileDecbufFactoryClosedFlag = int64(1) << 62

// FileDecbufFactory creates Decbufs for an index-header file on local disk.
// All Decbufs read the same file handle with positional reads.
type FileDecbufFactory struct {
	file    *os.File
	size    int
	openErr error

	// refs is the number of open Decbufs, with fileDecbufFactoryClosedFlag set once Close is called.
	refs      atomic.Int64
	closeOnce sync.Once
	closeErr  error
	releaseFn func()
}

func NewFileDecbufFactory(path string) *FileDecbufFactory {
	df := &FileDecbufFactory{}

	f, err := os.Open(path)
	if err != nil {
		df.openErr = errors.Wrap(err, "open file for decbuf")
		return df
	}

	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		df.openErr = errors.Wrap(err, "stat file for decbuf")
		return df
	}

	df.file = f
	df.size = int(stat.Size())
	// Bind once - passing df.release to each FileReader would allocate a new func value per Decbuf.
	df.releaseFn = df.release
	return df
}

func (df *FileDecbufFactory) acquire() error {
	if df.refs.Add(1)&fileDecbufFactoryClosedFlag != 0 {
		df.release()
		return ErrFileDecbufFactoryClosed
	}
	if df.openErr != nil {
		df.release()
		return df.openErr
	}
	return nil
}

func (df *FileDecbufFactory) release() {
	if df.refs.Add(-1) == fileDecbufFactoryClosedFlag {
		df.closeFile()
	}
}

func (df *FileDecbufFactory) closeFile() {
	df.closeOnce.Do(func() {
		if df.file != nil {
			df.closeErr = df.file.Close()
		}
	})
}

func (df *FileDecbufFactory) NewDecbufAtChecked(_ context.Context, offset int, table *crc32.Table) Decbuf {
	if err := df.acquire(); err != nil {
		return Decbuf{E: err}
	}

	// TODO: A particular index-header only has symbols and posting offsets. We should only need to read
	//  the length of each of those a single time per index-header (DecbufFactory). Should the factory
	//  cache the length? Should the table of contents be passed to the factory?
	var lenBuf [numLenBytes]byte
	n, err := df.file.ReadAt(lenBuf[:], int64(offset))
	if n != numLenBytes {
		df.release()
		if err == nil || errors.Is(err, io.EOF) {
			err = errors.Wrapf(ErrInvalidSize, "insufficient bytes read for size (got %d, wanted %d)", n, numLenBytes)
		}
		return Decbuf{E: err}
	}
	contentLength := int(binary.BigEndian.Uint32(lenBuf[:]))

	r, err := newFileReader(df.file, offset, numLenBytes+contentLength+crc32.Size, df.releaseFn)
	if err != nil {
		df.release()
		return Decbuf{E: errors.Wrap(err, "create file reader")}
	}

	d := Decbuf{r: r}

	if d.ResetAt(numLenBytes); d.Err() != nil {
		return d
	}

	if table != nil {
		if d.CheckCrc32(table); d.Err() != nil {
			return d
		}

		// reset to the beginning of the content after reading it all for the CRC.
		d.ResetAt(numLenBytes)
	}

	return d
}

func (df *FileDecbufFactory) NewDecbufAtUnchecked(ctx context.Context, offset int) Decbuf {
	return df.NewDecbufAtChecked(ctx, offset, nil)
}

func (df *FileDecbufFactory) NewDecbufInSection(_ context.Context, _, _, _ int) Decbuf {
	return Decbuf{E: fmt.Errorf("NewDecbufInSection not implemented for FileDecbufFactory")}
}

func (df *FileDecbufFactory) NewRawDecbuf(_ context.Context) Decbuf {
	if err := df.acquire(); err != nil {
		return Decbuf{E: err}
	}
	r, err := newFileReader(df.file, 0, df.size, df.releaseFn)
	if err != nil {
		df.release()
		return Decbuf{E: errors.Wrap(err, "create file reader")}
	}
	return Decbuf{r: r}
}

// Close prevents new Decbufs from being created. The underlying file handle
// is closed immediately if no Decbufs are open, or otherwise when the last one is closed.
func (df *FileDecbufFactory) Close() error {
	if df.refs.Or(fileDecbufFactoryClosedFlag) == 0 {
		df.closeFile()
		return df.closeErr
	}
	return nil
}
