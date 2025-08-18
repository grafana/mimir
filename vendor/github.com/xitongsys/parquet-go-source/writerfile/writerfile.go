package writerfile

import (
	"io"

	"github.com/xitongsys/parquet-go/source"
)

type WriterFile struct {
	Writer io.Writer
}

func NewWriterFile(writer io.Writer) source.ParquetFile {
	return &WriterFile{Writer: writer}
}

func (self *WriterFile) Create(name string) (source.ParquetFile, error) {
	return self, nil
}

func (self *WriterFile) Open(name string) (source.ParquetFile, error) {
	return self, nil
}

func (self *WriterFile) Seek(offset int64, pos int) (int64, error) {
	return 0, nil
}

func (self *WriterFile) Read(b []byte) (int, error) {
	return 0, nil
}

func (self *WriterFile) Write(b []byte) (int, error) {
	return self.Writer.Write(b)
}

func (self *WriterFile) Close() error {
	return nil
}
