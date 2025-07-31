package block

import (
	"context"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/prometheus-community/parquet-common/storage"
)

type MultiReaderFile struct {
	file           storage.ParquetFileView
	pendingReaders *sync.WaitGroup
}

// Close implements Reader.
func (f *MultiReaderFile) Close() error {
	f.pendingReaders.Wait()
	return nil
}

func (f *MultiReaderFile) Metadata() *format.FileMetaData {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.Metadata()
}

func (f *MultiReaderFile) Schema() *parquet.Schema {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.Schema()
}

func (f *MultiReaderFile) NumRows() int64 {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.NumRows()
}

func (f *MultiReaderFile) Lookup(key string) (string, bool) {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.Lookup(key)
}

func (f *MultiReaderFile) Size() int64 {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.Size()
}

func (f *MultiReaderFile) Root() *parquet.Column {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.Root()
}

func (f *MultiReaderFile) RowGroups() []parquet.RowGroup {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.RowGroups()
}

func (f *MultiReaderFile) ColumnIndexes() []format.ColumnIndex {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.ColumnIndexes()
}

func (f *MultiReaderFile) OffsetIndexes() []format.OffsetIndex {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.OffsetIndexes()
}

func (f *MultiReaderFile) GetPages(ctx context.Context, cc parquet.ColumnChunk, minOffset, maxOffset int64) (parquet.Pages, error) {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.GetPages(ctx, cc, minOffset, maxOffset)
}

func (f *MultiReaderFile) DictionaryPageBounds(rgIdx, colIdx int) (uint64, uint64) {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.DictionaryPageBounds(rgIdx, colIdx)
}

func (f *MultiReaderFile) WithContext(ctx context.Context) storage.SizeReaderAt {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.WithContext(ctx)
}

func (f *MultiReaderFile) SkipMagicBytes() bool {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.SkipMagicBytes()
}

func (f *MultiReaderFile) SkipPageIndex() bool {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.SkipPageIndex()
}

func (f *MultiReaderFile) SkipBloomFilters() bool {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.SkipBloomFilters()
}

func (f *MultiReaderFile) OptimisticRead() bool {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.OptimisticRead()
}

func (f *MultiReaderFile) ReadBufferSize() int {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.ReadBufferSize()
}

func (f *MultiReaderFile) ReadMode() parquet.ReadMode {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.ReadMode()
}

func (f *MultiReaderFile) PagePartitioningMaxGapSize() int {
	f.pendingReaders.Add(1)
	defer f.pendingReaders.Done()
	return f.file.PagePartitioningMaxGapSize()
}
