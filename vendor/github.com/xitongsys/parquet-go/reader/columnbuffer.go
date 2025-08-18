package reader

import (
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/source"
)

type ColumnBufferType struct {
	PFile        source.ParquetFile
	ThriftReader *thrift.TBufferedTransport

	Footer        *parquet.FileMetaData
	SchemaHandler *schema.SchemaHandler

	PathStr       string
	RowGroupIndex int64
	ChunkHeader   *parquet.ColumnChunk

	ChunkReadValues int64

	DictPage *layout.Page

	DataTable        *layout.Table
	DataTableNumRows int64
}

func NewColumnBuffer(pFile source.ParquetFile, footer *parquet.FileMetaData, schemaHandler *schema.SchemaHandler, pathStr string) (*ColumnBufferType, error) {
	newPFile, err := pFile.Open("")
	if err != nil {
		return nil, err
	}
	res := &ColumnBufferType{
		PFile:            newPFile,
		Footer:           footer,
		SchemaHandler:    schemaHandler,
		PathStr:          pathStr,
		DataTableNumRows: -1,
	}

	if err = res.NextRowGroup(); err == io.EOF {
		err = nil
	}
	return res, err
}

func (cbt *ColumnBufferType) NextRowGroup() error {
	var err error
	rowGroups := cbt.Footer.GetRowGroups()
	ln := int64(len(rowGroups))
	if cbt.RowGroupIndex >= ln {
		cbt.DataTableNumRows++ //very important, because DataTableNumRows is one smaller than real rows number
		return io.EOF
	}

	cbt.RowGroupIndex++

	columnChunks := rowGroups[cbt.RowGroupIndex-1].GetColumns()
	i := int64(0)
	ln = int64(len(columnChunks))
	for i = 0; i < ln; i++ {
		path := make([]string, 0)
		path = append(path, cbt.SchemaHandler.GetRootInName())
		path = append(path, columnChunks[i].MetaData.GetPathInSchema()...)

		if cbt.PathStr == common.PathToStr(path) {
			break
		}
	}

	if i >= ln {
		return fmt.Errorf("[NextRowGroup] Column not found: %v", cbt.PathStr)
	}

	cbt.ChunkHeader = columnChunks[i]
	if columnChunks[i].FilePath != nil {
		cbt.PFile.Close()
		if cbt.PFile, err = cbt.PFile.Open(*columnChunks[i].FilePath); err != nil {
			return err
		}
	}

	//offset := columnChunks[i].FileOffset
	offset := columnChunks[i].MetaData.DataPageOffset
	if columnChunks[i].MetaData.DictionaryPageOffset != nil {
		offset = *columnChunks[i].MetaData.DictionaryPageOffset
	}

	size := columnChunks[i].MetaData.GetTotalCompressedSize()
	if cbt.ThriftReader != nil {
		cbt.ThriftReader.Close()
	}

	cbt.ThriftReader = source.ConvertToThriftReader(cbt.PFile, offset, size)
	cbt.ChunkReadValues = 0
	cbt.DictPage = nil
	return nil
}

func (cbt *ColumnBufferType) ReadPage() error {
	if cbt.ChunkHeader != nil && cbt.ChunkHeader.MetaData != nil && cbt.ChunkReadValues < cbt.ChunkHeader.MetaData.NumValues {
		page, numValues, numRows, err := layout.ReadPage(cbt.ThriftReader, cbt.SchemaHandler, cbt.ChunkHeader.MetaData)
		if err != nil {
			//data is nil and rl/dl=0, no pages in file
			if err == io.EOF {
				if cbt.DataTable == nil {
					index := cbt.SchemaHandler.MapIndex[cbt.PathStr]
					cbt.DataTable = layout.NewEmptyTable()
					cbt.DataTable.Schema = cbt.SchemaHandler.SchemaElements[index]
					cbt.DataTable.Path = common.StrToPath(cbt.PathStr)

				}

				cbt.DataTableNumRows = cbt.ChunkHeader.MetaData.NumValues

				for cbt.ChunkReadValues < cbt.ChunkHeader.MetaData.NumValues {
					cbt.DataTable.Values = append(cbt.DataTable.Values, nil)
					cbt.DataTable.RepetitionLevels = append(cbt.DataTable.RepetitionLevels, int32(0))
					cbt.DataTable.DefinitionLevels = append(cbt.DataTable.DefinitionLevels, int32(0))
					cbt.ChunkReadValues++
				}
			}

			return err
		}

		if page.Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
			cbt.DictPage = page
			return nil
		}

		page.Decode(cbt.DictPage)

		if cbt.DataTable == nil {
			cbt.DataTable = layout.NewTableFromTable(page.DataTable)
		}

		cbt.DataTable.Merge(page.DataTable)
		cbt.ChunkReadValues += numValues

		cbt.DataTableNumRows += numRows
	} else {
		if err := cbt.NextRowGroup(); err != nil {
			return err
		}

		return cbt.ReadPage()
	}

	return nil
}

func (cbt *ColumnBufferType) ReadPageForSkip() (*layout.Page, error) {
	if cbt.ChunkHeader != nil && cbt.ChunkHeader.MetaData != nil && cbt.ChunkReadValues < cbt.ChunkHeader.MetaData.NumValues {
		page, err := layout.ReadPageRawData(cbt.ThriftReader, cbt.SchemaHandler, cbt.ChunkHeader.MetaData)
		if err != nil {
			return nil, err
		}

		numValues, numRows, err := page.GetRLDLFromRawData(cbt.SchemaHandler)
		if err != nil {
			return nil, err
		}

		if page.Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
			page.GetValueFromRawData(cbt.SchemaHandler)
			cbt.DictPage = page
			return page, nil
		}

		if cbt.DataTable == nil {
			cbt.DataTable = layout.NewTableFromTable(page.DataTable)
		}

		cbt.DataTable.Merge(page.DataTable)
		cbt.ChunkReadValues += numValues
		cbt.DataTableNumRows += numRows
		return page, nil

	} else {
		if err := cbt.NextRowGroup(); err != nil {
			return nil, err
		}

		return cbt.ReadPageForSkip()
	}
}

func (cbt *ColumnBufferType) SkipRows(num int64) int64 {
	var (
		err  error
		page *layout.Page
	)

	for cbt.DataTableNumRows < num && err == nil {
		page, err = cbt.ReadPageForSkip()
	}

	if num > cbt.DataTableNumRows {
		num = cbt.DataTableNumRows
	}

	if page != nil {
		if err = page.GetValueFromRawData(cbt.SchemaHandler); err != nil {
			return 0
		}

		page.Decode(cbt.DictPage)
		i, j := len(cbt.DataTable.Values)-1, len(page.DataTable.Values)-1
		for i >= 0 && j >= 0 {
			cbt.DataTable.Values[i] = page.DataTable.Values[j]
			i, j = i-1, j-1
		}
	}

	cbt.DataTable.Pop(num)
	cbt.DataTableNumRows -= num
	if cbt.DataTableNumRows <= 0 {
		tmp := cbt.DataTable
		cbt.DataTable = layout.NewTableFromTable(tmp)
		cbt.DataTable.Merge(tmp)
	}

	return num
}

func (cbt *ColumnBufferType) ReadRows(num int64) (*layout.Table, int64) {
	var err error

	for cbt.DataTableNumRows < num && err == nil {
		err = cbt.ReadPage()
	}

	if cbt.DataTableNumRows < 0 {
		cbt.DataTableNumRows = 0
		cbt.DataTable = layout.NewEmptyTable()
	}

	if num > cbt.DataTableNumRows {
		num = cbt.DataTableNumRows
	}

	res := cbt.DataTable.Pop(num)
	cbt.DataTableNumRows -= num

	if cbt.DataTableNumRows <= 0 { //release previous slice memory
		tmp := cbt.DataTable
		cbt.DataTable = layout.NewTableFromTable(tmp)
		cbt.DataTable.Merge(tmp)
	}
	return res, num

}
