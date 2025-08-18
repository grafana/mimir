package writer

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/marshal"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/source"
)

const (
	pageSize      = 8 * 1024
	rowGroupSize  = 128 * 1024 * 1024
	footerVersion = 1
	offset        = 4
)

// ArrowWriter extending the base ParqueWriter
type ArrowWriter struct {
	ParquetWriter
}

//NewArrowWriter creates arrow schema parquet writer given the native
//arrow schema, parquet file writer which contains the parquet file in
//which we will write the record along with the number of parallel threads
//which will write in the file.
func NewArrowWriter(arrowSchema *arrow.Schema, pfile source.ParquetFile,
	np int64) (*ArrowWriter, error) {
	var err error
	res := new(ArrowWriter)
	res.SchemaHandler, err = schema.NewSchemaHandlerFromArrow(arrowSchema)
	if err != nil {
		return res, fmt.Errorf("Unable to create schema from arrow definition: %s",
			err.Error())
	}

	res.PFile = pfile
	res.PageSize = pageSize
	res.RowGroupSize = rowGroupSize
	// Compression type is by default: parquet.CompressionCodec_SNAPPY
	res.CompressionType = parquet.CompressionCodec_GZIP
	res.PagesMapBuf = make(map[string][]*layout.Page)
	res.DictRecs = make(map[string]*layout.DictRecType)
	res.NP = np
	res.Footer = parquet.NewFileMetaData()
	res.Footer.Version = footerVersion
	res.Footer.Schema = append(res.Footer.Schema,
		res.SchemaHandler.SchemaElements...)
	res.Offset = offset
	_, err = res.PFile.Write([]byte("PAR1"))
	res.MarshalFunc = marshal.MarshalArrow
	return res, err
}

// WriteArrow wraps the base Write function provided by writer.ParquetWriter.
// The function transforms the data from the record, which the go arrow library
// gives as array of columns, to array of rows which the parquet-go library
// can understand as it does not accepts data by columns, but rather by rows.
func (w *ArrowWriter) WriteArrow(record array.Record) error {
	table := make([][]interface{}, 0)
	for i, column := range record.Columns() {
		columnFromRecord, err := common.ArrowColToParquetCol(
			record.Schema().Field(i),
			column,
			column.Len(),
			w.SchemaHandler.SchemaElements[i+1])

		if err != nil {
			return err
		}

		if len(columnFromRecord) > 0 {
			table = append(table, columnFromRecord)
		}
	}
	transposedTable := common.TransposeTable(table)
	for _, row := range transposedTable {
		err := w.Write(row)
		if err != nil {
			return err
		}
	}
	return nil
}
