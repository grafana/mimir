package writer

import (
	"fmt"
	"io"

	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/marshal"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/types"
)

type CSVWriter struct {
	ParquetWriter
}

func NewCSVWriterFromWriter(md []string, w io.Writer, np int64) (*CSVWriter, error) {
	wf := writerfile.NewWriterFile(w)
	return NewCSVWriter(md, wf, np)
}

//Create CSV writer
func NewCSVWriter(md []string, pfile source.ParquetFile, np int64) (*CSVWriter, error) {
	var err error
	res := new(CSVWriter)
	res.SchemaHandler, err = schema.NewSchemaHandlerFromMetadata(md)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema from metadata: %s", err.Error())
	}
	res.PFile = pfile
	res.PageSize = 8 * 1024              //8K
	res.RowGroupSize = 128 * 1024 * 1024 //128M
	res.CompressionType = parquet.CompressionCodec_SNAPPY
	res.PagesMapBuf = make(map[string][]*layout.Page)
	res.DictRecs = make(map[string]*layout.DictRecType)
	res.NP = np
	res.Footer = parquet.NewFileMetaData()
	res.Footer.Version = 1
	res.Footer.Schema = append(res.Footer.Schema, res.SchemaHandler.SchemaElements...)
	res.Offset = 4
	_, err = res.PFile.Write([]byte("PAR1"))
	res.MarshalFunc = marshal.MarshalCSV
	return res, err
}

//Write string values to parquet file
func (w *CSVWriter) WriteString(recsi interface{}) error {
	var err error
	recs := recsi.([]*string)
	lr := len(recs)
	rec := make([]interface{}, lr)
	for i := 0; i < lr; i++ {
		rec[i] = nil
		if recs[i] != nil {
			rec[i], err = types.StrToParquetType(*recs[i],
				w.SchemaHandler.SchemaElements[i+1].Type,
				w.SchemaHandler.SchemaElements[i+1].ConvertedType,
				int(w.SchemaHandler.SchemaElements[i+1].GetTypeLength()),
				int(w.SchemaHandler.SchemaElements[i+1].GetScale()),
			)
			if err != nil {
				return err
			}
		}
	}

	return w.Write(rec)
}
