package layout

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/encoding"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
)

//Chunk stores the ColumnChunk in parquet file
type Chunk struct {
	Pages       []*Page
	ChunkHeader *parquet.ColumnChunk
}

//Convert several pages to one chunk
func PagesToChunk(pages []*Page) *Chunk {
	ln := len(pages)
	var numValues int64 = 0
	var totalUncompressedSize int64 = 0
	var totalCompressedSize int64 = 0

	var maxVal interface{} = pages[0].MaxVal
	var minVal interface{} = pages[0].MinVal
	var nullCount int64 = 0
	pT, cT, logT, omitStats := pages[0].Schema.Type, pages[0].Schema.ConvertedType, pages[0].Schema.LogicalType, pages[0].Info.OmitStats
	funcTable := common.FindFuncTable(pT, cT, logT)

	for i := 0; i < ln; i++ {
		if pages[i].Header.DataPageHeader != nil {
			numValues += int64(pages[i].Header.DataPageHeader.NumValues)
		} else {
			numValues += int64(pages[i].Header.DataPageHeaderV2.NumValues)
		}
		totalUncompressedSize += int64(pages[i].Header.UncompressedPageSize) + int64(len(pages[i].RawData)) - int64(pages[i].Header.CompressedPageSize)
		totalCompressedSize += int64(len(pages[i].RawData))
		if !omitStats {
			minVal = common.Min(funcTable, minVal, pages[i].MinVal)
			maxVal = common.Max(funcTable, maxVal, pages[i].MaxVal)
			nullCount += *pages[i].NullCount
		}
	}

	chunk := new(Chunk)
	chunk.Pages = pages
	chunk.ChunkHeader = parquet.NewColumnChunk()
	metaData := parquet.NewColumnMetaData()
	metaData.Type = *pages[0].Schema.Type
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_RLE)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_BIT_PACKED)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_PLAIN)
	//metaData.Encodings = append(metaData.Encodings, parquet.Encoding_DELTA_BINARY_PACKED)
	metaData.Codec = pages[0].CompressType
	metaData.NumValues = numValues
	metaData.TotalCompressedSize = totalCompressedSize
	metaData.TotalUncompressedSize = totalUncompressedSize
	metaData.PathInSchema = pages[0].Path
	metaData.Statistics = parquet.NewStatistics()

	if !omitStats && maxVal != nil && minVal != nil {
		tmpBufMax := encoding.WritePlain([]interface{}{maxVal}, *pT)
		tmpBufMin := encoding.WritePlain([]interface{}{minVal}, *pT)
		if *pT == parquet.Type_BYTE_ARRAY {
			tmpBufMax = tmpBufMax[4:]
			tmpBufMin = tmpBufMin[4:]
		}
		metaData.Statistics.Max = tmpBufMax
		metaData.Statistics.Min = tmpBufMin
		metaData.Statistics.MaxValue = tmpBufMax
		metaData.Statistics.MinValue = tmpBufMin
	}

	if !omitStats {
		metaData.Statistics.NullCount = &nullCount
	}

	chunk.ChunkHeader.MetaData = metaData
	return chunk
}

//Convert several pages to one chunk with dict page first
func PagesToDictChunk(pages []*Page) *Chunk {
	if len(pages) < 2 {
		return nil
	}
	var numValues int64 = 0
	var totalUncompressedSize int64 = 0
	var totalCompressedSize int64 = 0

	var maxVal interface{} = pages[1].MaxVal
	var minVal interface{} = pages[1].MinVal
	var nullCount int64 = 0
	pT, cT, logT, omitStats := pages[1].Schema.Type, pages[1].Schema.ConvertedType, pages[1].Schema.LogicalType, pages[0].Info.OmitStats
	funcTable := common.FindFuncTable(pT, cT, logT)

	for i := 0; i < len(pages); i++ {
		if pages[i].Header.DataPageHeader != nil {
			numValues += int64(pages[i].Header.DataPageHeader.NumValues)
		} else if pages[i].Header.DataPageHeaderV2 != nil {
			numValues += int64(pages[i].Header.DataPageHeaderV2.NumValues)
		}
		totalUncompressedSize += int64(pages[i].Header.UncompressedPageSize) + int64(len(pages[i].RawData)) - int64(pages[i].Header.CompressedPageSize)
		totalCompressedSize += int64(len(pages[i].RawData))
		if !omitStats && i > 0 {
			minVal = common.Min(funcTable, minVal, pages[i].MinVal)
			maxVal = common.Max(funcTable, maxVal, pages[i].MaxVal)
			nullCount += *pages[i].NullCount
		}
	}

	chunk := new(Chunk)
	chunk.Pages = pages
	chunk.ChunkHeader = parquet.NewColumnChunk()
	metaData := parquet.NewColumnMetaData()
	metaData.Type = *pages[1].Schema.Type
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_RLE)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_BIT_PACKED)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_PLAIN)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_PLAIN_DICTIONARY)
	metaData.Encodings = append(metaData.Encodings, parquet.Encoding_RLE_DICTIONARY)

	metaData.Codec = pages[1].CompressType
	metaData.NumValues = numValues
	metaData.TotalCompressedSize = totalCompressedSize
	metaData.TotalUncompressedSize = totalUncompressedSize
	metaData.PathInSchema = pages[1].Path
	metaData.Statistics = parquet.NewStatistics()

	if !omitStats && maxVal != nil && minVal != nil {
		tmpBufMax := encoding.WritePlain([]interface{}{maxVal}, *pT)
		tmpBufMin := encoding.WritePlain([]interface{}{minVal}, *pT)
		if *pT == parquet.Type_BYTE_ARRAY {
			tmpBufMax = tmpBufMax[4:]
			tmpBufMin = tmpBufMin[4:]
		}
		metaData.Statistics.Max = tmpBufMax
		metaData.Statistics.Min = tmpBufMin
		metaData.Statistics.MaxValue = tmpBufMax
		metaData.Statistics.MinValue = tmpBufMin
	}

	if !omitStats {
		metaData.Statistics.NullCount = &nullCount
	}

	chunk.ChunkHeader.MetaData = metaData
	return chunk
}

//Decode a dict chunk
func DecodeDictChunk(chunk *Chunk) {
	dictPage := chunk.Pages[0]
	numPages := len(chunk.Pages)
	for i := 1; i < numPages; i++ {
		numValues := len(chunk.Pages[i].DataTable.Values)
		for j := 0; j < numValues; j++ {
			if chunk.Pages[i].DataTable.Values[j] != nil {
				index := chunk.Pages[i].DataTable.Values[j].(int64)
				chunk.Pages[i].DataTable.Values[j] = dictPage.DataTable.Values[index]
			}
		}
	}
	chunk.Pages = chunk.Pages[1:] // delete the head dict page
}

//Read one chunk from parquet file (Deprecated)
func ReadChunk(thriftReader *thrift.TBufferedTransport, schemaHandler *schema.SchemaHandler, chunkHeader *parquet.ColumnChunk) (*Chunk, error) {
	chunk := new(Chunk)
	chunk.ChunkHeader = chunkHeader

	var readValues int64 = 0
	var numValues int64 = chunkHeader.MetaData.GetNumValues()
	for readValues < numValues {
		page, cnt, _, err := ReadPage(thriftReader, schemaHandler, chunkHeader.GetMetaData())
		if err != nil {
			return nil, err
		}
		chunk.Pages = append(chunk.Pages, page)
		readValues += cnt
	}

	if len(chunk.Pages) > 0 && chunk.Pages[0].Header.GetType() == parquet.PageType_DICTIONARY_PAGE {
		DecodeDictChunk(chunk)
	}
	return chunk, nil
}
