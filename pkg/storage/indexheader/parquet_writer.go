// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"fmt"
	"os"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"

	"github.com/grafana/mimir/pkg/storage/indexheader/index"
)

type ParquetBuilder struct {
	postingsOffsetsNames  []string
	postingsOffsetsValues [][]index.PostingListOffset
}

func NewParquetBuilder() *ParquetBuilder {
	return &ParquetBuilder{}
}

func (b *ParquetBuilder) Add(name string, values []index.PostingListOffset) {
	b.postingsOffsetsNames = append(b.postingsOffsetsNames, name)
	b.postingsOffsetsValues = append(b.postingsOffsetsValues, values)
}

func (b *ParquetBuilder) GetAtLabelNameIndex(i int) (string, []index.PostingListOffset) {
	return b.postingsOffsetsNames[i], b.postingsOffsetsValues[i]
}

func (b *ParquetBuilder) WriteToParquet(filePath string) error {
	// 1. Create schema: TWO columns per label name
	fields := make([]arrow.Field, 0, len(b.postingsOffsetsNames)*2)

	for _, labelName := range b.postingsOffsetsNames {
		// Column 1: Label values (strings)
		fields = append(fields, arrow.Field{
			Name: fmt.Sprintf("%s_values", labelName),
			Type: arrow.BinaryTypes.String,
		})

		// Column 2: Ranges (fixed-size list of 2 int64)
		fields = append(fields, arrow.Field{
			Name: fmt.Sprintf("%s_ranges", labelName),
			Type: arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Int64),
		})
	}

	schema := arrow.NewSchema(fields, nil)

	// 2. Prepare data - each label is independent
	mem := memory.NewGoAllocator()
	builders := make(map[string]*array.StringBuilder)
	rangeBuilders := make(map[string]*array.FixedSizeListBuilder)

	for _, labelName := range b.postingsOffsetsNames {
		builders[labelName] = array.NewStringBuilder(mem)
		rangeBuilders[labelName] = array.NewFixedSizeListBuilder(mem, 2, arrow.PrimitiveTypes.Int64)
	}

	// 3. Fill data for each label
	for i, labelName := range b.postingsOffsetsNames {
		offsetsValues := b.postingsOffsetsValues[i]

		//// Sort label values
		//sort.Slice(offsetsValues, func(j, k int) bool {
		//	return offsetsValues[j].LabelValue < offsetsValues[k].LabelValue
		//})

		builder := builders[labelName]
		rangeBuilder := rangeBuilders[labelName]
		valueBuilder := rangeBuilder.ValueBuilder().(*array.Int64Builder)

		for _, offset := range offsetsValues {
			// Add label value
			builder.Append(offset.LabelValue)

			// Add range as [start, end]
			rangeBuilder.Append(true)
			valueBuilder.Append(offset.Off.Start)
			valueBuilder.Append(offset.Off.End)
		}
	}

	// 4. Create arrays
	arrays := make([]arrow.Array, 0, len(b.postingsOffsetsNames)*2)
	totalRows := 0

	for _, labelName := range b.postingsOffsetsNames {
		// Get string column
		values := builders[labelName].NewArray()
		defer values.Release()
		arrays = append(arrays, values)

		// Get ranges column
		ranges := rangeBuilders[labelName].NewArray()
		defer ranges.Release()
		arrays = append(arrays, ranges)

		// Track max rows for record
		if values.Len() > totalRows {
			totalRows = values.Len()
		}
	}

	// 5. Create record
	record := array.NewRecord(schema, arrays, int64(totalRows))
	defer record.Release()

	// 6. Create file
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// 7. Configure Parquet writer - OPTIMIZED FOR READ
	var propsOpts []parquet.WriterProperty
	for _, labelName := range b.postingsOffsetsNames {
		valuesCol := fmt.Sprintf("%s_values", labelName)
		rangesCol := fmt.Sprintf("%s_ranges", labelName)
		propsOpts = append(
			propsOpts,

			parquet.WithEncodingFor(valuesCol, parquet.Encodings.DeltaByteArray),
			parquet.WithStatsFor(valuesCol, true),
			parquet.WithDictionaryPageSizeLimit(128*1024), // 128KB max dict
			parquet.WithDataPageSize(256*1024),            // 256KB pages

			parquet.WithEncodingFor(rangesCol, parquet.Encodings.DeltaBinaryPacked),
		)
	}

	propsOpts = append(propsOpts,
		parquet.WithMaxRowGroupLength(10_000),
		parquet.WithDataPageSize(256*1024), // 256KB pages (fits in 1MB reads)
		// ... others
	)

	props := parquet.NewWriterProperties(
		propsOpts...,
	)

	// 8. Write
	writer, err := pqarrow.NewFileWriter(schema, file, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	defer writer.Close()

	return writer.Write(record)
}
