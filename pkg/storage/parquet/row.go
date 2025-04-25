// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/26344c3ec7409713fcf52a9c41cd0dce537b3100/pkg/storage/parquet/row.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package parquet

const (
	ColHash                        = "s_hash"
	ColIndexes                     = "s_col_indexes"
	ColData                        = "s_data"
	SortedColMetadataKey           = "sorting_col_metadata_key"
	NumberOfDataColumnsMetadataKey = "number_of_data_columns_metadata_key"
	IndexSizeLimitMetadataKey      = "index_size_limit_metadata_key"
)

type ParquetRow struct {
	Hash    uint64
	Data    [][]byte
	Columns map[string]string
}
