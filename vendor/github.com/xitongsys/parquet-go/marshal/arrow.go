package marshal

import (
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
)

// MarshalArrow accepts a slice of rows with which it creates a table object.
// We need to append row by row as opposed to the arrow go provided way of
// column by column since the wrapper ParquetWriter uses the number of rows
// to execute intermediate flush depending on the size of the objects,
// determined by row, which are currently written.
func MarshalArrow(recs []interface{}, schemaHandler *schema.SchemaHandler) (
	tb *map[string]*layout.Table, err error) {
	res := make(map[string]*layout.Table)
	if ln := len(recs); ln <= 0 {
		return &res, nil
	}

	for i := 0; i < len(recs[0].([]interface{})); i++ {
		pathStr := schemaHandler.GetRootInName() + common.PAR_GO_PATH_DELIMITER +
			schemaHandler.Infos[i+1].InName
		table := layout.NewEmptyTable()
		res[pathStr] = table
		table.Path = common.StrToPath(pathStr)
		table.MaxDefinitionLevel = 0
		table.MaxRepetitionLevel = 0
		table.RepetitionType = parquet.FieldRepetitionType_REQUIRED
		table.Schema =
			schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]]
		table.Info = schemaHandler.Infos[i+1]
		// Pre-allocate these arrays for efficiency
		table.Values = make([]interface{}, 0, len(recs))
		table.RepetitionLevels = make([]int32, len(recs))
		table.DefinitionLevels = make([]int32, len(recs))

		for j := 0; j < len(recs); j++ {
			rec := recs[j].([]interface{})[i]
			table.Values = append(table.Values, rec)
		}
	}
	return &res, nil
}
