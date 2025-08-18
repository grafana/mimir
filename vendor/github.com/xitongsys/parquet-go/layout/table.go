package layout

import (
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
)

func NewTableFromTable(src *Table) *Table {
	if src == nil {
		return nil
	}
	table := new(Table)
	table.Schema = src.Schema
	table.Path = append(table.Path, src.Path...)
	table.MaxDefinitionLevel = 0
	table.MaxRepetitionLevel = 0
	table.Info = src.Info
	return table
}

func NewEmptyTable() *Table {
	table := new(Table)
	table.Info = common.NewTag()
	return table
}

//Table is the core data structure used to store the values
type Table struct {
	//Repetition type of the values: REQUIRED/OPTIONAL/REPEATED
	RepetitionType parquet.FieldRepetitionType
	//Schema
	Schema *parquet.SchemaElement
	//Path of this column
	Path []string
	//Maximum of definition levels
	MaxDefinitionLevel int32
	//Maximum of repetition levels
	MaxRepetitionLevel int32

	//Parquet values
	Values []interface{}
	//Definition Levels slice
	DefinitionLevels []int32
	//Repetition Levels slice
	RepetitionLevels []int32

	//Tag info
	Info *common.Tag
}

//Merge several tables to one table(the first table)
func (t *Table) Merge(tables ...*Table) {
	ln := len(tables)
	if ln <= 0 {
		return
	}
	for i := 0; i < ln; i++ {
		if tables[i] == nil {
			continue
		}
		t.Values = append(t.Values, tables[i].Values...)
		t.RepetitionLevels = append(t.RepetitionLevels, tables[i].RepetitionLevels...)
		t.DefinitionLevels = append(t.DefinitionLevels, tables[i].DefinitionLevels...)
		if tables[i].MaxDefinitionLevel > t.MaxDefinitionLevel {
			t.MaxDefinitionLevel = tables[i].MaxDefinitionLevel
		}
		if tables[i].MaxRepetitionLevel > t.MaxRepetitionLevel {
			t.MaxRepetitionLevel = tables[i].MaxRepetitionLevel
		}
	}
}

func (t *Table) Pop(numRows int64) *Table {
	res := NewTableFromTable(t)
	endIndex := int64(0)
	ln := int64(len(t.Values))
	i, num := int64(0), int64(-1)
	for i = 0; i < ln; i++ {
		if t.RepetitionLevels[i] == 0 {
			num++
			if num >= numRows {
				break
			}
		}
		if res.MaxRepetitionLevel < t.RepetitionLevels[i] {
			res.MaxRepetitionLevel = t.RepetitionLevels[i]
		}
		if res.MaxDefinitionLevel < t.DefinitionLevels[i] {
			res.MaxDefinitionLevel = t.DefinitionLevels[i]
		}
	}
	endIndex = i

	res.RepetitionLevels = t.RepetitionLevels[:endIndex]
	res.DefinitionLevels = t.DefinitionLevels[:endIndex]
	res.Values = t.Values[:endIndex]

	t.RepetitionLevels = t.RepetitionLevels[endIndex:]
	t.DefinitionLevels = t.DefinitionLevels[endIndex:]
	t.Values = t.Values[endIndex:]

	return res
}
