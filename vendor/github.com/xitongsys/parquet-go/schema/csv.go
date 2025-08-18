package schema

import (
	"fmt"

	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
)

//Create a schema handler from CSV metadata
func NewSchemaHandlerFromMetadata(mds []string) (*SchemaHandler, error) {
	schemaList := make([]*parquet.SchemaElement, 0)
	infos := make([]*common.Tag, 0)

	rootSchema := parquet.NewSchemaElement()
	rootSchema.Name = "Parquet_go_root"
	rootNumChildren := int32(len(mds))
	rootSchema.NumChildren = &rootNumChildren
	rt := parquet.FieldRepetitionType_REQUIRED
	rootSchema.RepetitionType = &rt
	schemaList = append(schemaList, rootSchema)

	rootInfo := common.NewTag()
	rootInfo.InName = "Parquet_go_root"
	rootInfo.ExName = "parquet_go_root"
	rootInfo.RepetitionType = parquet.FieldRepetitionType_REQUIRED
	infos = append(infos, rootInfo)

	for _, md := range mds {
		info, err := common.StringToTag(md)
		if err != nil {
			return nil, fmt.Errorf("failed to parse metadata: %s", err.Error())
		}
		infos = append(infos, info)
		schema, err := common.NewSchemaElementFromTagMap(info)
		if err != nil {
			return nil, fmt.Errorf("failed to create schema from tag map: %s", err.Error())
		}
		//schema.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
		schemaList = append(schemaList, schema)
	}
	res := NewSchemaHandlerFromSchemaList(schemaList)
	res.Infos = infos
	res.CreateInExMap()
	return res, nil
}
