package schema

import (
	"fmt"

	"github.com/apache/arrow/go/arrow"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
)

// Schema metadata used to parse the native and converted types and
// creating the schema definitions
const (
	convertedMetaDataTemplate = "name=%s, type=%s, convertedtype=%s"
	primitiveMetaDataTemplate = "name=%s, type=%s"
	rootNodeName              = "Parquet45go45root"
)

// ConvertArrowToParquetSchema converts arrow schema to representation
// understandable by parquet-go library.
// We need this coversion and can't directly use arrow format because the
// go parquet type contains metadata which the base writer is using to
// determine the size of the objects.
func ConvertArrowToParquetSchema(schema *arrow.Schema) ([]string, error) {
	metaData := make([]string, len(schema.Fields()))
	var err error
	for k, v := range schema.Fields() {
		switch fieldType := v.Type; fieldType.Name() {
		case arrow.PrimitiveTypes.Int8.Name():
			metaData[k] = fmt.Sprintf(convertedMetaDataTemplate,
				v.Name, parquet.Type_INT32, parquet.ConvertedType_INT_8)
		case arrow.PrimitiveTypes.Int16.Name():
			metaData[k] = fmt.Sprintf(convertedMetaDataTemplate,
				v.Name, parquet.Type_INT32, parquet.ConvertedType_INT_16)
		case arrow.PrimitiveTypes.Int32.Name():
			metaData[k] = fmt.Sprintf(convertedMetaDataTemplate,
				v.Name, parquet.Type_INT32, parquet.ConvertedType_INT_32)
		case arrow.PrimitiveTypes.Int64.Name():
			metaData[k] = fmt.Sprintf(convertedMetaDataTemplate,
				v.Name, parquet.Type_INT64, parquet.ConvertedType_INT_64)
		case arrow.PrimitiveTypes.Uint8.Name():
			metaData[k] = fmt.Sprintf(convertedMetaDataTemplate,
				v.Name, parquet.Type_INT32, parquet.ConvertedType_UINT_8)
		case arrow.PrimitiveTypes.Uint16.Name():
			metaData[k] = fmt.Sprintf(convertedMetaDataTemplate,
				v.Name, parquet.Type_INT32, parquet.ConvertedType_UINT_16)
		case arrow.PrimitiveTypes.Uint32.Name():
			metaData[k] = fmt.Sprintf(convertedMetaDataTemplate,
				v.Name, parquet.Type_INT32, parquet.ConvertedType_UINT_32)
		case arrow.PrimitiveTypes.Uint64.Name():
			metaData[k] = fmt.Sprintf(convertedMetaDataTemplate,
				v.Name, parquet.Type_INT64, parquet.ConvertedType_UINT_64)
		case arrow.PrimitiveTypes.Float32.Name():
			metaData[k] = fmt.Sprintf(primitiveMetaDataTemplate, v.Name,
				parquet.Type_FLOAT)
		case arrow.PrimitiveTypes.Float64.Name():
			metaData[k] = fmt.Sprintf(primitiveMetaDataTemplate, v.Name,
				parquet.Type_DOUBLE)
		case arrow.PrimitiveTypes.Date32.Name(),
			arrow.PrimitiveTypes.Date64.Name():
			metaData[k] = fmt.Sprintf(convertedMetaDataTemplate, v.Name,
				parquet.Type_INT32, parquet.ConvertedType_DATE)
		case arrow.FixedWidthTypes.Date32.Name(), arrow.FixedWidthTypes.Date64.Name():
			metaData[k] = fmt.Sprintf(convertedMetaDataTemplate, v.Name,
				parquet.Type_INT32, parquet.ConvertedType_DATE)
		case arrow.BinaryTypes.Binary.Name():
			metaData[k] = fmt.Sprintf(primitiveMetaDataTemplate, v.Name,
				parquet.Type_BYTE_ARRAY)
		case arrow.BinaryTypes.String.Name():
			metaData[k] = fmt.Sprintf(convertedMetaDataTemplate, v.Name,
				parquet.Type_BYTE_ARRAY, parquet.ConvertedType_UTF8)
		case arrow.FixedWidthTypes.Boolean.Name():
			metaData[k] = fmt.Sprintf(primitiveMetaDataTemplate, v.Name,
				parquet.Type_BOOLEAN)
		case arrow.FixedWidthTypes.Time32ms.Name():
			metaData[k] = fmt.Sprintf(convertedMetaDataTemplate, v.Name,
				parquet.Type_INT32, parquet.ConvertedType_TIME_MILLIS)
		case arrow.FixedWidthTypes.Timestamp_ms.Name():
			metaData[k] = fmt.Sprintf(convertedMetaDataTemplate, v.Name,
				parquet.Type_INT64, parquet.ConvertedType_TIMESTAMP_MILLIS)
		default:
			return nil,
				fmt.Errorf("Unsupported arrow format: %s", fieldType.Name())
		}
	}
	return metaData, err
}

// NewSchemaHandlerFromArrow creates a schema handler from arrow format.
// This handler is needed since the base ParquetWriter does not understand
// arrow schema and we need to translate it to the native format which the
// parquet-go library understands.
func NewSchemaHandlerFromArrow(arrowSchema *arrow.Schema) (
	*SchemaHandler, error) {
	schemaList := make([]*parquet.SchemaElement, 0)
	infos := make([]*common.Tag, 0)

	fields, err := ConvertArrowToParquetSchema(arrowSchema)
	if err != nil {
		return nil, err
	}

	rootSchema := parquet.NewSchemaElement()
	rootSchema.Name = rootNodeName
	rootNumChildren := int32(len(fields))
	rootSchema.NumChildren = &rootNumChildren
	rt := parquet.FieldRepetitionType_REQUIRED
	rootSchema.RepetitionType = &rt
	schemaList = append(schemaList, rootSchema)

	rootInfo := common.NewTag()
	rootInfo.InName = rootNodeName
	rootInfo.ExName = rootNodeName
	rootInfo.RepetitionType = parquet.FieldRepetitionType_REQUIRED
	infos = append(infos, rootInfo)

	for _, field := range fields {
		info, err := common.StringToTag(field)
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
		schema, err := common.NewSchemaElementFromTagMap(info)
		if err != nil {
			return nil, err
		}
		schemaList = append(schemaList, schema)
	}
	res := NewSchemaHandlerFromSchemaList(schemaList)
	res.Infos = infos
	res.CreateInExMap()

	return res, nil
}
