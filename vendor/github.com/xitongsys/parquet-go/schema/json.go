package schema

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
)

type JSONSchemaItemType struct {
	Tag    string                `json:"Tag"`
	Fields []*JSONSchemaItemType `json:"Fields,omitempty"`
}

func NewJSONSchemaItem() *JSONSchemaItemType {
	return new(JSONSchemaItemType)
}

func NewSchemaHandlerFromJSON(str string) (sh *SchemaHandler, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unknown error")
			}
		}
	}()

	schema := NewJSONSchemaItem()
	if err := json.Unmarshal([]byte(str), schema); err != nil {
		return nil, fmt.Errorf("error in unmarshalling json schema string: %v", err.Error())
	}

	stack := make([]*JSONSchemaItemType, 0)
	stack = append(stack, schema)
	schemaElements := make([]*parquet.SchemaElement, 0)
	infos := make([]*common.Tag, 0)

	for len(stack) > 0 {
		ln := len(stack)
		item := stack[ln-1]
		stack = stack[:ln-1]
		info, err := common.StringToTag(item.Tag)
		if err != nil {
			return nil, fmt.Errorf("failed parse tag: %s", err.Error())
		}
		var newInfo *common.Tag
		if info.Type == "" { //struct
			schema := parquet.NewSchemaElement()
			schema.Name = info.InName
			rt := info.RepetitionType
			schema.RepetitionType = &rt
			numField := int32(len(item.Fields))
			schema.NumChildren = &numField
			schemaElements = append(schemaElements, schema)

			newInfo = common.NewTag()
			common.DeepCopy(info, newInfo)
			infos = append(infos, newInfo)

			for i := int(numField - 1); i >= 0; i-- {
				newItem := item.Fields[i]
				stack = append(stack, newItem)
			}

		} else if info.Type == "LIST" { //list
			schema := parquet.NewSchemaElement()
			schema.Name = info.InName
			rt1 := info.RepetitionType
			schema.RepetitionType = &rt1
			var numField1 int32 = 1
			schema.NumChildren = &numField1
			ct1 := parquet.ConvertedType_LIST
			schema.ConvertedType = &ct1
			schemaElements = append(schemaElements, schema)

			newInfo = common.NewTag()
			common.DeepCopy(info, newInfo)
			infos = append(infos, newInfo)

			schema = parquet.NewSchemaElement()
			schema.Name = "List"
			rt2 := parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt2
			var numField2 int32 = 1
			schema.NumChildren = &numField2
			schemaElements = append(schemaElements, schema)

			newInfo = common.NewTag()
			common.DeepCopy(info, newInfo)
			newInfo.InName, newInfo.ExName = "List", "list"
			infos = append(infos, newInfo)

			if len(item.Fields) != 1 {
				return nil, fmt.Errorf("LIST needs exact 1 field to define element type")
			}
			stack = append(stack, item.Fields[0])

		} else if info.Type == "MAP" { //map
			schema := parquet.NewSchemaElement()
			schema.Name = info.InName
			rt1 := info.RepetitionType
			schema.RepetitionType = &rt1
			var numField1 int32 = 1
			schema.NumChildren = &numField1
			ct1 := parquet.ConvertedType_MAP
			schema.ConvertedType = &ct1
			schemaElements = append(schemaElements, schema)

			newInfo = common.NewTag()
			common.DeepCopy(info, newInfo)
			infos = append(infos, newInfo)

			schema = parquet.NewSchemaElement()
			schema.Name = "Key_value"
			rt2 := parquet.FieldRepetitionType_REPEATED
			schema.RepetitionType = &rt2
			ct2 := parquet.ConvertedType_MAP_KEY_VALUE
			schema.ConvertedType = &ct2
			var numField2 int32 = 2
			schema.NumChildren = &numField2
			schemaElements = append(schemaElements, schema)

			newInfo = common.NewTag()
			common.DeepCopy(info, newInfo)
			newInfo.InName, newInfo.ExName = "Key_value", "key_value"
			infos = append(infos, newInfo)

			if len(item.Fields) != 2 {
				return nil, fmt.Errorf("MAP needs exact 2 fields to define key and value type")
			}
			stack = append(stack, item.Fields[1]) //put value
			stack = append(stack, item.Fields[0]) //put key

		} else { //normal variable
			schema, err := common.NewSchemaElementFromTagMap(info)
			if err != nil {
				return nil, fmt.Errorf("failed to create schema from tag map: %s", err.Error())
			}
			schemaElements = append(schemaElements, schema)

			newInfo = common.NewTag()
			common.DeepCopy(info, newInfo)
			infos = append(infos, newInfo)
		}
	}
	res := NewSchemaHandlerFromSchemaList(schemaElements)
	res.Infos = infos
	res.CreateInExMap()
	return res, nil
}
