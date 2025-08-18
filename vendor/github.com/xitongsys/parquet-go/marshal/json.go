package marshal

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/types"
)

//ss is []string
func MarshalJSON(ss []interface{}, schemaHandler *schema.SchemaHandler) (tb *map[string]*layout.Table, err error) {
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

	res := make(map[string]*layout.Table)
	pathMap := schemaHandler.PathMap
	nodeBuf := NewNodeBuf(1)

	for i := 0; i < len(schemaHandler.SchemaElements); i++ {
		schema := schemaHandler.SchemaElements[i]
		pathStr := schemaHandler.IndexMap[int32(i)]
		numChildren := schema.GetNumChildren()
		if numChildren == 0 {
			res[pathStr] = layout.NewEmptyTable()
			res[pathStr].Path = common.StrToPath(pathStr)
			res[pathStr].MaxDefinitionLevel, _ = schemaHandler.MaxDefinitionLevel(res[pathStr].Path)
			res[pathStr].MaxRepetitionLevel, _ = schemaHandler.MaxRepetitionLevel(res[pathStr].Path)
			res[pathStr].RepetitionType = schema.GetRepetitionType()
			res[pathStr].Schema = schemaHandler.SchemaElements[schemaHandler.MapIndex[pathStr]]
			res[pathStr].Info = schemaHandler.Infos[i]
		}
	}

	stack := make([]*Node, 0, 100)
	for i := 0; i < len(ss); i++ {
		stack = stack[:0]
		nodeBuf.Reset()

		node := nodeBuf.GetNode()
		var ui interface{}

		var d *json.Decoder

		switch t := ss[i].(type) {
		case string:
			d = json.NewDecoder(strings.NewReader(t))
		case []byte:
			d = json.NewDecoder(bytes.NewReader(t))
		}
		// `useNumber`causes the Decoder to unmarshal a number into an interface{} as a Number instead of as a float64.
		d.UseNumber()
		d.Decode(&ui)

		node.Val = reflect.ValueOf(ui)
		node.PathMap = pathMap

		stack = append(stack, node)

		for len(stack) > 0 {
			ln := len(stack)
			node = stack[ln-1]
			stack = stack[:ln-1]

			tk := node.Val.Type().Kind()

			pathStr := node.PathMap.Path

			schemaIndex, ok := schemaHandler.MapIndex[pathStr]
			//no schema item will be ignored
			if !ok {
				continue
			}

			schema := schemaHandler.SchemaElements[schemaIndex]

			if tk == reflect.Map {
				keys := node.Val.MapKeys()

				if schema.GetConvertedType() == parquet.ConvertedType_MAP { //real map
					pathStr = pathStr + common.PAR_GO_PATH_DELIMITER + "Key_value"
					if len(keys) <= 0 {
						for key, table := range res {
							if strings.HasPrefix(key, node.PathMap.Path) &&
								(len(key) == len(node.PathMap.Path) || key[len(node.PathMap.Path)] == common.PAR_GO_PATH_DELIMITER[0]) {
								table.Values = append(table.Values, nil)
								table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
								table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
							}
						}
					}

					rlNow, _ := schemaHandler.MaxRepetitionLevel(common.StrToPath(pathStr))
					for j := len(keys) - 1; j >= 0; j-- {
						key := keys[j]
						value := node.Val.MapIndex(key).Elem()

						newNode := nodeBuf.GetNode()
						newNode.PathMap = node.PathMap.Children["Key_value"].Children["Key"]
						newNode.Val = key
						newNode.DL = node.DL + 1
						if j == 0 {
							newNode.RL = node.RL
						} else {
							newNode.RL = rlNow
						}
						stack = append(stack, newNode)

						newNode = nodeBuf.GetNode()
						newNode.PathMap = node.PathMap.Children["Key_value"].Children["Value"]
						newNode.Val = value
						newNode.DL = node.DL + 1
						newPathStr := newNode.PathMap.Path // check again
						newSchemaIndex := schemaHandler.MapIndex[newPathStr]
						newSchema := schemaHandler.SchemaElements[newSchemaIndex]
						if newSchema.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL { //map value only be :optional or required
							newNode.DL++
						}

						if j == 0 {
							newNode.RL = node.RL
						} else {
							newNode.RL = rlNow
						}
						stack = append(stack, newNode)
					}

				} else { //struct
					keysMap := make(map[string]int)
					for j := 0; j < len(keys); j++ {
						//ExName to InName
						keysMap[common.StringToVariableName(keys[j].String())] = j
					}
					for key, _ := range node.PathMap.Children {
						ki, ok := keysMap[key]

						if ok && node.Val.MapIndex(keys[ki]).Elem().IsValid() {
							newNode := nodeBuf.GetNode()
							newNode.PathMap = node.PathMap.Children[key]
							newNode.Val = node.Val.MapIndex(keys[ki]).Elem()
							newNode.RL = node.RL
							newNode.DL = node.DL
							newPathStr := newNode.PathMap.Path
							newSchemaIndex := schemaHandler.MapIndex[newPathStr]
							newSchema := schemaHandler.SchemaElements[newSchemaIndex]
							if newSchema.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL {
								newNode.DL++
							}
							stack = append(stack, newNode)

						} else {
							newPathStr := node.PathMap.Children[key].Path
							for path, table := range res {
								if strings.HasPrefix(path, newPathStr) &&
									(len(path) == len(newPathStr) || path[len(newPathStr)] == common.PAR_GO_PATH_DELIMITER[0]) {

									table.Values = append(table.Values, nil)
									table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
									table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
								}
							}
						}
					}
				}

			} else if tk == reflect.Slice {
				ln := node.Val.Len()

				if schema.GetConvertedType() == parquet.ConvertedType_LIST { // real LIST
					pathStr = pathStr + common.PAR_GO_PATH_DELIMITER + "List" + common.PAR_GO_PATH_DELIMITER + "Element"
					if ln <= 0 {
						for key, table := range res {
							if strings.HasPrefix(key, node.PathMap.Path) &&
								(len(key) == len(node.PathMap.Path) || key[len(node.PathMap.Path)] == common.PAR_GO_PATH_DELIMITER[0]) {
								table.Values = append(table.Values, nil)
								table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
								table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
							}
						}
					}
					rlNow, _ := schemaHandler.MaxRepetitionLevel(common.StrToPath(pathStr))

					for j := ln - 1; j >= 0; j-- {
						newNode := nodeBuf.GetNode()
						newNode.PathMap = node.PathMap.Children["List"].Children["Element"]
						newNode.Val = node.Val.Index(j).Elem()
						if j == 0 {
							newNode.RL = node.RL
						} else {
							newNode.RL = rlNow
						}
						newNode.DL = node.DL + 1

						newPathStr := newNode.PathMap.Path
						newSchemaIndex := schemaHandler.MapIndex[newPathStr]
						newSchema := schemaHandler.SchemaElements[newSchemaIndex]
						if newSchema.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL { //element of LIST can only be optional or required
							newNode.DL++
						}

						stack = append(stack, newNode)
					}

				} else { //Repeated
					if ln <= 0 {
						for key, table := range res {
							if strings.HasPrefix(key, node.PathMap.Path) &&
								(len(key) == len(node.PathMap.Path) || key[len(node.PathMap.Path)] == common.PAR_GO_PATH_DELIMITER[0]) {
								table.Values = append(table.Values, nil)
								table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
								table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
							}
						}
					}
					rlNow, _ := schemaHandler.MaxRepetitionLevel(common.StrToPath(pathStr))

					for j := ln - 1; j >= 0; j-- {
						newNode := nodeBuf.GetNode()
						newNode.PathMap = node.PathMap
						newNode.Val = node.Val.Index(j).Elem()
						if j == 0 {
							newNode.RL = node.RL
						} else {
							newNode.RL = rlNow
						}
						newNode.DL = node.DL + 1
						stack = append(stack, newNode)
					}
				}

			} else {
				table := res[node.PathMap.Path]
				pT, cT := schema.Type, schema.ConvertedType
				val, err := types.JSONTypeToParquetType(node.Val, pT, cT, int(schema.GetTypeLength()), int(schema.GetScale()))
				if err != nil {
					return nil, err
				}

				table.Values = append(table.Values, val)
				table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
				table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
			}
		}
	}

	return &res, nil

}
