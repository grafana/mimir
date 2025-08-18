package marshal

import (
	"errors"
	"reflect"
	"strings"

	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/layout"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/schema"
	"github.com/xitongsys/parquet-go/types"
)

type Node struct {
	Val     reflect.Value
	PathMap *schema.PathMapType
	RL      int32
	DL      int32
}

//Improve Performance///////////////////////////
//NodeBuf
type NodeBufType struct {
	Index int
	Buf   []*Node
}

func NewNodeBuf(ln int) *NodeBufType {
	nodeBuf := new(NodeBufType)
	nodeBuf.Index = 0
	nodeBuf.Buf = make([]*Node, ln)
	for i := 0; i < ln; i++ {
		nodeBuf.Buf[i] = new(Node)
	}
	return nodeBuf
}

func (nbt *NodeBufType) GetNode() *Node {
	if nbt.Index >= len(nbt.Buf) {
		nbt.Buf = append(nbt.Buf, new(Node))
	}
	nbt.Index++
	return nbt.Buf[nbt.Index-1]
}

func (nbt *NodeBufType) Reset() {
	nbt.Index = 0
}

////////for improve performance///////////////////////////////////
type Marshaler interface {
	Marshal(node *Node, nodeBuf *NodeBufType) []*Node
}

type ParquetPtr struct{}

func (p *ParquetPtr) Marshal(node *Node, nodeBuf *NodeBufType) []*Node {
	nodes := make([]*Node, 0)
	if node.Val.IsNil() {
		return nodes
	}
	node.Val = node.Val.Elem()
	node.DL++
	nodes = append(nodes, node)
	return nodes
}

type ParquetStruct struct{}

func (p *ParquetStruct) Marshal(node *Node, nodeBuf *NodeBufType) []*Node {
	var ok bool

	numField := node.Val.Type().NumField()
	nodes := make([]*Node, 0, numField)
	for j := 0; j < numField; j++ {
		tf := node.Val.Type().Field(j)
		name := tf.Name
		newNode := nodeBuf.GetNode()

		//some ignored item
		if newNode.PathMap, ok = node.PathMap.Children[name]; !ok {
			continue
		}

		newNode.Val = node.Val.Field(j)
		newNode.RL = node.RL
		newNode.DL = node.DL
		nodes = append(nodes, newNode)
	}
	return nodes
}

type ParquetMapStruct struct{}

func (p *ParquetMapStruct) Marshal(node *Node, nodeBuf *NodeBufType) []*Node {
	var ok bool

	nodes := make([]*Node, 0)
	keys := node.Val.MapKeys()
	if len(keys) <= 0 {
		return nodes
	}

	for j := len(keys) - 1; j >= 0; j-- {
		key := keys[j]
		newNode := nodeBuf.GetNode()

		//some ignored item
		if newNode.PathMap, ok = node.PathMap.Children[key.String()]; !ok {
			continue
		}

		newNode.Val = node.Val.MapIndex(key)
		newNode.RL = node.RL
		newNode.DL = node.DL
		nodes = append(nodes, newNode)
	}
	return nodes
}

type ParquetSlice struct {
	schemaHandler *schema.SchemaHandler
}

func (p *ParquetSlice) Marshal(node *Node, nodeBuf *NodeBufType) []*Node {
	nodes := make([]*Node, 0)
	ln := node.Val.Len()
	pathMap := node.PathMap
	path := node.PathMap.Path
	if *p.schemaHandler.SchemaElements[p.schemaHandler.MapIndex[node.PathMap.Path]].RepetitionType != parquet.FieldRepetitionType_REPEATED {
		pathMap = pathMap.Children["List"].Children["Element"]
		path = path + common.PAR_GO_PATH_DELIMITER + "List" + common.PAR_GO_PATH_DELIMITER + "Element"
	}
	if ln <= 0 {
		return nodes
	}

	rlNow, _ := p.schemaHandler.MaxRepetitionLevel(common.StrToPath(path))
	for j := ln - 1; j >= 0; j-- {
		newNode := nodeBuf.GetNode()
		newNode.PathMap = pathMap
		newNode.Val = node.Val.Index(j)
		if j == 0 {
			newNode.RL = node.RL
		} else {
			newNode.RL = rlNow
		}
		newNode.DL = node.DL + 1
		nodes = append(nodes, newNode)
	}
	return nodes
}

type ParquetMap struct {
	schemaHandler *schema.SchemaHandler
}

func (p *ParquetMap) Marshal(node *Node, nodeBuf *NodeBufType) []*Node {
	nodes := make([]*Node, 0)
	path := node.PathMap.Path + common.PAR_GO_PATH_DELIMITER + "Key_value"
	keys := node.Val.MapKeys()
	if len(keys) <= 0 {
		return nodes
	}

	rlNow, _ := p.schemaHandler.MaxRepetitionLevel(common.StrToPath(path))
	for j := len(keys) - 1; j >= 0; j-- {
		key := keys[j]
		value := node.Val.MapIndex(key)
		newNode := nodeBuf.GetNode()
		newNode.PathMap = node.PathMap.Children["Key_value"].Children["Key"]
		newNode.Val = key
		newNode.DL = node.DL + 1
		if j == 0 {
			newNode.RL = node.RL
		} else {
			newNode.RL = rlNow
		}
		nodes = append(nodes, newNode)

		newNode = nodeBuf.GetNode()
		newNode.PathMap = node.PathMap.Children["Key_value"].Children["Value"]
		newNode.Val = value
		newNode.DL = node.DL + 1
		if j == 0 {
			newNode.RL = node.RL
		} else {
			newNode.RL = rlNow
		}
		nodes = append(nodes, newNode)
	}
	return nodes
}

//Convert the objects to table map. srcInterface is a slice of objects
func Marshal(srcInterface []interface{}, schemaHandler *schema.SchemaHandler) (tb *map[string]*layout.Table, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unkown error")
			}
		}
	}()

	src := reflect.ValueOf(srcInterface)
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
	for i := 0; i < len(srcInterface); i++ {
		stack = stack[:0]
		nodeBuf.Reset()

		node := nodeBuf.GetNode()
		node.Val = src.Index(i)
		if src.Index(i).Type().Kind() == reflect.Interface {
			node.Val = src.Index(i).Elem()
		}
		node.PathMap = pathMap
		stack = append(stack, node)

		for len(stack) > 0 {
			ln := len(stack)
			node := stack[ln-1]
			stack = stack[:ln-1]

			tk := node.Val.Type().Kind()
			var m Marshaler

			if tk == reflect.Ptr {
				m = &ParquetPtr{}
			} else if tk == reflect.Struct {
				m = &ParquetStruct{}
			} else if tk == reflect.Slice {
				m = &ParquetSlice{schemaHandler: schemaHandler}
			} else if tk == reflect.Map {
				schemaIndex := schemaHandler.MapIndex[node.PathMap.Path]
				sele := schemaHandler.SchemaElements[schemaIndex]
				if !sele.IsSetConvertedType() {
					m = &ParquetMapStruct{}
				} else {
					m = &ParquetMap{schemaHandler: schemaHandler}
				}
			} else {
				table := res[node.PathMap.Path]
				schemaIndex := schemaHandler.MapIndex[node.PathMap.Path]
				schema := schemaHandler.SchemaElements[schemaIndex]
				table.Values = append(table.Values, types.InterfaceToParquetType(node.Val.Interface(), schema.Type))
				table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
				table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
				continue
			}

			nodes := m.Marshal(node, nodeBuf)
			if len(nodes) == 0 {
				path := node.PathMap.Path
				index := schemaHandler.MapIndex[path]
				numChildren := schemaHandler.SchemaElements[index].GetNumChildren()
				if numChildren > int32(0) {
					for key, table := range res {
						if strings.HasPrefix(key, path) &&
							(len(key) == len(path) || key[len(path)] == common.PAR_GO_PATH_DELIMITER[0]) {
							table.Values = append(table.Values, nil)
							table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
							table.RepetitionLevels = append(table.RepetitionLevels, node.RL)
						}
					}
				} else {
					table := res[path]
					table.Values = append(table.Values, nil)
					table.DefinitionLevels = append(table.DefinitionLevels, node.DL)
					table.RepetitionLevels = append(table.RepetitionLevels, node.RL)

				}
			} else {
				for _, node := range nodes {
					stack = append(stack, node)
				}
			}
		}
	}

	return &res, nil
}
