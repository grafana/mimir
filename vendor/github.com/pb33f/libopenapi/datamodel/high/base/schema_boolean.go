// Copyright 2026 Dave Shanley / Quobix
// SPDX-License-Identifier: MIT

package base

import (
	"strconv"

	"github.com/pb33f/go-yaml"
	"github.com/pb33f/libopenapi/utils"
)

// booleanSchemaNode keeps source comments without mutating the parsed schema.
func (s *Schema) booleanSchemaNode() *yaml.Node {
	if s.BooleanSchema == nil {
		return nil
	}
	node := utils.CreateBoolNode(strconv.FormatBool(*s.BooleanSchema))
	if s.low != nil && s.low.BooleanSchema.ValueNode != nil {
		*node = *s.low.BooleanSchema.ValueNode
		node.Value = strconv.FormatBool(*s.BooleanSchema)
	}
	return node
}
