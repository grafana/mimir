// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"reflect"
	"strings"
	"testing"

	rw2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/stretchr/testify/require"
	"github.com/xlab/treeprint"

	"github.com/grafana/mimir/pkg/util/test"
)

// Tests related to Prometheus Remote Write v2 (RW2) compatibility.

func TestRW2TypesCompatible(t *testing.T) {
	expectedType := reflect.TypeOf(rw2.Request{})
	actualType := reflect.TypeOf(WriteRequestRW2{})

	expectedTree := treeprint.NewWithRoot("<root>")
	// We ignore the XXX_ fields because RW2 in Prometheus has them,
	// but we don't. Which also means that the offsets would be different.
	// But we are not going to cast between the two types, so offsets
	// don't matter.
	test.AddTypeToTree(expectedType, expectedTree, false, true, true, false)

	actualTree := treeprint.NewWithRoot("<root>")
	test.AddTypeToTree(actualType, actualTree, false, true, true, false)

	// mimirpb.Sample fields order MUST match promql.FPoint so that we can
	// cast types between them. However this makes test.RequireSameShape
	// fail because the order is different.
	// So we need to reverse the order of the fields in the tree.
	// Also the name of the Timestamp field is slightly different in the
	// two types.
	var firstValue, secondValue string
	rootNode, _ := actualTree.(*treeprint.Node)
	firstValue, _ = rootNode.Nodes[1].Nodes[1].Nodes[0].Value.(string)
	secondValue, _ = rootNode.Nodes[1].Nodes[1].Nodes[1].Value.(string)
	rootNode.Nodes[1].Nodes[1].Nodes[0].Value = secondValue
	rootNode.Nodes[1].Nodes[1].Nodes[1].Value = strings.ReplaceAll(firstValue, "TimestampMs", "Timestamp")

	require.Equal(t, expectedTree.String(), actualTree.String(), "Proto types are not compatible")
}
