// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"encoding/json"
	"testing"

	"github.com/gogo/protobuf/proto"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
)

func TestBorrowedLabelWireAndJSONCompatibility(t *testing.T) {
	for _, value := range []string{"", "normal", "世界", "quote\"and\nnewline", "\xff"} {
		borrowed := LabelAdapter{Name: BorrowString("label"), Value: BorrowString(value)}
		ordinary := prompb.Label{Name: "label", Value: value}
		got, err := borrowed.Marshal()
		require.NoError(t, err)
		want, err := proto.Marshal(&ordinary)
		require.NoError(t, err)
		require.Equal(t, want, got)
		require.Equal(t, len(got), borrowed.Size())

		var decoded LabelAdapter
		require.NoError(t, decoded.Unmarshal(want))
		require.Equal(t, borrowed, decoded)
		for _, marshal := range []func(any) ([]byte, error){json.Marshal, jsoniter.Marshal} {
			got, err := marshal(borrowed)
			require.NoError(t, err)
			// LabelAdapter historically uses the exported Go field names, unlike
			// prompb.Label's JSON tags. Preserve that existing JSON contract.
			want, err := marshal(struct{ Name, Value string }{"label", value})
			require.NoError(t, err)
			require.JSONEq(t, string(want), string(got))
		}
	}
}

func TestStringArrayWithCopyPreservesLabelBoundaries(t *testing.T) {
	input := []LabelAdapter{
		{Name: BorrowString(""), Value: BorrowString("first")},
		{Name: BorrowString("second"), Value: BorrowString("")},
		{Name: BorrowString("third"), Value: BorrowString("last")},
	}
	require.Equal(t, []string{"", "first", "second", "", "third", "last"}, FromLabelAdaptersToStringArrayWithCopy(input))
	require.Empty(t, FromLabelAdaptersToStringArrayWithCopy(nil))
}

func TestLabelConversionsOwnBorrowedBytes(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		convert func([]LabelAdapter) func()
	}{
		{
			name: "labels with copy",
			convert: func(input []LabelAdapter) func() {
				result := FromLabelAdaptersToLabelsWithCopy(input)
				return func() { require.Equal(t, "value", result.Get("label")) }
			},
		},
		{
			name: "string array with copy",
			convert: func(input []LabelAdapter) func() {
				result := FromLabelAdaptersToStringArrayWithCopy(input)
				return func() { require.Equal(t, []string{"label", "value"}, result) }
			},
		},
		{
			name: "metric",
			convert: func(input []LabelAdapter) func() {
				result := FromLabelAdaptersToMetric(input)
				return func() {
					require.Equal(t, model.Metric{model.LabelName("label"): model.LabelValue("value")}, result)
				}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			name := []byte("label")
			value := []byte("value")
			input := []LabelAdapter{{Name: BorrowString(yoloString(name)), Value: BorrowString(yoloString(value))}}
			check := testCase.convert(input)
			copy(name, "other")
			copy(value, "xxxxx")
			input[0] = LabelAdapter{}
			check()
		})
	}
}
