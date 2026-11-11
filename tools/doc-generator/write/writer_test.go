// SPDX-License-Identifier: AGPL-3.0-only

package write

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/tools/doc-generator/parse"
)

func TestMarkdownWriter(t *testing.T) {
	rootBlocks := []parse.RootBlock{
		{
			Name:       "the_root",
			Desc:       "Top-level of all configuration.",
			StructType: reflect.TypeOf(Root{}),
		},
	}

	blocks, err := parse.Config(&Root{}, nil, rootBlocks)
	require.NoError(t, err)

	md := &MarkdownWriter{}
	md.WriteConfigDoc(blocks, rootBlocks)
	threeBackticks := "```"
	expected := threeBackticks + `yaml
# The string
[some_string: <string> | default = ""]

# The nested config
nested_struct:
  # The other string
  [some_other_string: <string> | default = ""]

  # The timeout
  [timeout: <duration> | default = 30s]

# The nested list
nested_slice:
  - # A long description that will wrap onto a second line when rendered inside
    # a slice item element
    [long_desc: <string> | default = ""]

    # The username
    [username: <string> | default = ""]

    # The password
    [password: <string> | default = ""]
` + threeBackticks

	require.Equal(t, expected, md.String())
}

func TestSpecWriter_BlankCommentLines(t *testing.T) {
	for _, innerIndent := range []int{0, 2} {
		t.Run(fmt.Sprintf("inner_indent_%d", innerIndent), func(t *testing.T) {
			w := &specWriter{}
			w.writeWrappedString("First paragraph\n\nSecond paragraph", 0, 2, innerIndent)
			padding := strings.Repeat(" ", innerIndent)
			require.Equal(t, "# "+padding+"First paragraph\n  #\n  # "+padding+"Second paragraph", w.string())
		})
	}
}

type Root struct {
	SomeString       string               `yaml:"some_string" doc:"description=The string"`
	SomeNestedStruct NestedStruct         `yaml:"nested_struct" doc:"description=The nested config"`
	SomeNestedSlice  []NestedSliceElement `yaml:"nested_slice" doc:"description=The nested list"`
}

type NestedStruct struct {
	SomeOtherString string        `yaml:"some_other_string" doc:"description=The other string"`
	Timeout         time.Duration `yaml:"timeout" doc:"description=The timeout|default=30s"`
}

type NestedSliceElement struct {
	LongDesc string `yaml:"long_desc" doc:"description=A long description that will wrap onto a second line when rendered inside a slice item element"`
	Username string `yaml:"username" doc:"description=The username"`
	Password string `yaml:"password" doc:"description=The password"`
}
