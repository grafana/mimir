// SPDX-License-Identifier: AGPL-3.0-only

package write

import (
	"reflect"
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
  -
    # The username
    [username: <string> | default = ""]

    # The password
    [password: <string> | default = ""]
` + threeBackticks

	require.Equal(t, expected, md.String())
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
	Username string `yaml:"username" doc:"description=The username"`
	Password string `yaml:"password" doc:"description=The password"`
}
