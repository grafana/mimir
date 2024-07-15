// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"bytes"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
)

func TestValidateMatchersInConfigDesc(t *testing.T) {
	tests := []struct {
		name     string
		config   alertspb.AlertConfigDesc
		expected []string
	}{{
		name: "config is accepted",
		config: alertspb.AlertConfigDesc{
			User: "1",
			RawConfig: `
route:
  routes:
   - matchers:
      - foo=bar
inhibit_rules:
  - source_matchers:
    - bar=baz
  - target_matchers:
    - baz=qux
`,
		},
		expected: []string{
			`level=debug user=1 msg="Parsing with UTF-8 matchers parser, with fallback to classic matchers parser" input="foo=bar" origin=test`,
			`level=debug user=1 msg="Parsing with UTF-8 matchers parser, with fallback to classic matchers parser" input="bar=baz" origin=test`,
			`level=debug user=1 msg="Parsing with UTF-8 matchers parser, with fallback to classic matchers parser" input="baz=qux" origin=test`,
		},
	}, {
		name: "config contains invalid input",
		config: alertspb.AlertConfigDesc{
			User: "2",
			RawConfig: `
route:
  routes:
   - matchers:
      - foo!bar
inhibit_rules:
  - source_matchers:
    - bar!baz
  - target_matchers:
    - baz!qux
`,
		},
		expected: []string{
			`level=debug user=2 msg="Parsing with UTF-8 matchers parser, with fallback to classic matchers parser" input=foo!bar origin=test`,
			`level=debug user=2 msg="Invalid matcher in route" input=foo!bar origin=test err="bad matcher format: foo!bar"`,
			`level=debug user=2 msg="Parsing with UTF-8 matchers parser, with fallback to classic matchers parser" input=bar!baz origin=test`,
			`level=debug user=2 msg="Invalid matcher in inhibition rule source matchers" input=bar!baz origin=test err="bad matcher format: bar!baz"`,
			`level=debug user=2 msg="Parsing with UTF-8 matchers parser, with fallback to classic matchers parser" input=baz!qux origin=test`,
			`level=debug user=2 msg="Invalid matcher in inhibition rule target matchers" input=baz!qux origin=test err="bad matcher format: baz!qux"`,
		},
	}, {
		name: "config is accepted in matchers/parse but not pkg/labels",
		config: alertspb.AlertConfigDesc{
			User: "3",
			RawConfig: `
route:
  routes:
   - matchers:
      - foo🙂=bar
inhibit_rules:
  - source_matchers:
    - bar🙂=baz
  - target_matchers:
    - baz🙂=qux
`,
		},
		expected: []string{
			`level=debug user=3 msg="Parsing with UTF-8 matchers parser, with fallback to classic matchers parser" input="foo🙂=bar" origin=test`,
			`level=debug user=3 msg="Parsing with UTF-8 matchers parser, with fallback to classic matchers parser" input="bar🙂=baz" origin=test`,
			`level=debug user=3 msg="Parsing with UTF-8 matchers parser, with fallback to classic matchers parser" input="baz🙂=qux" origin=test`,
		},
	}, {
		name: "config is accepted in pkg/labels but not matchers/parse",
		config: alertspb.AlertConfigDesc{
			User: "4",
			RawConfig: `
route:
  routes:
   - matchers:
      - foo=
inhibit_rules:
  - source_matchers:
    - bar=
  - target_matchers:
    - baz=
`,
		},
		expected: []string{
			`level=debug user=4 msg="Parsing with UTF-8 matchers parser, with fallback to classic matchers parser" input="foo=" origin=test`,
			`level=warn user=4 msg="Alertmanager is moving to a new parser for labels and matchers, and this input is incompatible. Alertmanager has instead parsed the input using the classic matchers parser as a fallback. To make this input compatible with the UTF-8 matchers parser please make sure all regular expressions and values are double-quoted. If you are still seeing this message please open an issue." input="foo=" origin=test err="end of input: expected label value" suggestion="foo=\"\""`,
			`level=debug user=4 msg="Parsing with UTF-8 matchers parser, with fallback to classic matchers parser" input="bar=" origin=test`,
			`level=warn user=4 msg="Alertmanager is moving to a new parser for labels and matchers, and this input is incompatible. Alertmanager has instead parsed the input using the classic matchers parser as a fallback. To make this input compatible with the UTF-8 matchers parser please make sure all regular expressions and values are double-quoted. If you are still seeing this message please open an issue." input="bar=" origin=test err="end of input: expected label value" suggestion="bar=\"\""`,
			`level=debug user=4 msg="Parsing with UTF-8 matchers parser, with fallback to classic matchers parser" input="baz=" origin=test`,
			`level=warn user=4 msg="Alertmanager is moving to a new parser for labels and matchers, and this input is incompatible. Alertmanager has instead parsed the input using the classic matchers parser as a fallback. To make this input compatible with the UTF-8 matchers parser please make sure all regular expressions and values are double-quoted. If you are still seeing this message please open an issue." input="baz=" origin=test err="end of input: expected label value" suggestion="baz=\"\""`,
		},
	}, {
		name: "config contains disagreement",
		config: alertspb.AlertConfigDesc{
			User: "5",
			RawConfig: `
route:
  routes:
   - matchers:
      - foo="\xf0\x9f\x99\x82"
inhibit_rules:
  - source_matchers:
    - bar="\xf0\x9f\x99\x82"
  - target_matchers:
    - baz="\xf0\x9f\x99\x82"
`,
		},
		expected: []string{
			`level=debug user=5 msg="Parsing with UTF-8 matchers parser, with fallback to classic matchers parser" input="foo=\"\\xf0\\x9f\\x99\\x82\"" origin=test`,
			`level=warn user=5 msg="Matchers input has disagreement" input="foo=\"\\xf0\\x9f\\x99\\x82\"" origin=test`,
			`level=debug user=5 msg="Parsing with UTF-8 matchers parser, with fallback to classic matchers parser" input="bar=\"\\xf0\\x9f\\x99\\x82\"" origin=test`,
			`level=warn user=5 msg="Matchers input has disagreement" input="bar=\"\\xf0\\x9f\\x99\\x82\"" origin=test`,
			`level=debug user=5 msg="Parsing with UTF-8 matchers parser, with fallback to classic matchers parser" input="baz=\"\\xf0\\x9f\\x99\\x82\"" origin=test`,
			`level=warn user=5 msg="Matchers input has disagreement" input="baz=\"\\xf0\\x9f\\x99\\x82\"" origin=test`,
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buf := bytes.Buffer{}
			validateMatchersInConfigDesc(log.NewLogfmtLogger(&buf), "test", test.config)
			lines := strings.Split(strings.Trim(buf.String(), "\n"), "\n")
			require.Equal(t, len(test.expected), len(lines))
			for i := range lines {
				require.Equal(t, test.expected[i], lines[i])
			}
		})
	}
}
