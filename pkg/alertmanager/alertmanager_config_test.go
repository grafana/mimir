// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
)

func TestValidateMatchersInConfigDesc(t *testing.T) {
	tests := []struct {
		name        string
		config      alertspb.AlertConfigDesc
		expectedErr string
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
	}, {
		name: "config contains invalid input in route",
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
		expectedErr: "Invalid matcher in route: foo!bar: 3:4: !: expected one of '=~': expected an operator such as '=', '!=', '=~' or '!~'",
	}, {
		name: "config contains invalid input in inhibition rule source matchers",
		config: alertspb.AlertConfigDesc{
			User: "2",
			RawConfig: `
route:
  routes:
   - matchers:
      - foo=bar
inhibit_rules:
  - source_matchers:
    - bar!baz
  - target_matchers:
    - baz!qux
`,
		},
		expectedErr: "Invalid matcher in inhibition rule source matchers: bar!baz: 3:4: !: expected one of '=~': expected an operator such as '=', '!=', '=~' or '!~'",
	}, {
		name: "config contains invalid input in inhibition rule target matchers",
		config: alertspb.AlertConfigDesc{
			User: "2",
			RawConfig: `
route:
  routes:
   - matchers:
      - foo=bar
inhibit_rules:
  - source_matchers:
    - bar=baz
  - target_matchers:
    - baz!qux
`,
		},
		expectedErr: "Invalid matcher in inhibition rule target matchers: baz!qux: 3:4: !: expected one of '=~': expected an operator such as '=', '!=', '=~' or '!~'",
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
		expectedErr: "Invalid matcher in route: foo=: end of input: expected label value",
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
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateMatchersInConfigDesc(log.NewNopLogger(), "test", test.config)
			if test.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, test.expectedErr)
			}
		})
	}
}
