// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewEnvVarsWithPrefix(t *testing.T) {
	testCases := []struct {
		prefix                 string
		fieldsShouldMatchRegex string
	}{
		{
			prefix:                 "PREFIX",
			fieldsShouldMatchRegex: "^PREFIX_[A-Z].+",
		},
		{
			prefix:                 "PREFIX_",
			fieldsShouldMatchRegex: "^PREFIX_[A-Z].+",
		},
		{
			prefix:                 "",
			fieldsShouldMatchRegex: "^[A-Z].+",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.prefix, func(t *testing.T) {
			vars := reflect.ValueOf(NewEnvVarsWithPrefix(tc.prefix))
			for i := 0; i < vars.NumField(); i++ {
				t.Run(vars.Type().Field(i).Name, func(t *testing.T) {
					assert.Regexp(t, tc.fieldsShouldMatchRegex, vars.Field(i).Interface().(string))
				})
			}
		})
	}

}
