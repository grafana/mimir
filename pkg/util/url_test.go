// SPDX-License-Identifier: AGPL-3.0-only
package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)
func TestIsValidURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		endpoint       string
	}{
		{
			name:     "valid url",
			endpoint: "https://sts.eu-central-1.amazonaws.com",
		},
		{
			name:     "invalid url no scheme",
			endpoint: "sts.eu-central-1.amazonaws.com",
		},
		{
			name:     "invalid url invalid scheme setup",
			endpoint: "https:///sts.eu-central-1.amazonaws.com",
		},
		{
			name:     "invalid url no host",
			endpoint: "https://",
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			valid := IsValidURL(test.endpoint)

			if i == 0 {
				assert.True(t, valid)
			} else {
				assert.False(t, valid)
			}
		})
	}
}
