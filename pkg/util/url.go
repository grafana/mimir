// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"net/url"
)

func IsValidURL(endpoint string) bool {
	u, err := url.Parse(endpoint)
	if err != nil {
		return false
	}

	return u.Scheme != "" && u.Host != ""
}
