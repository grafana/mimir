// SPDX-License-Identifier: AGPL-3.0-only

package validation

type BlockedQuery struct {
	Pattern string `yaml:"pattern"`
	Regex   bool   `yaml:"regex"`
}
