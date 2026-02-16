// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"github.com/prometheus/prometheus/promql/parser"
)

var (
	// ParserOptions holds the global parser configuration options for mimirtool.
	// These can be modified by command-line flags before parsing queries.
	ParserOptions = parser.Options{
		EnableExperimentalFunctions:  false,
		ExperimentalDurationExpr:     true,
		EnableExtendedRangeSelectors: true,
		EnableBinopFillModifiers:     false,
	}
)

func CreateParser() parser.Parser {
	return parser.NewParser(ParserOptions)
}
