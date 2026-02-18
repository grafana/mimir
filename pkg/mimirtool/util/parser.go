package util

import "github.com/prometheus/prometheus/promql/parser"

func CreatePromQLParser(enableExperimentalFunctions bool) parser.Parser {
	return parser.NewParser(parser.Options{
		EnableExperimentalFunctions:  enableExperimentalFunctions,
		ExperimentalDurationExpr:     true,
		EnableExtendedRangeSelectors: false,
		EnableBinopFillModifiers:     false,
	})
}
