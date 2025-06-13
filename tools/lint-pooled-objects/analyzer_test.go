package main

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	testDataDir := analysistest.TestData()
	analysistest.Run(t, testDataDir, Analyzer, ".")
}
