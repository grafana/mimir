package main

import (
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

func main() {
	mimirDistributorURL := "http://localhost:8080"

	logger := log.NewLogfmtLogger(os.Stdout)

	// Build dataset.
	level.Info(logger).Log("msg", "Building dataset")
	ringStatus, err := getRingStatus(mimirDistributorURL)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to build dataset", "err", err)
		os.Exit(1)
	}
	level.Info(logger).Log("msg", "Successfully built dataset")

	// Run analysis.
	analyseRing(ringStatus)
}
