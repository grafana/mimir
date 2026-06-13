// SPDX-License-Identifier: AGPL-3.0-only

// Package integration holds the WarpstreamClient scenario tests. The package
// is unconditionally compiled — every file is a _test.go file, so it only
// runs as part of `go test`, but `go build`/`go vet` validate it on every
// build. Scenarios skip themselves unless the WARPSTREAM_INTEGRATION
// environment variable is set; this keeps the default test run fast while
// letting the same binary host long real-time scenarios on demand.
package integration

import (
	"os"
	"testing"
	"time"
)

// integrationEnvVar gates the scenario tests. Helpers in this package have
// regular unit tests that always run; only the long-running TestScenario_*
// functions consult this var.
const integrationEnvVar = "WARPSTREAM_INTEGRATION"

// skipIfNotIntegration short-circuits the test unless integrationEnvVar is
// set to any non-empty value.
func skipIfNotIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv(integrationEnvVar) == "" {
		t.Skipf("set %s=1 to run warpstream client integration scenarios", integrationEnvVar)
	}
}

const (
	// integrationTopic is the single topic every scenario produces to.
	integrationTopic = "integration-topic"

	// integrationClusterSize is both the number of brokers and the number
	// of partitions: the kgo-baseline latency wrapper uses rec.Partition
	// as the broker id, which only works while partitions and brokers are
	// 1:1 (each broker leads exactly one partition).
	integrationClusterSize = int32(50)
)

// Scenario timing. The warmup phase is sized so the WarpstreamClient stats
// tracker accumulates at least 3 buckets (10s each in production) before
// the observed phase begins; see pkg/warpstreamclient/agent_stats_tracker.go.
const (
	scenarioWarmupDuration   = 30 * time.Second
	scenarioObservedDuration = 60 * time.Second
	scenarioEventSpacing     = 500 * time.Millisecond

	// scenarioAppRequestTimeout caps each simulated application request:
	// an event whose partitions don't all complete within this window is
	// recorded as failed with context.DeadlineExceeded. Models a caller
	// that won't wait forever for the producer to ack.
	scenarioAppRequestTimeout = 5 * time.Second

	// Match pkg/storage/ingest/writer_client.go constants.
	integrationTestLinger                 = 50 * time.Millisecond
	integrationTestBatchMaxBytes          = 16_000_000
	integrationTestMaxInflight            = 20
	integrationTestMetadataRefresh        = 10 * time.Second
	integrationTestDialTimeout            = 2 * time.Second
	integrationTestWriteTimeout           = 10 * time.Second
	integrationTestRequestTimeoutOverhead = 2 * time.Second

	// integrationProduceAPIVersion mirrors the version pinned by
	// WarpstreamClient. Kept in sync explicitly because the constant in the
	// parent package is unexported.
	integrationProduceAPIVersion = int16(11)
)
