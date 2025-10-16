// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/compactor/compactor_ring.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
package compactor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
)

const (
	MaxDiskCheckAttempts = 5
)

type HealthCheckService struct {
	*services.BasicService
	testfile string
	logger   log.Logger
}

func NewHealthCheck(dataDir string, logger log.Logger) HealthCheckService {
	svc := HealthCheckService{
		testfile: filepath.Join(dataDir, ".rw-test"),
		logger:   logger,
	}
	svc.BasicService = services.NewBasicService(nil, svc.run, nil)
	return svc
}

// run performs a simple 'touch' operation on a local filesystem, ensuring our health check includes
// the ability to write/read to the mounted volume.  If the compactor is unable to interact with the
// mounted volume, it should fail the pod's /ready check.
// We track three types of operations as potential failures: write a test file, stat the test file,
// and read from the test file.  We also attempt to remove the file, but don't count this as an
// error if it fails.
func (hc HealthCheckService) run(ctx context.Context) error {
	ticker := time.NewTicker(30 * time.Second)

	defer func() {
		if _, err := os.Stat(hc.testfile); err == nil {
			_ = os.Remove(hc.testfile)
		}
		ticker.Stop()
	}()

	writeAttempts := 0
	readAttempts := 0
	existAttempts := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:

			if err := os.WriteFile(hc.testfile, []byte(""), 0o644); err != nil {
				level.Warn(hc.logger).Log("msg", fmt.Sprintf("error writing test file %s", hc.testfile), "err", err)

				if (writeAttempts + readAttempts + existAttempts) >= MaxDiskCheckAttempts {
					level.Error(hc.logger).Log("msg", fmt.Sprintf("failed to read/write test file %s after %d attempts", hc.testfile, writeAttempts+readAttempts+existAttempts), "err", err)
					return nil
				}
				writeAttempts++
				continue
			} else {
				writeAttempts = 0
			}

			if _, err := os.Stat(hc.testfile); err != nil {
				level.Warn(hc.logger).Log("msg", "failed to stat test file %s after it was created", "err", err)
				existAttempts++
			} else {
				existAttempts = 0
				if _, err := os.ReadFile(hc.testfile); err != nil {
					level.Warn(hc.logger).Log("msg", "failed to read test file %s after it was created", "err", err)
					readAttempts++
				} else {
					readAttempts = 0
				}
			}

			if err := os.Remove(hc.testfile); err != nil {
				level.Warn(hc.logger).Log("msg", fmt.Sprintf("error removing test file %s volume", hc.testfile), "err", err)
			}
		}
	}
}
