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
	// TODO: balance between a const and another config value?
	MaxAttempts = 3
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

// run performs a simple 'touch' operation on the /data volume, ensuring our health check includes
// the ability to read/write to this partition
func (hc HealthCheckService) run(ctx context.Context) error {
	ticker := time.NewTicker(30 * time.Second)

	// cleanup the file if it exists once the health check stops running
	defer func() {
		st, err := os.Stat(hc.testfile)
		if st != nil && err == nil {
			_ = os.Remove(hc.testfile)
		}
		ticker.Stop()
	}()

	attempts := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:

			// TODO: no check of the error
			if err := os.WriteFile(hc.testfile, []byte(""), 0o644); err != nil {
				level.Warn(hc.logger).Log("msg", fmt.Sprintf("error writing test file %s", hc.testfile), "err", err)

				if attempts >= MaxAttempts {
					level.Error(hc.logger).Log("msg", fmt.Sprintf("failed to write test file %s after %d attempts", hc.testfile, attempts), "err", err)
					return nil
				}
				attempts++
			}

			// TODO: if this fails, should we include it in the attempts?
			if err := os.Remove(hc.testfile); err != nil {
				level.Warn(hc.logger).Log("msg", fmt.Sprintf("error removing test file %s volume", hc.testfile), "err", err)
			}
		}
	}
}
