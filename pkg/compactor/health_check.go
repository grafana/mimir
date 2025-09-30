package compactor

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
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
	testfile := filepath.Join("/data", ".rw-test")
	ticker := time.NewTicker(30 * time.Second)

	// cleanup the file if it exists once the health check stops running
	defer func() {
		st, err := os.Stat(testfile)
		if st != nil && err == nil {
			_ = os.Remove(testfile)
		}
		ticker.Stop()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := os.WriteFile(testfile, []byte(""), 0o644); err != nil {
				level.Error(hc.logger).Log("msg", "error while touching test file in /data volume", "err", err)
				return err
			}
			if err := os.Remove(testfile); err != nil {
				level.Error(hc.logger).Log("msg", "error while removing test file in /data volume", "err", err)
				return err
			}
		}
	}
}
