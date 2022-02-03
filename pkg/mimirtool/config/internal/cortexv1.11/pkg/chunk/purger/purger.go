// SPDX-License-Identifier: AGPL-3.0-only

package purger

import (
	"flag"
	"time"
)

type Config struct {
	Enable                    bool          `yaml:"enable"`
	NumWorkers                int           `yaml:"num_workers"`
	ObjectStoreType           string        `yaml:"object_store_type"`
	DeleteRequestCancelPeriod time.Duration `yaml:"delete_request_cancel_period"`
}

const (
	millisecondPerDay           = int64(24 * time.Hour / time.Millisecond)
	statusSuccess               = "success"
	statusFail                  = "fail"
	loadRequestsInterval        = time.Hour
	retryFailedRequestsInterval = 15 * time.Minute
)

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enable, "purger.enable", false, "Enable purger to allow deletion of series. Be aware that Delete series feature is still experimental")
	f.IntVar(&cfg.NumWorkers, "purger.num-workers", 2, "Number of workers executing delete plans in parallel")
	f.StringVar(&cfg.ObjectStoreType, "purger.object-store-type", "", "Name of the object store to use for storing delete plans")
	f.DurationVar(&cfg.DeleteRequestCancelPeriod, "purger.delete-request-cancel-period", 24*time.Hour, "Allow cancellation of delete request until duration after they are created. Data would be deleted only after delete requests have been older than this duration. Ideally this should be set to at least 24h.")
}
