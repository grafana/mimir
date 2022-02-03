// SPDX-License-Identifier: AGPL-3.0-only

package local

import (
	"errors"
	"flag"
	"time"
)

type BoltDBConfig struct {
	Directory string `yaml:"directory"`
}

var (
	bucketName          = []byte("index")
	ErrUnexistentBoltDB = errors.New("boltdb file does not exist")
)

const (
	separator      = "\000"
	dbReloadPeriod = 10 * time.Minute

	DBOperationRead = iota
	DBOperationWrite

	openBoltDBFileTimeout = 5 * time.Second
)

func (cfg *BoltDBConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Directory, "boltdb.dir", "", "Location of BoltDB index files.")
}
