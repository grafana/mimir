// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"os"
	"time"

	"github.com/grafana/dskit/kv/memberlist"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/server"

	util_log "github.com/grafana/mimir/pkg/util/log"
)

var (
	memberlistConfig  = &memberlist.KVConfig{}
	serverConfig      = &server.Config{}
	keyChangeInterval = model.Duration(time.Minute)
	isLeader          bool
)

func init() {
	fs := flag.NewFlagSet("", flag.PanicOnError)
	fs.Var(&keyChangeInterval, "key-change-interval", "")
	fs.BoolVar(&isLeader, "is-leader", false, "")
	memberlistConfig.RegisterFlags(fs)
	serverConfig.RegisterFlags(fs)
	err := fs.Parse(os.Args[1:])
	if err != nil {
		panic(err)
	}
	util_log.InitLogger(serverConfig)
}
