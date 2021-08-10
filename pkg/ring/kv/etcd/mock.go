// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ring/kv/etcd/mock.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package etcd

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"time"

	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/flagext"

	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"

	"github.com/grafana/mimir/pkg/ring/kv/codec"
)

const etcdStartTimeout = 30 * time.Second

// Mock returns a Mock Etcd client.
// Inspired by https://github.com/ligato/cn-infra/blob/master/db/keyval/etcd/mocks/embeded_etcd.go.
func Mock(codec codec.Codec) (*Client, io.Closer, error) {
	dir, err := ioutil.TempDir("", "etcd")
	if err != nil {
		return nil, nil, err
	}

	cfg := embed.NewConfig()
	cfg.Logger = "zap"
	cfg.Dir = dir
	lpurl, _ := url.Parse("http://localhost:0")
	lcurl, _ := url.Parse("http://localhost:0")
	cfg.LPUrls = []url.URL{*lpurl}
	cfg.LCUrls = []url.URL{*lcurl}

	etcd, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, nil, err
	}

	select {
	case <-etcd.Server.ReadyNotify():
	case <-time.After(etcdStartTimeout):
		etcd.Server.Stop() // trigger a shutdown
		return nil, nil, fmt.Errorf("server took too long to start")
	}

	closer := util.CloserFunc(func() error {
		etcd.Server.Stop()
		return nil
	})

	var config Config
	flagext.DefaultValues(&config)

	client := &Client{
		cfg:   config,
		codec: codec,
		cli:   v3client.New(etcd.Server),
	}

	return client, closer, nil
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix(f, "")
}
