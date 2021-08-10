// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ring/kv/memberlist/kv_init_service_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package memberlist

import (
	"bytes"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/flagext"
)

func TestPage(t *testing.T) {
	conf := memberlist.DefaultLANConfig()
	ml, err := memberlist.Create(conf)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = ml.Shutdown()
	})

	require.NoError(t, pageTemplate.Execute(&bytes.Buffer{}, pageData{
		Now:           time.Now(),
		Memberlist:    ml,
		SortedMembers: ml.Members(),
		Store:         nil,
		ReceivedMessages: []message{{
			ID:   10,
			Time: time.Now(),
			Size: 50,
			Pair: KeyValuePair{
				Key:   "hello",
				Value: []byte("world"),
				Codec: "codec",
			},
			Version: 20,
			Changes: []string{"A", "B", "C"},
		}},

		SentMessages: []message{{
			ID:   10,
			Time: time.Now(),
			Size: 50,
			Pair: KeyValuePair{
				Key:   "hello",
				Value: []byte("world"),
				Codec: "codec",
			},
			Version: 20,
			Changes: []string{"A", "B", "C"},
		}},
	}))
}

func TestStop(t *testing.T) {
	var cfg KVConfig
	flagext.DefaultValues(&cfg)
	kvinit := NewKVInitService(&cfg, nil)
	require.NoError(t, kvinit.stopping(nil))
}
