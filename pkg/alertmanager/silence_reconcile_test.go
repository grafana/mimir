// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"net/url"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/alertmanager/featurecontrol"
	"github.com/prometheus/alertmanager/silence/silencepb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/grafana/mimir/pkg/alertmanager/alertmanagerpb"
)

func newTestAlertmanager(t *testing.T) *Alertmanager {
	t.Helper()

	am, err := New(&Config{
		UserID:            "test",
		Logger:            log.NewNopLogger(),
		Limits:            &mockAlertManagerLimits{},
		Features:          featurecontrol.NoopFlags{},
		TenantDataDir:     t.TempDir(),
		ExternalURL:       &url.URL{Path: "/am"},
		ShardingEnabled:   true,
		Store:             prepareInMemoryAlertStore(),
		Replicator:        &stubReplicator{},
		ReplicationFactor: 1,
		PersisterConfig:   PersisterConfig{Interval: time.Hour},
	}, prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	t.Cleanup(am.StopAndWait)

	return am
}

func addTestSilence(t *testing.T, am *Alertmanager, comment string) string {
	t.Helper()
	sil := &silencepb.Silence{
		Matchers: []*silencepb.Matcher{{Name: "a", Pattern: "b"}},
		StartsAt: timestamppb.Now(),
		EndsAt:   timestamppb.New(time.Now().Add(5 * time.Minute)),
		Comment:  comment,
	}
	require.NoError(t, am.silences.Set(t.Context(), sil))
	return sil.Id
}

func TestAlertmanagerGetState(t *testing.T) {
	am := newTestAlertmanager(t)
	addTestSilence(t, am, "one")

	t.Run("silences not set returns everything", func(t *testing.T) {
		full, err := am.getState(&alertmanagerpb.ReadStateRequest{})
		require.NoError(t, err)
		assert.Len(t, full.Parts, 2, "expected a silences and an nflog part")
	})

	t.Run("silences set returns only the silences part", func(t *testing.T) {
		got, err := am.getState(&alertmanagerpb.ReadStateRequest{OnlySilences: true})
		require.NoError(t, err)
		require.Len(t, got.Parts, 1, "nflog must not be marshaled or returned")
		assert.Equal(t, silencesStateKeyPrefix+"test", got.Parts[0].Key)
		assert.NotEmpty(t, got.Parts[0].Data, "the silence itself must be marshaled, not just an empty part")
	})
}
