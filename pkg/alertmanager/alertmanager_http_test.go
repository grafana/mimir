// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/alertmanager_http_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	"io"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/alertmanager/featurecontrol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/test"
)

func TestMultitenantAlertmanager_GetStatusHandler(t *testing.T) {
	store := prepareInMemoryAlertStore()
	reg := prometheus.NewPedanticRegistry()
	cfg := mockAlertmanagerConfig(t)
	am := setupSingleMultitenantAlertmanager(t, cfg, store, nil, featurecontrol.NoopFlags{}, test.NewTestingLogger(t), reg)

	req := httptest.NewRequest("GET", "http://alertmanager.cortex/status", nil)
	w := httptest.NewRecorder()
	am.StatusHandler(w, req)

	resp := w.Result()
	require.Equal(t, 200, w.Code)
	body, _ := io.ReadAll(resp.Body)
	content := string(body)
	require.Contains(t, content, "Alertmanager Status: Running")
}
