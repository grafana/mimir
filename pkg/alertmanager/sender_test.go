// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	alertingHttp "github.com/grafana/alerting/http"
	alertingReceivers "github.com/grafana/alerting/receivers"
	"github.com/stretchr/testify/require"

	util_net "github.com/grafana/mimir/pkg/util/net"
	"github.com/grafana/mimir/pkg/util/version"
)

func TestSendWebhook(t *testing.T) {
	var got *http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/error" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		got = r
		w.WriteHeader(http.StatusOK)
	}))
	s, err := alertingHttp.NewClient(nil, alertingHttp.WithUserAgent(version.UserAgent()))
	require.NoError(t, err)

	// The method should be either POST or PUT.
	cmd := alertingReceivers.SendWebhookSettings{
		HTTPMethod: http.MethodGet,
		URL:        server.URL,
	}
	require.ErrorIs(t, s.SendWebhook(context.Background(), log.NewNopLogger(), &cmd), alertingHttp.ErrInvalidMethod)

	// If the method is not specified, it should default to POST.
	// Content type should default to application/json.
	testHeaders := map[string]string{
		"test-header-1": "test-1",
		"test-header-2": "test-2",
		"test-header-3": "test-3",
	}
	cmd = alertingReceivers.SendWebhookSettings{
		URL:        server.URL,
		HTTPHeader: testHeaders,
	}
	require.NoError(t, s.SendWebhook(context.Background(), log.NewNopLogger(), &cmd))
	require.Equal(t, http.MethodPost, got.Method)
	require.Equal(t, "application/json", got.Header.Get("Content-Type"))

	// User agent should be correctly set.
	require.Equal(t, version.UserAgent(), got.Header.Get("User-Agent"))

	// No basic auth should be set if user and password are not provided.
	_, _, ok := got.BasicAuth()
	require.False(t, ok)

	// Request heders should be set.
	for k, v := range testHeaders {
		require.Equal(t, v, got.Header.Get(k))
	}

	// Basic auth should be correctly set.
	testUser := "test-user"
	testPassword := "test-password"
	cmd = alertingReceivers.SendWebhookSettings{
		URL:      server.URL,
		User:     testUser,
		Password: testPassword,
	}

	require.NoError(t, s.SendWebhook(context.Background(), log.NewNopLogger(), &cmd))
	user, password, ok := got.BasicAuth()
	require.True(t, ok)
	require.Equal(t, testUser, user)
	require.Equal(t, testPassword, password)

	// Validation errors should be returned.
	testErr := errors.New("test")
	cmd = alertingReceivers.SendWebhookSettings{
		URL:        server.URL,
		Validation: func([]byte, int) error { return testErr },
	}

	require.ErrorIs(t, s.SendWebhook(context.Background(), log.NewNopLogger(), &cmd), testErr)

	// A non-200 status code should cause an error.
	cmd = alertingReceivers.SendWebhookSettings{
		URL: server.URL + "/error",
	}
	require.Error(t, s.SendWebhook(context.Background(), log.NewNopLogger(), &cmd))

	// Firewall dialer should prevent local connections.
	limits := &mockAlertManagerLimits{
		receiversBlockPrivateAddresses: true,
	}
	firewallDialer := util_net.NewFirewallDialer(newFirewallDialerConfigProvider("test", limits))
	cmd = alertingReceivers.SendWebhookSettings{
		URL: server.URL,
	}
	s, err = alertingHttp.NewClient(nil, alertingHttp.WithUserAgent(version.UserAgent()), alertingHttp.WithDialer(*firewallDialer.Dialer()))
	require.NoError(t, err)
	err = s.SendWebhook(context.Background(), log.NewNopLogger(), &cmd)
	require.Error(t, err)
	require.ErrorContains(t, err, "blocked address")
}
