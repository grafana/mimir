// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/client/rules_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestMimirClient_X(t *testing.T) {
	requestCh := make(chan *http.Request, 1)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCh <- r
		fmt.Fprintln(w, "hello")
	}))
	defer ts.Close()

	for _, tc := range []struct {
		test            string
		namespace       string
		name            string
		expURLPath      string
		useLegacyRoutes bool
		mimirHTTPPrefix string
	}{
		{
			test:       "regular-characters",
			namespace:  "my-namespace",
			name:       "my-name",
			expURLPath: "/prometheus/config/v1/rules/my-namespace/my-name",
		},
		{
			test:       "special-characters-spaces",
			namespace:  "My: Namespace",
			name:       "My: Name",
			expURLPath: "/prometheus/config/v1/rules/My:%20Namespace/My:%20Name",
		},
		{
			test:       "special-characters-slashes",
			namespace:  "My/Namespace",
			name:       "My/Name",
			expURLPath: "/prometheus/config/v1/rules/My%2FNamespace/My%2FName",
		},
		{
			test:       "special-characters-slash-first",
			namespace:  "My/Namespace",
			name:       "/first-char-slash",
			expURLPath: "/prometheus/config/v1/rules/My%2FNamespace/%2Ffirst-char-slash",
		},
		{
			test:       "special-characters-slash-last",
			namespace:  "My/Namespace",
			name:       "last-char-slash/",
			expURLPath: "/prometheus/config/v1/rules/My%2FNamespace/last-char-slash%2F",
		},
		{
			test:            "use legacy routes with mimir-http-prefix",
			namespace:       "my-namespace",
			name:            "my-name",
			useLegacyRoutes: true,
			mimirHTTPPrefix: "/foo",
			expURLPath:      "/foo/api/v1/rules/my-namespace/my-name",
		},
		{
			test:            "use non legacy routes with mimir-http-prefix ignored",
			namespace:       "my-namespace",
			name:            "my-name",
			useLegacyRoutes: false,
			mimirHTTPPrefix: "/foo",
			expURLPath:      "/prometheus/config/v1/rules/my-namespace/my-name",
		},
	} {
		t.Run(tc.test, func(t *testing.T) {
			ctx := context.Background()
			client, err := New(Config{
				Address: ts.URL,
				ID:      "my-id",
				Key:     "my-key",
				ExtraHeaders: map[string]string{
					"key1": "value1",
					"key2": "value2",
				},
				UseLegacyRoutes: tc.useLegacyRoutes,
				MimirHTTPPrefix: tc.mimirHTTPPrefix,
			}, log.NewNopLogger())
			require.NoError(t, err)
			require.NoError(t, client.DeleteRuleGroup(ctx, tc.namespace, tc.name))

			req := <-requestCh
			require.Equal(t, tc.expURLPath, req.URL.EscapedPath())

			require.Equal(t, "value1", req.Header.Get("key1"))
			require.Equal(t, "value2", req.Header.Get("key2"))
			user, pass, ok := req.BasicAuth()
			require.True(t, ok)
			require.Equal(t, "my-id", user)
			require.Equal(t, "my-key", pass)
		})
	}

}

func TestMimirClient_GetRuleGroup_DoesNotWriteToStdout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "name: my-group\nrules:\n  - record: my_metric\n    expr: up")
	}))
	defer ts.Close()

	client, err := New(Config{Address: ts.URL, ID: "my-id"}, log.NewNopLogger())
	require.NoError(t, err)

	// "mimirtool rules get" prints the rule group to stdout, so anything else
	// written there makes the output invalid YAML.
	r, w, err := os.Pipe()
	require.NoError(t, err)
	origStdout := os.Stdout
	os.Stdout = w
	rg, getErr := client.GetRuleGroup(context.Background(), "my-namespace", "my-group")
	os.Stdout = origStdout
	require.NoError(t, w.Close())

	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, getErr)
	require.Equal(t, "my-group", rg.Name)
	require.Empty(t, string(out))
}
