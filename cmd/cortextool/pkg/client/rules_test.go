package client

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCortexClient_X(t *testing.T) {
	requestCh := make(chan *http.Request, 1)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCh <- r
		fmt.Fprintln(w, "hello")
	}))
	defer ts.Close()

	client, err := New(Config{
		Address: ts.URL,
		ID:      "my-id",
		Key:     "my-key",
	})
	require.NoError(t, err)

	for _, tc := range []struct {
		test       string
		namespace  string
		name       string
		expURLPath string
	}{
		{
			test:       "regular-characters",
			namespace:  "my-namespace",
			name:       "my-name",
			expURLPath: "/api/v1/rules/my-namespace/my-name",
		},
		{
			test:       "special-characters-spaces",
			namespace:  "My: Namespace",
			name:       "My: Name",
			expURLPath: "/api/v1/rules/My:%20Namespace/My:%20Name",
		},
		{
			test:       "special-characters-slashes",
			namespace:  "My/Namespace",
			name:       "My/Name",
			expURLPath: "/api/v1/rules/My%2FNamespace/My%2FName",
		},
	} {
		t.Run(tc.test, func(t *testing.T) {
			ctx := context.Background()
			require.NoError(t, client.DeleteRuleGroup(ctx, tc.namespace, tc.name))

			req := <-requestCh
			require.Equal(t, tc.expURLPath, req.URL.EscapedPath())

		})
	}

}
