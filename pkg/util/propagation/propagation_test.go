// SPDX-License-Identifier: AGPL-3.0-only

package propagation

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultiExtractor_ReadFromCarrier_HappyPath(t *testing.T) {
	c := MapCarrier{
		"key-1": {"value-1"},
		"key-2": {"value-2", "value-3"},
	}

	p := MultiExtractor{
		Extractors: []Extractor{
			&testExtractor{key: "key-1"},
			&testExtractor{key: "key-2"},
		},
	}

	ctx, err := p.ReadFromCarrier(context.WithValue(context.Background(), testExtractorContextKey("base-key"), "base-value"), c)
	require.NoError(t, err)
	require.Equal(t, "value-1", ctx.Value(testExtractorContextKey("key-1")))
	require.Equal(t, "value-2", ctx.Value(testExtractorContextKey("key-2")))
	require.Equal(t, "base-value", ctx.Value(testExtractorContextKey("base-key")), "should use provided context as parent context")
}

func TestMultiExtractor_ReadFromCarrier_Error(t *testing.T) {
	c := MapCarrier{}

	p := MultiExtractor{
		Extractors: []Extractor{
			&testExtractor{err: errors.New("something went wrong")},
		},
	}

	_, err := p.ReadFromCarrier(context.Background(), c)
	require.EqualError(t, err, "something went wrong")
}

type testExtractor struct {
	key string
	err error
}

type testExtractorContextKey string

func (e *testExtractor) ReadFromCarrier(ctx context.Context, carrier Carrier) (context.Context, error) {
	if e.err != nil {
		return nil, e.err
	}

	return context.WithValue(ctx, testExtractorContextKey(e.key), carrier.Get(e.key)), nil
}

func TestMapCarrier(t *testing.T) {
	m := map[string][]string{
		"Foo":         {"bar"},
		"Multi-Value": {"value-1", "value-2"},
	}

	c := MapCarrier(m)

	require.Equal(t, "bar", c.Get("Foo"))
	require.Equal(t, "value-1", c.Get("Multi-Value"))
	require.Equal(t, "", c.Get("not-set"))
}

func TestHttpHeaderCarrier(t *testing.T) {
	h := http.Header{
		"Foo":         []string{"bar"},
		"Multi-Value": []string{"value-1", "value-2"},
	}

	c := HttpHeaderCarrier(h)

	require.Equal(t, "bar", c.Get("Foo"))
	require.Equal(t, "value-1", c.Get("Multi-Value"))
	require.Equal(t, "", c.Get("not-set"))
}
