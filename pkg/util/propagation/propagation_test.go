// SPDX-License-Identifier: AGPL-3.0-only

package propagation

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultiPropagator_ReadFromCarrier_HappyPath(t *testing.T) {
	c := MapCarrier{
		"key-1": {"value-1"},
		"key-2": {"value-2", "value-3"},
	}

	p := MultiPropagator{
		Propagators: []Propagator{
			&testPropagator{key: "key-1"},
			&testPropagator{key: "key-2"},
		},
	}

	ctx, err := p.ReadFromCarrier(context.WithValue(context.Background(), testPropagatorContextKey("base-key"), "base-value"), c)
	require.NoError(t, err)
	require.Equal(t, "value-1", ctx.Value(testPropagatorContextKey("key-1")))
	require.Equal(t, "value-2", ctx.Value(testPropagatorContextKey("key-2")))
	require.Equal(t, "base-value", ctx.Value(testPropagatorContextKey("base-key")), "should use provided context as parent context")
}

func TestMultiPropagator_ReadFromCarrier_Error(t *testing.T) {
	c := MapCarrier{}

	p := MultiPropagator{
		Propagators: []Propagator{
			&testPropagator{err: errors.New("something went wrong")},
		},
	}

	_, err := p.ReadFromCarrier(context.Background(), c)
	require.EqualError(t, err, "something went wrong")
}

type testPropagator struct {
	key string
	err error
}

type testPropagatorContextKey string

func (p *testPropagator) ReadFromCarrier(ctx context.Context, carrier Carrier) (context.Context, error) {
	if p.err != nil {
		return nil, p.err
	}

	return context.WithValue(ctx, testPropagatorContextKey(p.key), carrier.Get(p.key)), nil
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
