// SPDX-License-Identifier: AGPL-3.0-only

package propagation

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultiExtractor_ExtractFromCarrier_HappyPath(t *testing.T) {
	c := MapCarrier{
		"Key-1": {"value-1"},
		"Key-2": {"value-2", "value-3"},
	}

	extractor := MultiExtractor{
		Extractors: []Extractor{
			&testExtractor{key: "Key-1"},
			&testExtractor{key: "Key-2"},
		},
	}

	ctx, err := extractor.ExtractFromCarrier(context.WithValue(context.Background(), testContextKey("Base-Key"), "base-value"), c)
	require.NoError(t, err)
	require.Equal(t, "value-1", ctx.Value(testContextKey("Key-1")))
	require.Equal(t, "value-2", ctx.Value(testContextKey("Key-2")))
	require.Equal(t, "base-value", ctx.Value(testContextKey("Base-Key")), "should use provided context as parent context")
}

func TestMultiExtractor_ExtractFromCarrier_Error(t *testing.T) {
	c := MapCarrier{}

	extractor := MultiExtractor{
		Extractors: []Extractor{
			&testExtractor{err: errors.New("something went wrong")},
		},
	}

	_, err := extractor.ExtractFromCarrier(context.Background(), c)
	require.EqualError(t, err, "something went wrong")
}

func TestMultiInjector_InjectToCarrier_HappyPath(t *testing.T) {
	injector := MultiInjector{
		Injectors: []Injector{
			&testInjector{key: "Key-1"},
			&testInjector{key: "Key-2"},
		},
	}

	c := MapCarrier{}
	ctx := context.Background()
	ctx = context.WithValue(ctx, testContextKey("Key-1"), "value-1")
	ctx = context.WithValue(ctx, testContextKey("Key-2"), "value-2")
	err := injector.InjectToCarrier(ctx, c)

	require.NoError(t, err)
	require.Equal(t, "value-1", c.Get("Key-1"))
	require.Equal(t, "value-2", c.Get("Key-2"))
}

func TestMultiInjector_InjectToCarrier_Error(t *testing.T) {
	injector := MultiInjector{
		Injectors: []Injector{
			&testInjector{err: errors.New("something went wrong")},
		},
	}

	c := MapCarrier{}
	ctx := context.Background()
	err := injector.InjectToCarrier(ctx, c)
	require.EqualError(t, err, "something went wrong")
}

type testContextKey string

type testExtractor struct {
	key string
	err error
}

func (e *testExtractor) ExtractFromCarrier(ctx context.Context, carrier Carrier) (context.Context, error) {
	if e.err != nil {
		return nil, e.err
	}

	return context.WithValue(ctx, testContextKey(e.key), carrier.Get(e.key)), nil
}

type testInjector struct {
	key string
	err error
}

func (i *testInjector) InjectToCarrier(ctx context.Context, carrier Carrier) error {
	if i.err != nil {
		return i.err
	}

	carrier.Add(i.key, ctx.Value(testContextKey(i.key)).(string))
	return nil
}

func testCarrier(t *testing.T, c Carrier) {
	c.SetAll("Foo", []string{"bar"})
	c.SetAll("Multi-Value", []string{"value-1", "value-2"})
	c.SetAll("not-canonical", []string{"canonical-value"})

	require.Equal(t, "bar", c.Get("Foo"))
	require.Equal(t, []string{"bar"}, c.GetAll("Foo"))
	require.Equal(t, "value-1", c.Get("Multi-Value"))
	require.Equal(t, []string{"value-1", "value-2"}, c.GetAll("Multi-Value"))
	require.Equal(t, "canonical-value", c.Get("Not-Canonical"))
	require.Equal(t, []string{"canonical-value"}, c.GetAll("Not-Canonical"))
	require.Equal(t, "", c.Get("not-set"))
	require.Empty(t, c.GetAll("not-set"))

	// Check that using non-canonical names works too.
	require.Equal(t, "bar", c.Get("fOO"))
	require.Equal(t, []string{"bar"}, c.GetAll("fOO"))

	c.Add("another", "value")
	require.Equal(t, []string{"value"}, c.GetAll("Another"))
	c.Add("Another", "other-value")
	require.Equal(t, []string{"value", "other-value"}, c.GetAll("Another"))
}

func TestMapCarrier(t *testing.T) {
	testCarrier(t, MapCarrier{})
}

func TestHttpHeaderCarrier(t *testing.T) {
	testCarrier(t, HttpHeaderCarrier{})
}
