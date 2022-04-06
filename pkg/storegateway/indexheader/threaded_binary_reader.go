// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/block/indexheader"
)

type threadedReader struct {
	pool   *Threadpool
	reader indexheader.Reader
}

func NewThreadedReader(pool *Threadpool, reader indexheader.Reader) indexheader.Reader {
	return &threadedReader{
		pool:   pool,
		reader: reader,
	}
}

func (t *threadedReader) Close() error {
	_, err := t.pool.Execute(func() (interface{}, error) {
		return nil, t.reader.Close()
	})

	return err
}

func (t *threadedReader) IndexVersion() (int, error) {
	val, err := t.pool.Execute(func() (interface{}, error) {
		return t.reader.IndexVersion()
	})

	if err != nil {
		return 0, err
	}

	return val.(int), err
}

func (t *threadedReader) PostingsOffset(name string, value string) (index.Range, error) {
	val, err := t.pool.Execute(func() (interface{}, error) {
		return t.reader.PostingsOffset(name, value)
	})

	if err != nil {
		return index.Range{}, err
	}

	return val.(index.Range), err
}

func (t *threadedReader) LookupSymbol(o uint32) (string, error) {
	val, err := t.pool.Execute(func() (interface{}, error) {
		return t.reader.LookupSymbol(o)
	})

	if err != nil {
		return "", err
	}

	return val.(string), err
}

func (t *threadedReader) LabelValues(name string) ([]string, error) {
	val, err := t.pool.Execute(func() (interface{}, error) {
		return t.reader.LabelValues(name)
	})

	if err != nil {
		return nil, err
	}

	return val.([]string), err
}

func (t *threadedReader) LabelNames() ([]string, error) {
	val, err := t.pool.Execute(func() (interface{}, error) {
		return t.reader.LabelNames()
	})

	if err != nil {
		return nil, err
	}

	return val.([]string), err
}
