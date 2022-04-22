// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"errors"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"
)

var (
	errValidationNegativeOffsetDisabled = errors.New("negative offsets are not supported")
)

type errorTranslateQueryEngine struct {
	engine v1.QueryEngine
}

func (qe errorTranslateQueryEngine) SetQueryLogger(l promql.QueryLogger) {
	qe.engine.SetQueryLogger(l)
}

func (qe errorTranslateQueryEngine) NewInstantQuery(q storage.Queryable, opts *promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	query, err := qe.engine.NewInstantQuery(q, opts, qs, ts)
	return query, qe.translate(err)
}

func (qe errorTranslateQueryEngine) NewRangeQuery(q storage.Queryable, opts *promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	query, err := qe.engine.NewRangeQuery(q, opts, qs, start, end, interval)
	return query, qe.translate(err)
}

func (qe errorTranslateQueryEngine) translate(err error) error {
	switch err {
	case promql.ErrValidationNegativeOffsetDisabled:
		return errValidationNegativeOffsetDisabled
	default:
		// includes err == nil
		return err
	}
}
