package api

import (
	"errors"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"
)

var (
	errValidationAtModifierDisabled = errors.New("@ modifier is disabled, use -querier.at-modifier-enabled to enable it")
)

type errorTranslateQueryEngine struct {
	engine v1.QueryEngine
}

func (qe errorTranslateQueryEngine) SetQueryLogger(l promql.QueryLogger) {
	qe.engine.SetQueryLogger(l)
}

func (qe errorTranslateQueryEngine) NewInstantQuery(q storage.Queryable, qs string, ts time.Time) (promql.Query, error) {
	query, err := qe.engine.NewInstantQuery(q, qs, ts)
	return query, qe.translate(err)
}

func (qe errorTranslateQueryEngine) NewRangeQuery(q storage.Queryable, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	query, err := qe.engine.NewRangeQuery(q, qs, start, end, interval)
	return query, qe.translate(err)
}

func (qe errorTranslateQueryEngine) translate(err error) error {
	switch err {
	case promql.ErrValidationAtModifierDisabled:
		return errValidationAtModifierDisabled
	default:
		// includes err == nil
		return err
	}
}
