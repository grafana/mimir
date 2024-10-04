// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/embedded.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"encoding/json"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

/*
Design:

The prometheus api package enforces a (*promql.Engine argument), making it infeasible to do lazy AST
evaluation and substitution from within this package.
This leaves the (storage.Queryable) interface as the remaining target for conducting application level sharding.

The main idea is to analyze the AST and determine which subtrees can be parallelized. With those in hand, the queries may
be remapped into vector or matrix selectors utilizing a reserved label containing the original query. These may then be parallelized in the storage implementation.
*/

const (
	// EmbeddedQueriesLabelName is a reserved label name containing embedded queries.
	EmbeddedQueriesLabelName = "__queries__"

	// EmbeddedQueriesMetricName is a reserved metric name denoting a special metric which contains embedded queries.
	EmbeddedQueriesMetricName = "__embedded_queries__"
)

// EmbeddedQueries is a wrapper type for encoding queries
type EmbeddedQueries struct {
	Concat []EmbeddedQuery `json:"Concat"`
}

type EmbeddedQuery struct {
	Expr   string            `json:"Expr"`
	Params map[string]string `json:"Params,omitempty"`
}

func NewEmbeddedQuery(expr string, params map[string]string) EmbeddedQuery {
	return EmbeddedQuery{
		Expr:   expr,
		Params: params,
	}
}

// JSONCodec is a Codec that uses JSON representations of EmbeddedQueries structs
var JSONCodec jsonCodec

type jsonCodec struct{}

func (c jsonCodec) Encode(queries []EmbeddedQuery) (string, error) {
	embedded := EmbeddedQueries{
		Concat: queries,
	}
	b, err := json.Marshal(embedded)
	return string(b), err
}

func (c jsonCodec) Decode(encoded string) (queries []EmbeddedQuery, err error) {
	var embedded EmbeddedQueries
	err = json.Unmarshal([]byte(encoded), &embedded)
	if err != nil {
		return nil, err
	}

	return embedded.Concat, nil
}

// VectorSquash reduces an AST into a single vector query which can be hijacked by a Queryable impl.
// It always uses a VectorSelector as the substitution expr.
// This is important because logical/set binops can only be applied against vectors and not matrices.
func VectorSquasher(exprs ...EmbeddedQuery) (parser.Expr, error) {
	encoded, err := JSONCodec.Encode(exprs)
	if err != nil {
		return nil, err
	}

	embeddedQuery, err := labels.NewMatcher(labels.MatchEqual, EmbeddedQueriesLabelName, encoded)
	if err != nil {
		return nil, err
	}

	return &parser.VectorSelector{
		Name:          EmbeddedQueriesMetricName,
		LabelMatchers: []*labels.Matcher{embeddedQuery},
	}, nil
}
