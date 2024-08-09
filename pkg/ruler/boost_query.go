package ruler

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/promql/parser"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type BoostQueryRequest struct {
	Query string `json:"query"`
}

const (
	queryBoosterNamespace = "query_booster"
	queryBoosterMetric    = "QUERY_BOOSTER"
	boostedQueryLabelName = "__boosted_query__"
)

var (
	errClientError = errors.New("client error")
	errServerError = errors.New("server error")
)

func (a *API) BoostQuery(w http.ResponseWriter, r *http.Request) {
	logger, ctx := spanlogger.NewWithLogger(r.Context(), a.logger, "API.BoostQuery")
	defer logger.Finish()

	fmt.Println()

	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		respondInvalidRequest(logger, w, err.Error())
		return
	}

	req := BoostQueryRequest{}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	err = dec.Decode(&req)
	if err != nil {
		respondInvalidRequest(logger, w, err.Error())
		return
	}

	level.Info(logger).Log("msg", "query submitted for boost", "query", req.Query)

	ruleGroup, err := ruleGroupForQuery(req)
	if err != nil {
		if errors.Is(err, errClientError) {
			respondInvalidRequest(logger, w, err.Error())
			return
		}
		respondServerError(logger, w, err.Error())
		return
	}

	errs := a.ruler.manager.ValidateRuleGroup(ruleGroup)
	if len(errs) > 0 {
		e := []string{}
		for _, err := range errs {
			level.Error(logger).Log("msg", "unable to validate rule group payload", "err", err.Error())
			e = append(e, err.Error())
		}

		respondServerError(a.logger, w, strings.Join(e, "\n"))
		return
	}

	rgProto := rulespb.ToProto(userID, queryBoosterNamespace, ruleGroup)
	err = a.store.SetRuleGroup(ctx, userID, queryBoosterNamespace, rgProto)
	if err != nil {
		respondServerError(a.logger, w, err.Error())
		return
	}

	respondAccepted(w, a.logger)
}

func ruleGroupForQuery(req BoostQueryRequest) (rulefmt.RuleGroup, error) {
	queryExpr, err := parser.ParseExpr(req.Query)
	if err != nil {
		return rulefmt.RuleGroup{}, fmt.Errorf("%w: failed to parse query: %w", errClientError, err)
	}

	qe := queryExpr.String()
	h := xxhash.New()
	_, _ = h.WriteString(qe)

	ruleTemplate := `record: %s
expr: label_replace(%s, "%s", "%s", "", "")
`

	ruleYaml := fmt.Sprintf(ruleTemplate, queryBoosterMetric, qe, boostedQueryLabelName, strings.ReplaceAll(qe, `"`, `\"`))
	ruleNode := rulefmt.RuleNode{}
	err = yaml.Unmarshal([]byte(ruleYaml), &ruleNode)
	if err != nil {
		return rulefmt.RuleGroup{}, fmt.Errorf("%w: failed to parse rule: %w", errServerError, err)
	}

	rg := rulefmt.RuleGroup{
		Name:  base64.StdEncoding.EncodeToString(h.Sum(nil)),
		Rules: []rulefmt.RuleNode{ruleNode},
	}
	return rg, nil
}
