// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/textproto"
	"os"
	"path"
	"time"

	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/grpcclient"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/ruler"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

func main() {
	rulesSource := "/Users/charleskorn/Desktop/rules.yaml"
	orgID := "9960"
	queryFrontendAddress := "localhost:9095"
	evaluationTime := time.Now().UTC().Add(-5 * time.Minute)
	outputDir := "/Users/charleskorn/Desktop/queries/original-format"

	bytes, err := os.ReadFile(rulesSource)

	if err != nil {
		fmt.Printf("Error reading rules: %v\n", err)
		os.Exit(1)
	}

	ruleGroups := []rulefmt.RuleGroup{}
	if err := yaml.Unmarshal(bytes, &ruleGroups); err != nil {
		fmt.Printf("Error decoding rules: %v\n", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(outputDir, 0700); err != nil {
		fmt.Printf("Error creating output directory: %v\n", err)
		os.Exit(1)
	}

	cfg := ruler.QueryFrontendConfig{
		Address: queryFrontendAddress,
		GRPCClientConfig: grpcclient.Config{
			MaxRecvMsgSize:      100 << 20,
			MaxSendMsgSize:      100 << 20,
			GRPCCompression:     "",
			RateLimit:           0,
			RateLimitBurst:      0,
			BackoffOnRatelimits: false,
			TLSEnabled:          false,
			BackoffConfig: backoff.Config{
				MinBackoff: 100 * time.Millisecond,
				MaxBackoff: 10 * time.Second,
				MaxRetries: 10,
			},
		},
	}

	client, err := DialQueryFrontend(cfg, orgID)
	if err != nil {
		fmt.Printf("Error creating query frontend client: %v\n", err)
		os.Exit(1)
	}

	querier := NewRemoteQuerier(client, 30*time.Second, "/prometheus", util_log.Logger, WithOrgIDMiddleware(orgID))

	for _, ruleGroup := range ruleGroups {
		fmt.Printf("Evaluating group '%v'\n", ruleGroup.Name)
		ruleGroupOutputDir := path.Join(outputDir, ruleGroup.Name)

		if err := os.MkdirAll(ruleGroupOutputDir, 0700); err != nil {
			fmt.Printf("Error creating output directory: %v\n", err)
			os.Exit(1)
		}

		for _, rule := range ruleGroup.Rules {
			if rule.Alert.Value != "" {
				continue
			}

			name := rule.Record.Value

			fmt.Printf("> Evaluating rule '%v'\n", name)
			_, response, err := querier.Query(context.Background(), rule.Expr.Value, evaluationTime, util_log.Logger)

			if err != nil {
				fmt.Printf("Evaluation failed: %v\n", err)
				os.Exit(1)
			}

			bytes, err := json.Marshal(response)

			if err != nil {
				fmt.Printf("Marshalling response as JSON failed: %v\n", err)
				os.Exit(1)
			}

			if err := os.WriteFile(path.Join(ruleGroupOutputDir, name+".json"), bytes, 0700); err != nil {
				fmt.Printf("Writing response to file failed: %v\n", err)
			}
		}
	}
}

func WithOrgIDMiddleware(orgID string) func(ctx context.Context, req *httpgrpc.HTTPRequest) error {
	return func(ctx context.Context, req *httpgrpc.HTTPRequest) error {
		req.Headers = append(req.Headers, &httpgrpc.Header{
			Key:    textproto.CanonicalMIMEHeaderKey(user.OrgIDHeaderName),
			Values: []string{orgID},
		})
		return nil
	}
}
