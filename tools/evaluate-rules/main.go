// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/textproto"
	"os"
	"path"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/ruler"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

type config struct {
	rulesFile           string
	orgID               string
	outputDir           string
	evaluationTime      flagext.Time
	queryFrontendConfig ruler.QueryFrontendConfig
}

// Run this with a command line like the following:
// go run . --rules-file /tmp/rules.yaml --org-id 9960 --output-dir /tmp/queries/ --evaluation-time 2023-01-04T00:00:00Z --ruler.query-frontend.address localhost:9095
func main() {
	cfg := config{}
	cfg.queryFrontendConfig.RegisterFlags(flag.CommandLine)
	flag.StringVar(&cfg.rulesFile, "rules-file", "", "Path to YAML file containing rules to evaluate")
	flag.StringVar(&cfg.orgID, "org-id", "", "Org ID to use when evaluating rules")
	flag.StringVar(&cfg.outputDir, "output-dir", "", "Path to directory to write query results to")
	flag.Var(&cfg.evaluationTime, "evaluation-time", "Timestamp to use when evaluating queries")
	flag.Parse()

	if cfg.rulesFile == "" {
		fmt.Println("Missing required command line flag: rules file")
		os.Exit(1)
	}

	if cfg.orgID == "" {
		fmt.Println("Missing required command line flag: org ID")
		os.Exit(1)
	}

	if cfg.outputDir == "" {
		fmt.Println("Missing required command line flag: output directory")
		os.Exit(1)
	}

	if cfg.evaluationTime == flagext.Time(time.Time{}) {
		fmt.Println("Missing required command line flag: evaluation time")
		os.Exit(1)
	}

	bytes, err := os.ReadFile(cfg.rulesFile)

	if err != nil {
		fmt.Printf("Error reading rules: %v\n", err)
		os.Exit(1)
	}

	ruleGroups := []rulefmt.RuleGroup{}
	if err := yaml.Unmarshal(bytes, &ruleGroups); err != nil {
		fmt.Printf("Error decoding rules: %v\n", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(cfg.outputDir, 0700); err != nil {
		fmt.Printf("Error creating output directory: %v\n", err)
		os.Exit(1)
	}

	client, err := DialQueryFrontend(cfg.queryFrontendConfig, cfg.orgID)
	if err != nil {
		fmt.Printf("Error creating query frontend client: %v\n", err)
		os.Exit(1)
	}

	querier := NewRemoteQuerier(client, 30*time.Second, "/prometheus", util_log.Logger, WithOrgIDMiddleware(cfg.orgID))

	for _, ruleGroup := range ruleGroups {
		fmt.Printf("Evaluating group '%v'\n", ruleGroup.Name)
		ruleGroupOutputDir := path.Join(cfg.outputDir, ruleGroup.Name)

		for _, rule := range ruleGroup.Rules {
			if rule.Alert.Value != "" {
				continue
			}

			name := rule.Record.Value

			fmt.Printf("> Evaluating rule '%v'\n", name)
			_, response, err := querier.Query(context.Background(), rule.Expr.Value, time.Time(cfg.evaluationTime), util_log.Logger)

			if err != nil {
				fmt.Printf("Evaluation failed: %v\n", err)
				os.Exit(1)
			}

			bytes, err := json.Marshal(response)

			if err != nil {
				fmt.Printf("Marshalling response as JSON failed: %v\n", err)
				os.Exit(1)
			}

			if err := os.MkdirAll(ruleGroupOutputDir, 0700); err != nil {
				fmt.Printf("Error creating output directory: %v\n", err)
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
