// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type app struct {
	rootUrl      string
	expr         string
	tenantID     string
	instantQuery bool

	// Instant query:
	at flagext.Time

	// Range query:
	from flagext.Time
	to   flagext.Time
	step time.Duration

	planner *streamingpromql.QueryPlanner
	logger  log.Logger
}

func main() {
	a := &app{
		planner: streamingpromql.NewQueryPlanner(streamingpromql.NewTestEngineOpts()),
		logger:  log.NewLogfmtLogger(os.Stderr),
	}

	if err := a.Run(); err != nil {
		fmt.Printf("Application failed with an error: %v\n", err)
		os.Exit(1)
	}
}

func (a *app) ParseFlags() error {
	flag.StringVar(&a.rootUrl, "root-url", "http://localhost:8005", "root URL of querier")
	flag.StringVar(&a.expr, "expr", "", "expression to evaluate")
	flag.Var(&a.at, "at", "instant query time")
	flag.Var(&a.from, "from", "start of range query time range")
	flag.Var(&a.to, "to", "end of range query time range")
	flag.DurationVar(&a.step, "step", time.Minute, "range query time step")
	flag.StringVar(&a.tenantID, "tenant-id", "anonymous", "tenant ID")
	flag.Parse()

	if a.expr == "" {
		return errors.New("expression is required")
	}

	if time.Time(a.at).IsZero() {
		if time.Time(a.from).IsZero() || time.Time(a.to).IsZero() {
			return errors.New("either range query time range or instant query time is required")
		}

		if time.Time(a.from).After(time.Time(a.to)) {
			return fmt.Errorf("from time (%v) must be before to time (%v)", time.Time(a.from).Format(time.RFC3339Nano), time.Time(a.to).Format(time.RFC3339Nano))
		}

		if a.step <= 0 {
			return errors.New("step must be greater than zero")
		}
	} else {
		a.instantQuery = true

		if !time.Time(a.from).IsZero() || !time.Time(a.to).IsZero() {
			return errors.New("either range query time range or instant query time is required, but not both")
		}
	}

	return nil
}

func (a *app) Run() error {
	if err := a.ParseFlags(); err != nil {
		return err
	}

	ctx := context.Background()
	plan, err := a.planner.NewQueryPlan(ctx, a.expr, a.getQueryTimeRange(), streamingpromql.NoopPlanningObserver{})
	if err != nil {
		return fmt.Errorf("could not create plan: %w", err)
	}

	encodedPlan, err := plan.ToEncodedPlan(false, true)
	if err != nil {
		return fmt.Errorf("could not encode plan: %w", err)
	}

	body := &querierpb.EvaluateQueryRequest{
		Plan: *encodedPlan,
		Nodes: []querierpb.EvaluationNode{
			{
				TimeRange: encodedPlan.TimeRange,
				NodeIndex: encodedPlan.RootNode,
			},
		},
	}
	requestBody, err := proto.Marshal(body)
	if err != nil {
		return fmt.Errorf("could not marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", a.rootUrl+"/api/v1/evaluate", bytes.NewReader(requestBody))
	if err != nil {
		return fmt.Errorf("could not create request: %w", err)
	}

	req.Header.Set("Content-Type", fmt.Sprintf("application/protobuf; proto=%v", proto.MessageName(body)))
	req.Header.Set("X-Scope-OrgID", a.tenantID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("could not send request: %w", err)
	}

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("unexpected status code: %d (%v)", resp.StatusCode, resp.Status)
	}

	defer resp.Body.Close()

	decoder := &streamingResponseDecoder{resp.Body}

	for {
		msg, err := decoder.Next()
		if err != nil {
			return err
		}

		// Reached end of stream. We're done.
		if msg == nil {
			return nil
		}

		fmt.Println(proto.MarshalTextString(msg))
		fmt.Println("----")
	}
}

func (a *app) getQueryTimeRange() types.QueryTimeRange {
	if a.instantQuery {
		return types.NewInstantQueryTimeRange(time.Time(a.at))
	}

	return types.NewRangeQueryTimeRange(time.Time(a.from), time.Time(a.to), a.step)
}

type streamingResponseDecoder struct {
	r io.Reader
}

func (d *streamingResponseDecoder) Next() (*querierpb.EvaluateQueryResponse, error) {
	// Read the message length. If we get an EOF here, the stream is finished.
	l := uint64(0)
	if err := binary.Read(d.r, binary.LittleEndian, &l); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}

		return nil, fmt.Errorf("could not read message length: %w", err)
	}

	// Read the message body. If we get an EOF here, something has gone wrong.
	buf := make([]byte, l)
	if _, err := io.ReadFull(d.r, buf); err != nil {
		return nil, fmt.Errorf("could not read message body: %w", err)
	}

	resp := &querierpb.EvaluateQueryResponse{}
	if err := proto.Unmarshal(buf, resp); err != nil {
		return nil, fmt.Errorf("could not unmarshal response: %w", err)
	}

	return resp, nil
}
