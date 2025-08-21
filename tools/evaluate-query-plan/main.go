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

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
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
	time flagext.Time

	// Range query:
	start flagext.Time
	end   flagext.Time
	step  time.Duration

	planner *streamingpromql.QueryPlanner
	logger  log.Logger
}

func main() {
	logger := log.NewLogfmtLogger(os.Stderr)
	a := &app{
		planner: streamingpromql.NewQueryPlanner(streamingpromql.NewTestEngineOpts(), logger),
		logger:  logger,
	}

	if err := a.Run(); err != nil {
		fmt.Printf("Application failed with an error: %v\n", err)
		os.Exit(1)
	}
}

func (a *app) ParseFlags() error {
	flag.StringVar(&a.rootUrl, "root-url", "http://localhost:8005", "root URL of querier")
	flag.StringVar(&a.expr, "expr", "", "PromQL expression to evaluate")
	flag.Var(&a.time, "time", "instant query time")
	flag.Var(&a.start, "start", "start of range query time range")
	flag.Var(&a.end, "end", "end of range query time range")
	flag.DurationVar(&a.step, "step", time.Minute, "range query time step")
	flag.StringVar(&a.tenantID, "tenant-id", "anonymous", "tenant ID")

	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		return err
	}

	if a.expr == "" {
		return errors.New("expression is required")
	}

	if time.Time(a.time).IsZero() {
		if time.Time(a.start).IsZero() || time.Time(a.end).IsZero() {
			return errors.New("either range query time range or instant query time is required")
		}

		if time.Time(a.start).After(time.Time(a.end)) {
			return fmt.Errorf("start time (%v) must be before end time (%v)", time.Time(a.start).Format(time.RFC3339Nano), time.Time(a.end).Format(time.RFC3339Nano))
		}

		if a.step <= 0 {
			return errors.New("step must be greater than zero")
		}
	} else {
		a.instantQuery = true

		if !time.Time(a.start).IsZero() || !time.Time(a.end).IsZero() {
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
		return types.NewInstantQueryTimeRange(time.Time(a.time))
	}

	return types.NewRangeQueryTimeRange(time.Time(a.start), time.Time(a.end), a.step)
}

type streamingResponseDecoder struct {
	r io.Reader
}

func (d *streamingResponseDecoder) Next() (*frontendv2pb.QueryResultStreamRequest, error) {
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

	resp := &frontendv2pb.QueryResultStreamRequest{}
	if err := proto.Unmarshal(buf, resp); err != nil {
		return nil, fmt.Errorf("could not unmarshal response: %w", err)
	}

	return resp, nil
}
