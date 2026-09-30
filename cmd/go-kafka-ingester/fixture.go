// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/limits"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/record"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/segment"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/service"
	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/store"
	"github.com/grafana/mimir/pkg/ingester/client"
)

type fixtureArgs struct {
	listen        string
	profileListen string
	version       uint
	tenant        string
	dataDir       string
	restoreOnly   bool
	offset        int64
	timestampMs   int64
	limits        limits.Args
}

// serveFixture serves a store holding one record read from stdin, and what a previous run
// persisted in --data-dir, for the Go client compatibility tests.
func serveFixture(args fixtureArgs) error {
	defaults, err := args.limits.ToLimits()
	if err != nil {
		return err
	}
	s := store.Default().WithOverrides(limits.NewOverrides(defaults))
	readRecord := func() (record.DecodedRequest, error) {
		input, err := io.ReadAll(os.Stdin)
		if err != nil {
			return record.DecodedRequest{}, err
		}
		return record.DecodeRecord(uint32(args.version), input)
	}
	if args.dataDir != "" {
		log, recovered, err := segment.Open(args.dataDir, 0, "fixture", 0, segment.Retention{})
		if err != nil {
			return err
		}
		for _, recoveredRecord := range recovered {
			if hasData(&recoveredRecord.Request) {
				if err := s.IngestRecovered(recoveredRecord.Tenant, recoveredRecord.Request, recoveredRecord.IngestedMs); err != nil {
					return err
				}
			}
		}
		if !args.restoreOnly {
			request, err := readRecord()
			if err != nil {
				return err
			}
			// Logged before the store sorts the series' labels, as they came.
			if err := log.Append(args.offset, args.timestampMs, args.tenant, &request); err != nil {
				return err
			}
			if err := s.Ingest(args.tenant, request); err != nil {
				return err
			}
		}
		if err := log.Close(); err != nil {
			return err
		}
	} else {
		if args.restoreOnly {
			return errors.New("--restore-only requires --data-dir")
		}
		request, err := readRecord()
		if err != nil {
			return err
		}
		if err := s.Ingest(args.tenant, request); err != nil {
			return err
		}
	}
	if args.profileListen != "" {
		if err := startProfiling(args.profileListen); err != nil {
			return err
		}
	}
	listener, err := net.Listen("tcp", args.listen)
	if err != nil {
		return fmt.Errorf("parse listen address: %w", err)
	}
	server := grpc.NewServer(service.ServerCodec())
	client.RegisterIngesterServer(server, service.New(s))
	return server.Serve(listener)
}

func hasData(request *record.DecodedRequest) bool {
	return len(request.Series) > 0 || len(request.Metadata) > 0
}
