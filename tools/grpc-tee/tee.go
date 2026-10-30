// SPDX-License-Identifier: AGPL-3.0-only

package grpctee

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/tools/grpc-tee/backend"
)

type Tee struct {
	backendType *backend.BackendType
	logger      log.Logger
}

func NewTee(backendType *backend.BackendType, logger log.Logger) *Tee {
	return &Tee{
		backendType: backendType,
		logger:      logger,
	}
}

func (t *Tee) Handle(_ any, serverStream grpc.ServerStream) error {
	var err error

	fullMethod, ok := grpc.MethodFromServerStream(serverStream)
	if !ok {
		return status.Errorf(codes.Internal, "ServerTransportStream not found in context")
	}

	var frame Frame
	if err := serverStream.RecvMsg(&frame); err != nil {
		return status.Errorf(codes.Internal, "receive request: %v", err)
	}

	decodedReq, err := t.backendType.DecodeRequest(fullMethod, frame)
	if err != nil {
		level.Error(t.logger).Log("msg", "failed to decode request", "method", fullMethod, "err", err)
		return status.Errorf(codes.InvalidArgument, "decode request: %v", err)
	}
	level.Info(t.logger).Log("msg", "decoded request", "method", fullMethod, "bytes", len(frame), "request", decodedReq.String())

	var respBytes []byte
	decodedResp, err := t.backendType.DecodeResponse(fullMethod, respBytes)
	if err != nil {
		return status.Errorf(codes.Internal, "decode response: %v", err)
	}
	level.Info(t.logger).Log("msg", "decoded response", "method", fullMethod, "bytes", len(respBytes), "response", decodedResp.String())

	if err := serverStream.SendMsg(decodedResp); err != nil {
		return status.Errorf(codes.Internal, "send response: %v", err)
	}

	// return status.Errorf(codes.Unimplemented, "tee does not forward %s yet", fullMethod
	return nil
}
