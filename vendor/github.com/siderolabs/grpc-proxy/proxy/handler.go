// Copyright 2017 Michal Witkowski. All Rights Reserved.
// Copyright 2019 Andrey Smirnov. All Rights Reserved.
// See LICENSE for licensing terms.

package proxy

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var clientStreamDescForProxying = &grpc.StreamDesc{
	ServerStreams: true,
	ClientStreams: true,
}

type handlerOptions struct {
	streamedMethods  map[string]struct{}
	streamedDetector StreamedDetectorFunc
	serviceName      string
	methodNames      []string
}

type handler struct {
	director StreamDirector
	options  handlerOptions
}

type backendConnection struct {
	backend Backend

	backendConn *grpc.ClientConn
	connError   error

	clientStream grpc.ClientStream
}

// handler is where the real magic of proxying happens.
// It is invoked like any gRPC server stream and uses the gRPC server framing to get and receive bytes from the wire,
// forwarding it to a ClientStream established against the relevant ClientConn.
func (s *handler) handler(_ any, serverStream grpc.ServerStream) error {
	// little bit of gRPC internals never hurt anyone
	fullMethodName, ok := grpc.MethodFromServerStream(serverStream)
	if !ok {
		return status.Errorf(codes.Internal, "lowLevelServerStream doesn't exist in the context")
	}

	mode, backends, err := s.director(serverStream.Context(), fullMethodName)
	if err != nil {
		return err
	}

	backendConnections := make([]backendConnection, len(backends))

	clientCtx, clientCancel := context.WithCancel(serverStream.Context())
	defer clientCancel()

	for i := range backends {
		backendConnections[i].backend = backends[i]

		// We require that the backend's returned context inherits from the serverStream.Context().
		var outgoingCtx context.Context

		outgoingCtx, backendConnections[i].backendConn, backendConnections[i].connError = backends[i].GetConnection(clientCtx, fullMethodName)

		if backendConnections[i].connError != nil {
			continue
		}

		backendConnections[i].clientStream, backendConnections[i].connError = grpc.NewClientStream(outgoingCtx, clientStreamDescForProxying,
			backendConnections[i].backendConn, fullMethodName)

		if backendConnections[i].connError != nil {
			continue
		}
	}

	switch mode {
	case One2One:
		if len(backendConnections) != 1 {
			return status.Errorf(codes.Internal, "one2one proxying should have exactly one connection (got %d)", len(backendConnections))
		}

		return s.handlerOne2One(serverStream, backendConnections)
	case One2Many:
		if len(backendConnections) == 0 {
			return status.Errorf(codes.Unavailable, "no backend connections for proxying")
		}

		return s.handlerOne2Many(fullMethodName, serverStream, backendConnections)
	default:
		return status.Errorf(codes.Internal, "unsupported proxy mode")
	}
}
