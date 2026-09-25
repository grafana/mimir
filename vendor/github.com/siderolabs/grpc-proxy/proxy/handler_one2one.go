// Copyright 2017 Michal Witkowski. All Rights Reserved.
// Copyright 2019 Andrey Smirnov. All Rights Reserved.
// See LICENSE for licensing terms.

package proxy

import (
	"errors"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *handler) handlerOne2One(serverStream grpc.ServerStream, backendConnections []backendConnection) error {
	// case of proxying one to one:
	if backendConnections[0].connError != nil {
		return backendConnections[0].connError
	}

	// Explicitly *do not close* s2cErrChan and c2sErrChan, otherwise the select below will not terminate.
	// Channels do not have to be closed, it is just a control flow mechanism, see
	// https://groups.google.com/forum/#!msg/golang-nuts/pZwdYRGxCIk/qpbHxRRPJdUJ
	s2cErrChan := s.forwardServerToClient(serverStream, &backendConnections[0])
	c2sErrChan := s.forwardClientToServer(&backendConnections[0], serverStream)
	// We don't know which side is going to stop sending first, so we need a select between the two.
	for range 2 {
		select {
		case s2cErr := <-s2cErrChan:
			if errors.Is(s2cErr, io.EOF) {
				// this is the happy case where the sender has encountered io.EOF, and won't be sending anymore./
				// the clientStream>serverStream may continue pumping though.
				//nolint: errcheck
				backendConnections[0].clientStream.CloseSend()
			} else {
				// however, we may have gotten a receive error (stream disconnected, a read error etc) in which case we need
				// to cancel the clientStream to the backend, let all of its goroutines be freed up by the CancelFunc and
				// exit with an error to the stack
				return status.Errorf(codes.Internal, "failed proxying s2c: %v", s2cErr)
			}
		case c2sErr := <-c2sErrChan:
			// This happens when the clientStream has nothing else to offer (io.EOF), returned a gRPC error. In those two
			// cases we may have received Trailers as part of the call. In case of other errors (stream closed) the trailers
			// will be nil.
			serverStream.SetTrailer(backendConnections[0].clientStream.Trailer())
			// c2sErr will contain RPC error from client code. If not io.EOF return the RPC error as server stream error.
			if !errors.Is(c2sErr, io.EOF) {
				return c2sErr
			}

			return nil
		}
	}

	return status.Errorf(codes.Internal, "gRPC proxying should never reach this stage.")
}

func (s *handler) forwardClientToServer(src *backendConnection, dst grpc.ServerStream) chan error {
	ret := make(chan error, 1)

	go func() {
		// Send the header metadata first
		md, err := src.clientStream.Header()
		if err != nil {
			ret <- err

			return
		}

		if md != nil {
			if err = dst.SendHeader(md); err != nil {
				ret <- err

				return
			}
		}

		f := NewFrame(nil)

		for {
			if err = src.clientStream.RecvMsg(f); err != nil {
				ret <- err // this can be io.EOF which is happy case

				break
			}

			if err = dst.SendMsg(f); err != nil {
				ret <- err

				break
			}
		}
	}()

	return ret
}

func (s *handler) forwardServerToClient(src grpc.ServerStream, dst *backendConnection) chan error {
	ret := make(chan error, 1)

	go func() {
		f := NewFrame(nil)

		for {
			if err := src.RecvMsg(f); err != nil {
				ret <- err // this can be io.EOF which is happy case

				break
			}

			if err := dst.clientStream.SendMsg(f); err != nil {
				ret <- err

				break
			}
		}
	}()

	return ret
}
