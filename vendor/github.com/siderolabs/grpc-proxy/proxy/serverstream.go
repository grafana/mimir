// Copyright 2019 Andrey Smirnov. All Rights Reserved.
// See LICENSE for licensing terms.

package proxy

import (
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ServerStreamWrapper wraps grpc.ServerStream and adds locking to the send path.
type ServerStreamWrapper struct {
	grpc.ServerStream

	sendMu sync.Mutex
}

// SetHeader sets the header metadata.
//
// It may be called multiple times.
// When call multiple times, all the provided metadata will be merged.
// All the metadata will be sent out when one of the following happens:
//   - ServerStream.SendHeader() is called;
//   - The first response is sent out;
//   - An RPC status is sent out (error or success).
func (wrapper *ServerStreamWrapper) SetHeader(md metadata.MD) error {
	wrapper.sendMu.Lock()
	defer wrapper.sendMu.Unlock()

	err := wrapper.ServerStream.SetHeader(md)
	if err != nil && err.Error() == "transport: the stream is done or WriteHeader was already called" {
		// hack: swallow grpc.internal.transport.ErrIllegalHeaderWrite
		err = nil
	}

	return err
}

// SendHeader sends the header metadata.
// The provided md and headers set by SetHeader() will be sent.
// It fails if called multiple times.
func (wrapper *ServerStreamWrapper) SendHeader(md metadata.MD) error {
	wrapper.sendMu.Lock()
	defer wrapper.sendMu.Unlock()

	err := wrapper.ServerStream.SendHeader(md)
	if err.Error() == "transport: the stream is done or WriteHeader was already called" {
		// hack: swallow grpc.internal.transport.ErrIllegalHeaderWrite
		err = nil
	}

	return err
}

// SetTrailer sets the trailer metadata which will be sent with the RPC status.
// When called more than once, all the provided metadata will be merged.
func (wrapper *ServerStreamWrapper) SetTrailer(md metadata.MD) {
	wrapper.sendMu.Lock()
	defer wrapper.sendMu.Unlock()

	wrapper.ServerStream.SetTrailer(md)
}

// SendMsg sends a message. On error, SendMsg aborts the stream and the
// error is returned directly.
//
// SendMsg blocks until:
//   - There is sufficient flow control to schedule m with the transport, or
//   - The stream is done, or
//   - The stream breaks.
//
// SendMsg does not wait until the message is received by the client. An
// untimely stream closure may result in lost messages.
//
// It is safe to have a goroutine calling SendMsg and another goroutine
// calling RecvMsg on the same stream at the same time, but it is not safe
// to call SendMsg on the same stream in different goroutines.
func (wrapper *ServerStreamWrapper) SendMsg(m any) error {
	wrapper.sendMu.Lock()
	defer wrapper.sendMu.Unlock()

	return wrapper.ServerStream.SendMsg(m)
}
