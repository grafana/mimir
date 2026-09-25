// Copyright 2017 Michal Witkowski. All Rights Reserved.
// Copyright 2019 Andrey Smirnov. All Rights Reserved.
// See LICENSE for licensing terms.

package proxy

import (
	"context"

	"google.golang.org/grpc"
)

// Backend wraps information about upstream connection.
//
// For simple one-to-one proxying, not much should be done in the Backend, simply
// providing a connection is enough.
//
// When proxying one-to-many and aggregating results, Backend might be used to
// append additional fields to upstream response to support more complicated
// proxying.
type Backend interface {
	// String provides backend name for logging and errors.
	String() string

	// GetConnection returns a grpc connection to the backend.
	//
	// The context returned from this function should be the context for the *outgoing* (to backend) call. In case you want
	// to forward any Metadata between the inbound request and outbound requests, you should do it manually. However, you
	// *must* propagate the cancel function (`context.WithCancel`) of the inbound context to the one returned.
	GetConnection(ctx context.Context, fullMethodName string) (context.Context, *grpc.ClientConn, error)

	// AppendInfo is called to enhance response from the backend with additional data.
	//
	// Parameter streaming indicates if response is delivered in streaming mode or not.
	//
	// Usecase might be appending backend endpoint (or name) to the protobuf serialized response, so that response is enhanced
	// with source information. This is particularly important for one to many calls, when it is required to identify
	// response from each of the backends participating in the proxying.
	//
	// If not additional proxying is required, simply returning the buffer without changes works fine.
	AppendInfo(streaming bool, resp []byte) ([]byte, error)

	// BuildError is called to convert error from upstream into response field.
	//
	// BuildError is never called for one to one proxying, in that case all the errors are returned back to the caller
	// as grpc errors. Parameter streaming indicates if response is delivered in streaming mode or not.
	//
	// When proxying one to many, if one the requests fails or upstream returns an error, it is undesirable to fail the whole
	// request and discard responses from other backends. BuildError converts (marshals) error from backend into protobuf encoded
	// response which is analyzed by the caller, so that caller reaching out to N upstreams receives N1 successful responses and
	// N2 error responses so that N1 + N2 == N.
	//
	// If BuildError returns nil, error is returned as grpc error (failing whole request).
	BuildError(streaming bool, err error) ([]byte, error)
}

// SingleBackend implements a simple wrapper around get connection function of one to one proxying.
//
// SingleBackend implements Backend interface and might be used as an easy wrapper for one to one proxying.
type SingleBackend struct {
	// GetConn returns a grpc connection to the backend.
	//
	// The context returned from this function should be the context for the *outgoing* (to backend) call. In case you want
	// to forward any Metadata between the inbound request and outbound requests, you should do it manually. However, you
	// *must* propagate the cancel function (`context.WithCancel`) of the inbound context to the one returned.
	GetConn func(ctx context.Context) (context.Context, *grpc.ClientConn, error)
}

func (sb *SingleBackend) String() string {
	return "backend"
}

// GetConnection returns a grpc connection to the backend.
func (sb *SingleBackend) GetConnection(ctx context.Context, _ string) (context.Context, *grpc.ClientConn, error) {
	return sb.GetConn(ctx)
}

// AppendInfo is called to enhance response from the backend with additional data.
func (sb *SingleBackend) AppendInfo(_ bool, resp []byte) ([]byte, error) {
	return resp, nil
}

// BuildError is called to convert error from upstream into response field.
func (sb *SingleBackend) BuildError(bool, error) ([]byte, error) {
	return nil, nil
}

// StreamDirector returns a list of Backend objects to forward the call to.
//
// There are two proxying modes:
//  1. one to one: StreamDirector returns a single Backend object - proxying is done verbatim, Backend.AppendInfo might
//     be used to enhance response with source information (or it might be skipped).
//  2. one to many: StreamDirector returns more than one Backend object - for unary calls responses from Backend objects
//     are aggregated by concatenating protobuf responses (requires top-level `repeated` protobuf definition) and errors
//     are wrapped as responses via BuildError. Responses are potentially enhanced via AppendInfo.
//
// The presence of the `Context` allows for rich filtering, e.g. based on Metadata (headers).
// If no handling is meant to be done, a `codes.NotImplemented` gRPC error should be returned.
//
// It is worth noting that the StreamDirector will be fired *after* all server-side stream interceptors
// are invoked. So decisions around authorization, monitoring etc. are better to be handled there.
//
// See the rather rich example.
type StreamDirector func(ctx context.Context, fullMethodName string) (Mode, []Backend, error)
