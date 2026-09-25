// Copyright 2017 Michal Witkowski. All Rights Reserved.
// Copyright 2019 Andrey Smirnov. All Rights Reserved.
// See LICENSE for licensing terms.

package proxy

import "google.golang.org/grpc"

// Mode specifies proxying mode: one2one (transparent) or one2many (aggregation, error wrapping).
type Mode int

// Mode constants.
const (
	One2One Mode = iota
	One2Many
)

// StreamedDetectorFunc reports is gRPC is doing streaming (only for one2many proxying).
type StreamedDetectorFunc func(fullMethodName string) bool

// Option configures gRPC proxy.
type Option func(*handlerOptions)

// WithMethodNames configures list of method names to proxy for non-transparent handler.
func WithMethodNames(methodNames ...string) Option {
	return func(o *handlerOptions) {
		o.methodNames = append([]string(nil), methodNames...)
	}
}

// WithStreamedMethodNames configures list of streamed method names.
//
// This is only important for one2many proxying.
// This option can't be used with TransparentHandler.
func WithStreamedMethodNames(streamedMethodNames ...string) Option {
	return func(o *handlerOptions) {
		o.streamedMethods = map[string]struct{}{}

		for _, methodName := range streamedMethodNames {
			o.streamedMethods["/"+o.serviceName+"/"+methodName] = struct{}{}
		}

		o.streamedDetector = func(fullMethodName string) bool {
			_, exists := o.streamedMethods[fullMethodName]

			return exists
		}
	}
}

// WithStreamedDetector configures a function to detect streamed methods.
//
// This is only important for one2many proxying.
func WithStreamedDetector(detector StreamedDetectorFunc) Option {
	return func(o *handlerOptions) {
		o.streamedDetector = detector
	}
}

// RegisterService sets up a proxy handler for a particular gRPC service and method.
// The behavior is the same as if you were registering a handler method, e.g. from a codegenerated pb.go file.
//
// This can *only* be used if the `server` also uses grpc.CustomCodec() ServerOption.
func RegisterService(server grpc.ServiceRegistrar, director StreamDirector, serviceName string, options ...Option) {
	streamer := &handler{
		director: director,
		options: handlerOptions{
			serviceName: serviceName,
		},
	}

	for _, o := range options {
		o(&streamer.options)
	}

	fakeDesc := &grpc.ServiceDesc{
		ServiceName: serviceName,
		HandlerType: (*any)(nil),
	}

	for _, m := range streamer.options.methodNames {
		streamDesc := grpc.StreamDesc{
			StreamName:    m,
			Handler:       streamer.handler,
			ServerStreams: true,
			ClientStreams: true,
		}
		fakeDesc.Streams = append(fakeDesc.Streams, streamDesc)
	}

	server.RegisterService(fakeDesc, streamer)
}

// TransparentHandler returns a handler that attempts to proxy all requests that are not registered in the server.
// The indented use here is as a transparent proxy, where the server doesn't know about the services implemented by the
// backends. It should be used as a `grpc.UnknownServiceHandler`.
//
// This can *only* be used if the `server` also uses grpc.CustomCodec() ServerOption.
func TransparentHandler(director StreamDirector, options ...Option) grpc.StreamHandler {
	streamer := &handler{
		director: director,
	}

	for _, o := range options {
		o(&streamer.options)
	}

	return streamer.handler
}
