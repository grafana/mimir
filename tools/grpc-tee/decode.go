// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"errors"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
)

// proxiedMessage is one gRPC message that the proxy forwarded.
type proxiedMessage struct {
	// Payload is the raw message. The proxy owns it, and it does not refer to a pooled gRPC buffer.
	Payload []byte
	// Decoded is the decoded message, or nil if the backend type has no codec.
	Decoded proto.Message
}

// backendCall is the result of one call to one backend.
type backendCall struct {
	Backend string

	// Responses are the response messages, in the order that the backend sent them.
	Responses     []proxiedMessage
	ResponseBytes int

	// Err is the final gRPC status of the call, or nil if the call succeeded.
	Err error
	// Duration is the time from when the proxy opened the backend stream to the final status.
	//
	// The primary and secondary durations are not directly comparable. The proxy forwards each primary response
	// to the client before it receives the next one, and gRPC flow control makes that send wait while the client
	// reads slowly. For example, the querier reads a Series stream at the speed of the query evaluation.
	// So the primary Duration includes the time that the client takes to read the responses.
	// The secondary responses go to no client, so the secondary Duration does not include that time.
	// As a result, the secondary often looks faster than the primary. Use TimeToFirstResponse to compare the
	// latency of the backends.
	Duration time.Duration
	// TimeToFirstResponse is the time from when the proxy opened the backend stream to the first response message.
	// It is 0 if the backend sent no response message.
	//
	// The proxy measures it when it receives the first message from the backend, before it sends anything to the
	// client, so the primary and secondary values are comparable.
	TimeToFirstResponse time.Duration

	// Aborted is true if the proxy stopped the call before it was complete, so the responses are incomplete.
	Aborted bool
	// DecodeErr holds the errors that occurred when the proxy decoded the responses.
	// If DecodeErr is not nil, Responses can be incomplete.
	DecodeErr error
}

// teeCall is one proxied gRPC call: the request messages from the client, and the call to each backend.
// A unary or server-streaming call has one request message. A client-streaming or bidirectional call can have more.
type teeCall struct {
	FullMethod string

	// Requests are the request messages, in the order that the client sent them.
	Requests     []proxiedMessage
	RequestBytes int
	// RequestDecodeErr holds the errors that occurred when the proxy decoded the requests.
	RequestDecodeErr error

	Primary backendCall
	// Secondary is nil if the proxy did not send the call to a secondary backend.
	Secondary *backendCall
}

// messageRecorder records the messages of one direction of a call, and decodes each message when it arrives.
// It is safe to use from more than one goroutine.
type messageRecorder struct {
	decode func([]byte) (proto.Message, error)
	start  time.Time

	mtx sync.Mutex
	// done is true after finish. A proxy goroutine can still receive a message after the handler returned,
	// so the recorder drops the messages that arrive after that.
	done         bool
	messages     []proxiedMessage
	bytes        int
	firstMessage time.Duration
	decodeErr    error
}

// newMessageRecorder returns a recorder. If decode is nil, the recorder keeps only the raw payloads.
func newMessageRecorder(decode func([]byte) (proto.Message, error), start time.Time) *messageRecorder {
	return &messageRecorder{decode: decode, start: start}
}

func (r *messageRecorder) record(payload []byte) {
	msg := proxiedMessage{Payload: payload}
	var decodeErr error
	if r.decode != nil {
		msg.Decoded, decodeErr = r.decode(payload)
	}
	elapsed := time.Since(r.start)

	r.mtx.Lock()
	defer r.mtx.Unlock()
	if r.done {
		return
	}
	if len(r.messages) == 0 {
		r.firstMessage = elapsed
	}
	r.messages = append(r.messages, msg)
	r.bytes += len(payload)
	r.decodeErr = errors.Join(r.decodeErr, decodeErr)
}

// finish stops the recording, and returns the recorded messages.
func (r *messageRecorder) finish() (messages []proxiedMessage, bytes int, firstMessage time.Duration, decodeErr error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.done = true
	return r.messages, r.bytes, r.firstMessage, r.decodeErr
}

// finishBackendCall stops the recording of the responses of a backend call, and returns the backend call.
func (r *messageRecorder) finishBackendCall(backend string, err error, aborted bool) backendCall {
	responses, bytes, firstResponse, decodeErr := r.finish()
	return backendCall{
		Backend:             backend,
		Responses:           responses,
		ResponseBytes:       bytes,
		Err:                 err,
		Duration:            time.Since(r.start),
		TimeToFirstResponse: firstResponse,
		Aborted:             aborted,
		DecodeErr:           decodeErr,
	}
}

// requestDecoder returns the function that decodes the requests of fullMethod, or nil if codec is nil.
func requestDecoder(codec MessageCodec, fullMethod string) func([]byte) (proto.Message, error) {
	if codec == nil {
		return nil
	}
	return func(data []byte) (proto.Message, error) { return codec.DecodeRequest(fullMethod, data) }
}

// responseDecoder returns the function that decodes the responses of fullMethod, or nil if codec is nil.
func responseDecoder(codec MessageCodec, fullMethod string) func([]byte) (proto.Message, error) {
	if codec == nil {
		return nil
	}
	return func(data []byte) (proto.Message, error) { return codec.DecodeResponse(fullMethod, data) }
}
