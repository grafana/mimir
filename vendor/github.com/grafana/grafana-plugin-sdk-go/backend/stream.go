package backend

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/grafana/grafana-plugin-sdk-go/genproto/pluginv2"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// StreamHandler handles streams.
// This is EXPERIMENTAL and is a subject to change till Grafana 8.
type StreamHandler interface {
	// SubscribeStream called when a user tries to subscribe to a plugin/datasource
	// managed channel path – thus plugin can check subscribe permissions and communicate
	// options with Grafana Core. As soon as first subscriber joins channel RunStream
	// will be called.
	SubscribeStream(context.Context, *SubscribeStreamRequest) (*SubscribeStreamResponse, error)
	// PublishStream called when a user tries to publish to a plugin/datasource
	// managed channel path. Here plugin can check publish permissions and
	// modify publication data if required.
	PublishStream(context.Context, *PublishStreamRequest) (*PublishStreamResponse, error)
	// RunStream will be initiated by Grafana to consume a stream. RunStream will be
	// called once for the first client successfully subscribed to a channel path.
	// When Grafana detects that there are no longer any subscribers inside a channel,
	// the call will be terminated until next active subscriber appears. Call termination
	// can happen with a delay.
	RunStream(context.Context, *RunStreamRequest, *StreamSender) error
}

// SubscribeStreamRequest is EXPERIMENTAL and is a subject to change till Grafana 8.
type SubscribeStreamRequest struct {
	PluginContext PluginContext
	Path          string
	Data          json.RawMessage
}

// SubscribeStreamStatus is a status of subscription response.
type SubscribeStreamStatus int32

const (
	// SubscribeStreamStatusOK means subscription is allowed.
	SubscribeStreamStatusOK SubscribeStreamStatus = 0
	// SubscribeStreamStatusNotFound means stream does not exist at all.
	SubscribeStreamStatusNotFound SubscribeStreamStatus = 1
	// SubscribeStreamStatusPermissionDenied means that user is not allowed to subscribe.
	SubscribeStreamStatusPermissionDenied SubscribeStreamStatus = 2
)

// SubscribeStreamResponse is EXPERIMENTAL and is a subject to change till Grafana 8.
type SubscribeStreamResponse struct {
	Status      SubscribeStreamStatus
	InitialData *InitialData
}

// InitialData to send to a client upon a successful subscription to a channel.
type InitialData struct {
	data []byte
}

// Data allows to get prepared bytes of initial data.
func (d *InitialData) Data() []byte {
	return d.data
}

// NewInitialFrame allows creating frame as subscription InitialData.
func NewInitialFrame(frame *data.Frame, include data.FrameInclude) (*InitialData, error) {
	frameJSON, err := data.FrameToJSON(frame, include)
	if err != nil {
		return nil, err
	}
	return &InitialData{
		data: frameJSON,
	}, nil
}

// NewInitialData allows sending JSON on subscription
func NewInitialData(data json.RawMessage) (*InitialData, error) {
	if !json.Valid(data) {
		return nil, fmt.Errorf("invalid JSON data")
	}
	return &InitialData{
		data: data,
	}, nil
}

// PublishStreamRequest is EXPERIMENTAL and is a subject to change till Grafana 8.
type PublishStreamRequest struct {
	PluginContext PluginContext
	Path          string
	Data          json.RawMessage
}

// PublishStreamStatus is a status of publication response.
type PublishStreamStatus int32

const (
	// PublishStreamStatusOK means publication is allowed.
	PublishStreamStatusOK PublishStreamStatus = 0
	// PublishStreamStatusNotFound means stream does not exist at all.
	PublishStreamStatusNotFound PublishStreamStatus = 1
	// PublishStreamStatusPermissionDenied means that user is not allowed to publish.
	PublishStreamStatusPermissionDenied PublishStreamStatus = 2
)

// PublishStreamResponse is EXPERIMENTAL and is a subject to change till Grafana 8.
type PublishStreamResponse struct {
	Status PublishStreamStatus
	Data   json.RawMessage
}

// RunStreamRequest is EXPERIMENTAL and is a subject to change till Grafana 8.
type RunStreamRequest struct {
	PluginContext PluginContext
	Path          string
	Data          json.RawMessage
}

// StreamPacket is EXPERIMENTAL and is a subject to change till Grafana 8.
type StreamPacket struct {
	Data json.RawMessage
}

// StreamPacketSender is EXPERIMENTAL and is a subject to change till Grafana 8.
type StreamPacketSender interface {
	Send(*StreamPacket) error
}

// StreamSender allows sending data to a stream.
// StreamSender is EXPERIMENTAL and is a subject to change till Grafana 8.
type StreamSender struct {
	packetSender StreamPacketSender
}

func NewStreamSender(packetSender StreamPacketSender) *StreamSender {
	return &StreamSender{packetSender: packetSender}
}

// SendFrame allows sending data.Frame to a stream.
func (s *StreamSender) SendFrame(frame *data.Frame, include data.FrameInclude) error {
	frameJSON, err := data.FrameToJSON(frame, include)
	if err != nil {
		return err
	}
	packet := &pluginv2.StreamPacket{
		Data: frameJSON,
	}
	return s.packetSender.Send(FromProto().StreamPacket(packet))
}

// SendJSON allow sending arbitrary JSON to a stream. When sending data.Frame
// prefer using SendFrame method.
func (s *StreamSender) SendJSON(data []byte) error {
	if !json.Valid(data) {
		return fmt.Errorf("invalid JSON data")
	}
	packet := &pluginv2.StreamPacket{
		Data: data,
	}
	return s.packetSender.Send(FromProto().StreamPacket(packet))
}

// SendBytes allow sending arbitrary Bytes to a stream. When sending data.Frame
// prefer using SendFrame method. When sending an arbitrary raw JSON prefer
// using SendJSON method.
func (s *StreamSender) SendBytes(data []byte) error {
	packet := &pluginv2.StreamPacket{
		Data: data,
	}
	return s.packetSender.Send(FromProto().StreamPacket(packet))
}
