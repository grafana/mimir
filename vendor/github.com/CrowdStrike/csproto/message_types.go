package csproto

import (
	"reflect"
	"sync"

	gogo "github.com/gogo/protobuf/proto"
	google "google.golang.org/protobuf/proto"
)

// MessageType defines the types of Protobuf message implementations this API supports.
type MessageType int

const (
	// MessageTypeUnknown indicates that we don't know which type of Protobuf message a type is.
	MessageTypeUnknown MessageType = iota
	// MessageTypeGogo indicates that a type was generated using Gogo Protobuf.
	MessageTypeGogo
	// MessageTypeGoogleV1 indicates that a type was generated using v1 of Google's Protobuf tools.
	MessageTypeGoogleV1
	// MessageTypeGoogle indicates that a type was generated using v2 of Google's Protobuf tools.
	MessageTypeGoogle
)

var unmarshalMap sync.Map // in-memory cache of the mapping of Go types to MessageType

// MsgType accepts a protobuf message and returns the corresponding MessageType value.
func MsgType(msg interface{}) MessageType {
	typ := reflect.TypeOf(msg)
	val, found := unmarshalMap.Load(typ)
	if found {
		return val.(MessageType)
	}
	mt := deduceMsgType(msg, typ)
	unmarshalMap.Store(typ, mt)
	return mt
}

func deduceMsgType(msg interface{}, typ reflect.Type) MessageType {
	// does the message satisfy Google's v2 csproto.Message interface?
	if _, ok := msg.(google.Message); ok {
		return MessageTypeGoogle
	}
	if typ.Kind() != reflect.Ptr {
		return MessageTypeUnknown
	}
	// does the message satisfy Gogo's csproto.Message interface
	if gogoMsg, ok := msg.(gogo.Message); ok {
		// secondary check that the message type is registered with the Gogo runtime b/c
		// Gogo's csproto.Message and Google's v1 csproto.Message are the same
		if gogo.MessageName(gogoMsg) != "" {
			return MessageTypeGogo
		}
	}
	return MessageTypeGoogleV1
}
