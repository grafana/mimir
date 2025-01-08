package csproto

import (
	"encoding"
	"fmt"

	gogo "github.com/gogo/protobuf/proto"
	googlev1 "github.com/golang/protobuf/proto" //nolint: staticcheck // we're using this deprecated package intentionally"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// MarshalText converts the specified message to prototext string format
func MarshalText(msg interface{}) (string, error) {
	if tm, ok := msg.(encoding.TextMarshaler); ok {
		res, err := tm.MarshalText()
		if err != nil {
			return "", err
		}
		return string(res), nil
	}

	switch MsgType(msg) {
	case MessageTypeGoogle:
		return prototext.Format(msg.(proto.Message)), nil
	case MessageTypeGoogleV1:
		return googlev1.MarshalTextString(msg.(googlev1.Message)), nil
	case MessageTypeGogo:
		return gogo.MarshalTextString(msg.(gogo.Message)), nil
	default:
		return "", fmt.Errorf("unsupported message type: %T", msg)
	}
}
