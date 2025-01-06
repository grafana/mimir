package csproto

import (
	gogo "github.com/gogo/protobuf/proto"
	google "github.com/golang/protobuf/proto" //nolint: staticcheck // we're using this deprecated package intentionally
	googlev2 "google.golang.org/protobuf/proto"
)

// Clone returns a deep copy of m, delegating to the appropriate underlying Protobuf API based on
// the concrete type of m.  Since the underlying runtimes return different types, this function returns
// interface{} and the caller will need to type-assert back to the concrete type of m.
//
// If m is not one of the supported message types, this function returns nil.
func Clone(m interface{}) interface{} {
	switch MsgType(m) {
	case MessageTypeGoogle:
		return googlev2.Clone(m.(googlev2.Message))
	case MessageTypeGoogleV1:
		return google.Clone(m.(google.Message))
	case MessageTypeGogo:
		return gogo.Clone(m.(gogo.Message))
	default:
		return nil
	}
}
