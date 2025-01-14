package csproto

import (
	gogo "github.com/gogo/protobuf/proto"
	google "github.com/golang/protobuf/proto" //nolint: staticcheck // we're using this deprecated package intentionally
	googlev2 "google.golang.org/protobuf/proto"
)

// Equal returns true iff m1 and m2 are equal.
//
// If m1 and m2 are not the same "type" (generated for the same runtime) then they are not equal.  Otherwise,
// we delegate comparison to the appropriate underlying Protobuf API based on the concrete type of the
// messages
func Equal(m1, m2 interface{}) bool {
	t1, t2 := MsgType(m1), MsgType(m2)
	if t1 != t2 {
		return false
	}
	switch t1 {
	case MessageTypeGoogleV1:
		return google.Equal(m1.(google.Message), m2.(google.Message))
	case MessageTypeGoogle:
		return googlev2.Equal(m1.(googlev2.Message), m2.(googlev2.Message))
	case MessageTypeGogo:
		return gogo.Equal(m1.(gogo.Message), m2.(gogo.Message))
	default:
		return false
	}
}
