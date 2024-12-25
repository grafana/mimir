package csproto

// GrpcCodec implements gRPC encoding.Codec.
// See: https://pkg.go.dev/google.golang.org/grpc/encoding#Codec
type GrpcCodec struct{}

// Marshal returns the wire format of v.
func (GrpcCodec) Marshal(v interface{}) ([]byte, error) {
	return Marshal(v)
}

// Unmarshal parses the wire format into v.
func (GrpcCodec) Unmarshal(data []byte, v interface{}) error {
	return Unmarshal(data, v)
}

// Name returns the name of the Codec implementation.
func (GrpcCodec) Name() string {
	return "proto"
}
