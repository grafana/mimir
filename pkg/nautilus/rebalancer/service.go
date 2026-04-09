// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// GetAssignmentsRequest is the request message for GetAssignments.
type GetAssignmentsRequest struct{}

func (m *GetAssignmentsRequest) Reset()                            {}
func (m *GetAssignmentsRequest) String() string                    { return "{}" }
func (m *GetAssignmentsRequest) ProtoMessage()                     {}
func (m *GetAssignmentsRequest) Marshal() ([]byte, error)          { return nil, nil }
func (m *GetAssignmentsRequest) MarshalTo(dAtA []byte) (int, error) { return 0, nil }
func (m *GetAssignmentsRequest) Unmarshal(dAtA []byte) error       { return nil }
func (m *GetAssignmentsRequest) Size() int                         { return 0 }

// GetAssignmentsResponse contains the full set of timed assignments.
// Wire format uses JSON encoding inside a single length-delimited
// protobuf field for simplicity in this prototype.
type GetAssignmentsResponse struct {
	Assignments []TimedAssignmentProto `json:"assignments"`
}

// TimedAssignmentProto is the wire representation of a timed assignment.
type TimedAssignmentProto struct {
	FromUnixMs  int64                    `json:"from_unix_ms"`
	Entries     []AssignmentEntryProto   `json:"entries"`
}

// AssignmentEntryProto is the wire representation of a hash-range-to-partition mapping.
type AssignmentEntryProto struct {
	Lo          uint32 `json:"lo"`
	Hi          uint32 `json:"hi"`
	PartitionID int32  `json:"partition_id"`
}

func (m *GetAssignmentsResponse) Reset()         { *m = GetAssignmentsResponse{} }
func (m *GetAssignmentsResponse) String() string { b, _ := json.Marshal(m); return string(b) }
func (m *GetAssignmentsResponse) ProtoMessage()  {}

func (m *GetAssignmentsResponse) Marshal() ([]byte, error) {
	return json.Marshal(m)
}
func (m *GetAssignmentsResponse) MarshalTo(dAtA []byte) (int, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return 0, err
	}
	copy(dAtA, b)
	return len(b), nil
}
func (m *GetAssignmentsResponse) Unmarshal(dAtA []byte) error {
	return json.Unmarshal(dAtA, m)
}
func (m *GetAssignmentsResponse) Size() int {
	b, _ := json.Marshal(m)
	return len(b)
}

// ToTimedAssignmentSet converts the proto response to the domain type.
func (m *GetAssignmentsResponse) ToTimedAssignmentSet() *assignment.TimedAssignmentSet {
	s := &assignment.TimedAssignmentSet{
		Assignments: make([]assignment.TimedAssignment, len(m.Assignments)),
	}
	for i, ta := range m.Assignments {
		entries := make([]assignment.Entry, len(ta.Entries))
		for j, e := range ta.Entries {
			entries[j] = assignment.Entry{
				Range:       assignment.HashRange{Lo: e.Lo, Hi: e.Hi},
				PartitionID: e.PartitionID,
			}
		}
		s.Assignments[i] = assignment.TimedAssignment{
			From:       time.UnixMilli(ta.FromUnixMs),
			Assignment: &assignment.Assignment{Entries: entries},
		}
	}
	return s
}

// TimedAssignmentSetToProto converts the domain type to the proto response.
func TimedAssignmentSetToProto(s *assignment.TimedAssignmentSet) *GetAssignmentsResponse {
	resp := &GetAssignmentsResponse{
		Assignments: make([]TimedAssignmentProto, len(s.Assignments)),
	}
	for i, ta := range s.Assignments {
		entries := make([]AssignmentEntryProto, len(ta.Assignment.Entries))
		for j, e := range ta.Assignment.Entries {
			entries[j] = AssignmentEntryProto{
				Lo:          e.Range.Lo,
				Hi:          e.Range.Hi,
				PartitionID: e.PartitionID,
			}
		}
		resp.Assignments[i] = TimedAssignmentProto{
			FromUnixMs: ta.From.UnixMilli(),
			Entries:    entries,
		}
	}
	return resp
}

// NautilusRebalancerServer is the gRPC server interface.
type NautilusRebalancerServer interface {
	GetAssignments(context.Context, *GetAssignmentsRequest) (*GetAssignmentsResponse, error)
}

// NautilusRebalancerClient is the gRPC client interface.
type NautilusRebalancerClient interface {
	GetAssignments(ctx context.Context, in *GetAssignmentsRequest, opts ...grpc.CallOption) (*GetAssignmentsResponse, error)
}

type nautilusRebalancerClient struct {
	cc grpc.ClientConnInterface
}

func NewNautilusRebalancerClient(cc grpc.ClientConnInterface) NautilusRebalancerClient {
	return &nautilusRebalancerClient{cc: cc}
}

func (c *nautilusRebalancerClient) GetAssignments(ctx context.Context, in *GetAssignmentsRequest, opts ...grpc.CallOption) (*GetAssignmentsResponse, error) {
	out := new(GetAssignmentsResponse)
	err := c.cc.Invoke(ctx, "/nautilus.NautilusRebalancer/GetAssignments", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// UnimplementedNautilusRebalancerServer must be embedded for forward compat.
type UnimplementedNautilusRebalancerServer struct{}

func (*UnimplementedNautilusRebalancerServer) GetAssignments(context.Context, *GetAssignmentsRequest) (*GetAssignmentsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAssignments not implemented")
}

func _NautilusRebalancer_GetAssignments_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetAssignmentsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NautilusRebalancerServer).GetAssignments(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/nautilus.NautilusRebalancer/GetAssignments",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NautilusRebalancerServer).GetAssignments(ctx, req.(*GetAssignmentsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// NautilusRebalancerServiceDesc is the gRPC service descriptor.
var NautilusRebalancerServiceDesc = grpc.ServiceDesc{
	ServiceName: "nautilus.NautilusRebalancer",
	HandlerType: (*NautilusRebalancerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetAssignments",
			Handler:    _NautilusRebalancer_GetAssignments_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "nautilus/rebalancer/service.proto",
}

// assignmentStore provides thread-safe access to the timed assignment set.
type assignmentStore struct {
	mu  sync.RWMutex
	set assignment.TimedAssignmentSet
}

func (s *assignmentStore) add(from time.Time, a *assignment.Assignment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.set.Add(from, a)
}

func (s *assignmentStore) snapshot() assignment.TimedAssignmentSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := assignment.TimedAssignmentSet{
		Assignments: make([]assignment.TimedAssignment, len(s.set.Assignments)),
	}
	copy(cp.Assignments, s.set.Assignments)
	return cp
}

func (s *assignmentStore) latest() *assignment.Assignment {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.set.Latest()
}
