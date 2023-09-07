package client

import (
	"context"

	"google.golang.org/grpc"
)

type IngesterServerWithInstanceLimits interface {
	IngesterServer

	StartPushRequest() error
	FinishPushRequest()
}

func RegisterIngesterServerWithLimitsTracking(s *grpc.Server, srv IngesterServerWithInstanceLimits) {
	var desc grpc.ServiceDesc
	desc = _Ingester_serviceDesc

	for ix, m := range _Ingester_serviceDesc.Methods {
		if m.MethodName == "Push" {
			_Ingester_serviceDesc.Methods[ix].Handler = _IngesterPushHandlerWithLimitsTracking
		}
	}

	s.RegisterService(&desc, srv)
}

func _IngesterPushHandlerWithLimitsTracking(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	srvil := srv.(IngesterServerWithInstanceLimits)

	err := srvil.StartPushRequest()
	if err != nil {
		return nil, err
	}
	defer srvil.FinishPushRequest()

	return _Ingester_Push_Handler(srv, ctx, dec, interceptor)
}
