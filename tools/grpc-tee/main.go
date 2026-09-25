package main

import (
	"context"
	"flag"
	"log"
	"net"

	"github.com/siderolabs/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func main() {
	listenAddress := flag.String("server.grpc-listen-address", ":9095", "Address to listen on for gRPC requests.")
	backendAddress := flag.String("backend.address", "localhost:9096", "Address of the gRPC backend to forward requests to.")
	flag.Parse()

	// The client uses the raw codec, so response frames return as opaque bytes.
	conn, err := grpc.NewClient(
		*backendAddress,
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(proxy.Codec())),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to create backend client: %v", err)
	}
	defer conn.Close()

	backend := &proxy.SingleBackend{
		GetConn: func(ctx context.Context) (context.Context, *grpc.ClientConn, error) {
			// Forward the inbound metadata (for example X-Scope-OrgID) to the backend.
			md, _ := metadata.FromIncomingContext(ctx)
			return metadata.NewOutgoingContext(ctx, md.Copy()), conn, nil
		},
	}

	director := func(_ context.Context, fullMethodName string) (proxy.Mode, []proxy.Backend, error) {
		log.Printf("proxying %s to %s", fullMethodName, *backendAddress)
		return proxy.One2One, []proxy.Backend{backend}, nil
	}

	// The server registers no services, so every call goes to the transparent handler.
	// The server also uses the raw codec, so request frames stay opaque bytes.
	server := grpc.NewServer(
		grpc.ForceServerCodecV2(proxy.Codec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(director)),
	)

	lis, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *listenAddress, err)
	}

	log.Printf("grpc-tee listening on %s, forwarding to %s", *listenAddress, *backendAddress)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
