From time to time we think about how to make gRPC communication more efficient, and idea that comes back over and over is pooling of byte buffers for message marshalling or unmarshalling.

In this document we would like to write down notes about doing that. This document may not be fully correct (it's easy to miss something) or up-to-date (code changes and evolves). Updates are appreciated.

## Usage of buffers when sending messages over gRPC connection from client to server

This originated from discussion in https://github.com/grafana/mimir/pull/5195.

In that PR we have introduced pooling of buffers used for marshalling messages sent by Distributor to Ingester.

From the discussion:

> One thing to realize is that we're doing this buffering on the client side. "gRPC" that we talk about here is grpc connection to ingester.
>
> Re middlewares... We [setup our grpc connection](https://github.com/grafana/mimir/blob/c8d7c56ed225c706da534e230ff1d6892080db40/pkg/ingester/client/client.go#L42) with following interceptors:
>
> * [grpcclient.Instrument](https://github.com/grafana/mimir/blob/c8d7c56ed225c706da534e230ff1d6892080db40/pkg/ingester/client/client.go#L42), which in turns [adds](https://github.com/grafana/mimir/blob/b5e7bce69311da4df36e363dce3c3d27c8ed0fb1/vendor/github.com/grafana/dskit/grpcclient/instrumentation.go#L13-L15):
>   * `otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer())`
>   * `middleware.ClientUserHeaderInterceptor`
>   * `middleware.UnaryClientInstrumentInterceptor(requestDuration)`
> * Interceptors added by [`(*Config).DialOption`](https://github.com/grafana/mimir/blob/36f96f19bde5d6e5552600b25893244971b007c8/vendor/github.com/grafana/dskit/grpcclient/grpcclient.go#L95-L152):
>   * [BackoffRetry](https://github.com/grafana/mimir/blob/4b8cf2019a88d39231ab0b230701eeeb02079d12/vendor/github.com/grafana/dskit/grpcclient/backoff_retry.go#L14-L31)
>   * [RateLimiter](https://github.com/grafana/mimir/blob/4b8cf2019a88d39231ab0b230701eeeb02079d12/vendor/github.com/grafana/dskit/grpcclient/ratelimit.go#L13-L26)
>
> Looking at implementation of these interceptors...
>
> * [`OpenTracingClientInterceptor`](https://github.com/grafana/mimir/blob/59fed803141e0a8dfbff32ea93cc6c4a7691ca4f/vendor/github.com/opentracing-contrib/go-grpc/client.go#L30-L75) has ability to 1) pass request to span inclusion function,, 2) log the payload (request) and 3) decorate the span using the request. Note that [we don't configure](https://github.com/grafana/mimir/blob/b5e7bce69311da4df36e363dce3c3d27c8ed0fb1/vendor/github.com/grafana/dskit/grpcclient/instrumentation.go#L13) `OpenTracingClientInterceptor` with any of these options.
> * `middleware.ClientUserHeaderInterceptor`, `middleware.UnaryClientInstrumentInterceptor`, `BackoffRetry` and `RateLimiter` interceptors don't use request in any way (apart from passing it further)
>
> More places where our buffer could be passed around:
>
> * [statsHandlers](https://github.com/grafana/mimir/blob/36f96f19bde5d6e5552600b25893244971b007c8/vendor/google.golang.org/grpc/stream.go#L1041-L1043)
> * [binglogs](https://github.com/grafana/mimir/blob/36f96f19bde5d6e5552600b25893244971b007c8/vendor/google.golang.org/grpc/stream.go#L887-L895)
>
> Let's start with binlogs. This is functionality configured by env variable `GRPC_BINARY_LOG_FILTER`. When configured, gRPC logs each RPC in the format defined by proto GrpcLogEntry. Design for this feature is at https://github.com/grpc/proposal/blob/master/A16-binary-logging.md.
>
> When used, request is passed to logging method inside `binarylog.ClientMessage` struct. This struct has [`toProto()`](https://github.com/grafana/mimir/blob/36f96f19bde5d6e5552600b25893244971b007c8/vendor/google.golang.org/grpc/internal/binarylog/method_logger.go#L227) which will call another `Marhal` on our message. Once `binary.ClientMessage` is converted to `binlogpb.GrpcLogEntry` (now it contains our buffer from the pool), it's forwarded to the "sink" for writing.
>
> There are three `sink` implementations in grpc: [`bufferedSink`](https://github.com/grafana/mimir/blob/f3afcb7a598c2147980371a64d985d20819fb49c/vendor/google.golang.org/grpc/internal/binarylog/sink.go#L99-L111), `noopSink` (does nothing) and [`writerSink`](https://github.com/grafana/mimir/blob/f3afcb7a598c2147980371a64d985d20819fb49c/vendor/google.golang.org/grpc/internal/binarylog/sink.go#L69-L84).
>
> `writerSink` marshals `*binlogpb.GrpcLogEntry` and writes it to some writer. [`bufferedSink` simply wraps `writerSink` but gives it a buffer to write output to](https://github.com/grafana/mimir/blob/f3afcb7a598c2147980371a64d985d20819fb49c/vendor/google.golang.org/grpc/internal/binarylog/sink.go#L154-L170), and also runs a goroutine to periodically flush the buffer. Important point is that it doesn't keep `*binlogpb.GrpcLogEntry` around, but marshals it immediately.
>
> My conclusion is: using binary logging feature is safe when using our message buffering.
>
> `statsHandlers` for the grpc connection are configured via dial options. We don't use that feature in our code.
>
> It's possible that I've missed something, but it seems to me that it's safe to reuse the buffer used to Marshal as soon as `(*grpc.ClientConn).Invoke` returns.

