Why does this package exist?

We currently have two ways of running sharding: as a middleware in the query-frontend request processing pipeline,
and as a MQE optimization pass.

The MQE optimization pass implementation depends on the `querymiddleware` package, so we can't test it from there,
as that would create a circular dependency.

So we have this separate package that can be used for testing both implementations.

Once the MQE optimization pass is the only way to run query sharding, then all of the sharding logic and tests can
be moved into `pkg/streamingpromql/optimize/ast/sharding`.
