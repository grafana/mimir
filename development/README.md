# Mimir Development

Mimir offers a few docker-compose based development environments for different [deployment modes][deployment-modes], including:

- [Microservices mode](./mimir-microservices-mode)
- [Monolothic mode](./mimir-monolithic-mode)
- [Monolithic mode with swift storage](./mimir-monolithic-mode-with-swift-storage)
- [Read-write mode](./mimir-read-write-mode)

## Running

Choose a sub-folder for whichever Mimir deployment mode you want to run. Then, to build Mimir from source and run it via docker-compose, run:

```bash
./compose-up.sh
```

This should give you a running Mimir system with Grafana available at [htttp://localhost:3000](http://localhost:3000), and other Mimir service UIs available via different ports (check `docker ps` for exact ports).

The Minio console is available in most dev environments at [http://localhost:9001](http://localhost:9001), with the credentials defined in [mimir.yaml][minio-creds].

## Profiles

By default a Prometheus and Grafana Agent (static mode) is also started. You can override what agent is being started by using profiles.

> **Note**: Agent profiles are only available in `mimir-monolithic-mode` at the moment.

```bash
./compose-up.sh --profile <profile1> --profile <profile2>
```

Available profiles:

1. grafana-agent-static (started by default)
1. prometheus (started by default)
1. grafana-alloy
1. otel-collector-remote-write (don't use with the other otel collector, both use the default ports for clients)
1. otel-collector-otlp-push (don't use with the other otel collector, both use the default ports for clients)

> **Note**: Compose down will stop all profiles unless specified.

## OTEL collector

Experimental support for running OpenTelemetry collector in the Monolithic mode.

### OTEL collector with Prometheus remote write

Run the following command in `mimir-monolithic-mode/` to start an OpenTelemetry collector in addition to all services.
The new service will accept OTEL metrics on port 4317 (gRPC) and 4318 (http).
In addition the collector will scrape its own metrics.
All received and scraped metrics are sent to Mimir via the Prometheus remote write protocol.

```bash
./compose-up.sh --profile otel-collector-remote-write
```

### OTEL collector with OTLP push

Run the following command in `mimir-monolithic-mode/` to start an OpenTelemetry collector in addition to all services.
The new service will accept OTEL metrics on port 4317 (gRPC) and 4318 (http).
In addition the collector will scrape its own metrics.
All received and scraped metrics are sent to Mimir via OTLP, endpoint `/otlp/v1/metrics`.

```bash
./compose-up.sh --profile otel-collector-otlp-push
```

## Configuring Mimir

The Mimir configuration is available in each environment in the `/config` directory, along with configurations for other apps that run in the environment. Some deployment related configurations are also available at the top of each environment's `docker-compose.jsonnet` file.

## Debugging

The [mimir-microservices-mode](./mimir-microservices-mode) environment supports debugging running components. To enable debug, set `debug: true` at the top of the [docker-compose.jsonnet](./mimir-microservices-mode/docker-compose.jsonnet). When Mimir is running, debug ports are at the service's API port + 10000. Run configurations for Goland are available in the [mimir-microservices-mode/goland](./mimir-microservices-mode/goland) folder, which will connect to each service on their debug port.

[deployment-modes]: https://grafana.com/docs/mimir/latest/operators-guide/architecture/deployment-modes/
[minio-creds]: ./mimir-microservices-mode/config/mimir.yaml
