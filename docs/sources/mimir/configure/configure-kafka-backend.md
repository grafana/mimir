---
aliases:
  - ../operators-guide/configure/configure-kafka-backend/
description: Learn how to configure Grafana Mimir to use Kafka for ingest storage.
menuTitle: Kafka
title: Configure the Grafana Mimir Kafka backend
weight: 130
---

# Configure the Grafana Mimir Kafka backend

Grafana Mimir supports using Kafka as the first layer of ingestion in the ingest storage architecture. This configuration allows for scalable, decoupled ingestion that separates write and read paths to improve performance and resilience.

Starting with Mimir 3.0, ingest storage is the preferred and stable architecture for running Mimir.

## Configure ingest storage

Set the following configuration flags to enable Mimir to use ingest storage through a Kafka backend:

- `-ingest-storage.enabled=true`<br />
  You must explicitly enable the ingest storage architecture in all Mimir components.

- `-ingest-storage.kafka.address=<host:port>[,<host:port>...]`<br />
  The `<host:port>` is a Kafka seed broker address used to bootstrap the connection. You can configure a comma-separated list of seed broker addresses for higher bootstrap availability.

- `-ingest-storage.kafka.topic=<name>`<br />
  The `<name>` is the name of the Kafka topic that is used for ingesting data.

- `-ingest-storage.kafka.auto-create-topic-default-partitions=<number>`<br />
  If the configured topic doesn't exist in the Kafka backend, the Mimir components, either consumers or producers,
  create the topic on first access. The `<number>` parameter sets the number of partitions to create when the topic is automatically created. The number of partitions must be at least the number of ingesters in one zone.

Additionally, you can use these recommended configuration options when running Mimir with ingest storage architecture:

- `-distributor.remote-timeout=5s`<br />
  Use this setting to increase the default remote write timeout. This is recommended for writing to Kafka, because pushing
  to Kafka-compatible backends might be slower than writing directly to ingesters.

Refer to Mimir [configuration parameters](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configuration-parameters/) for detailed descriptions of all available configuration options.

## Different Kafka backend implementations

Some Kafka-compatible implementations have different behavior for the Kafka API.
To set up Mimir to work with different Kafka backends, you need to configure some parameters.
Here are the Kafka flavors and additional configurations needed to set them up in Mimir.

### Apache Kafka

In your Kafka broker configuration file (for example, `server.properties`), set the following property to support the default Mimir record size:

```
message.max.bytes=16000000
```

Mimir's default `-ingest-storage.kafka.producer-max-record-size-bytes` is approximately 15.2 MB.
Apache Kafka's default `message.max.bytes` is 1 MB.
Increase Kafka's `message.max.bytes` to at least `16000000` to match Mimir's maximum batch size; otherwise, Kafka rejects records larger than 1 MB.

To configure the limit at the topic level instead of the broker level, run:

```bash
bin/kafka-configs.sh --bootstrap-server <host:port> \
  --alter --entity-type topics --entity-name <topic-name> \
  --add-config max.message.bytes=16000000
```

### Confluent Kafka

In your Kafka broker configuration file (for example, `server.properties`), set the following property to support the default Mimir record size:

```
message.max.bytes=16000000
```

Mimir's default `-ingest-storage.kafka.producer-max-record-size-bytes` is approximately 15.2 MB.
Confluent Kafka's default `message.max.bytes` is 1 MB.
Increase Kafka's `message.max.bytes` to at least `16000000` to match Mimir's maximum batch size; otherwise, Kafka rejects records larger than 1 MB.

To configure the limit at the topic level instead of the broker level, run:

```bash
bin/kafka-configs.sh --bootstrap-server <host:port> \
  --alter --entity-type topics --entity-name <topic-name> \
  --add-config max.message.bytes=16000000
```

### Warpstream

Configure the following CLI flags or their YAML equivalent.

```
-ingest-storage.kafka.use-compressed-bytes-as-fetch-max-bytes=false
```

## Authentication

Mimir supports multiple ways of authenticating with a Kafka cluster.
Most methods use the Simple Authentication and Security Layer (SASL) framework, which supports the following mechanisms:

- `PLAIN`: Authenticate with a username and password.
- `SCRAM-SHA-256` and `SCRAM-SHA-512`: Authenticate with a username and password using Salted Challenge Response Authentication Mechanism (SCRAM).
- `OAUTHBEARER`: Authenticate with an OAuth 2.0 bearer token.
- `AWS_MSK_IAM`: Authenticate to Amazon Managed Streaming for Apache Kafka (Amazon MSK) using AWS Identity and Access Management (IAM).

You can also connect to Kafka over Transport Layer Security (TLS), including mutual TLS (mTLS) for certificate-based authentication.

### Username and password (PLAIN, SCRAM)

Set the following configuration flags to authenticate with a username and password:

- `-ingest-storage.kafka.sasl-mechanism`<br />
  The SASL mechanism used to authenticate to Kafka. Set to `SCRAM-SHA-256`, `SCRAM-SHA-512`, or `PLAIN`. The default value is `PLAIN`.

- `-ingest-storage.kafka.sasl-username`<br />
  The username used to authenticate to Kafka.

- `-ingest-storage.kafka.sasl-password`<br />
  The password used to authenticate to Kafka.

To enable SASL, configure both the username and the password. For backwards compatibility, the `PLAIN` mechanism with no username and no password disables SASL.

### OAUTHBEARER or AWS_MSK_IAM

Set the following configuration flags to authenticate with OAuth or MSK IAM:

- `-ingest-storage.kafka.sasl-mechanism`<br />
  Set to `OAUTHBEARER` or `AWS_MSK_IAM`.

Both mechanisms share common patterns: either statically, with values set at startup and then don't change, or dynamically, through a configuration file or an HTTP callback that are checked anew every time reauthentication is required.

#### With static configuration

You can set authentication configuration directly as configuration flags. The flags that each mechanism requires are listed in the following sections.

{{< admonition type="note" >}}
Mimir components that connect to Kafka must be restarted whenever reauthentication is required.
{{< /admonition >}}


#### With path to configuration file

Set the following configuration flag to specify the path to a file that contains a JSON object configuring authentication credentials:

- `-ingest-storage.kafka.sasl-<MECHANISM>-file-path`<br />
  Replace `<MECHANISM>` with `oauthbearer` or `msk-iam`.

Refer to the following sections for the shape that the JSON object in the file must take.

This file is opened and read anew whenever reauthentication is required.
Mimir doesn't signal when this happens, so you must handle the authentication credentials lifetime and file updates out-of-band.
Alternatively, you can configure an HTTP callback, which receives an HTTP request whenever reauthentication is required.

#### With HTTP callback

Set the following configuration flag to specify the path to a Unix domain socket:

- `-ingest-storage.kafka.sasl-<MECHANISM>-http-socket-path`<br />
  Replace `<MECHANISM>` with `oauthbearer` or `msk-iam`.

You can also configure the timeout for the HTTP request:

- `-ingest-storage.kafka.sasl-<MECHANISM>-http-socket-timeout`<br />
  The timeout for requesting credentials from the HTTP socket. Replace `<MECHANISM>` with `oauthbearer` or `msk-iam`. The default value is `10s`.

A server must be listening for connections on this Unix domain socket. On receiving an HTTP GET request to path `/`, the server must respond with status 200 OK and a JSON object containing the authentication configuration.

Refer to the following sections for the shape that the JSON object must take.

Using an HTTP callback has some important benefits compared to the static configuration and the file-based approaches:

- Because Mimir issues an HTTP request whenever reauthentication is required, you don't need to manage authentication lifetime out-of-band.
- Authentication credentials don't need to be persisted. They can be obtained on-the-fly and sent back to Mimir for the lifespan of a single HTTP request.

#### OAUTHBEARER configuration details

For static configuration, set the following configuration flags:

- `-ingest-storage.kafka.sasl-oauthbearer-token`
- `-ingest-storage.kafka.sasl-oauthbearer-zid`
- `-ingest-storage.kafka.sasl-oauthbearer-extensions` (optional)

For dynamic configuration (file or HTTP callback), set a JSON object with the following shape:

```json
{
  "token": "<token>",
  "zid": "<authorization ID>",
  "extensions": {
    "<key>": "<value>"
  } // (optional)
}
```

Only `token` is required.

#### MSK_IAM configuration details

For static configuration, set the following configuration flags:

- `-ingest-storage.kafka.sasl-msk-iam-access-key`
- `-ingest-storage.kafka.sasl-msk-iam-secret-key`
- `-ingest-storage.kafka.sasl-msk-iam-session-token` (optional)
- `-ingest-storage.kafka.sasl-msk-iam-user-agent` (optional)

For dynamic configuration (file or HTTP callback), set a JSON object with the following shape:

```json
{
  "AccessKey": "<access key ID>",
  "SecretKey": "<secret access key>",
  "SessionToken": "<session token>", // (optional)
  "UserAgent": "<user agent>" // (optional)
}
```

### TLS / mTLS

Set the following configuration parameter to enable connecting to the Kafka cluster over TLS:

- `-ingest-storage.kafka.tls-enabled=true`

For mutual authentication (mTLS), set the following additional flags:

- `-ingest-storage.kafka.tls-ca-path`<br />
  Path to the CA certificates used to validate the server certificate. If you don't set this flag, Mimir uses the host's root CA certificates.

- `-ingest-storage.kafka.tls-cert-path`<br />
  Path to the client certificate used to authenticate with the server. Requires the key path to also be configured.

- `-ingest-storage.kafka.tls-key-path`<br />
  Path to the key for the client certificate. Requires the client certificate to also be configured.

You can also set the following optional flags to customize the TLS connection:

- `-ingest-storage.kafka.tls-server-name`<br />
  Override the expected name on the server certificate.

- `-ingest-storage.kafka.tls-insecure-skip-verify`<br />
  Skip validating the server certificate. This option is insecure and is intended only for testing.

Refer to Mimir [configuration parameters](https://grafana.com/docs/mimir/<MIMIR_VERSION>/configure/configuration-parameters/) for details and additional options, such as `-ingest-storage.kafka.tls-cipher-suites` and `-ingest-storage.kafka.tls-min-version`.
