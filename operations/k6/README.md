# Load testing Grafana Mimir with k6

[Grafana's k6](https://k6.io/) is an open source load testing tool.

## Pre-requisites

Install xk6, used for building k6 with additional modules

```sh
go install go.k6.io/xk6/cmd/xk6@latest
```

Build k6 with k6-client-prometheus-remote support using xk6

```sh
xk6 build --with github.com/grafana/xk6-client-prometheus-remote@latest
```

## Run the test script

The [load-testing-with-k6.js] script can be configured using the following environment variables:

| Environment variable          | Required | Default value | Description                                                                           |
| ----------------------------- | -------- | ------------- | ------------------------------------------------------------------------------------- |
| `K6_WRITE_HOSTNAME`           | Yes      |               | Mimir hostname to connect to on the write path.                                       |
| `K6_READ_HOSTNAME`            | Yes      |               | Mimir hostname to connect to on the read path.                                        |
| `K6_SCHEME`                   |          | http          | The protocol scheme used for requests.                                                |
| `K6_WRITE_USERNAME`           |          | ''            | Mimir username to use for HTTP basic authentication for writes.                       |
| `K6_READ_USERNAME`            |          | ''            | Mimir username to use for HTTP basic authentication for reads.                        |
| `K6_WRITE_TOKEN`              |          | ''            | Authentication token to use for HTTP bearer authentication on requests to write path. |
| `K6_READ_TOKEN`               |          | ''            | Authentication token to use for HTTP bearer authentication on requests to read path.  |
| `K6_WRITE_REQUEST_RATE`       |          | 1             | Number of remote write requests to send every `K6_SCRAPE_INTERVAL_SECONDS`.           |
| `K6_WRITE_SERIES_PER_REQUEST` |          | 1000          | Number of series per remote write request.                                            |
| `K6_READ_REQUEST_RATE`        |          | 1             | Number of query requests per second.                                                  |
| `K6_DURATION_MIN`             |          | 720           | Duration of the load test in minutes (including ramp up and down).                    |
| `K6_RAMP_UP_MIN`              |          | 0             | Duration of the ramp up period in minutes.                                            |
| `K6_RAMP_DOWN_MIN`            |          | 0             | Duration of the ramp down period in minutes.                                          |
| `K6_SCRAPE_INTERVAL_SECONDS`  |          | 20            | Simulated Prometheus scrape interval in seconds.                                      |
| `K6_HA_REPLICAS`              |          | 1             | Number of HA replicas to simulate (use 1 for no HA).                                  |
| `K6_HA_CLUSTERS`              |          | 1             | Number of HA clusters to simulate.                                                    |
| `K6_WRITE_TENANT_ID`          |          | ''            | Tenant ID to write metrics to when not using HTTP basic auth.                         |
| `K6_READ_TENANT_ID`           |          | ''            | Tenant ID to read metrics from when not using HTTP basic auth.                        |

For example, if Mimir is running on `localhost:80` you can run a small scale test with this command:

```sh
k6 run load-testing-with-k6.js -e K6_WRITE_HOSTNAME="localhost:80" -e K6_READ_HOSTNAME="localhost:80"
```

Assuming Mimir is scaled up appropriately and you have enough k6 workers capacity, you can load test Mimir with 1 billion active series running this command:

```sh
k6 run load-testing-with-k6.js \
    -e K6_WRITE_HOSTNAME="mimir:80" \
    -e K6_READ_HOSTNAME="mimir:80" \
    -e K6_WRITE_REQUEST_RATE="50000" \
    -e K6_WRITE_SERIES_PER_REQUEST="20000" \
    -e K6_READ_REQUEST_RATE="200" \
    -e RAMP_UP_MIN="2"
```

### Running the test in k6 cloud

Alternatively, you can use [k6 cloud](https://k6.io/cloud/) to run the load test.
Since the script requires a custom extension, you need a [k6 enterprise plan](https://k6.io/pricing/).

Assuming your k6 account has an enterprise plan:

1. Log into k6 cloud
1. Go to https://app.k6.io/tests/new/cli to retrieve the command to authenticate the CLI tool with k6 cloud. It should look something like:
   ```sh
   k6 login cloud -t <token>
   ```
1. Run the load test in k6 cloud
   ```sh
   k6 cloud load-testing-with-k6.js
   ```

[load-testing-with-k6.js]: ./load-testing-with-k6.js
