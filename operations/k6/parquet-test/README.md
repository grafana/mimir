### Credentials

The script will look for the `USERNAME` and `PASSWORD` environment variables. `USERNAME` should be a Mimir tenant ID and `PASSWORD` should be a valid token.

### Parquet Query Test

Example of running the parquet query test locally:
```shell
k6 cloud -e API_URL=${MIMIR_URL} -e USERNAME=${MIMIR_TENANT_ID} -e PASSWORD=${MIMIR_PASSWORD} -e K6_PROJECT_ID=${K6_PROJECT_ID}  -e TEST_NAME=parquetQueries k6-test.js
```

Example running the test in the cloud:
```shell
k6 cloud -e API_URL=${MIMIR_URL} -e USERNAME=${MIMIR_TENANT_ID} -e PASSWORD=${MIMIR_PASSWORD} -e K6_PROJECT_ID=${K6_PROJECT_ID}  -e TEST_NAME=parquetQueries k6-test.js
```

### Notes

TEST_NAME=parquetQueries - executes CPU usage and memory queries in random 12-hour time windows throughout June 2025, with cache-busting timestamps to avoid cached results

The query executed is which had 40k cardinality in the test environment:
```
sum(rate(container_cpu_usage_seconds_total{namespace="mimir-dev-11", pod=~"ingester.*"}[5m])) + {timestamp}
```

Features: Runs concurrent VUs with 1 query each every 10 seconds with random 12-hour windows over the dates specified in the test, 5m rate intervals, cache-busting timestamps