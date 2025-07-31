import http from 'k6/http';
import { sleep, check } from 'k6';
import { URL } from 'https://jslib.k6.io/url/1.0.0/index.js';
import { b64encode } from 'k6/encoding';

const PROJECT_ID = __ENV.K6_PROJECT_ID;
const TEST_NAME = __ENV.TEST_NAME;
const API_URL = __ENV.API_URL;
export const ORG_ID = __ENV.ORG_ID // Used when bypassing auth from inside the cluster
export const USERNAME = __ENV.USERNAME // Prometheus Tenant ID
export const PASSWORD = __ENV.PASSWORD // Prometheus Write Key
const TEST_K6_REQUEST_TAG_NAME = __ENV.TEST_K6_REQUEST_TAG_NAME; // https://k6.io/docs/using-k6/http-requests/#http-request-tags

const queryTests = {
  parquetQueries: {
    scenarios: {
      parquetQueries: {
        exec: 'parquetQueries',
        executor: 'constant-vus',
        vus: 2,
        duration: '30m',
      },
    },
    thresholds: {
      http_req_failed: ['rate<0.05'],
      http_req_duration: ['p(95)<60000'],
    }
  }
}

function buildUrlParams() {
  let params = {
    "headers": {
      "Content-Type": "application/json",
    },
    "tags": {},
  }

  if (ORG_ID != null) {
    params["headers"]['X-Scope-OrgID'] = ORG_ID;
  }
  if (USERNAME != null && PASSWORD != null) {
    const encodedCredentials = b64encode(`${USERNAME}:${PASSWORD}`);
    params["headers"]['Authorization'] = `Basic ${encodedCredentials}`;
  }

  if (TEST_K6_REQUEST_TAG_NAME != null) {
    params["tags"]["name"] = TEST_K6_REQUEST_TAG_NAME;
  }

  return params;
}

function doGet(path, queryParams) {
  const url = new URL(path, API_URL);
  const params = buildUrlParams();
  Object.keys(queryParams).forEach(k => url.searchParams.append(k, queryParams[k]));
  console.info(`URL is ${url}`)
  return http.get(url.toString(), params);
}

function getNowTsSec() {
  const now = Date.now();
  const tsSec = Math.floor(now / 1000);
  return tsSec;
}


function runRangeQuery(queryParams) {
  console.info(`Range querying: ${queryParams.query}`)

  const res = doGet('/api/prom/api/v1/query_range', queryParams);
  if (res.error_code !== 0 && res.error_code !== 1429) {
    console.warn(`GET range request received error code ${res.error_code}: ${res.body}`);
  }

  check(res, {
    'is status 200 or 429': (r) => r.status === 200 || r.status === 429,
  });

  return res;
}

function runParquetQueries() {
  // Query time boundaries
  const startDate = new Date('2025-05-01T00:00:00Z');
  const endDate = new Date('2025-06-01T00:00:00Z');
  
  const startTime = Math.floor(startDate.getTime() / 1000);
  const endTime = Math.floor(endDate.getTime() / 1000);
  
  const cacheBuster = Math.floor(Date.now() / 1000);
  
  const windowDuration = 12 * 60 * 60; // 12 hours
  
  const maxStartTime = endTime - windowDuration;
  const randomStartTime = startTime + Math.floor(Math.random() * (maxStartTime - startTime));
  const randomEndTime = randomStartTime + windowDuration;
  
  const queries = [
    'sum(rate(container_cpu_usage_seconds_total{namespace="mimir-dev-11", pod=~"ingester.*"}[5m]))',
    // 'sum(container_memory_working_set_bytes{namespace="mimir-dev-11", container=~"(gateway|cortex-gw|cortex-gw-internal)"})'
  ];
  
  // Pick a random query
  const baseQuery = queries[Math.floor(Math.random() * queries.length)];
  const queryWithCacheBuster = `${baseQuery} + ${cacheBuster}`;
  
  const queryType = baseQuery.includes('cpu') ? 'CPU usage' : 'Memory usage';
  console.info(`Querying ${queryType} for random 12-hour window: ${new Date(randomStartTime * 1000).toISOString()} to ${new Date(randomEndTime * 1000).toISOString()} (cache buster: ${cacheBuster})`);
  
  const queryParams = {
    query: queryWithCacheBuster,
    start: randomStartTime,
    end: randomEndTime,
    step: '60s' // 1 minute step
  };
  
  runRangeQuery(queryParams);
  sleep(10); // 10 second pause between queries
}

const tests = queryTests;

const options = {
  scenarios: tests[TEST_NAME].scenarios,
  thresholds: tests[TEST_NAME].thresholds,
  ext: {
    loadimpact: {
      name: TEST_NAME,
      projectID: PROJECT_ID,
    },
  },
  setupTimeout: '10s',
  teardownTimeout: '10s',
};

export function setup() {
  const startTime = new Date().toISOString();
  console.log(`🚀 Load test starting at: ${startTime}`);
  return { startTime: startTime };
}

export function teardown(data) {
  const endTime = new Date().toISOString();
  console.log(`✅ Load test completed at: ${endTime}`);
  
  const startTime = new Date(data.startTime);
  const duration = Math.round((new Date() - startTime) / 1000);
  console.log(`⏱️  Total test duration: ${duration} seconds`);
}

function parquetQueries() {
  runParquetQueries()
}

export { options, tests, parquetQueries };
