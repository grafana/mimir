import { check, fail } from 'k6';
import encoding from 'k6/encoding';
import exec from 'k6/execution';
import remote from 'k6/x/remotewrite';

import { describe, expect } from 'https://jslib.k6.io/k6chaijs/4.3.4.1/index.js';
import { randomIntBetween } from "https://jslib.k6.io/k6-utils/1.1.0/index.js";
import { Httpx } from 'https://jslib.k6.io/httpx/0.0.6/index.js';

/**
 * Mimir hostname to connect to on the write path.
 * @constant {string}
 */
const WRITE_HOSTNAME = __ENV.K6_WRITE_HOSTNAME || fail('K6_WRITE_HOSTNAME environment variable missing: set it to the ingress hostname on the write path (eg. distributor hostname)');
/**
 * Mimir hostname to connect to on the read path.
 * @constant {string}
 */
const READ_HOSTNAME = __ENV.K6_READ_HOSTNAME || fail('K6_READ_HOSTNAME environment variable missing: set it to the ingress hostname on the read path (eg. query-frontend hostname)');
/**
 * Configures the protocol scheme used for requests.
 * @constant {string}
 */
const SCHEME = __ENV.K6_SCHEME || 'http';
/**
 * Username to use for HTTP basic authentication for writes.
 * @constant {string}
 */
const WRITE_USERNAME = __ENV.K6_WRITE_USERNAME || '';
/**
 * Username to use for HTTP basic authentication for reads.
 * @constant {string}
 */
const READ_USERNAME = __ENV.K6_READ_USERNAME || '';
/**
 * Authentication token to use for HTTP basic authentication on requests to write path.
 * @constant {string}
 */
const WRITE_TOKEN = __ENV.K6_WRITE_TOKEN || '';
/**
 * Authentication token to use for HTTP basic authentication on requests to read path.
 * @constant {string}
 */
const READ_TOKEN = __ENV.K6_READ_TOKEN || '';
/**
 * Number of remote write requests to send every SCRAPE_INTERVAL_SECONDS.
 * Note that the effective rate is multiplied by HA_REPLICAS.
 * @constant {number}
 */
const WRITE_REQUEST_RATE = parseInt(__ENV.K6_WRITE_REQUEST_RATE || 1);
/**
 * Number of series per remote write request.
 * @constant {number}
 */
const WRITE_SERIES_PER_REQUEST = parseInt(__ENV.K6_WRITE_SERIES_PER_REQUEST || 1000);
/**
 * Total number of unique series to generate and write.
 * @constant {number}
 */
const TOTAL_SERIES = WRITE_REQUEST_RATE * WRITE_SERIES_PER_REQUEST;
/**
 * Number of query requests per second.
 * @constant {number}
 */
const READ_REQUEST_RATE = parseInt(__ENV.K6_READ_REQUEST_RATE ||  1);
/**
 * Duration of the load test in minutes (including ramp up and down).
 * @constant {number}
 */
const DURATION_MIN = parseInt(__ENV.K6_DURATION_MIN || (12*60));
/**
 * Duration of the ramp up period in minutes.
 * @constant {number}
 */
const RAMP_UP_MIN = parseInt(__ENV.K6_RAMP_UP_MIN || 0);
/**
 * Duration of the ramp down period in minutes.
 * @constant {number}
 */
const RAMP_DOWN_MIN = parseInt(__ENV.K6_RAMP_DOWN_MIN || 0);
/**
 * Simulated Prometheus scrape interval in seconds.
 * @constant {number}
 */
const SCRAPE_INTERVAL_SECONDS = parseInt(__ENV.K6_SCRAPE_INTERVAL_SECONDS || 20);
/**
 * Number of HA replicas to simulate (use 1 for no HA).
 * Causes every series to be sent this number of times.
 * @constant {number}
 */
const HA_REPLICAS = parseInt(__ENV.K6_HA_REPLICAS || 1);
/**
 * Number of HA clusters to simulate.
 * @constant {number}
 */
const HA_CLUSTERS = parseInt(__ENV.K6_HA_CLUSTERS || 1);
/**
 * Tenant ID to write to. Note that this option is mutually exclusive with
 * using the write username (HTTP basic auth).
 * By default, no tenant ID is specified requiring the cluster to have multi-tenancy disabled.
 * @constant {string}
 */
const WRITE_TENANT_ID = __ENV.K6_WRITE_TENANT_ID || '';

/**
 * Tenant ID to read from to. Note that this option is mutually exclusive with
 * using the read username (HTTP basic auth).
 * By default, no tenant ID is specified requiring the cluster to have multi-tenancy disabled.
 * @constant {string}
 */
const READ_TENANT_ID = __ENV.K6_READ_TENANT_ID || '';

/**
 * Project ID to send k6 cloud tests to.
 * @type {*|string}
 */
const PROJECT_ID = __ENV.K6_PROJECT_ID || '';

const remote_write_url = get_remote_write_url();
console.debug("Remote write URL:", remote_write_url)

const write_client_headers = get_write_authentication_headers()

const write_client = new remote.Client({ url: remote_write_url, timeout: '32s', tenant_name: WRITE_TENANT_ID, headers:  write_client_headers });

const query_client_headers = {
    'User-Agent': 'k6-load-test',
    "Content-Type": 'application/x-www-form-urlencoded',
}

get_read_authentication_headers().forEach((value, key) => {
    query_client_headers[key] = value
})

const query_client = new Httpx({
    baseURL: `${SCHEME}://${READ_HOSTNAME}/prometheus/api/v1`,
    headers: query_client_headers,
    timeout: 120e3 // 120s timeout.
});

const query_request_rates = {
    instant_low_cardinality_queries: Math.ceil(READ_REQUEST_RATE * 1.00), // 99%
    instant_high_cardinality_queries: Math.ceil(READ_REQUEST_RATE * 0.00), // 1%
};

/**
 * Exported configuration options for the k6 workers.
 * @constant {object}
 */
export const options = {
    ext: {
        loadimpact: {
            projectID: PROJECT_ID || '',
        }
    },

    thresholds: {
        // SLA: 99.9% of writes succeed.
        'checks{type:write}': ['rate > 0.999'],
        // 99.9% of writes take less than 10s (SLA has no guarantee on write latency).
        [`http_req_duration{url:${remote_write_url}}`]: ['p(99.9) < 10000'],
        // SLA: 99.9% of queries succeed.
        'checks{type:read}': ['rate > 0.999'],
        // SLA: average query time for any 3 hours of data is less than 2s (not including Internet latency).
        'http_req_duration{type:read}': ['avg < 2000'],
    },
    scenarios: {
        reads_instant_queries_low_cardinality: {
            executor: 'constant-arrival-rate',
            rate: query_request_rates.instant_low_cardinality_queries,
            timeUnit: '1s',

            duration: `${DURATION_MIN}m`,
            exec: 'run_instant_query_low_cardinality',

            // The number of VUs should be adjusted based on how much load we're pushing on the read path.
            // We estimated about 5 VU every query/sec.
            preAllocatedVUs: query_request_rates.instant_low_cardinality_queries,
            maxVus: query_request_rates.instant_low_cardinality_queries,
        },
        // reads_instant_queries_high_cardinality: {
        //     executor: 'constant-arrival-rate',
        //     rate: query_request_rates.instant_high_cardinality_queries,
        //     timeUnit: '1s',
        //
        //     duration: `${DURATION_MIN}m`,
        //     exec: 'run_instant_query_high_cardinality',
        //
        //     // The number of VUs should be adjusted based on how much load we're pushing on the read path.
        //     // We estimated about 10 VU every query/sec.
        //     preAllocatedVUs: query_request_rates.instant_high_cardinality_queries,
        //     maxVus: query_request_rates.instant_high_cardinality_queries,
        // },
    },
};

//
// Read path config + helpers.
//

/**
 * Number of seconds in one hour.
 * @constant {number}
 */
const HOUR_SECONDS = 60 * 60;
/**
 * Number of seconds in one day.
 * @constant {number}
 */
const DAY_SECONDS = 24 * HOUR_SECONDS;
/**
 * Number of seconds in one week.
 * @constant {number}
 */
const WEEK_SECONDS = 7 * DAY_SECONDS;

/**
 * The percentage of metrics we expect to touch when querying as a decimal. It's not realistic
 * that a customer would query back all metrics they push but instead they typically touch a subset of these
 * metrics when querying.
 * @constant {number}
 */
const QUERY_METRICS_SUBSET = 0.2; // 20%

/**
 * The oldest timestamp (in seconds) that can be queried with range queries. This is used to prevent to
 * query metrics having a different set of labels in the past and thus causing the error:
 * "vector cannot contain metrics with the same labelset".
 * @constant {number}
 */
const QUERY_MIN_TIME_SECONDS = Date.parse("2022-03-30T13:00:00Z") / 1000

const query_distribution = {
    9: 'sum by(cardinality_1e2) (rate($metric[$rate_interval] offset $offset))',
    11: 'sum(rate($metric[$rate_interval] offset $offset))',
    80: 'sum by(cardinality_1e1) (rate($metric[$rate_interval] offset $offset))',
}

/**
 * The distribution of rate intervals and queries used to generate random instant queries executed on the target.
 * For each configurable setting the key is the distribution percentage as float from 0 to 100.
 * The value is the setting value that will be used a % (distribution) number of times.
 * @constant {object}
 */
const instant_query_low_cardinality_distribution = {
    rate_interval: {
        0.1: '24h',
        0.9: '6h',
        5: '1h',
        14: '10m',
        80: '5m',
    },
    // offset: {
    //     20: '8h',
    //     30: '6h',
    //     50: '1m',
    // },
    offset: {
        100: '5m',
    },
    query: query_distribution,
    cardinality: {
        // The cardinality must be a power of 10 (because of how "cardinality labels" are generated).
        90: 1e1, // 10
        10: 1e2, // 100
    },
};

/**
 * The distribution of rate intervals and queries used to generate random instant queries executed on the target.
 * For each configurable setting the key is the distribution percentage as float from 0 to 100.
 * The value is the setting value that will be used a % (distribution) number of times.
 * @constant {object}
 */
const instant_query_high_cardinality_distribution = {
    rate_interval: {
        20: '5m',
        80: '1m',
    },
    // offset: {
    //     20: '8h',
    //     30: '6h',
    //     50: '1m',
    // },
    offset: {
        100: '5m',
    },
    query: query_distribution,
    cardinality: {
        // The cardinality must be a power of 10 (because of how "cardinality labels" are generated).
        90: 1e3, // 1000
        10: 1e7, // 100k
    },
};

// Ensure all distributions sum up to 100%.
assert_distribution_config(instant_query_low_cardinality_distribution);
assert_distribution_config(instant_query_high_cardinality_distribution);

function assert_distribution_config(config) {
    for (const [name, distribution] of Object.entries(config)) {
        let sum = 0;
        for (const [percent, range] of Object.entries(distribution)) {
            sum += parseFloat(percent);
        }

        if (sum != 100) {
            throw new Error(`${name} distribution is invalid (total sum is ${sum}% while 100% was expected)`);
        }
    }
}

/**
 * Returns a random float number between min and max.
 * @param {number} min The min value.
 * @param {number} max The max value
 * @returns {number}
 */
function random_float_between(min, max) {
    return min + (Math.random() * (max - min))
}

/**
 * Returns a random entry from the provided config object where the object keys are the percentage of the distribution as a float.
 * Given a sufficiently large number of calls to this function, each
 * value is returned from the config a number of times (in %) close to the configured distribution (key).
 * @param {object} config An object with keys representing the percentage within the distribution.
 * @return {any} A random value from the config object.
 */
function get_random_entry_from_config(config) {
    const rand = random_float_between(0, 100);
    let sum = 0;

    for (const [percent, value] of Object.entries(config)) {
        sum += parseFloat(percent);

        if (sum >= rand) {
            return value;
        }
    }

    throw new Error(`get_random_entry_from_config() has not been able to find a distribution config for random value ${rand} but this should never happen`);
}

/**
 * Returns a random rate() query from the with possibly substituted values for metric names and rate intervals.
 * @param {object} config An object with keys representing the percentage within the distribution.
 * Values should be query strings with that may contain $metric and $rate_interval for substitution.
 * @param {string} rate_interval The rate interval as a string parseable by the Prometheus duration format.
 * @return {string} A Prometheus query expression.
 */
function get_random_query_from_config(config, cardinality, rate_interval, offset) {
    const cardinality_exp = Math.log10(cardinality)

    // Find the max group ID (0 based) based on the given query cardinality.
    const cardinality_group_max_id = Math.floor(TOTAL_SERIES / cardinality) - 1;

    // Query a group between 0 and the max, honoring QUERY_METRICS_SUBSET. Pick a random one
    // instead of relying it on "iterationInTest" in order to reduce cache hit ratio (eg. when
    // the test restarts).
    const cardinality_group_id = randomIntBetween(0, Math.ceil(cardinality_group_max_id * QUERY_METRICS_SUBSET));
    const metric_selector = `{cardinality_1e${cardinality_exp}="${cardinality_group_id}"}`;

    // Get a random query from config and replace placeholders.
    let query = get_random_entry_from_config(config);
    query = query.replace(/\$metric/g, metric_selector);
    query = query.replace(/\$rate_interval/g, rate_interval);
    query = query.replace(/\$offset/g, offset);
    return query;
}

/**
 * Roughly simulates the behavior of the Grafana step calculation based on time range.
 * @param {number} time_range Query time range in seconds.
 * @return {number} The step in seconds.
 */
function get_range_query_step(time_range) {
    return Math.max(15, Math.ceil(time_range / 1440));
}

/**
 * Aligns the provided timestamp to step.
 * @param {number} ts Timestamp in seconds.
 * @param {number} step Step in seconds.
 * @returns {number}
 */
function align_timestamp_to_step(ts, step) {
    if (ts % step === 0) {
        return ts
    }

    return ts - (ts % step)
}

/**
 * Returns the write URL.
 * @returns {string}
 */
function get_remote_write_url() {
    return `${SCHEME}://${WRITE_HOSTNAME}/api/v1/push`;
}

/**
 * Returns the HTTP Authentication header to use on the read path.
 * @returns {map}
 */
function get_read_authentication_headers() {
    let auth_headers = new Map();

    if (READ_USERNAME !== '' || READ_TOKEN !== '') {
        auth_headers.set('Authorization', `Basic ${encoding.b64encode(`${READ_USERNAME}:${READ_TOKEN}`)}`)
    }

    if (READ_TENANT_ID !== '') {
        auth_headers.set('X-Scope-OrgID', READ_TENANT_ID)
    }

    return auth_headers;
}

/**
 * Returns the HTTP Authentication header to use on the write path.
 * @returns {map}
 */
function get_write_authentication_headers() {
    let auth_headers = new Map();

    if (WRITE_USERNAME !== '' || WRITE_TOKEN !== '') {
        auth_headers.set('Authorization', `Basic ${encoding.b64encode(`${WRITE_USERNAME}:${WRITE_TOKEN}`)}`)
    }

    if (WRITE_TENANT_ID !== '') {
        auth_headers.set('X-Scope-OrgID', WRITE_TENANT_ID)
    }

    return auth_headers;
}
/**
 * See run_instant_query().
 */
export function run_instant_query_low_cardinality() {
    run_instant_query("instant query low cardinality", instant_query_low_cardinality_distribution)
}

/**
 * See run_instant_query().
 */
export function run_instant_query_high_cardinality() {
    run_instant_query("instant query high cardinality", instant_query_high_cardinality_distribution)
}

/**
 * Runs an instant query randomly generated based on the configured distribution defined in instant_query_distribution.
 * It validates that a successful response is received and tags requests with { type: "read" } so that requests can be distinguished from writes.
 * Instant queries are run with a time one minute in the past to simulate rule evaluations.
 */
export function run_instant_query(name, config) {
    const time = Math.ceil(Date.now() / 1000) - 60;
    const rate_interval = get_random_entry_from_config(config.rate_interval);
    const offset = get_random_entry_from_config(config.offset);
    const cardinality = get_random_entry_from_config(config.cardinality);
    const query = get_random_query_from_config(config.query, cardinality, rate_interval, offset);

    console.debug(name, " - query: ", query)
    describe(name, () => {
        const res = query_client.post('/query', {
            query: query,
            time: time,
        }, {
            tags: {
                name: name,
                type: "read",
            }
        });
        expect(res.status, "request status").to.equal(200);
        expect(res).to.have.validJsonBody();
        expect(res.json('status'), "status field").to.equal("success");
        expect(res.json('data.resultType'), "data.resultType field").to.equal("vector");
    });
}
