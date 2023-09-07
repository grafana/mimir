import { check } from 'k6';
import http from 'k6/http';

const targetAddress = __ENV.TARGET_ADDRESS;

export default function () {
    const res = http.get(`http://${targetAddress}/prometheus/api/v1/query_range?query=up{}&start=2023-09-07T05:00:00Z&end=2023-09-07T05:30:00Z&step=15`);

    check(res, {
        'is status 200': (r) => r.status === 200,
    });

    // TODO: verify result payload is as expected
}
