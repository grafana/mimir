import { check } from 'k6';
import http from 'k6/http';

const targetAddress = __ENV.TARGET_ADDRESS;
const targetPath = __ENV.TARGET_PATH;
const url = `http://${targetAddress}/${targetPath}`;

export default function () {
    const res = http.get(url);

    check(res, {
        'is status 200': (r) => r.status === 200,
    });

    // TODO: verify result payload is as expected
}
