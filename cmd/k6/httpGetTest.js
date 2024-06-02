import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = 'http://127.0.0.0';

export let options = {
    stages: [
        { duration: '20s', target: 10 },
        { duration: '20s', target: 10 },
        { duration: '1m', target: 0 },
    ],
    thresholds: {
        'http_req_duration': ['p(95)<300'], // 95% of requests should complete within 500ms
    },
};

export default function () {
    let nodeId = Math.floor(Math.random() * 3); // Randomly select a node ID between 0 and 3
    let key = 'key' + Math.floor(Math.random() * 1000); // Generate a random key

    let getRes = http.get(`${BASE_URL}:808${nodeId}/get?key=${key}`, {
        tags: { name: 'GetRequest'},
    });
    check(getRes, {
        'status is 200': (r) => r.status === 200,
    });
}
